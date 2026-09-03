use super::*;
use crate::persistent_sequence::{
    LogicalLength, PersistentRoot, PersistentSequence, SequenceRange, SequenceRepresentation,
};

const MESSAGE_IDENTITY_NULL_PREFIX: &[u8] = b"{\"identity\":null,";
const MESSAGE_CANONICAL_PREFIX: &[u8] = b"{\"identity\":null,\"messages\":[";
const MESSAGE_CANONICAL_SUFFIX: &[u8] = b"]}";
const LEGACY_V1_HASH_STREAM_CHUNK_BYTES: u64 = 64 * 1024;

impl CheckpointStore {
    /// Opens or creates a checkpoint store and reconstructs its complete
    /// committed logical state from sealed streams plus the valid hot suffix.
    ///
    /// If a previous process died after publishing a manifest but before WAL
    /// recycle, `open` recognizes the previous-generation hot geometry and
    /// completes the already-authorized recycle before returning.
    ///
    /// # Errors
    ///
    /// Returns an error if persisted authoritative data is malformed or an I/O
    /// operation fails.
    pub fn open(
        dir: impl AsRef<Path>,
        config: CheckpointStoreConfig,
    ) -> Result<Self, CheckpointStoreError> {
        validate_config(config)?;
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;
        let writer_lock = WriterLock::acquire(&dir)?;
        let manifest = load_manifest(&dir)?;
        let manifest = ensure_format_v1_manifest(&dir, manifest)?;
        let (mut state, lazy_base) = if config.recovery_mode == CheckpointStoreRecoveryMode::Lazy {
            let lazy = LazyCheckpointStore::open_for_writable_store(&dir)?;
            let state = state_from_lazy_reader(&dir, &manifest, &lazy)?;
            (state, Some(RefCell::new(lazy)))
        } else {
            (
                materialize_sealed_state(&dir, &manifest, config.recovery_mode)?,
                None,
            )
        };
        reclaim_unreferenced_generation_files(&dir, &manifest)?;
        let hot_path = dir.join(HOT_WAL_FILE);
        ensure_hot_file_exists(&hot_path, config)?;

        let current_geometry = Geometry::from_manifest(&manifest)?;
        let current_parse = parse_hot_prefix(&hot_path, current_geometry)?;
        let mut normalized_tail = current_parse.logical_tail;
        let mut txs = current_parse.transactions;

        if txs.is_empty() && file_starts_with_tx_magic(&hot_path)? {
            if let Some(previous) = Geometry::previous_generation(&manifest)? {
                let previous_parse = parse_hot_prefix(&hot_path, previous)?;
                if !previous_parse.transactions.is_empty() {
                    let overlap_end = previous_parse
                        .transactions
                        .iter()
                        .filter(|tx| tx.checkpoint_count <= manifest.checkpoint_count)
                        .map(|tx| tx.end_offset)
                        .max()
                        .unwrap_or(0);
                    if overlap_end > 0 {
                        let suffix_len = previous_parse.logical_tail.saturating_sub(overlap_end);
                        recycle_hot_file(
                            &dir,
                            &hot_path,
                            overlap_end,
                            previous_parse.logical_tail,
                            config,
                        )?;
                        let reparsed = parse_hot_prefix(&hot_path, current_geometry)?;
                        if reparsed.logical_tail != suffix_len {
                            return Err(format_error(
                                "post-crash WAL normalization suffix mismatch",
                            ));
                        }
                        normalized_tail = reparsed.logical_tail;
                        txs = reparsed.transactions;
                    }
                }
            }
        }

        for tx in &txs {
            apply_transaction(&mut state, tx)?;
        }
        let hot = HotWal::open_at(&hot_path, normalized_tail, config)?;
        let store = Self {
            dir,
            config,
            manifest,
            state,
            _writer_lock: writer_lock,
            hot,
            lazy_base,
            range_sizes: RefCell::new(Vec::new()),
        };
        if store.lazy_base.is_none() {
            let roots = store
                .state
                .versions
                .iter()
                .flatten()
                .copied()
                .collect::<Vec<_>>();
            for root in roots {
                store.root_term_size(root)?;
            }
        }
        Ok(store)
    }

    /// Returns this store's persistent identity.
    #[must_use]
    pub fn store_id(&self) -> StoreId {
        self.manifest
            .store_id
            .expect("public-format stores always have a persistent StoreId")
    }

    /// Assigns a new identity after a directory has been copied as an
    /// explicitly independent store. Checkpoints and request-ledger bytes are
    /// unchanged; this method does not create a branch or copy any files.
    pub fn reidentify_copied_store(&mut self) -> Result<StoreId, CheckpointStoreError> {
        let store_id = StoreId::generate()?;
        let mut next_manifest = self.manifest.clone();
        next_manifest.store_id = Some(store_id);
        staged_write_new(
            &self.dir.join(MANIFEST_FILE),
            &manifest_bytes(&next_manifest)?,
        )?;
        self.manifest = next_manifest;
        Ok(store_id)
    }

    /// Deletes one checkpoint and all of its descendants in the current
    /// one-parent thread lineage, then rewrites only live reachable history.
    ///
    /// The operation is deliberately restricted to a fully sealed store. The
    /// replacement segment and route become durable before the old generation
    /// is removed, and deleted checkpoint/request identities remain durable
    /// tombstones so they cannot be reused after reopen.
    pub fn delete_checkpoint_subtree(
        &mut self,
        thread_id: &str,
        checkpoint_id: &str,
    ) -> Result<PruneReport, CheckpointStoreError> {
        validate_checkpoint_identifier(thread_id, "thread id")?;
        validate_checkpoint_identifier(checkpoint_id, "checkpoint id")?;
        if self.hot.logical_tail() != 0 {
            return Err(CheckpointStoreError::PruneRequiresSealedStore);
        }
        if self.lazy_base.is_some() {
            return Err(CheckpointStoreError::PruneRequiresEagerRecovery);
        }
        let target_key = (thread_id.to_owned(), checkpoint_id.to_owned());
        if !self.state.checkpoint_ordinals.contains_key(&target_key) {
            return Err(if self.state.deleted_checkpoints.contains(&target_key) {
                CheckpointStoreError::CheckpointDeleted
            } else {
                CheckpointStoreError::CheckpointNotFound
            });
        }
        let mut deleted_keys = HashSet::new();
        deleted_keys.insert(target_key);
        loop {
            let mut changed = false;
            for checkpoint in &self.state.checkpoints {
                if deleted_keys.contains(&(
                    checkpoint.thread_id.clone(),
                    checkpoint.checkpoint_id.clone(),
                )) {
                    continue;
                }
                if checkpoint.thread_id == thread_id
                    && checkpoint
                        .parent_checkpoint_id
                        .as_ref()
                        .is_some_and(|parent| {
                            deleted_keys.contains(&(thread_id.to_owned(), parent.clone()))
                        })
                {
                    deleted_keys.insert((
                        checkpoint.thread_id.clone(),
                        checkpoint.checkpoint_id.clone(),
                    ));
                    changed = true;
                }
            }
            if !changed {
                break;
            }
        }

        self.delete_checkpoint_set(deleted_keys)
    }

    /// Compacts an explicitly validated set of checkpoints from a fully
    /// sealed store. Repository adapters use this boundary when their
    /// persisted metadata contains more parent edges than the generic
    /// first-parent checkpoint record.
    pub(crate) fn delete_checkpoint_set(
        &mut self,
        deleted_keys: HashSet<(String, String)>,
    ) -> Result<PruneReport, CheckpointStoreError> {
        if self.hot.logical_tail() != 0 {
            return Err(CheckpointStoreError::PruneRequiresSealedStore);
        }
        if self.lazy_base.is_some() {
            return Err(CheckpointStoreError::PruneRequiresEagerRecovery);
        }
        if deleted_keys.is_empty() {
            return Err(format_error("checkpoint prune set is empty"));
        }
        for key in &deleted_keys {
            if !self.state.checkpoint_ordinals.contains_key(key) {
                return Err(if self.state.deleted_checkpoints.contains(key) {
                    CheckpointStoreError::CheckpointDeleted
                } else {
                    CheckpointStoreError::CheckpointNotFound
                });
            }
        }
        let before = tree_storage(&self.dir)?;
        let compacted = compact_live_state(&self.state, &deleted_keys)?;
        let generation = self
            .manifest
            .generation
            .checked_add(1)
            .ok_or_else(|| format_error("prune manifest generation overflow"))?;
        let replacement =
            write_compacted_generation(&self.dir, generation, self.config, &compacted)?;
        let segment_final = self.dir.join(&replacement.finalized.meta.file);
        publish_existing_tmp(&replacement.finalized.tmp_path, &segment_final)?;

        let route_path = self.dir.join(&replacement.route_meta.file);
        staged_write_new(&route_path, &replacement.route_bytes)?;
        let next_manifest = Manifest {
            generation,
            sealed_end_wal_bytes: 0,
            checkpoint_count: u64::try_from(compacted.state.checkpoints.len())
                .map_err(|_| format_error("pruned checkpoint count overflow"))?,
            version_count: u64::try_from(compacted.state.versions.len())
                .map_err(|_| format_error("pruned version count overflow"))?,
            thread_count: u64::try_from(compacted.state.threads.len())
                .map_err(|_| format_error("pruned thread count overflow"))?,
            stream_sizes: replacement.finalized.meta.stream_ends.clone(),
            segments: vec![replacement.finalized.meta.clone()],
            routes: vec![replacement.route_meta.clone()],
            store_id: self.manifest.store_id,
            deleted_checkpoints: compacted
                .deleted_checkpoints
                .iter()
                .map(|(thread_id, checkpoint_id)| CheckpointTombstone {
                    thread_id: thread_id.clone(),
                    checkpoint_id: checkpoint_id.clone(),
                })
                .collect(),
            retired_requests: compacted
                .retired_requests
                .iter()
                .map(|(key, operation_digest)| RetiredRequestRecord {
                    key: key.clone(),
                    operation_digest: *operation_digest,
                })
                .collect(),
        };
        staged_write_new(
            &self.dir.join(MANIFEST_FILE),
            &manifest_bytes(&next_manifest)?,
        )?;
        let coexistence = tree_storage(&self.dir)?;

        self.manifest = next_manifest;
        self.state = compacted.state;
        self.range_sizes.borrow_mut().clear();
        reclaim_unreferenced_generation_files(&self.dir, &self.manifest)?;
        let reclaimed = tree_storage(&self.dir)?;
        Ok(PruneReport {
            generation,
            deleted_checkpoint_count: u64::try_from(deleted_keys.len())
                .map_err(|_| format_error("deleted checkpoint count overflow"))?,
            retained_checkpoint_count: u64::try_from(self.state.checkpoints.len())
                .map_err(|_| format_error("retained checkpoint count overflow"))?,
            rewritten_bytes: replacement
                .finalized
                .meta
                .segment_file_bytes
                .checked_add(replacement.route_meta.route_file_bytes)
                .ok_or_else(|| format_error("prune rewritten byte count overflow"))?,
            before,
            coexistence,
            reclaimed,
        })
    }

    /// Drains unreferenced sealed generations when no sealed reader lease is
    /// active. If a reader is still active, this call is safe and leaves the
    /// obsolete files for a later drain.
    pub fn reclaim_deferred_generations(&self) -> Result<StoreStorage, CheckpointStoreError> {
        reclaim_unreferenced_generation_files(&self.dir, &self.manifest)?;
        tree_storage(&self.dir)
    }

    /// Appends one adapter-encoded T2W1 transaction through the production
    /// preinitialized WAL and updates the in-memory committed state after the
    /// durability barrier succeeds.
    ///
    /// # Errors
    ///
    /// Returns an error if the transaction is malformed, does not continue the
    /// current global topology, or cannot be durably written.
    pub(crate) fn append_encoded_transaction(
        &mut self,
        transaction: &[u8],
    ) -> Result<HotWalAppendReport, CheckpointStoreError> {
        let requestless = parse_transaction_unchecked(transaction, 0)?;
        reject_deleted_checkpoint(&self.state, &requestless)?;
        let geometry = Geometry::from_state(&self.state)?;
        let tx = parse_transaction(transaction, geometry, 0)?;
        self.append_parsed_transaction(transaction, tx)
    }

    /// Appends one checkpoint from identity bytes and checkpoint metadata.
    ///
    /// This is the semantic boundary for adapters that should not assemble
    /// private T2W1 records or choose the store's global byte, node, and
    /// version watermarks. The identity bytes are stored as one leaf in the
    /// existing checkpoint format; the canonical reader continues to expose
    /// them through the existing `{"identity":...,"messages":[]}` framing.
    ///
    /// # Errors
    ///
    /// Returns an error if identifiers, topology counters, lengths, or the
    /// parent relationship are invalid, or if the durable append fails.
    pub fn append_checkpoint(
        &mut self,
        thread_id: &str,
        checkpoint_id: &str,
        checkpoint_no: u32,
        parent_checkpoint_id: Option<&str>,
        identity: &[u8],
    ) -> Result<HotWalAppendReport, CheckpointStoreError> {
        let geometry = self.state.geometry()?;
        let transaction = encode_single_identity_transaction(
            &geometry,
            thread_id,
            checkpoint_id,
            checkpoint_no,
            parent_checkpoint_id,
            identity,
        )?;
        self.append_encoded_transaction(&transaction)
    }

    /// Appends one checkpoint containing new values for an append-only
    /// `messages` channel while structurally sharing the selected parent's
    /// prior message history.
    ///
    /// This is the production semantic boundary used by message-oriented
    /// agent adapters. The canonical state is
    /// `{"identity":null,"messages":[...]}`. Each supplied value is encoded
    /// once; a child checkpoint adds one leaf plus one binary node regardless
    /// of the parent's logical message-history length.
    ///
    /// Format v1 persists a whole-canonical-state XXH3-64 value. This path
    /// therefore still scans the selected parent's canonical bytes to preserve
    /// released v1 hash semantics, but it no longer materializes the complete
    /// parent in one temporary vector. Child length comes from persisted parent
    /// metadata and legacy hashing is fed through bounded range chunks. A
    /// writable Format-v2 commitment is still required to remove the O(parent)
    /// read/hash work itself.
    ///
    /// # Errors
    ///
    /// Returns an error if identifiers or parent topology are invalid, if the
    /// store contains incompatible non-message checkpoints, if a message value
    /// exceeds the compact-node length limit, or if the durable append fails.
    pub fn append_messages_checkpoint(
        &mut self,
        thread_id: &str,
        checkpoint_id: &str,
        checkpoint_no: u32,
        parent_checkpoint_id: Option<&str>,
        messages: &[Value],
    ) -> Result<HotWalAppendReport, CheckpointStoreError> {
        if messages.is_empty() {
            return Err(format_error("message checkpoint delta is empty"));
        }

        let mut message_body = Vec::new();
        for (index, message) in messages.iter().enumerate() {
            if index > 0 {
                message_body.push(b',');
            }
            serde_json::to_writer(&mut message_body, message)?;
        }

        let parent = match parent_checkpoint_id {
            Some(parent_id) => {
                let ordinal = self
                    .state
                    .checkpoint_ordinals
                    .get(&(thread_id.to_owned(), parent_id.to_owned()))
                    .copied()
                    .ok_or(CheckpointStoreError::CheckpointNotFound)?;
                let info = self
                    .state
                    .checkpoints
                    .get(
                        usize::try_from(ordinal)
                            .map_err(|_| format_error("parent checkpoint ordinal exceeds usize"))?,
                    )
                    .ok_or_else(|| format_error("parent checkpoint ordinal is absent"))?
                    .clone();
                let messages_version = info.messages_version.ok_or_else(|| {
                    format_error("parent checkpoint has no append-only messages channel")
                })?;
                let messages_root = self.version_root_for_read(messages_version)?;
                Some((info, messages_root, messages_version))
            }
            None => None,
        };

        if parent.is_none() {
            if let Some(first) = self.state.checkpoints.first() {
                let prefix_len = u64::try_from(MESSAGE_IDENTITY_NULL_PREFIX.len())
                    .map_err(|_| format_error("message identity prefix length exceeds u64"))?;
                let prefix = self.read_checkpoint_range(
                    &first.thread_id,
                    &first.checkpoint_id,
                    0,
                    prefix_len,
                )?;
                if prefix != MESSAGE_IDENTITY_NULL_PREFIX {
                    return Err(format_error(
                        "message checkpoints cannot reuse a non-null identity root",
                    ));
                }
            }
        }

        let (canonical_state_len, canonical_state_hash) =
            if let Some((info, _, _)) = parent.as_ref() {
                self.legacy_v1_message_child_metadata(info, &message_body)?
            } else {
                Self::legacy_v1_message_root_metadata(&message_body)?
            };

        let geometry = self.state.geometry()?;
        let transaction = encode_message_append_transaction(
            &geometry,
            self.state.checkpoints.first(),
            thread_id,
            checkpoint_id,
            checkpoint_no,
            parent_checkpoint_id,
            parent
                .as_ref()
                .map(|(info, root, version)| (info.identity_version, *root, *version)),
            &message_body,
            canonical_state_len,
            canonical_state_hash,
        )?;
        self.append_encoded_transaction(&transaction)
    }

    fn legacy_v1_message_root_metadata(
        message_body: &[u8],
    ) -> Result<(u64, u64), CheckpointStoreError> {
        let prefix_len = u64::try_from(MESSAGE_CANONICAL_PREFIX.len())
            .map_err(|_| format_error("message canonical prefix length exceeds u64"))?;
        let message_len = u64::try_from(message_body.len())
            .map_err(|_| format_error("message canonical body length exceeds u64"))?;
        let suffix_len = u64::try_from(MESSAGE_CANONICAL_SUFFIX.len())
            .map_err(|_| format_error("message canonical suffix length exceeds u64"))?;
        let canonical_state_len = prefix_len
            .checked_add(message_len)
            .and_then(|length| length.checked_add(suffix_len))
            .ok_or_else(|| format_error("canonical message state length exceeds u64"))?;
        let mut hasher = Xxh3::new();
        hasher.update(MESSAGE_CANONICAL_PREFIX);
        hasher.update(message_body);
        hasher.update(MESSAGE_CANONICAL_SUFFIX);
        Ok((canonical_state_len, hasher.digest()))
    }

    fn legacy_v1_message_child_metadata(
        &self,
        parent: &CheckpointInfo,
        message_body: &[u8],
    ) -> Result<(u64, u64), CheckpointStoreError> {
        let prefix_len = u64::try_from(MESSAGE_CANONICAL_PREFIX.len())
            .map_err(|_| format_error("message canonical prefix length exceeds u64"))?;
        let suffix_len = u64::try_from(MESSAGE_CANONICAL_SUFFIX.len())
            .map_err(|_| format_error("message canonical suffix length exceeds u64"))?;
        let framing_len = prefix_len
            .checked_add(suffix_len)
            .ok_or_else(|| format_error("message canonical framing length overflow"))?;
        if parent.logical_state_len < framing_len {
            return Err(format_error(
                "parent canonical state is shorter than the append-only message framing",
            ));
        }

        let prefix = self.read_checkpoint_range(
            &parent.thread_id,
            &parent.checkpoint_id,
            0,
            prefix_len,
        )?;
        let parent_without_suffix_len = parent
            .logical_state_len
            .checked_sub(suffix_len)
            .ok_or_else(|| format_error("parent canonical message suffix underflow"))?;
        let suffix = self.read_checkpoint_range(
            &parent.thread_id,
            &parent.checkpoint_id,
            parent_without_suffix_len,
            suffix_len,
        )?;
        if prefix != MESSAGE_CANONICAL_PREFIX || suffix != MESSAGE_CANONICAL_SUFFIX {
            return Err(format_error(
                "parent canonical state is not the append-only message schema",
            ));
        }

        let message_len = u64::try_from(message_body.len())
            .map_err(|_| format_error("message canonical body length exceeds u64"))?;
        let canonical_state_len = parent
            .logical_state_len
            .checked_add(1)
            .and_then(|length| length.checked_add(message_len))
            .ok_or_else(|| format_error("canonical message state length exceeds u64"))?;

        let mut hasher = Xxh3::new();
        self.stream_checkpoint_range(
            &parent.thread_id,
            &parent.checkpoint_id,
            0,
            parent_without_suffix_len,
            LEGACY_V1_HASH_STREAM_CHUNK_BYTES,
            &mut |chunk| {
                hasher.update(chunk);
                Ok(())
            },
        )?;
        hasher.update(b",");
        hasher.update(message_body);
        hasher.update(MESSAGE_CANONICAL_SUFFIX);
        Ok((canonical_state_len, hasher.digest()))
    }

    #[allow(dead_code)] // Reserved for a future zero-copy repository adapter feature.
    pub(crate) fn identity_leaf_refs(
        &self,
        thread_id: &str,
        checkpoint_id: &str,
    ) -> Result<Vec<IdentityLeafRef>, CheckpointStoreError> {
        let ordinal = self
            .state
            .checkpoint_ordinals
            .get(&(thread_id.to_owned(), checkpoint_id.to_owned()))
            .copied()
            .ok_or(CheckpointStoreError::CheckpointNotFound)?;
        let checkpoint = self
            .state
            .checkpoints
            .get(
                usize::try_from(ordinal)
                    .map_err(|_| format_error("checkpoint ordinal overflow"))?,
            )
            .ok_or_else(|| format_error("checkpoint ordinal outside state"))?;
        let root = self.version_root_for_read(checkpoint.identity_version)?;
        let mut leaves = Vec::new();
        let identity_len = self.collect_identity_leaf_refs(root, 0, &mut leaves)?;
        let expected_len = checkpoint
            .logical_state_len
            .checked_sub(CANONICAL_STATE_PREFIX_BYTES)
            .and_then(|value| value.checked_sub(CANONICAL_STATE_SUFFIX_BYTES))
            .ok_or_else(|| format_error("canonical checkpoint is shorter than its framing"))?;
        if identity_len != expected_len {
            return Err(format_error(
                "identity leaf lengths disagree with checkpoint metadata",
            ));
        }
        Ok(leaves)
    }

    #[allow(dead_code)] // Reserved for a future zero-copy repository adapter feature.
    pub(crate) fn read_identity_leaf(
        &self,
        reference: IdentityLeafRef,
    ) -> Result<Vec<u8>, CheckpointStoreError> {
        let geometry = self.state.geometry()?;
        if reference.node_id >= geometry.node_count {
            return Err(format_error(
                "identity leaf node is outside committed state",
            ));
        }
        let node = self.decode_node_for_read(reference.node_id)?;
        if node.kind != 0 || node.b != reference.logical_len {
            return Err(format_error(
                "identity leaf reference does not identify a leaf",
            ));
        }
        let mut bytes = Vec::new();
        self.append_payload_range(node.a, node.b, &mut bytes)?;
        Ok(bytes)
    }

    #[allow(dead_code)] // Reserved for a future zero-copy repository adapter feature.
    fn collect_identity_leaf_refs(
        &self,
        node_id: u64,
        logical_start: u64,
        leaves: &mut Vec<IdentityLeafRef>,
    ) -> Result<u64, CheckpointStoreError> {
        let node = self.decode_node_for_read(node_id)?;
        if node.kind == 0 {
            leaves.push(IdentityLeafRef {
                node_id,
                logical_start,
                logical_len: node.b,
            });
            return Ok(node.b);
        }
        let left_len = self.collect_identity_leaf_refs(node.a, logical_start, leaves)?;
        let right_start = logical_start
            .checked_add(left_len)
            .ok_or_else(|| format_error("identity leaf logical offset overflow"))?;
        let right_len = self.collect_identity_leaf_refs(node.b, right_start, leaves)?;
        left_len
            .checked_add(right_len)
            .ok_or_else(|| format_error("identity tree length overflow"))
    }

    #[allow(dead_code)] // Reserved for a future zero-copy repository adapter feature.
    pub(crate) fn append_checkpoint_from_identity_leaves(
        &mut self,
        thread_id: &str,
        checkpoint_id: &str,
        checkpoint_no: u32,
        parent_checkpoint_id: Option<&str>,
        leaves: &[IdentityLeafSource<'_>],
        canonical_state_len: u64,
        canonical_state_hash: u64,
    ) -> Result<HotWalAppendReport, CheckpointStoreError> {
        let geometry = self.state.geometry()?;
        let transaction = encode_identity_leaf_transaction(
            &geometry,
            thread_id,
            checkpoint_id,
            checkpoint_no,
            parent_checkpoint_id,
            leaves,
            canonical_state_len,
            canonical_state_hash,
        )?;
        self.append_encoded_transaction(&transaction)
    }

    #[allow(dead_code)] // Reserved for a future zero-copy repository adapter feature.
    pub(crate) fn append_checkpoint_from_identity_leaves_with_bounded_lifecycle(
        &mut self,
        thread_id: &str,
        checkpoint_id: &str,
        checkpoint_no: u32,
        parent_checkpoint_id: Option<&str>,
        leaves: &[IdentityLeafSource<'_>],
        canonical_state_len: u64,
        canonical_state_hash: u64,
        policy: BoundedWalLifecyclePolicy,
    ) -> Result<BoundedWalAppendReport, CheckpointStoreError> {
        let geometry = self.state.geometry()?;
        let transaction = encode_identity_leaf_transaction(
            &geometry,
            thread_id,
            checkpoint_id,
            checkpoint_no,
            parent_checkpoint_id,
            leaves,
            canonical_state_len,
            canonical_state_hash,
        )?;
        self.append_encoded_transaction_with_bounded_lifecycle(&transaction, policy)
    }

    #[allow(dead_code)] // Reserved for a future zero-copy repository adapter feature.
    pub(crate) fn committed_node_count(&self) -> Result<u64, CheckpointStoreError> {
        Ok(self.state.geometry()?.node_count)
    }

    /// Appends one semantic checkpoint through an explicit bounded WAL
    /// lifecycle policy.
    ///
    /// This combines [`Self::append_checkpoint`] with the existing sequential
    /// seal-before-append policy. The transaction is rejected before any WAL
    /// mutation when it exceeds the policy hard limit.
    ///
    /// # Errors
    ///
    /// Returns an error if metadata or topology is invalid, the transaction is
    /// larger than the policy hard limit, or the durable append/seal fails.
    pub fn append_checkpoint_with_bounded_lifecycle(
        &mut self,
        thread_id: &str,
        checkpoint_id: &str,
        checkpoint_no: u32,
        parent_checkpoint_id: Option<&str>,
        identity: &[u8],
        policy: BoundedWalLifecyclePolicy,
    ) -> Result<BoundedWalAppendReport, CheckpointStoreError> {
        let geometry = self.state.geometry()?;
        let transaction = encode_single_identity_transaction(
            &geometry,
            thread_id,
            checkpoint_id,
            checkpoint_no,
            parent_checkpoint_id,
            identity,
        )?;
        self.append_encoded_transaction_with_bounded_lifecycle(&transaction, policy)
    }

    /// Appends one transaction while applying an explicit sequential hot-WAL bound.
    ///
    /// If the next transaction would cross the soft limit, the already committed
    /// hot prefix is sealed before this transaction is appended. A transaction
    /// larger than the hard limit is rejected before any WAL or seal mutation.
    /// This method performs no background maintenance and does not change the
    /// default [`Self::append_encoded_transaction`] behavior.
    pub(crate) fn append_encoded_transaction_with_bounded_lifecycle(
        &mut self,
        transaction: &[u8],
        policy: BoundedWalLifecyclePolicy,
    ) -> Result<BoundedWalAppendReport, CheckpointStoreError> {
        policy.validate()?;
        let transaction_bytes = u64::try_from(transaction.len())
            .map_err(|_| format_error("bounded WAL transaction length exceeds u64"))?;
        if transaction_bytes > policy.hard_logical_bytes {
            return Err(format_error(
                "transaction exceeds bounded WAL hard logical limit",
            ));
        }
        let requestless = parse_transaction_unchecked(transaction, 0)?;
        reject_deleted_checkpoint(&self.state, &requestless)?;
        let geometry = Geometry::from_state(&self.state)?;
        let tx = parse_transaction(transaction, geometry, 0)?;
        let current_hot = self.hot.logical_tail();
        let projected_hot = current_hot
            .checked_add(transaction_bytes)
            .ok_or_else(|| format_error("bounded WAL projected logical tail overflow"))?;
        let automatic_seal = if current_hot > 0 && projected_hot > policy.soft_logical_bytes {
            let checkpoint_count = u64::try_from(self.state.checkpoints.len())
                .map_err(|_| format_error("bounded WAL checkpoint count exceeds u64"))?;
            Some(self.seal_through(checkpoint_count)?)
        } else {
            None
        };
        let append = self.append_parsed_transaction(transaction, tx)?;
        Ok(BoundedWalAppendReport {
            append,
            automatic_seal,
        })
    }

    /// Appends one transaction with a durable request/idempotency key.
    ///
    /// An exact retry returns [`CheckpointStoreAppendOutcome::AlreadyCommitted`]
    /// without appending another WAL record. Reusing the key for different
    /// transaction bytes returns [`CheckpointStoreError::RequestIdConflict`].
    ///
    /// # Errors
    ///
    /// Returns an error if the request key or transaction is malformed, the
    /// transaction conflicts with the current topology, the request key was
    /// reused for different bytes, or durability fails.
    #[cfg(test)]
    pub(crate) fn append_encoded_transaction_with_request_id(
        &mut self,
        request_id: &[u8],
        transaction: &[u8],
    ) -> Result<CheckpointStoreAppendOutcome, CheckpointStoreError> {
        validate_request_id(request_id)?;
        let requestless = parse_transaction_unchecked(transaction, 0)?;
        if requestless.request_id.is_some() {
            return Err(format_error(
                "request-id append expects a requestless transaction",
            ));
        }
        let encoded = encode_transaction_with_request_id(transaction, request_id)?;
        let candidate = parse_transaction_unchecked(&encoded, 0)?;
        if let Some(existing) = self.state.request_records.get(request_id) {
            if existing.operation_digest == candidate.operation_digest {
                return Ok(CheckpointStoreAppendOutcome::AlreadyCommitted);
            }
            return Err(CheckpointStoreError::RequestIdConflict);
        }
        if let Some(existing) = self.state.retired_requests.get(request_id) {
            if existing == &candidate.operation_digest {
                return Err(CheckpointStoreError::CheckpointDeleted);
            }
            return Err(CheckpointStoreError::RequestIdConflict);
        }
        reject_deleted_checkpoint(&self.state, &candidate)?;
        let geometry = Geometry::from_state(&self.state)?;
        let tx = parse_transaction(&encoded, geometry, 0)?;
        let report = self.append_parsed_transaction(&encoded, tx)?;
        Ok(CheckpointStoreAppendOutcome::Appended(report))
    }

    fn append_parsed_transaction(
        &mut self,
        transaction: &[u8],
        tx: ParsedTransaction,
    ) -> Result<HotWalAppendReport, CheckpointStoreError> {
        validate_transaction_against_state(&self.state, &tx)?;
        let report = self.hot.append(transaction)?;
        maybe_crash("after-hot-sync-before-memory-publication");
        apply_transaction(&mut self.state, &tx)?;
        Ok(report)
    }

    /// Seals all currently hot transactions through `checkpoint_count`,
    /// publishes immutable segment/route/manifest authority, then recycles
    /// the represented WAL prefix into a fresh preinitialized writable reserve.
    ///
    /// # Errors
    ///
    /// Returns an error if the target is not a strict advancement, the hot WAL
    /// does not contain the requested committed prefix, structured encoding
    /// fails, or any durability step fails.
    pub fn seal_through(
        &mut self,
        checkpoint_count: u64,
    ) -> Result<SealReport, CheckpointStoreError> {
        if checkpoint_count <= self.manifest.checkpoint_count {
            return Err(format_error(
                "seal target must advance checkpoint authority",
            ));
        }
        if checkpoint_count
            > u64::try_from(self.state.checkpoints.len())
                .map_err(|_| format_error("checkpoint count does not fit u64"))?
        {
            return Err(format_error(
                "seal target exceeds committed checkpoint count",
            ));
        }
        let before = tree_storage(&self.dir)?;
        let hot_path = self.dir.join(HOT_WAL_FILE);
        let base = Geometry::from_manifest(&self.manifest)?;
        let parsed = parse_hot_prefix(&hot_path, base)?;
        let target_index = parsed
            .transactions
            .iter()
            .position(|tx| tx.checkpoint_count == checkpoint_count)
            .ok_or_else(|| format_error("seal target is not present in current hot WAL"))?;
        let selected = &parsed.transactions[..=target_index];
        let source_offset = selected
            .last()
            .map(|tx| tx.end_offset)
            .ok_or_else(|| format_error("empty seal transaction selection"))?;
        let generation = self
            .manifest
            .generation
            .checked_add(1)
            .ok_or_else(|| format_error("manifest generation overflow"))?;
        let mut writer = StreamingSegmentWriter::new(
            &self.dir,
            generation,
            self.manifest.stream_sizes.clone(),
            self.manifest.checkpoint_count,
            self.manifest.version_count,
            self.config.sealed_block_size,
            self.config.zstd_level,
        )?;
        let mut new_threads = Vec::<(String, u32)>::new();
        let mut new_checkpoints = Vec::<(String, String, u32)>::new();
        let mut new_requests = Vec::<RequestRecord>::new();
        let mut next_thread_ordinal = u32::try_from(self.manifest.thread_count)
            .map_err(|_| format_error("thread count exceeds u32"))?;
        for tx in selected {
            write_transaction_to_segment(
                &mut writer,
                tx,
                &self.state,
                &mut next_thread_ordinal,
                &mut new_threads,
                &mut new_checkpoints,
                &mut new_requests,
            )?;
        }
        let version_end = selected
            .last()
            .map(|tx| tx.version_count)
            .ok_or_else(|| format_error("missing selected transaction"))?;
        let new_wal_end = self
            .manifest
            .sealed_end_wal_bytes
            .checked_add(source_offset)
            .ok_or_else(|| format_error("global sealed WAL watermark overflow"))?;
        let finalized = writer.finalize(
            self.manifest.sealed_end_wal_bytes,
            new_wal_end,
            checkpoint_count,
            version_end,
        )?;
        let segment_final = self.dir.join(&finalized.meta.file);
        maybe_crash("after-segment-write");
        publish_existing_tmp(&finalized.tmp_path, &segment_final)?;

        let (route_bytes, route_hash) =
            build_route_file(generation, &new_threads, &new_checkpoints, &new_requests)?;
        let route_name = format!("route-g{generation:06}.t3r");
        let route_path = self.dir.join(&route_name);
        staged_write_new(&route_path, &route_bytes)?;
        let route_meta = RouteMeta {
            generation,
            file: route_name,
            thread_entry_count: u64::try_from(new_threads.len())
                .map_err(|_| format_error("thread route count overflow"))?,
            checkpoint_entry_count: u64::try_from(new_checkpoints.len())
                .map_err(|_| format_error("checkpoint route count overflow"))?,
            route_file_bytes: u64::try_from(route_bytes.len())
                .map_err(|_| format_error("route file length overflow"))?,
            route_index_xxh3_64: route_hash,
        };
        let mut segments = self.manifest.segments.clone();
        segments.push(finalized.meta.clone());
        let mut routes = self.manifest.routes.clone();
        routes.push(route_meta);
        let next_manifest = Manifest {
            generation,
            sealed_end_wal_bytes: new_wal_end,
            checkpoint_count,
            version_count: version_end,
            thread_count: u64::from(next_thread_ordinal),
            stream_sizes: finalized.meta.stream_ends.clone(),
            segments,
            routes,
            store_id: self.manifest.store_id,
            deleted_checkpoints: self.manifest.deleted_checkpoints.clone(),
            retired_requests: self.manifest.retired_requests.clone(),
        };
        staged_write_new(
            &self.dir.join(MANIFEST_FILE),
            &manifest_bytes(&next_manifest)?,
        )?;
        let coexistence = tree_storage(&self.dir)?;
        let recycle = recycle_hot_file(
            &self.dir,
            &hot_path,
            source_offset,
            parsed.logical_tail,
            self.config,
        )?;
        let suffix_len = parsed.logical_tail.saturating_sub(source_offset);
        self.hot.replace_after_recycle(suffix_len)?;
        self.manifest = next_manifest;
        if let Some(lazy) = &self.lazy_base {
            *lazy.borrow_mut() = LazyCheckpointStore::open_for_writable_store(&self.dir)?;
            self.state.base_geometry = Some(Geometry::from_manifest(&self.manifest)?);
            self.state.arena_bytes.clear();
            self.state.compact_nodes.clear();
            self.state.wide_nodes.clear();
        }
        self.range_sizes.borrow_mut().clear();
        let reclaimed = tree_storage(&self.dir)?;
        Ok(SealReport {
            generation,
            checkpoint_count,
            newly_sealed_wal_bytes: source_offset,
            hot_suffix_logical_bytes: suffix_len,
            before,
            coexistence,
            recycle_peak: recycle.peak,
            reclaimed,
        })
    }

    /// Reads and reconstructs one exact canonical checkpoint state.
    ///
    /// # Errors
    ///
    /// Returns an error if the checkpoint does not exist or persisted DAG bytes
    /// are structurally invalid.
    pub fn read_checkpoint(
        &self,
        thread_id: &str,
        checkpoint_id: &str,
    ) -> Result<Vec<u8>, CheckpointStoreError> {
        let ordinal = self
            .state
            .checkpoint_ordinals
            .get(&(thread_id.to_owned(), checkpoint_id.to_owned()))
            .copied()
            .ok_or_else(|| {
                if self
                    .state
                    .deleted_checkpoints
                    .contains(&(thread_id.to_owned(), checkpoint_id.to_owned()))
                {
                    CheckpointStoreError::CheckpointDeleted
                } else {
                    CheckpointStoreError::CheckpointNotFound
                }
            })?;
        let index = usize::try_from(ordinal)
            .map_err(|_| format_error("checkpoint ordinal does not fit usize"))?;
        let checkpoint = self
            .state
            .checkpoints
            .get(index)
            .ok_or_else(|| format_error("checkpoint ordinal outside metadata"))?;
        if self.lazy_base.is_some() {
            self.reconstruct_checkpoint_lazy(checkpoint)
        } else {
            reconstruct_checkpoint(&self.state, checkpoint)
        }
    }

    /// Reads an exact half-open byte range from one canonical checkpoint.
    ///
    /// The returned bytes are identical to
    /// `read_checkpoint(thread_id, checkpoint_id)?[offset..offset + length]`,
    /// but the complete canonical checkpoint is not first assembled in one
    /// process-owned vector. The current persisted node format may still
    /// require additional topology reads to locate a range.
    ///
    /// # Errors
    ///
    /// Returns an error if the checkpoint does not exist, the range overflows
    /// or is outside the exact canonical state, or persisted DAG bytes are
    /// structurally invalid.
    pub fn read_checkpoint_range(
        &self,
        thread_id: &str,
        checkpoint_id: &str,
        offset: u64,
        length: u64,
    ) -> Result<Vec<u8>, CheckpointStoreError> {
        let ordinal = self
            .state
            .checkpoint_ordinals
            .get(&(thread_id.to_owned(), checkpoint_id.to_owned()))
            .copied()
            .ok_or_else(|| {
                if self
                    .state
                    .deleted_checkpoints
                    .contains(&(thread_id.to_owned(), checkpoint_id.to_owned()))
                {
                    CheckpointStoreError::CheckpointDeleted
                } else {
                    CheckpointStoreError::CheckpointNotFound
                }
            })?;
        let index = usize::try_from(ordinal)
            .map_err(|_| format_error("checkpoint ordinal does not fit usize"))?;
        let checkpoint = self
            .state
            .checkpoints
            .get(index)
            .ok_or_else(|| format_error("checkpoint ordinal outside metadata"))?;
        let end = offset
            .checked_add(length)
            .ok_or_else(|| format_error("checkpoint range end overflow"))?;
        if end > checkpoint.logical_state_len {
            return Err(format_error("checkpoint range outside canonical state"));
        }
        if length == 0 {
            return Ok(Vec::new());
        }
        let output_len = usize::try_from(length)
            .map_err(|_| format_error("checkpoint range length exceeds usize"))?;
        let mut output = Vec::with_capacity(output_len);
        self.append_canonical_range(checkpoint, offset, end, &mut output)?;
        if output.len() != output_len {
            return Err(format_error("checkpoint range produced unexpected length"));
        }
        Ok(output)
    }

    #[allow(dead_code)] // Reserved for a future zero-copy repository adapter feature.
    pub(crate) fn read_identity_range(
        &self,
        thread_id: &str,
        checkpoint_id: &str,
        offset: u64,
        length: u64,
    ) -> Result<Vec<u8>, CheckpointStoreError> {
        let ordinal = self
            .state
            .checkpoint_ordinals
            .get(&(thread_id.to_owned(), checkpoint_id.to_owned()))
            .copied()
            .ok_or_else(|| {
                if self
                    .state
                    .deleted_checkpoints
                    .contains(&(thread_id.to_owned(), checkpoint_id.to_owned()))
                {
                    CheckpointStoreError::CheckpointDeleted
                } else {
                    CheckpointStoreError::CheckpointNotFound
                }
            })?;
        let index = usize::try_from(ordinal)
            .map_err(|_| format_error("checkpoint ordinal does not fit usize"))?;
        let checkpoint = self
            .state
            .checkpoints
            .get(index)
            .ok_or_else(|| format_error("checkpoint ordinal outside metadata"))?;
        if checkpoint.messages_version.is_some() || checkpoint.result_version.is_some() {
            return Err(format_error(
                "identity range requires a checkpoint without message or result channels",
            ));
        }
        let identity_len = checkpoint
            .logical_state_len
            .checked_sub(CANONICAL_STATE_PREFIX_BYTES)
            .and_then(|value| value.checked_sub(CANONICAL_STATE_SUFFIX_BYTES))
            .ok_or_else(|| format_error("canonical checkpoint is shorter than identity framing"))?;
        let end = offset
            .checked_add(length)
            .ok_or_else(|| format_error("identity range end overflow"))?;
        if end > identity_len {
            return Err(format_error("identity range outside canonical identity"));
        }
        if length == 0 {
            return Ok(Vec::new());
        }
        let root_node = self.version_root_for_read(checkpoint.identity_version)?;
        let root = PersistentRoot::legacy_v1(root_node, LogicalLength::new(identity_len));
        let range = SequenceRange::new(LogicalLength::new(offset), LogicalLength::new(length))
            .ok_or_else(|| format_error("identity range end overflow"))?;
        let output_len =
            usize::try_from(length).map_err(|_| format_error("identity range exceeds usize"))?;
        let mut output = Vec::with_capacity(output_len);
        LegacyV1Sequence { store: self }.read_range(root, range, &mut output)?;
        if output.len() != output_len {
            return Err(format_error("identity range produced unexpected length"));
        }
        Ok(output)
    }

    /// Delivers an exact canonical checkpoint range in bounded chunks.
    ///
    /// The callback runs synchronously and the supplied slice is valid only
    /// for the duration of the callback. The callback may return a
    /// `CheckpointStoreError` to stop delivery. This path bounds the temporary
    /// output allocation to `chunk_bytes`, but does not claim that the
    /// filesystem read path touched only sublinear physical blocks.
    ///
    /// # Errors
    ///
    /// Returns an error if the checkpoint/range is invalid, `chunk_bytes` is
    /// zero, persisted DAG bytes are malformed, or the callback fails.
    pub fn stream_checkpoint_range(
        &self,
        thread_id: &str,
        checkpoint_id: &str,
        offset: u64,
        length: u64,
        chunk_bytes: u64,
        sink: &mut dyn FnMut(&[u8]) -> Result<(), CheckpointStoreError>,
    ) -> Result<(), CheckpointStoreError> {
        if chunk_bytes == 0 {
            return Err(format_error(
                "checkpoint stream chunk size must be positive",
            ));
        }
        if length == 0 {
            self.read_checkpoint_range(thread_id, checkpoint_id, offset, 0)?;
            return Ok(());
        }
        let end = offset
            .checked_add(length)
            .ok_or_else(|| format_error("checkpoint stream range end overflow"))?;
        let mut cursor = offset;
        while cursor < end {
            let take = chunk_bytes.min(end - cursor);
            let chunk = self.read_checkpoint_range(thread_id, checkpoint_id, cursor, take)?;
            sink(&chunk)?;
            cursor = cursor
                .checked_add(take)
                .ok_or_else(|| format_error("checkpoint stream cursor overflow"))?;
        }
        Ok(())
    }

    #[allow(dead_code)] // Reserved for a future zero-copy repository adapter feature.
    pub(crate) fn stream_identity_range(
        &self,
        thread_id: &str,
        checkpoint_id: &str,
        offset: u64,
        length: u64,
        chunk_bytes: u64,
        sink: &mut dyn FnMut(&[u8]) -> Result<(), CheckpointStoreError>,
    ) -> Result<(), CheckpointStoreError> {
        if chunk_bytes == 0 {
            return Err(format_error("identity stream chunk size must be positive"));
        }
        if length == 0 {
            self.read_identity_range(thread_id, checkpoint_id, offset, 0)?;
            return Ok(());
        }
        let end = offset
            .checked_add(length)
            .ok_or_else(|| format_error("identity stream range end overflow"))?;
        let mut cursor = offset;
        while cursor < end {
            let take = chunk_bytes.min(end - cursor);
            let chunk = self.read_identity_range(thread_id, checkpoint_id, cursor, take)?;
            sink(&chunk)?;
            cursor = cursor
                .checked_add(take)
                .ok_or_else(|| format_error("identity stream cursor overflow"))?;
        }
        Ok(())
    }

    /// Reconstructs every committed checkpoint and checks stored length/hash.
    ///
    /// # Errors
    ///
    /// Returns an error if the DAG cannot be structurally reconstructed.
    pub fn verify_all(&self) -> Result<VerificationReport, CheckpointStoreError> {
        let mut failures = 0u64;
        for checkpoint in &self.state.checkpoints {
            let bytes = if self.lazy_base.is_some() {
                self.reconstruct_checkpoint_lazy(checkpoint)?
            } else {
                reconstruct_checkpoint(&self.state, checkpoint)?
            };
            let len = u64::try_from(bytes.len())
                .map_err(|_| format_error("reconstructed state length overflow"))?;
            if len != checkpoint.logical_state_len || xxh3_64(&bytes) != checkpoint.state_hash {
                failures = failures.saturating_add(1);
            }
        }
        Ok(VerificationReport {
            checkpoint_count: u64::try_from(self.state.checkpoints.len())
                .map_err(|_| format_error("checkpoint count overflow"))?,
            failures,
        })
    }

    fn reconstruct_checkpoint_lazy(
        &self,
        checkpoint: &CheckpointInfo,
    ) -> Result<Vec<u8>, CheckpointStoreError> {
        let identity = self.extract_version_lazy(checkpoint.identity_version)?;
        let messages = checkpoint
            .messages_version
            .map(|version| self.extract_version_lazy(version))
            .transpose()?
            .unwrap_or_default();
        let result = checkpoint
            .result_version
            .map(|version| self.extract_version_lazy(version))
            .transpose()?;
        let mut out = Vec::new();
        out.extend_from_slice(b"{\"identity\":");
        out.extend_from_slice(&identity);
        out.extend_from_slice(b",\"messages\":[");
        out.extend_from_slice(&messages);
        out.extend_from_slice(b"]");
        if let Some(result) = result {
            out.extend_from_slice(b",\"result\":");
            out.extend_from_slice(&result);
        }
        out.extend_from_slice(b"}");
        Ok(out)
    }

    fn extract_version_lazy(&self, version: u32) -> Result<Vec<u8>, CheckpointStoreError> {
        let root = self
            .state
            .versions
            .get(usize::try_from(version).map_err(|_| format_error("version index overflow"))?)
            .copied()
            .flatten()
            .ok_or_else(|| format_error("lazy version has no root"))?;
        self.extract_root_lazy(root)
    }

    fn extract_root_lazy(&self, root: u64) -> Result<Vec<u8>, CheckpointStoreError> {
        let mut out = Vec::new();
        let mut stack = vec![root];
        while let Some(node_id) = stack.pop() {
            let node = self.decode_node_lazy(node_id)?;
            if node.kind == 0 {
                let end = node
                    .a
                    .checked_add(node.b)
                    .ok_or_else(|| format_error("lazy leaf byte end overflow"))?;
                let base = self.state.lazy_base_geometry();
                if node.a < base.byte_len {
                    if end > base.byte_len {
                        return Err(format_error("lazy leaf crosses base/overlay boundary"));
                    }
                    let lazy = self
                        .lazy_base
                        .as_ref()
                        .ok_or_else(|| format_error("lazy base reader is missing"))?;
                    lazy.borrow_mut()
                        .append_stream_range(PAYLOAD, node.a, node.b, &mut out)?;
                } else {
                    let start = usize::try_from(node.a - base.byte_len)
                        .map_err(|_| format_error("lazy overlay leaf start overflow"))?;
                    let length = usize::try_from(node.b)
                        .map_err(|_| format_error("lazy overlay leaf length overflow"))?;
                    let end = start
                        .checked_add(length)
                        .ok_or_else(|| format_error("lazy overlay leaf end overflow"))?;
                    out.extend_from_slice(
                        self.state
                            .arena_bytes
                            .get(start..end)
                            .ok_or_else(|| format_error("lazy overlay leaf outside payload"))?,
                    );
                }
            } else {
                stack.push(node.b);
                stack.push(node.a);
            }
        }
        Ok(out)
    }

    fn decode_node_lazy(&self, node_id: u64) -> Result<DecodedNode, CheckpointStoreError> {
        let base = self.state.lazy_base_geometry();
        if node_id < base.node_count {
            let lazy = self
                .lazy_base
                .as_ref()
                .ok_or_else(|| format_error("lazy base reader is missing"))?;
            return lazy.borrow_mut().decode_node(node_id);
        }
        decode_node(&self.state, node_id)
    }

    fn append_canonical_range(
        &self,
        checkpoint: &CheckpointInfo,
        start: u64,
        end: u64,
        output: &mut Vec<u8>,
    ) -> Result<(), CheckpointStoreError> {
        let identity_root = self.persistent_root_for_read(checkpoint.identity_version)?;
        let messages_root = checkpoint
            .messages_version
            .map(|version| self.persistent_root_for_read(version))
            .transpose()?;
        let result_root = checkpoint
            .result_version
            .map(|version| self.persistent_root_for_read(version))
            .transpose()?;

        let mut cursor = 0u64;
        cursor = self.append_static_range(b"{\"identity\":", cursor, start, end, output)?;
        cursor = self.append_root_segment_range(identity_root, cursor, start, end, output)?;
        cursor = self.append_static_range(b",\"messages\":[", cursor, start, end, output)?;
        if let Some(root) = messages_root {
            cursor = self.append_root_segment_range(root, cursor, start, end, output)?;
        }
        cursor = self.append_static_range(b"]", cursor, start, end, output)?;
        if let Some(root) = result_root {
            cursor = self.append_static_range(b",\"result\":", cursor, start, end, output)?;
            cursor = self.append_root_segment_range(root, cursor, start, end, output)?;
        }
        cursor = self.append_static_range(b"}", cursor, start, end, output)?;
        if cursor != checkpoint.logical_state_len {
            return Err(format_error(
                "canonical checkpoint length disagrees with metadata",
            ));
        }
        Ok(())
    }

    fn append_static_range(
        &self,
        bytes: &[u8],
        segment_start: u64,
        request_start: u64,
        request_end: u64,
        output: &mut Vec<u8>,
    ) -> Result<u64, CheckpointStoreError> {
        let segment_len = u64::try_from(bytes.len())
            .map_err(|_| format_error("canonical static segment length overflow"))?;
        let segment_end = segment_start
            .checked_add(segment_len)
            .ok_or_else(|| format_error("canonical static segment end overflow"))?;
        let overlap_start = request_start.max(segment_start);
        let overlap_end = request_end.min(segment_end);
        if overlap_start < overlap_end {
            let local_start = usize::try_from(overlap_start - segment_start)
                .map_err(|_| format_error("canonical static range start overflow"))?;
            let local_end = usize::try_from(overlap_end - segment_start)
                .map_err(|_| format_error("canonical static range end overflow"))?;
            output.extend_from_slice(
                bytes
                    .get(local_start..local_end)
                    .ok_or_else(|| format_error("canonical static range outside segment"))?,
            );
        }
        Ok(segment_end)
    }

    fn append_root_segment_range(
        &self,
        root: PersistentRoot,
        segment_start: u64,
        request_start: u64,
        request_end: u64,
        output: &mut Vec<u8>,
    ) -> Result<u64, CheckpointStoreError> {
        let sequence = LegacyV1Sequence { store: self };
        let root_len = sequence.logical_len(root)?.get();
        let segment_end = segment_start
            .checked_add(root_len)
            .ok_or_else(|| format_error("canonical root segment end overflow"))?;
        let overlap_start = request_start.max(segment_start);
        let overlap_end = request_end.min(segment_end);
        if overlap_start < overlap_end {
            let range = SequenceRange::new(
                LogicalLength::new(overlap_start - segment_start),
                LogicalLength::new(overlap_end - overlap_start),
            )
            .ok_or_else(|| format_error("checkpoint root range end overflow"))?;
            sequence.read_range(root, range, output)?;
        }
        Ok(segment_end)
    }

    fn version_root_for_read(&self, version: u32) -> Result<u64, CheckpointStoreError> {
        self.state
            .versions
            .get(usize::try_from(version).map_err(|_| format_error("version index overflow"))?)
            .copied()
            .flatten()
            .ok_or_else(|| format_error("version has no root"))
    }

    fn persistent_root_for_read(
        &self,
        version: u32,
    ) -> Result<PersistentRoot, CheckpointStoreError> {
        let node_id = self.version_root_for_read(version)?;
        let logical_len = LogicalLength::new(self.root_term_size(node_id)?);
        Ok(PersistentRoot::legacy_v1(node_id, logical_len))
    }

    fn decode_node_for_read(&self, node_id: u64) -> Result<DecodedNode, CheckpointStoreError> {
        if self.lazy_base.is_some() {
            self.decode_node_lazy(node_id)
        } else {
            decode_node(&self.state, node_id)
        }
    }

    fn root_term_size(&self, root: u64) -> Result<u64, CheckpointStoreError> {
        let index =
            usize::try_from(root).map_err(|_| format_error("checkpoint root index overflow"))?;
        if let Some(cached) = self
            .range_sizes
            .borrow()
            .get(index)
            .and_then(|value| *value)
        {
            return Ok(cached);
        }
        let node = self.decode_node_for_read(root)?;
        let total = if node.kind == 0 {
            node.b
        } else {
            self.root_term_size(node.a)?
                .checked_add(self.root_term_size(node.b)?)
                .ok_or_else(|| format_error("checkpoint root term size overflow"))?
        };
        let mut cache = self.range_sizes.borrow_mut();
        if cache.len() <= index {
            cache.resize(index + 1, None);
        }
        cache[index] = Some(total);
        Ok(total)
    }

    fn append_root_range_with_known_size(
        &self,
        root: u64,
        root_len: u64,
        offset: u64,
        length: u64,
        output: &mut Vec<u8>,
    ) -> Result<(), CheckpointStoreError> {
        let end = offset
            .checked_add(length)
            .ok_or_else(|| format_error("checkpoint root range end overflow"))?;
        if end > root_len {
            return Err(format_error("checkpoint root range outside root"));
        }
        let mut stack = vec![(root, offset, length)];
        while let Some((node_id, request_offset, request_length)) = stack.pop() {
            if request_length == 0 {
                continue;
            }
            let node = self.decode_node_for_read(node_id)?;
            if node.kind == 0 {
                if request_offset >= node.b {
                    return Err(format_error("checkpoint leaf range outside leaf"));
                }
                let take = node.b - request_offset;
                let take = take.min(request_length);
                let payload_start = node
                    .a
                    .checked_add(request_offset)
                    .ok_or_else(|| format_error("checkpoint leaf range start overflow"))?;
                self.append_payload_range(payload_start, take, output)?;
                continue;
            }
            let left_size = self.root_term_size(node.a)?;
            let request_end = request_offset
                .checked_add(request_length)
                .ok_or_else(|| format_error("checkpoint node range end overflow"))?;
            if request_end > left_size {
                let right_offset = request_offset.saturating_sub(left_size);
                let left_take = left_size.saturating_sub(request_offset);
                let right_length = request_length.saturating_sub(left_take);
                if right_length > 0 {
                    stack.push((node.b, right_offset, right_length));
                }
            }
            if request_offset < left_size {
                let left_length = (left_size - request_offset).min(request_length);
                if left_length > 0 {
                    stack.push((node.a, request_offset, left_length));
                }
            }
        }
        Ok(())
    }

    fn append_payload_range(
        &self,
        start: u64,
        length: u64,
        output: &mut Vec<u8>,
    ) -> Result<(), CheckpointStoreError> {
        let base = self.state.lazy_base_geometry();
        let end = start
            .checked_add(length)
            .ok_or_else(|| format_error("checkpoint payload range end overflow"))?;
        if start < base.byte_len {
            if end > base.byte_len {
                return Err(format_error(
                    "checkpoint payload range crosses base/overlay",
                ));
            }
            let lazy = self
                .lazy_base
                .as_ref()
                .ok_or_else(|| format_error("lazy base reader is missing"))?;
            lazy.borrow_mut()
                .append_stream_range(PAYLOAD, start, length, output)?;
        } else {
            let local_start = usize::try_from(start - base.byte_len)
                .map_err(|_| format_error("checkpoint overlay range start overflow"))?;
            let local_length = usize::try_from(length)
                .map_err(|_| format_error("checkpoint overlay range length overflow"))?;
            let local_end = local_start
                .checked_add(local_length)
                .ok_or_else(|| format_error("checkpoint overlay range end overflow"))?;
            output.extend_from_slice(
                self.state
                    .arena_bytes
                    .get(local_start..local_end)
                    .ok_or_else(|| format_error("checkpoint overlay range outside payload"))?,
            );
        }
        Ok(())
    }

    /// Returns complete current directory storage accounting.
    ///
    /// # Errors
    ///
    /// Returns an error if directory metadata cannot be read.
    pub fn storage(&self) -> Result<StoreStorage, CheckpointStoreError> {
        tree_storage(&self.dir)
    }

    /// Returns the current checkpoint count including the hot suffix.
    #[must_use]
    pub fn checkpoint_count(&self) -> usize {
        self.state.checkpoints.len()
    }

    /// Returns the current version-root count including the hot suffix.
    #[must_use]
    pub fn version_count(&self) -> usize {
        self.state.versions.len()
    }

    /// Returns the currently sealed checkpoint prefix length.
    #[must_use]
    pub fn sealed_checkpoint_count(&self) -> u64 {
        self.manifest.checkpoint_count
    }

    /// Returns the committed logical hot-WAL suffix length.
    #[must_use]
    pub fn hot_logical_bytes(&self) -> u64 {
        self.hot.logical_tail()
    }

    /// Returns the current physical hot-WAL capacity.
    ///
    /// # Errors
    ///
    /// Returns an error if WAL metadata cannot be read.
    pub fn hot_capacity_bytes(&self) -> Result<u64, CheckpointStoreError> {
        self.hot.capacity()
    }

    #[allow(dead_code)] // Reserved for adapter-specific recovery diagnostics.
    pub(crate) fn lazy_read_metrics(&self) -> Option<LazyReadMetrics> {
        self.lazy_base
            .as_ref()
            .map(|lazy| lazy.borrow().read_metrics())
    }

    /// Returns checkpoint metadata in commit order.
    #[must_use]
    pub fn checkpoints(&self) -> &[CheckpointInfo] {
        &self.state.checkpoints
    }
}

struct LegacyV1Sequence<'a> {
    store: &'a CheckpointStore,
}

impl LegacyV1Sequence<'_> {
    fn validate_root(&self, root: PersistentRoot) -> Result<(), CheckpointStoreError> {
        if root.representation() != SequenceRepresentation::LegacyV1 {
            return Err(format_error(
                "legacy v1 sequence adapter received an incompatible root",
            ));
        }
        Ok(())
    }
}

impl PersistentSequence for LegacyV1Sequence<'_> {
    type Error = CheckpointStoreError;

    fn logical_len(&self, root: PersistentRoot) -> Result<LogicalLength, Self::Error> {
        self.validate_root(root)?;
        Ok(root.logical_len())
    }

    fn read_range(
        &self,
        root: PersistentRoot,
        range: SequenceRange,
        output: &mut Vec<u8>,
    ) -> Result<(), Self::Error> {
        self.validate_root(root)?;
        if range.end().get() > root.logical_len().get() {
            return Err(format_error("checkpoint root range outside root"));
        }
        self.store.append_root_range_with_known_size(
            root.node_id(),
            root.logical_len().get(),
            range.offset().get(),
            range.length().get(),
            output,
        )
    }
}
