use super::*;

impl LazyCheckpointStore {
    /// Opens a fully sealed checkpoint-store prefix without materializing its
    /// payload or node streams into process-owned vectors.
    ///
    /// The current candidate rejects a non-empty hot suffix because its purpose
    /// is to isolate sealed-state recovery mechanics before integrating a lazy
    /// reader with the writable suffix path.
    pub fn open(dir: impl AsRef<Path>) -> Result<Self, CheckpointStoreError> {
        Self::open_internal(dir.as_ref(), false)
    }

    pub(super) fn open_for_writable_store(
        dir: impl AsRef<Path>,
    ) -> Result<Self, CheckpointStoreError> {
        Self::open_internal(dir.as_ref(), true)
    }

    pub(super) fn open_internal(
        dir: &Path,
        allow_hot_suffix: bool,
    ) -> Result<Self, CheckpointStoreError> {
        let dir = dir.to_path_buf();
        fs::create_dir_all(&dir)?;
        let reader_reclaim_lease = ReaderReclaimLease::acquire_shared(&dir)?;
        let manifest = load_manifest(&dir)?;
        let deleted_checkpoints = manifest
            .deleted_checkpoints
            .iter()
            .map(|tombstone| (tombstone.thread_id.clone(), tombstone.checkpoint_id.clone()))
            .collect();
        let segments = load_lazy_segments(&dir, &manifest)?;
        let cache_capacity = lazy_block_cache_capacity()?;
        let mut reader = Self {
            _reader_reclaim_lease: reader_reclaim_lease,
            manifest,
            deleted_checkpoints,
            segments,
            metadata: LazyMetadata::default(),
            cache: HashMap::with_capacity(cache_capacity),
            cache_order: VecDeque::with_capacity(cache_capacity),
            block_entries: HashMap::with_capacity(cache_capacity),
            block_entry_order: VecDeque::with_capacity(cache_capacity),
            cache_capacity,
            payload_fast: None,
            metrics: LazyReadMetrics::default(),
        };
        if reader.manifest.generation == 0 {
            return Ok(reader);
        }
        let hot_path = dir.join(HOT_WAL_FILE);
        if !allow_hot_suffix && hot_path.exists() {
            let geometry = Geometry::from_manifest(&reader.manifest)?;
            if !parse_hot_prefix(&hot_path, geometry)?
                .transactions
                .is_empty()
            {
                return Err(format_error(
                    "lazy sealed reader requires a fully sealed store with no hot suffix",
                ));
            }
        }
        reader.metadata = reader.load_metadata()?;
        let payload_len = reader
            .manifest
            .stream_sizes
            .get(PAYLOAD)
            .copied()
            .ok_or_else(|| format_error("lazy payload stream is missing from manifest"))?;
        if lazy_payload_fast_path_allowed(payload_len) {
            reader.payload_fast = Some(reader.read_stream_range(PAYLOAD, 0, payload_len)?);
            reader.cache.clear();
            reader.cache_order.clear();
        }
        Ok(reader)
    }

    /// Returns the number of checkpoints in the sealed prefix.
    #[must_use]
    pub fn checkpoint_count(&self) -> usize {
        self.metadata.checkpoints.len()
    }

    /// Returns the number of versions in the sealed prefix.
    #[must_use]
    pub fn version_count(&self) -> usize {
        self.metadata.versions.len()
    }

    /// Returns checkpoint metadata in commit order.
    #[must_use]
    pub fn checkpoints(&self) -> &[CheckpointInfo] {
        &self.metadata.checkpoints
    }

    /// Returns the bounded cache capacity in blocks.
    #[must_use]
    pub const fn cache_capacity_blocks() -> usize {
        LAZY_BLOCK_CACHE_CAPACITY
    }

    /// Returns the number of currently cached decoded blocks.
    #[must_use]
    pub fn cached_block_count(&self) -> usize {
        self.cache.len()
    }

    /// Returns whether the bounded small-payload fast path is active.
    #[must_use]
    pub fn payload_fast_path_active(&self) -> bool {
        self.payload_fast.is_some()
    }

    /// Returns a snapshot of lazy-read block and byte counters.
    #[must_use]
    pub fn read_metrics(&self) -> LazyReadMetrics {
        self.metrics
    }

    /// Clears the lazy-read block and byte counters without evicting the cache.
    pub fn reset_read_metrics(&mut self) {
        self.metrics = LazyReadMetrics::default();
    }

    /// Reads and reconstructs one exact checkpoint state through the bounded
    /// sealed-block reader.
    pub fn read_checkpoint(
        &mut self,
        thread_id: &str,
        checkpoint_id: &str,
    ) -> Result<Vec<u8>, CheckpointStoreError> {
        let ordinal = self
            .metadata
            .checkpoint_ordinals
            .get(&(thread_id.to_owned(), checkpoint_id.to_owned()))
            .copied()
            .ok_or_else(|| {
                if self
                    .deleted_checkpoints
                    .contains(&(thread_id.to_owned(), checkpoint_id.to_owned()))
                {
                    CheckpointStoreError::CheckpointDeleted
                } else {
                    CheckpointStoreError::CheckpointNotFound
                }
            })?;
        self.read_checkpoint_ordinal(ordinal)
    }

    /// Reconstructs every sealed checkpoint and checks stored length/hash.
    pub fn verify_all(&mut self) -> Result<VerificationReport, CheckpointStoreError> {
        self.verify_segment_index_checksums()?;
        let mut failures = 0u64;
        for ordinal in 0..self.metadata.checkpoints.len() {
            let checkpoint = self
                .metadata
                .checkpoints
                .get(ordinal)
                .ok_or_else(|| format_error("checkpoint ordinal outside metadata"))?
                .clone();
            let bytes = self.reconstruct_checkpoint(&checkpoint)?;
            let len = u64::try_from(bytes.len())
                .map_err(|_| format_error("reconstructed state length overflow"))?;
            if len != checkpoint.logical_state_len || xxh3_64(&bytes) != checkpoint.state_hash {
                failures = failures.saturating_add(1);
            }
        }
        Ok(VerificationReport {
            checkpoint_count: u64::try_from(self.metadata.checkpoints.len())
                .map_err(|_| format_error("checkpoint count overflow"))?,
            failures,
        })
    }

    pub(super) fn verify_segment_index_checksums(&self) -> Result<(), CheckpointStoreError> {
        for segment in &self.segments {
            let index_bytes_len = segment
                .index
                .header
                .stream_table_bytes
                .checked_add(segment.index.header.block_table_bytes)
                .ok_or_else(|| format_error("lazy segment index length overflow"))?;
            let mut index_bytes = vec![
                0u8;
                usize::try_from(index_bytes_len).map_err(
                    |_| format_error("lazy segment index length exceeds usize")
                )?
            ];
            read_file_exact_at(
                &segment.file,
                &mut index_bytes,
                segment.index.header.index_offset,
            )?;
            if xxh3_64(&index_bytes) != segment.index.header.index_xxh3_64 {
                return Err(format_error("lazy segment index checksum mismatch"));
            }
        }
        Ok(())
    }

    pub(super) fn load_metadata(&mut self) -> Result<LazyMetadata, CheckpointStoreError> {
        let roots = self.read_stream_all(VERSION_ROOT)?;
        let parents = self.read_stream_all(VERSION_PARENT)?;
        if roots.len() % 8 != 0 || parents.len() % 4 != 0 || roots.len() / 8 != parents.len() / 4 {
            return Err(format_error("lazy version column widths disagree"));
        }
        let mut versions = Vec::with_capacity(roots.len() / 8);
        let mut version_parents = Vec::with_capacity(parents.len() / 4);
        for index in 0..roots.len() / 8 {
            let raw_root = read_u64(&roots, index * 8)?;
            let raw_parent = read_u32(&parents, index * 4)?;
            versions.push(if raw_root == NONE_ROOT {
                None
            } else {
                Some(raw_root)
            });
            version_parents.push(if raw_parent == NONE_PARENT {
                None
            } else {
                Some(raw_parent)
            });
        }
        if versions.len()
            != usize::try_from(self.manifest.version_count)
                .map_err(|_| format_error("lazy version count overflow"))?
        {
            return Err(format_error("lazy version count disagrees with manifest"));
        }

        let threads = decode_string_table(
            &self.read_stream_all(THREAD_OFFSETS)?,
            &self.read_stream_all(THREAD_BYTES)?,
        )?;
        if threads.len()
            != usize::try_from(self.manifest.thread_count)
                .map_err(|_| format_error("lazy thread count overflow"))?
        {
            return Err(format_error("lazy thread count disagrees with manifest"));
        }
        let mut thread_ordinals = HashMap::with_capacity(threads.len());
        for (index, thread) in threads.iter().enumerate() {
            thread_ordinals.insert(
                thread.clone(),
                u32::try_from(index).map_err(|_| format_error("thread ordinal overflow"))?,
            );
        }

        let checkpoint_count = usize::try_from(self.manifest.checkpoint_count)
            .map_err(|_| format_error("lazy checkpoint count overflow"))?;
        let checkpoint_ids = decode_string_table(
            &self.read_stream_all(CP_ID_OFFSETS)?,
            &self.read_stream_all(CP_ID_BYTES)?,
        )?;
        if checkpoint_ids.len() != checkpoint_count {
            return Err(format_error("lazy checkpoint id table count mismatch"));
        }
        let cp_thread = self.read_stream_all(CP_THREAD)?;
        let cp_no = self.read_stream_all(CP_NO)?;
        let cp_parent = self.read_stream_all(CP_PARENT_ORDINAL)?;
        let cp_identity = self.read_stream_all(CP_IDENTITY_VERSION)?;
        let cp_messages = self.read_stream_all(CP_MESSAGES_VERSION)?;
        let cp_result = self.read_stream_all(CP_RESULT_VERSION)?;
        let cp_len = self.read_stream_all(CP_LOGICAL_LEN)?;
        let cp_hash = self.read_stream_all(CP_STATE_HASH)?;
        for (bytes, width) in [
            (&cp_thread, 4usize),
            (&cp_no, 4),
            (&cp_parent, 4),
            (&cp_identity, 4),
            (&cp_messages, 4),
            (&cp_result, 4),
            (&cp_len, 8),
            (&cp_hash, 8),
        ] {
            if bytes.len()
                != checkpoint_count
                    .checked_mul(width)
                    .ok_or_else(|| format_error("lazy checkpoint metadata width overflow"))?
            {
                return Err(format_error("lazy checkpoint metadata width mismatch"));
            }
        }
        let mut checkpoints = Vec::with_capacity(checkpoint_count);
        let mut checkpoint_ordinals = HashMap::with_capacity(checkpoint_count);
        for index in 0..checkpoint_count {
            let thread_ordinal = read_u32(&cp_thread, index * 4)?;
            let thread = threads
                .get(
                    usize::try_from(thread_ordinal)
                        .map_err(|_| format_error("thread ordinal overflow"))?,
                )
                .cloned()
                .ok_or_else(|| format_error("lazy checkpoint thread ordinal outside table"))?;
            let parent_ordinal = read_u32(&cp_parent, index * 4)?;
            let parent_checkpoint_id = if parent_ordinal == NONE_PARENT {
                None
            } else {
                let parent_index = usize::try_from(parent_ordinal)
                    .map_err(|_| format_error("lazy parent ordinal overflow"))?;
                if parent_index >= index {
                    return Err(format_error("lazy checkpoint parent is not prior"));
                }
                Some(
                    checkpoint_ids
                        .get(parent_index)
                        .cloned()
                        .ok_or_else(|| format_error("lazy parent checkpoint id missing"))?,
                )
            };
            let optional = |raw: u32| if raw == NONE_VERSION { None } else { Some(raw) };
            let checkpoint_id = checkpoint_ids
                .get(index)
                .cloned()
                .ok_or_else(|| format_error("lazy checkpoint id missing"))?;
            let info = CheckpointInfo {
                ordinal: u32::try_from(index)
                    .map_err(|_| format_error("checkpoint ordinal overflow"))?,
                thread_id: thread.clone(),
                checkpoint_no: read_u32(&cp_no, index * 4)?,
                checkpoint_id: checkpoint_id.clone(),
                parent_checkpoint_id,
                identity_version: read_u32(&cp_identity, index * 4)?,
                messages_version: optional(read_u32(&cp_messages, index * 4)?),
                result_version: optional(read_u32(&cp_result, index * 4)?),
                logical_state_len: read_u64(&cp_len, index * 8)?,
                state_hash: read_u64(&cp_hash, index * 8)?,
            };
            checkpoint_ordinals.insert((thread, checkpoint_id), info.ordinal);
            checkpoints.push(info);
        }
        let node_count = self.manifest.stream_sizes[NODE_KIND];
        if self.manifest.stream_sizes[NODE_FIELD0]
            != node_count
                .checked_mul(8)
                .ok_or_else(|| format_error("lazy node field0 width overflow"))?
            || self.manifest.stream_sizes[NODE_FIELD1]
                != node_count
                    .checked_mul(4)
                    .ok_or_else(|| format_error("lazy node field1 width overflow"))?
        {
            return Err(format_error("lazy compact node column widths disagree"));
        }
        let wide_count = self.manifest.stream_sizes[WIDE_KIND];
        for stream_id in [WIDE_A, WIDE_B, WIDE_C] {
            if self.manifest.stream_sizes[stream_id]
                != wide_count
                    .checked_mul(8)
                    .ok_or_else(|| format_error("lazy wide column width overflow"))?
            {
                return Err(format_error("lazy wide node column widths disagree"));
            }
        }
        if std::env::var_os("TULYA_LAZY_EAGER_TOPOLOGY_DIAGNOSTIC").is_some() {
            for stream_id in [
                NODE_KIND,
                NODE_FIELD0,
                NODE_FIELD1,
                WIDE_KIND,
                WIDE_A,
                WIDE_B,
                WIDE_C,
            ] {
                let _ = self.read_stream_all(stream_id)?;
            }
        }
        Ok(LazyMetadata {
            versions,
            version_parents,
            threads,
            thread_ordinals,
            checkpoints,
            checkpoint_ordinals,
        })
    }

    pub(super) fn read_checkpoint_ordinal(
        &mut self,
        ordinal: u32,
    ) -> Result<Vec<u8>, CheckpointStoreError> {
        let index =
            usize::try_from(ordinal).map_err(|_| format_error("checkpoint ordinal overflow"))?;
        let checkpoint = self
            .metadata
            .checkpoints
            .get(index)
            .ok_or_else(|| format_error("checkpoint ordinal outside metadata"))?
            .clone();
        self.reconstruct_checkpoint(&checkpoint)
    }

    pub(super) fn reconstruct_checkpoint(
        &mut self,
        checkpoint: &CheckpointInfo,
    ) -> Result<Vec<u8>, CheckpointStoreError> {
        let identity = self.extract_version(checkpoint.identity_version)?;
        let messages = checkpoint
            .messages_version
            .map(|version| self.extract_version(version))
            .transpose()?
            .unwrap_or_default();
        let result = checkpoint
            .result_version
            .map(|version| self.extract_version(version))
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

    pub(super) fn extract_version(
        &mut self,
        version: u32,
    ) -> Result<Vec<u8>, CheckpointStoreError> {
        let index = usize::try_from(version).map_err(|_| format_error("version index overflow"))?;
        let root = self
            .metadata
            .versions
            .get(index)
            .copied()
            .flatten()
            .ok_or_else(|| format_error("version has no root"))?;
        self.extract_root(root)
    }

    pub(super) fn extract_root(&mut self, root: u64) -> Result<Vec<u8>, CheckpointStoreError> {
        let mut out = Vec::new();
        let mut stack = vec![root];
        while let Some(node_id) = stack.pop() {
            let node = self.decode_node(node_id)?;
            if node.kind == 0 {
                let start = node.a;
                let length = node.b;
                let end = start
                    .checked_add(length)
                    .ok_or_else(|| format_error("lazy leaf byte end overflow"))?;
                self.append_stream_range(
                    PAYLOAD,
                    start,
                    end.checked_sub(start)
                        .ok_or_else(|| format_error("lazy leaf range underflow"))?,
                    &mut out,
                )?;
            } else {
                stack.push(node.b);
                stack.push(node.a);
            }
        }
        Ok(out)
    }

    pub(super) fn decode_node(
        &mut self,
        node_id: u64,
    ) -> Result<DecodedNode, CheckpointStoreError> {
        let node_count = self.manifest.stream_sizes[NODE_KIND];
        if node_id >= node_count {
            return Err(format_error("lazy node id outside committed nodes"));
        }
        let field0_offset = node_id
            .checked_mul(8)
            .ok_or_else(|| format_error("lazy node field0 offset overflow"))?;
        let field1_offset = node_id
            .checked_mul(4)
            .ok_or_else(|| format_error("lazy node field1 offset overflow"))?;
        let kind = self
            .read_stream_range(NODE_KIND, node_id, 1)?
            .first()
            .copied()
            .ok_or_else(|| format_error("lazy node kind missing"))?;
        match kind {
            0 => {
                let field0 = self.read_stream_range(NODE_FIELD0, field0_offset, 8)?;
                let field1 = self.read_stream_range(NODE_FIELD1, field1_offset, 4)?;
                Ok(DecodedNode {
                    kind: 0,
                    a: read_u64(&field0, 0)?,
                    b: u64::from(read_u32(&field1, 0)?),
                })
            }
            1 => {
                let field0 = self.read_stream_range(NODE_FIELD0, field0_offset, 8)?;
                let left_delta = u64::from(read_u32(&field0, 0)?);
                let right_delta = u64::from(read_u32(&field0, 4)?);
                if left_delta == 0
                    || right_delta == 0
                    || left_delta > node_id
                    || right_delta > node_id
                {
                    return Err(format_error("lazy compact binary delta underflow"));
                }
                Ok(DecodedNode {
                    kind: 1,
                    a: node_id - left_delta,
                    b: node_id - right_delta,
                })
            }
            2 => {
                let field0 = self.read_stream_range(NODE_FIELD0, field0_offset, 8)?;
                let wide_index = read_u64(&field0, 0)?;
                if wide_index >= self.manifest.stream_sizes[WIDE_KIND] {
                    return Err(format_error("lazy wide index outside committed records"));
                }
                let wide_offset = wide_index
                    .checked_mul(8)
                    .ok_or_else(|| format_error("lazy wide offset overflow"))?;
                let wide_kind = self
                    .read_stream_range(WIDE_KIND, wide_index, 1)?
                    .first()
                    .copied()
                    .ok_or_else(|| format_error("lazy wide kind missing"))?;
                let wide_a = self.read_stream_range(WIDE_A, wide_offset, 8)?;
                let wide_b = self.read_stream_range(WIDE_B, wide_offset, 8)?;
                match wide_kind {
                    0 => Ok(DecodedNode {
                        kind: 0,
                        a: read_u64(&wide_a, 0)?,
                        b: read_u64(&wide_b, 0)?,
                    }),
                    1 => {
                        let left = read_u64(&wide_a, 0)?;
                        let right = read_u64(&wide_b, 0)?;
                        if left >= node_id || right >= node_id {
                            return Err(format_error("lazy wide binary child not prior"));
                        }
                        Ok(DecodedNode {
                            kind: 1,
                            a: left,
                            b: right,
                        })
                    }
                    _ => Err(format_error("lazy wide node kind invalid")),
                }
            }
            _ => Err(format_error("lazy node kind invalid")),
        }
    }

    pub(super) fn read_stream_all(
        &mut self,
        stream_id: usize,
    ) -> Result<Vec<u8>, CheckpointStoreError> {
        let length = *self
            .manifest
            .stream_sizes
            .get(stream_id)
            .ok_or_else(|| format_error("lazy stream id outside manifest"))?;
        self.read_stream_range(stream_id, 0, length)
    }

    pub(super) fn read_stream_range(
        &mut self,
        stream_id: usize,
        start: u64,
        length: u64,
    ) -> Result<Vec<u8>, CheckpointStoreError> {
        let output_capacity =
            usize::try_from(length).map_err(|_| format_error("lazy stream range exceeds usize"))?;
        let mut output = Vec::with_capacity(output_capacity);
        self.append_stream_range(stream_id, start, length, &mut output)?;
        Ok(output)
    }

    pub(super) fn append_stream_range(
        &mut self,
        stream_id: usize,
        start: u64,
        length: u64,
        output: &mut Vec<u8>,
    ) -> Result<(), CheckpointStoreError> {
        let stream_size = *self
            .manifest
            .stream_sizes
            .get(stream_id)
            .ok_or_else(|| format_error("lazy stream id outside manifest"))?;
        let end = start
            .checked_add(length)
            .ok_or_else(|| format_error("lazy stream range overflow"))?;
        if end > stream_size {
            return Err(format_error("lazy stream range outside manifest"));
        }
        let output_start = output.len();
        if length == 0 {
            return Ok(());
        }
        if stream_id == PAYLOAD {
            if let Some(payload) = self.payload_fast.as_ref() {
                let start = usize::try_from(start)
                    .map_err(|_| format_error("lazy fast payload start exceeds usize"))?;
                let end = usize::try_from(end)
                    .map_err(|_| format_error("lazy fast payload end exceeds usize"))?;
                output.extend_from_slice(payload.get(start..end).ok_or_else(|| {
                    format_error("lazy fast payload range outside decoded stream")
                })?);
                return Ok(());
            }
        }
        let mut cursor = start;
        while cursor < end {
            let (segment_index, stream_entry) = self
                .segments
                .iter()
                .enumerate()
                .find_map(|(index, segment)| {
                    let entry = segment.index.streams.get(stream_id).copied()?;
                    (entry.global_start <= cursor && cursor < entry.global_end)
                        .then_some((index, entry))
                })
                .ok_or_else(|| format_error("lazy stream range has no sealed segment"))?;
            let segment_end = end.min(stream_entry.global_end);
            let local_start = cursor
                .checked_sub(stream_entry.global_start)
                .ok_or_else(|| format_error("lazy stream local offset underflow"))?;
            let local_end = segment_end
                .checked_sub(stream_entry.global_start)
                .ok_or_else(|| format_error("lazy stream local end underflow"))?;
            let block_count = u64::from(stream_entry.block_count);
            if block_count == 0 {
                return Err(format_error("lazy stream has no blocks"));
            }
            let block_size = u64::from(
                self.segments
                    .get(segment_index)
                    .ok_or_else(|| format_error("lazy segment index outside reader"))?
                    .index
                    .header
                    .block_size,
            );
            let mut local_cursor = local_start;
            while local_cursor < local_end {
                let block_position = (local_cursor / block_size).min(block_count - 1);
                let block_index = stream_entry
                    .first_block
                    .checked_add(
                        u32::try_from(block_position)
                            .map_err(|_| format_error("lazy block position overflow"))?,
                    )
                    .ok_or_else(|| format_error("lazy block index overflow"))?;
                let block = self.read_block_entry(segment_index, block_index)?;
                if usize::try_from(block.stream_id)
                    .map_err(|_| format_error("lazy block stream id overflow"))?
                    != stream_id
                {
                    return Err(format_error("lazy block belongs to wrong stream"));
                }
                let block_end_raw = block
                    .raw_offset
                    .checked_add(u64::from(block.raw_len))
                    .ok_or_else(|| format_error("lazy block raw end overflow"))?;
                if block.raw_offset > local_cursor
                    || block_end_raw <= local_cursor
                    || block_end_raw > stream_entry.raw_len
                {
                    return Err(format_error("lazy block exceeds stream raw length"));
                }
                let decoded = self.load_block(
                    segment_index,
                    usize::try_from(block_index)
                        .map_err(|_| format_error("lazy block index exceeds usize"))?,
                    block,
                )?;
                let copy_start = usize::try_from(local_cursor - block.raw_offset)
                    .map_err(|_| format_error("lazy block copy start overflow"))?;
                let copy_end = usize::try_from((local_end.min(block_end_raw)) - block.raw_offset)
                    .map_err(|_| format_error("lazy block copy end overflow"))?;
                output.extend_from_slice(
                    decoded.get(copy_start..copy_end).ok_or_else(|| {
                        format_error("lazy block copy range outside decoded block")
                    })?,
                );
                local_cursor = stream_entry
                    .global_start
                    .checked_add(block_end_raw.min(local_end))
                    .and_then(|global| global.checked_sub(stream_entry.global_start))
                    .ok_or_else(|| format_error("lazy stream cursor overflow"))?;
            }
            cursor = segment_end;
        }
        if output.len().saturating_sub(output_start)
            != usize::try_from(length)
                .map_err(|_| format_error("lazy stream range exceeds usize"))?
        {
            return Err(format_error("lazy stream range returned unexpected length"));
        }
        Ok(())
    }

    pub(super) fn read_block_entry(
        &mut self,
        segment_index: usize,
        block_index: u32,
    ) -> Result<BlockEntry, CheckpointStoreError> {
        let block_index_usize = usize::try_from(block_index)
            .map_err(|_| format_error("lazy block index exceeds usize"))?;
        let key = (segment_index, block_index_usize);
        if let Some(entry) = self.block_entries.get(&key).copied() {
            self.metrics.block_entry_cache_hits =
                self.metrics.block_entry_cache_hits.saturating_add(1);
            return Ok(entry);
        }
        let entry = {
            let segment = self
                .segments
                .get(segment_index)
                .ok_or_else(|| format_error("lazy segment index outside reader"))?;
            read_lazy_block_entry(&segment.file, &segment.index, block_index)?
        };
        self.metrics.block_entry_cache_misses =
            self.metrics.block_entry_cache_misses.saturating_add(1);
        self.metrics.block_entry_bytes_read = self.metrics.block_entry_bytes_read.saturating_add(
            u64::try_from(BLOCK_ENTRY_SIZE)
                .map_err(|_| format_error("block entry size exceeds u64"))?,
        );
        self.block_entries.insert(key, entry);
        self.block_entry_order.push_back(key);
        if self.block_entries.len() > self.cache_capacity {
            let evicted = self
                .block_entry_order
                .pop_front()
                .ok_or_else(|| format_error("lazy block-entry eviction order is empty"))?;
            self.block_entries.remove(&evicted);
        }
        Ok(entry)
    }

    pub(super) fn load_block(
        &mut self,
        segment_index: usize,
        block_index: usize,
        block: BlockEntry,
    ) -> Result<&[u8], CheckpointStoreError> {
        let key = (segment_index, block_index);
        if self.cache.contains_key(&key) {
            self.metrics.cache_hits = self.metrics.cache_hits.saturating_add(1);
            return self
                .cache
                .get(&key)
                .map(Vec::as_slice)
                .ok_or_else(|| format_error("lazy cache entry disappeared"));
        }
        let decoded = {
            let segment = self
                .segments
                .get_mut(segment_index)
                .ok_or_else(|| format_error("lazy segment index outside reader"))?;
            let payload_offset = segment
                .index
                .header
                .payload_offset
                .checked_add(block.encoded_offset)
                .ok_or_else(|| format_error("lazy encoded block offset overflow"))?;
            let mut encoded = vec![
                0u8;
                usize::try_from(block.encoded_len).map_err(|_| format_error(
                    "lazy encoded block length overflow"
                ))?
            ];
            read_file_exact_at(&segment.file, &mut encoded, payload_offset)?;
            segment.decompressor.decompress(
                &encoded,
                usize::try_from(block.raw_len)
                    .map_err(|_| format_error("lazy raw block length overflow"))?,
            )?
        };
        if decoded.len()
            != usize::try_from(block.raw_len)
                .map_err(|_| format_error("lazy raw block length overflow"))?
            || xxh3_64(&decoded) != block.raw_xxh3_64
        {
            return Err(format_error(
                "lazy sealed block decompression/hash mismatch",
            ));
        }
        self.metrics.cache_misses = self.metrics.cache_misses.saturating_add(1);
        self.metrics.encoded_bytes_read = self
            .metrics
            .encoded_bytes_read
            .saturating_add(u64::from(block.encoded_len));
        self.metrics.raw_bytes_decompressed = self
            .metrics
            .raw_bytes_decompressed
            .saturating_add(u64::from(block.raw_len));
        self.cache.insert(key, decoded);
        self.cache_order.push_back(key);
        if self.cache.len() > self.cache_capacity {
            let evicted = self
                .cache_order
                .pop_front()
                .ok_or_else(|| format_error("lazy cache eviction order is empty"))?;
            self.cache.remove(&evicted);
        }
        self.cache
            .get(&key)
            .map(Vec::as_slice)
            .ok_or_else(|| format_error("lazy cache insertion failed"))
    }
}
