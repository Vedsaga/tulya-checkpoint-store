use super::*;

pub(super) fn read_u32(data: &[u8], offset: usize) -> Result<u32, CheckpointStoreError> {
    let end = offset
        .checked_add(4)
        .ok_or_else(|| format_error("u32 offset overflow"))?;
    let bytes: [u8; 4] = data
        .get(offset..end)
        .ok_or_else(|| format_error("truncated u32"))?
        .try_into()
        .map_err(|_| format_error("u32 slice width mismatch"))?;
    Ok(u32::from_le_bytes(bytes))
}

pub(super) fn read_u64(data: &[u8], offset: usize) -> Result<u64, CheckpointStoreError> {
    let end = offset
        .checked_add(8)
        .ok_or_else(|| format_error("u64 offset overflow"))?;
    let bytes: [u8; 8] = data
        .get(offset..end)
        .ok_or_else(|| format_error("truncated u64"))?
        .try_into()
        .map_err(|_| format_error("u64 slice width mismatch"))?;
    Ok(u64::from_le_bytes(bytes))
}

pub(super) fn parse_root(
    record: &[u8],
    expected_id: u64,
) -> Result<(Option<u64>, Option<u32>), CheckpointStoreError> {
    if record.len() != ROOT_RECORD_SIZE || record.get(..4) != Some(ROOT_MAGIC.as_slice()) {
        return Err(format_error("invalid root record"));
    }
    if xxh3_64(
        record
            .get(..24)
            .ok_or_else(|| format_error("truncated root checksum input"))?,
    ) != read_u64(record, 24)?
    {
        return Err(format_error("root checksum mismatch"));
    }
    let id = u64::from(read_u32(record, 4)?);
    if id != expected_id {
        return Err(format_error("root id is not sequential"));
    }
    let raw_root = read_u64(record, 8)?;
    let raw_parent = read_u32(record, 16)?;
    let root = if raw_root == NONE_ROOT {
        None
    } else {
        Some(raw_root)
    };
    let parent = if raw_parent == NONE_PARENT {
        None
    } else if u64::from(raw_parent) < id {
        Some(raw_parent)
    } else {
        return Err(format_error("root parent is not topologically prior"));
    };
    Ok((root, parent))
}

pub(super) fn parse_checkpoint_record(
    record: &[u8],
    version_count: u64,
    ordinal: u32,
) -> Result<CheckpointInfo, CheckpointStoreError> {
    if record.len() < CHECKPOINT_PREFIX_SIZE + 8
        || record.get(..4) != Some(CHECKPOINT_MAGIC.as_slice())
    {
        return Err(format_error("invalid checkpoint record"));
    }
    if usize::try_from(read_u32(record, 4)?)
        .map_err(|_| format_error("checkpoint record length overflow"))?
        != record.len()
    {
        return Err(format_error("checkpoint record length mismatch"));
    }
    let checksum_at = record.len() - 8;
    if xxh3_64(
        record
            .get(..checksum_at)
            .ok_or_else(|| format_error("checkpoint checksum input outside record"))?,
    ) != read_u64(record, checksum_at)?
    {
        return Err(format_error("checkpoint checksum mismatch"));
    }
    let checkpoint_no = read_u32(record, 8)?;
    let identity = read_u32(record, 12)?;
    let messages = read_u32(record, 16)?;
    let result = read_u32(record, 20)?;
    if u64::from(identity) >= version_count {
        return Err(format_error("identity version out of bounds"));
    }
    let optional_version = |raw: u32| -> Result<Option<u32>, CheckpointStoreError> {
        if raw == NONE_VERSION {
            Ok(None)
        } else if u64::from(raw) < version_count {
            Ok(Some(raw))
        } else {
            Err(format_error("optional checkpoint version out of bounds"))
        }
    };
    let thread_len = usize::try_from(read_u32(record, 24)?)
        .map_err(|_| format_error("thread length overflow"))?;
    let checkpoint_len = usize::try_from(read_u32(record, 28)?)
        .map_err(|_| format_error("checkpoint-id length overflow"))?;
    let parent_len = usize::try_from(read_u32(record, 32)?)
        .map_err(|_| format_error("parent-id length overflow"))?;
    if thread_len == 0 || thread_len > MAX_CHECKPOINT_IDENTIFIER_BYTES {
        return Err(format_error("thread id is empty or exceeds the byte limit"));
    }
    if checkpoint_len == 0 || checkpoint_len > MAX_CHECKPOINT_IDENTIFIER_BYTES {
        return Err(format_error(
            "checkpoint id is empty or exceeds the byte limit",
        ));
    }
    if parent_len > MAX_CHECKPOINT_IDENTIFIER_BYTES {
        return Err(format_error("parent checkpoint id exceeds the byte limit"));
    }
    let logical_state_len = read_u64(record, 36)?;
    let state_hash = read_u64(record, 44)?;
    let payload_end = CHECKPOINT_PREFIX_SIZE
        .checked_add(thread_len)
        .and_then(|n| n.checked_add(checkpoint_len))
        .and_then(|n| n.checked_add(parent_len))
        .ok_or_else(|| format_error("checkpoint payload length overflow"))?;
    if payload_end + 8 != record.len() {
        return Err(format_error("checkpoint payload geometry mismatch"));
    }
    let mut cursor = CHECKPOINT_PREFIX_SIZE;
    let thread_end = cursor + thread_len;
    let thread_id = std::str::from_utf8(
        record
            .get(cursor..thread_end)
            .ok_or_else(|| format_error("thread bytes outside checkpoint record"))?,
    )
    .map_err(|_| format_error("thread id is not UTF-8"))?
    .to_owned();
    cursor = thread_end;
    let checkpoint_end = cursor + checkpoint_len;
    let checkpoint_id = std::str::from_utf8(
        record
            .get(cursor..checkpoint_end)
            .ok_or_else(|| format_error("checkpoint id bytes outside record"))?,
    )
    .map_err(|_| format_error("checkpoint id is not UTF-8"))?
    .to_owned();
    cursor = checkpoint_end;
    let parent_end = cursor + parent_len;
    let parent = std::str::from_utf8(
        record
            .get(cursor..parent_end)
            .ok_or_else(|| format_error("parent id bytes outside record"))?,
    )
    .map_err(|_| format_error("parent checkpoint id is not UTF-8"))?
    .to_owned();
    Ok(CheckpointInfo {
        ordinal,
        thread_id,
        checkpoint_no,
        checkpoint_id,
        parent_checkpoint_id: if parent.is_empty() {
            None
        } else {
            Some(parent)
        },
        identity_version: identity,
        messages_version: optional_version(messages)?,
        result_version: optional_version(result)?,
        logical_state_len,
        state_hash,
    })
}

pub(super) fn validate_request_id(request_id: &[u8]) -> Result<(), CheckpointStoreError> {
    if request_id.is_empty() || request_id.len() > MAX_REQUEST_ID_BYTES {
        return Err(format_error(
            "request id is empty or exceeds the byte limit",
        ));
    }
    Ok(())
}

pub(super) fn validate_checkpoint_identifier(
    value: &str,
    label: &str,
) -> Result<(), CheckpointStoreError> {
    if value.is_empty() || value.len() > MAX_CHECKPOINT_IDENTIFIER_BYTES {
        return Err(format_error(format!(
            "{label} is empty or exceeds the byte limit"
        )));
    }
    Ok(())
}

pub(super) fn encode_message_append_transaction(
    geometry: &Geometry,
    first_checkpoint: Option<&CheckpointInfo>,
    thread_id: &str,
    checkpoint_id: &str,
    checkpoint_no: u32,
    parent_checkpoint_id: Option<&str>,
    parent: Option<(u32, u64, u32)>,
    message_body: &[u8],
    canonical_state_len: u64,
    canonical_state_hash: u64,
) -> Result<Vec<u8>, CheckpointStoreError> {
    validate_checkpoint_identifier(thread_id, "thread id")?;
    validate_checkpoint_identifier(checkpoint_id, "checkpoint id")?;
    if let Some(parent_id) = parent_checkpoint_id {
        validate_checkpoint_identifier(parent_id, "parent checkpoint id")?;
    }
    if message_body.is_empty() {
        return Err(format_error("message checkpoint body is empty"));
    }

    let version_start = u32::try_from(geometry.version_count)
        .map_err(|_| format_error("version count exceeds the T2W1 u32 limit"))?;
    let checkpoint_count = geometry
        .checkpoint_count
        .checked_add(1)
        .ok_or_else(|| format_error("checkpoint count overflows the T2W1 u64 limit"))?;
    let checkpoint_count = u32::try_from(checkpoint_count)
        .map_err(|_| format_error("checkpoint count exceeds the T2W1 u32 limit"))?;

    let mut payload = Vec::new();
    let mut compact_nodes = Vec::<[u8; COMPACT_NODE_SIZE]>::new();
    let mut roots = Vec::<(u32, u64, Option<u32>)>::new();
    let (identity_version, messages_version, version_count) = if geometry.checkpoint_count == 0 {
        if parent.is_some() || parent_checkpoint_id.is_some() || first_checkpoint.is_some() {
            return Err(format_error(
                "first message checkpoint cannot have a parent",
            ));
        }
        let identity_node = geometry.node_count;
        let messages_node = identity_node
            .checked_add(1)
            .ok_or_else(|| format_error("message node id overflow"))?;
        payload.extend_from_slice(b"null");
        payload.extend_from_slice(message_body);

        let mut identity_leaf = [0u8; COMPACT_NODE_SIZE];
        identity_leaf[..8].copy_from_slice(&geometry.byte_len.to_le_bytes());
        identity_leaf[8..12].copy_from_slice(&4u32.to_le_bytes());
        identity_leaf[12..16].copy_from_slice(&KIND_LEAF.to_le_bytes());
        compact_nodes.push(identity_leaf);

        let message_offset = geometry
            .byte_len
            .checked_add(4)
            .ok_or_else(|| format_error("message leaf offset overflow"))?;
        let mut message_leaf = [0u8; COMPACT_NODE_SIZE];
        message_leaf[..8].copy_from_slice(&message_offset.to_le_bytes());
        message_leaf[8..12].copy_from_slice(
            &u32::try_from(message_body.len())
                .map_err(|_| format_error("message delta exceeds compact-node length"))?
                .to_le_bytes(),
        );
        message_leaf[12..16].copy_from_slice(&KIND_LEAF.to_le_bytes());
        compact_nodes.push(message_leaf);

        let messages_version = version_start
            .checked_add(1)
            .ok_or_else(|| format_error("message version id overflow"))?;
        let version_count = messages_version
            .checked_add(1)
            .ok_or_else(|| format_error("message version count overflow"))?;
        roots.push((version_start, identity_node, None));
        roots.push((messages_version, messages_node, None));
        (version_start, messages_version, version_count)
    } else if let Some((identity_version, parent_root, parent_messages_version)) = parent {
        if parent_checkpoint_id.is_none() {
            return Err(format_error("message parent metadata is inconsistent"));
        }
        payload.push(b',');
        payload.extend_from_slice(message_body);

        let leaf_node = geometry.node_count;
        let root_node = leaf_node
            .checked_add(1)
            .ok_or_else(|| format_error("message binary root id overflow"))?;
        let mut message_leaf = [0u8; COMPACT_NODE_SIZE];
        message_leaf[..8].copy_from_slice(&geometry.byte_len.to_le_bytes());
        message_leaf[8..12].copy_from_slice(
            &u32::try_from(payload.len())
                .map_err(|_| format_error("message delta exceeds compact-node length"))?
                .to_le_bytes(),
        );
        message_leaf[12..16].copy_from_slice(&KIND_LEAF.to_le_bytes());
        compact_nodes.push(message_leaf);

        let left_delta = root_node
            .checked_sub(parent_root)
            .ok_or_else(|| format_error("message parent root is not topologically prior"))?;
        let right_delta = root_node
            .checked_sub(leaf_node)
            .ok_or_else(|| format_error("message leaf is not topologically prior"))?;
        let mut binary = [0u8; COMPACT_NODE_SIZE];
        binary[..4].copy_from_slice(
            &u32::try_from(left_delta)
                .map_err(|_| format_error("message parent delta exceeds compact-node limit"))?
                .to_le_bytes(),
        );
        binary[4..8].copy_from_slice(
            &u32::try_from(right_delta)
                .map_err(|_| format_error("message leaf delta exceeds compact-node limit"))?
                .to_le_bytes(),
        );
        binary[12..16].copy_from_slice(&KIND_BINARY.to_le_bytes());
        compact_nodes.push(binary);

        let version_count = version_start
            .checked_add(1)
            .ok_or_else(|| format_error("message version count overflow"))?;
        roots.push((version_start, root_node, Some(parent_messages_version)));
        (identity_version, version_start, version_count)
    } else {
        if parent_checkpoint_id.is_some() {
            return Err(format_error("message parent checkpoint is absent"));
        }
        let first = first_checkpoint
            .ok_or_else(|| format_error("non-empty store is missing its first checkpoint"))?;
        payload.extend_from_slice(message_body);
        let root_node = geometry.node_count;
        let mut message_leaf = [0u8; COMPACT_NODE_SIZE];
        message_leaf[..8].copy_from_slice(&geometry.byte_len.to_le_bytes());
        message_leaf[8..12].copy_from_slice(
            &u32::try_from(message_body.len())
                .map_err(|_| format_error("message delta exceeds compact-node length"))?
                .to_le_bytes(),
        );
        message_leaf[12..16].copy_from_slice(&KIND_LEAF.to_le_bytes());
        compact_nodes.push(message_leaf);
        let version_count = version_start
            .checked_add(1)
            .ok_or_else(|| format_error("message version count overflow"))?;
        roots.push((version_start, root_node, None));
        (first.identity_version, version_start, version_count)
    };

    let parent_id = parent_checkpoint_id.unwrap_or("");
    let checkpoint_record_len = CHECKPOINT_PREFIX_SIZE
        .checked_add(thread_id.len())
        .and_then(|length| length.checked_add(checkpoint_id.len()))
        .and_then(|length| length.checked_add(parent_id.len()))
        .and_then(|length| length.checked_add(TX_CHECKSUM_SIZE))
        .ok_or_else(|| format_error("checkpoint record length overflow"))?;
    let checkpoint_record_len_u32 = u32::try_from(checkpoint_record_len)
        .map_err(|_| format_error("checkpoint record exceeds the u32 length limit"))?;
    let mut checkpoint_record = Vec::with_capacity(checkpoint_record_len);
    checkpoint_record.extend_from_slice(&CHECKPOINT_MAGIC);
    checkpoint_record.extend_from_slice(&checkpoint_record_len_u32.to_le_bytes());
    checkpoint_record.extend_from_slice(&checkpoint_no.to_le_bytes());
    checkpoint_record.extend_from_slice(&identity_version.to_le_bytes());
    checkpoint_record.extend_from_slice(&messages_version.to_le_bytes());
    checkpoint_record.extend_from_slice(&NONE_VERSION.to_le_bytes());
    checkpoint_record.extend_from_slice(
        &u32::try_from(thread_id.len())
            .map_err(|_| format_error("thread id length exceeds the u32 limit"))?
            .to_le_bytes(),
    );
    checkpoint_record.extend_from_slice(
        &u32::try_from(checkpoint_id.len())
            .map_err(|_| format_error("checkpoint id length exceeds the u32 limit"))?
            .to_le_bytes(),
    );
    checkpoint_record.extend_from_slice(
        &u32::try_from(parent_id.len())
            .map_err(|_| format_error("parent checkpoint id length exceeds the u32 limit"))?
            .to_le_bytes(),
    );
    checkpoint_record.extend_from_slice(&canonical_state_len.to_le_bytes());
    checkpoint_record.extend_from_slice(&canonical_state_hash.to_le_bytes());
    checkpoint_record.extend_from_slice(thread_id.as_bytes());
    checkpoint_record.extend_from_slice(checkpoint_id.as_bytes());
    checkpoint_record.extend_from_slice(parent_id.as_bytes());
    checkpoint_record.extend_from_slice(&xxh3_64(&checkpoint_record).to_le_bytes());

    let mut root_records = Vec::with_capacity(
        roots
            .len()
            .checked_mul(ROOT_RECORD_SIZE)
            .ok_or_else(|| format_error("root record bytes overflow"))?,
    );
    for (version, root, parent_version) in roots {
        let mut record = Vec::with_capacity(ROOT_RECORD_SIZE);
        record.extend_from_slice(&ROOT_MAGIC);
        record.extend_from_slice(&version.to_le_bytes());
        record.extend_from_slice(&root.to_le_bytes());
        record.extend_from_slice(&parent_version.unwrap_or(NONE_PARENT).to_le_bytes());
        record.extend_from_slice(&0u32.to_le_bytes());
        record.extend_from_slice(&xxh3_64(&record).to_le_bytes());
        root_records.extend_from_slice(&record);
    }

    let compact_node_bytes = compact_nodes
        .len()
        .checked_mul(COMPACT_NODE_SIZE)
        .ok_or_else(|| format_error("compact message node bytes overflow"))?;
    let total = TX_HEADER_SIZE
        .checked_add(payload.len())
        .and_then(|length| length.checked_add(compact_node_bytes))
        .and_then(|length| length.checked_add(root_records.len()))
        .and_then(|length| length.checked_add(checkpoint_record.len()))
        .and_then(|length| length.checked_add(TX_CHECKSUM_SIZE))
        .ok_or_else(|| format_error("message transaction length overflow"))?;
    let total_u32 = u32::try_from(total)
        .map_err(|_| format_error("message transaction exceeds the u32 length limit"))?;
    let root_count = version_count
        .checked_sub(version_start)
        .ok_or_else(|| format_error("message root count underflow"))?;
    let mut transaction = Vec::with_capacity(total);
    transaction.extend_from_slice(&TX_MAGIC);
    transaction.extend_from_slice(&total_u32.to_le_bytes());
    transaction.extend_from_slice(&version_start.to_le_bytes());
    transaction.extend_from_slice(&version_count.to_le_bytes());
    transaction.extend_from_slice(&checkpoint_count.to_le_bytes());
    transaction.extend_from_slice(&root_count.to_le_bytes());
    transaction.extend_from_slice(&geometry.byte_len.to_le_bytes());
    transaction.extend_from_slice(
        &u64::try_from(payload.len())
            .map_err(|_| format_error("message payload length exceeds u64"))?
            .to_le_bytes(),
    );
    transaction.extend_from_slice(&geometry.node_count.to_le_bytes());
    transaction.extend_from_slice(
        &u32::try_from(compact_nodes.len())
            .map_err(|_| format_error("message compact node count exceeds u32"))?
            .to_le_bytes(),
    );
    transaction.extend_from_slice(&0u32.to_le_bytes());
    transaction.extend_from_slice(&geometry.wide_count.to_le_bytes());
    transaction.extend_from_slice(&checkpoint_record_len_u32.to_le_bytes());
    transaction.extend_from_slice(&0u32.to_le_bytes());
    transaction.extend_from_slice(&payload);
    for node in compact_nodes {
        transaction.extend_from_slice(&node);
    }
    transaction.extend_from_slice(&root_records);
    transaction.extend_from_slice(&checkpoint_record);
    transaction.extend_from_slice(&xxh3_64(&transaction).to_le_bytes());
    Ok(transaction)
}

pub(super) fn encode_single_identity_transaction(
    geometry: &Geometry,
    thread_id: &str,
    checkpoint_id: &str,
    checkpoint_no: u32,
    parent_checkpoint_id: Option<&str>,
    identity: &[u8],
) -> Result<Vec<u8>, CheckpointStoreError> {
    validate_checkpoint_identifier(thread_id, "thread id")?;
    validate_checkpoint_identifier(checkpoint_id, "checkpoint id")?;
    if let Some(parent) = parent_checkpoint_id {
        validate_checkpoint_identifier(parent, "parent checkpoint id")?;
    }

    let version_start = u32::try_from(geometry.version_count)
        .map_err(|_| format_error("version count exceeds the T2W1 u32 limit"))?;
    let version_count = version_start
        .checked_add(1)
        .ok_or_else(|| format_error("version count overflows the T2W1 u32 limit"))?;
    let checkpoint_count = geometry
        .checkpoint_count
        .checked_add(1)
        .ok_or_else(|| format_error("checkpoint count overflows the T2W1 u32 limit"))?;
    let checkpoint_count = u32::try_from(checkpoint_count)
        .map_err(|_| format_error("checkpoint count exceeds the T2W1 u32 limit"))?;
    let identity_len = u64::try_from(identity.len())
        .map_err(|_| format_error("identity length does not fit u64"))?;
    let identity_len_u32 = u32::try_from(identity.len())
        .map_err(|_| format_error("identity length exceeds the compact leaf limit"))?;
    let parent = parent_checkpoint_id.unwrap_or("");
    let canonical_len = u64::try_from(b"{\"identity\":".len())
        .map_err(|_| format_error("canonical prefix length does not fit u64"))?
        .checked_add(identity_len)
        .and_then(|length| length.checked_add(u64::try_from(b",\"messages\":[]}".len()).ok()?))
        .ok_or_else(|| format_error("canonical state length overflow"))?;
    let mut canonical = Vec::new();
    canonical.extend_from_slice(b"{\"identity\":");
    canonical.extend_from_slice(identity);
    canonical.extend_from_slice(b",\"messages\":[]}");
    let state_hash = xxh3_64(&canonical);

    let checkpoint_record_len = CHECKPOINT_PREFIX_SIZE
        .checked_add(thread_id.len())
        .and_then(|length| length.checked_add(checkpoint_id.len()))
        .and_then(|length| length.checked_add(parent.len()))
        .and_then(|length| length.checked_add(TX_CHECKSUM_SIZE))
        .ok_or_else(|| format_error("checkpoint record length overflow"))?;
    let checkpoint_record_len_u32 = u32::try_from(checkpoint_record_len)
        .map_err(|_| format_error("checkpoint record exceeds the u32 length limit"))?;

    let mut checkpoint_record = Vec::with_capacity(checkpoint_record_len);
    checkpoint_record.extend_from_slice(&CHECKPOINT_MAGIC);
    checkpoint_record.extend_from_slice(&checkpoint_record_len_u32.to_le_bytes());
    checkpoint_record.extend_from_slice(&checkpoint_no.to_le_bytes());
    checkpoint_record.extend_from_slice(&version_start.to_le_bytes());
    checkpoint_record.extend_from_slice(&NONE_VERSION.to_le_bytes());
    checkpoint_record.extend_from_slice(&NONE_VERSION.to_le_bytes());
    checkpoint_record.extend_from_slice(
        &u32::try_from(thread_id.len())
            .map_err(|_| format_error("thread id length exceeds the u32 limit"))?
            .to_le_bytes(),
    );
    checkpoint_record.extend_from_slice(
        &u32::try_from(checkpoint_id.len())
            .map_err(|_| format_error("checkpoint id length exceeds the u32 limit"))?
            .to_le_bytes(),
    );
    checkpoint_record.extend_from_slice(
        &u32::try_from(parent.len())
            .map_err(|_| format_error("parent checkpoint id length exceeds the u32 limit"))?
            .to_le_bytes(),
    );
    checkpoint_record.extend_from_slice(&canonical_len.to_le_bytes());
    checkpoint_record.extend_from_slice(&state_hash.to_le_bytes());
    checkpoint_record.extend_from_slice(thread_id.as_bytes());
    checkpoint_record.extend_from_slice(checkpoint_id.as_bytes());
    checkpoint_record.extend_from_slice(parent.as_bytes());
    checkpoint_record.extend_from_slice(&xxh3_64(&checkpoint_record).to_le_bytes());

    let mut slot = [0u8; COMPACT_NODE_SIZE];
    slot[..8].copy_from_slice(&geometry.byte_len.to_le_bytes());
    slot[8..12].copy_from_slice(&identity_len_u32.to_le_bytes());
    slot[12..16].copy_from_slice(&KIND_LEAF.to_le_bytes());

    let root_id = geometry.node_count;
    let root_id_u32 = version_start;
    let root_parent = version_start.checked_sub(1).unwrap_or(NONE_PARENT);
    let mut root_record = Vec::with_capacity(ROOT_RECORD_SIZE);
    root_record.extend_from_slice(&ROOT_MAGIC);
    root_record.extend_from_slice(&root_id_u32.to_le_bytes());
    root_record.extend_from_slice(&root_id.to_le_bytes());
    root_record.extend_from_slice(&root_parent.to_le_bytes());
    root_record.extend_from_slice(&0u32.to_le_bytes());
    root_record.extend_from_slice(&xxh3_64(&root_record).to_le_bytes());

    let total = TX_HEADER_SIZE
        .checked_add(identity.len())
        .and_then(|length| length.checked_add(COMPACT_NODE_SIZE))
        .and_then(|length| length.checked_add(ROOT_RECORD_SIZE))
        .and_then(|length| length.checked_add(checkpoint_record.len()))
        .and_then(|length| length.checked_add(TX_CHECKSUM_SIZE))
        .ok_or_else(|| format_error("transaction length overflow"))?;
    let total_u32 = u32::try_from(total)
        .map_err(|_| format_error("transaction exceeds the u32 length limit"))?;

    let mut transaction = Vec::with_capacity(total);
    transaction.extend_from_slice(&TX_MAGIC);
    transaction.extend_from_slice(&total_u32.to_le_bytes());
    transaction.extend_from_slice(&version_start.to_le_bytes());
    transaction.extend_from_slice(&version_count.to_le_bytes());
    transaction.extend_from_slice(&checkpoint_count.to_le_bytes());
    transaction.extend_from_slice(&1u32.to_le_bytes());
    transaction.extend_from_slice(&geometry.byte_len.to_le_bytes());
    transaction.extend_from_slice(&identity_len.to_le_bytes());
    transaction.extend_from_slice(&geometry.node_count.to_le_bytes());
    transaction.extend_from_slice(&1u32.to_le_bytes());
    transaction.extend_from_slice(&0u32.to_le_bytes());
    transaction.extend_from_slice(&geometry.wide_count.to_le_bytes());
    transaction.extend_from_slice(&checkpoint_record_len_u32.to_le_bytes());
    transaction.extend_from_slice(&0u32.to_le_bytes());
    transaction.extend_from_slice(identity);
    transaction.extend_from_slice(&slot);
    transaction.extend_from_slice(&root_record);
    transaction.extend_from_slice(&checkpoint_record);
    transaction.extend_from_slice(&xxh3_64(&transaction).to_le_bytes());
    Ok(transaction)
}

#[allow(dead_code)] // Reserved for a future zero-copy repository adapter feature.
pub(super) fn encode_identity_leaf_transaction(
    geometry: &Geometry,
    thread_id: &str,
    checkpoint_id: &str,
    checkpoint_no: u32,
    parent_checkpoint_id: Option<&str>,
    leaves: &[IdentityLeafSource<'_>],
    canonical_state_len: u64,
    canonical_state_hash: u64,
) -> Result<Vec<u8>, CheckpointStoreError> {
    validate_checkpoint_identifier(thread_id, "thread id")?;
    validate_checkpoint_identifier(checkpoint_id, "checkpoint id")?;
    if let Some(parent) = parent_checkpoint_id {
        validate_checkpoint_identifier(parent, "parent checkpoint id")?;
    }
    if leaves.is_empty() {
        return Err(format_error("identity tree must contain at least one leaf"));
    }

    let version_start = u32::try_from(geometry.version_count)
        .map_err(|_| format_error("version count exceeds the T2W1 u32 limit"))?;
    let version_count = version_start
        .checked_add(1)
        .ok_or_else(|| format_error("version count overflows the T2W1 u32 limit"))?;
    let checkpoint_count = geometry
        .checkpoint_count
        .checked_add(1)
        .ok_or_else(|| format_error("checkpoint count overflows the T2W1 u64 limit"))?;
    let checkpoint_count = u32::try_from(checkpoint_count)
        .map_err(|_| format_error("checkpoint count exceeds the u32 limit"))?;
    let mut byte_delta = Vec::new();
    let mut leaf_ids = Vec::with_capacity(leaves.len());
    let mut identity_len = 0u64;
    let mut nodes = Vec::<[u8; COMPACT_NODE_SIZE]>::new();
    for leaf in leaves {
        let (node_id, leaf_len) = match leaf {
            IdentityLeafSource::New(bytes) => {
                let offset = geometry
                    .byte_len
                    .checked_add(
                        u64::try_from(byte_delta.len())
                            .map_err(|_| format_error("identity byte delta length exceeds u64"))?,
                    )
                    .ok_or_else(|| format_error("identity leaf offset overflow"))?;
                let leaf_len = u64::try_from(bytes.len())
                    .map_err(|_| format_error("identity leaf length exceeds u64"))?;
                let leaf_len_u32 = u32::try_from(leaf_len)
                    .map_err(|_| format_error("identity leaf length exceeds compact-node limit"))?;
                byte_delta.extend_from_slice(bytes);
                let node_id = geometry
                    .node_count
                    .checked_add(
                        u64::try_from(nodes.len())
                            .map_err(|_| format_error("identity node count exceeds u64"))?,
                    )
                    .ok_or_else(|| format_error("identity node id overflow"))?;
                let mut slot = [0u8; COMPACT_NODE_SIZE];
                slot[..8].copy_from_slice(&offset.to_le_bytes());
                slot[8..12].copy_from_slice(&leaf_len_u32.to_le_bytes());
                slot[12..16].copy_from_slice(&KIND_LEAF.to_le_bytes());
                nodes.push(slot);
                (node_id, leaf_len)
            }
            IdentityLeafSource::Existing(reference) => {
                if reference.node_id >= geometry.node_count {
                    return Err(format_error("identity reuse node is not committed"));
                }
                (reference.node_id, reference.logical_len)
            }
        };
        identity_len = identity_len
            .checked_add(leaf_len)
            .ok_or_else(|| format_error("identity length overflow"))?;
        leaf_ids.push(node_id);
    }
    let expected_state_len = CANONICAL_STATE_PREFIX_BYTES
        .checked_add(identity_len)
        .and_then(|value| value.checked_add(CANONICAL_STATE_SUFFIX_BYTES))
        .ok_or_else(|| format_error("canonical state length overflow"))?;
    if expected_state_len != canonical_state_len {
        return Err(format_error(
            "canonical state length disagrees with identity leaves",
        ));
    }

    let mut level = leaf_ids;
    while level.len() > 1 {
        let mut next = Vec::with_capacity(level.len().div_ceil(2));
        for pair in level.chunks(2) {
            if pair.len() == 1 {
                next.push(pair[0]);
                continue;
            }
            let node_id = geometry
                .node_count
                .checked_add(
                    u64::try_from(nodes.len())
                        .map_err(|_| format_error("identity node count exceeds u64"))?,
                )
                .ok_or_else(|| format_error("identity node id overflow"))?;
            let left_delta = node_id
                .checked_sub(pair[0])
                .ok_or_else(|| format_error("identity binary left child is not prior"))?;
            let right_delta = node_id
                .checked_sub(pair[1])
                .ok_or_else(|| format_error("identity binary right child is not prior"))?;
            let mut slot = [0u8; COMPACT_NODE_SIZE];
            slot[..4].copy_from_slice(
                &u32::try_from(left_delta)
                    .map_err(|_| format_error("identity binary left delta exceeds u32"))?
                    .to_le_bytes(),
            );
            slot[4..8].copy_from_slice(
                &u32::try_from(right_delta)
                    .map_err(|_| format_error("identity binary right delta exceeds u32"))?
                    .to_le_bytes(),
            );
            slot[12..16].copy_from_slice(&KIND_BINARY.to_le_bytes());
            nodes.push(slot);
            next.push(node_id);
        }
        level = next;
    }
    let root_id = *level
        .first()
        .ok_or_else(|| format_error("identity tree root is missing"))?;

    let parent = parent_checkpoint_id.unwrap_or("");
    let checkpoint_record_len = CHECKPOINT_PREFIX_SIZE
        .checked_add(thread_id.len())
        .and_then(|length| length.checked_add(checkpoint_id.len()))
        .and_then(|length| length.checked_add(parent.len()))
        .and_then(|length| length.checked_add(TX_CHECKSUM_SIZE))
        .ok_or_else(|| format_error("checkpoint record length overflow"))?;
    let checkpoint_record_len_u32 = u32::try_from(checkpoint_record_len)
        .map_err(|_| format_error("checkpoint record exceeds the u32 length limit"))?;
    let mut checkpoint_record = Vec::with_capacity(checkpoint_record_len);
    checkpoint_record.extend_from_slice(&CHECKPOINT_MAGIC);
    checkpoint_record.extend_from_slice(&checkpoint_record_len_u32.to_le_bytes());
    checkpoint_record.extend_from_slice(&checkpoint_no.to_le_bytes());
    checkpoint_record.extend_from_slice(&version_start.to_le_bytes());
    checkpoint_record.extend_from_slice(&NONE_VERSION.to_le_bytes());
    checkpoint_record.extend_from_slice(&NONE_VERSION.to_le_bytes());
    checkpoint_record.extend_from_slice(
        &u32::try_from(thread_id.len())
            .map_err(|_| format_error("thread id length exceeds the u32 limit"))?
            .to_le_bytes(),
    );
    checkpoint_record.extend_from_slice(
        &u32::try_from(checkpoint_id.len())
            .map_err(|_| format_error("checkpoint id length exceeds the u32 limit"))?
            .to_le_bytes(),
    );
    checkpoint_record.extend_from_slice(
        &u32::try_from(parent.len())
            .map_err(|_| format_error("parent checkpoint id length exceeds the u32 limit"))?
            .to_le_bytes(),
    );
    checkpoint_record.extend_from_slice(&canonical_state_len.to_le_bytes());
    checkpoint_record.extend_from_slice(&canonical_state_hash.to_le_bytes());
    checkpoint_record.extend_from_slice(thread_id.as_bytes());
    checkpoint_record.extend_from_slice(checkpoint_id.as_bytes());
    checkpoint_record.extend_from_slice(parent.as_bytes());
    checkpoint_record.extend_from_slice(&xxh3_64(&checkpoint_record).to_le_bytes());

    let root_parent = version_start.checked_sub(1).unwrap_or(NONE_PARENT);
    let mut root_record = Vec::with_capacity(ROOT_RECORD_SIZE);
    root_record.extend_from_slice(&ROOT_MAGIC);
    root_record.extend_from_slice(&version_start.to_le_bytes());
    root_record.extend_from_slice(&root_id.to_le_bytes());
    root_record.extend_from_slice(&root_parent.to_le_bytes());
    root_record.extend_from_slice(&0u32.to_le_bytes());
    root_record.extend_from_slice(&xxh3_64(&root_record).to_le_bytes());

    let total = TX_HEADER_SIZE
        .checked_add(byte_delta.len())
        .and_then(|length| length.checked_add(nodes.len().checked_mul(COMPACT_NODE_SIZE)?))
        .and_then(|length| length.checked_add(ROOT_RECORD_SIZE))
        .and_then(|length| length.checked_add(checkpoint_record.len()))
        .and_then(|length| length.checked_add(TX_CHECKSUM_SIZE))
        .ok_or_else(|| format_error("transaction length overflow"))?;
    let total_u32 = u32::try_from(total)
        .map_err(|_| format_error("transaction exceeds the u32 length limit"))?;
    let mut transaction = Vec::with_capacity(total);
    transaction.extend_from_slice(&TX_MAGIC);
    transaction.extend_from_slice(&total_u32.to_le_bytes());
    transaction.extend_from_slice(&version_start.to_le_bytes());
    transaction.extend_from_slice(&version_count.to_le_bytes());
    transaction.extend_from_slice(&checkpoint_count.to_le_bytes());
    transaction.extend_from_slice(&1u32.to_le_bytes());
    transaction.extend_from_slice(&geometry.byte_len.to_le_bytes());
    transaction.extend_from_slice(
        &u64::try_from(byte_delta.len())
            .map_err(|_| format_error("identity byte delta length exceeds u64"))?
            .to_le_bytes(),
    );
    transaction.extend_from_slice(&geometry.node_count.to_le_bytes());
    transaction.extend_from_slice(
        &u32::try_from(nodes.len())
            .map_err(|_| format_error("identity node count exceeds u32"))?
            .to_le_bytes(),
    );
    transaction.extend_from_slice(&0u32.to_le_bytes());
    transaction.extend_from_slice(&geometry.wide_count.to_le_bytes());
    transaction.extend_from_slice(&checkpoint_record_len_u32.to_le_bytes());
    transaction.extend_from_slice(&0u32.to_le_bytes());
    transaction.extend_from_slice(&byte_delta);
    for node in nodes {
        transaction.extend_from_slice(&node);
    }
    transaction.extend_from_slice(&root_record);
    transaction.extend_from_slice(&checkpoint_record);
    transaction.extend_from_slice(&xxh3_64(&transaction).to_le_bytes());
    Ok(transaction)
}

#[cfg(test)]
pub(super) fn encode_transaction_with_request_id(
    transaction: &[u8],
    request_id: &[u8],
) -> Result<Vec<u8>, CheckpointStoreError> {
    validate_request_id(request_id)?;
    if transaction.len() < TX_HEADER_SIZE + TX_CHECKSUM_SIZE
        || transaction.get(..4) != Some(TX_MAGIC.as_slice())
    {
        return Err(format_error("invalid requestless transaction"));
    }
    if read_u32(transaction, 68)? != 0 {
        return Err(format_error(
            "requestless transaction has a non-zero request field",
        ));
    }
    let new_len = transaction
        .len()
        .checked_add(request_id.len())
        .ok_or_else(|| format_error("request-bearing transaction length overflow"))?;
    let new_len_u32 = u32::try_from(new_len)
        .map_err(|_| format_error("request-bearing transaction exceeds u32 length"))?;
    let mut encoded = Vec::with_capacity(new_len);
    encoded.extend_from_slice(
        transaction
            .get(..4)
            .ok_or_else(|| format_error("transaction magic missing"))?,
    );
    encoded.extend_from_slice(&new_len_u32.to_le_bytes());
    encoded.extend_from_slice(
        transaction
            .get(8..68)
            .ok_or_else(|| format_error("transaction header missing"))?,
    );
    encoded.extend_from_slice(
        &u32::try_from(request_id.len())
            .map_err(|_| format_error("request id length exceeds u32"))?
            .to_le_bytes(),
    );
    encoded.extend_from_slice(request_id);
    encoded.extend_from_slice(
        transaction
            .get(TX_HEADER_SIZE..transaction.len() - TX_CHECKSUM_SIZE)
            .ok_or_else(|| format_error("transaction body missing"))?,
    );
    let digest = xxh3_64(&encoded);
    encoded.extend_from_slice(&digest.to_le_bytes());
    Ok(encoded)
}

pub(super) fn parse_transaction_unchecked(
    record: &[u8],
    start_offset: u64,
) -> Result<ParsedTransaction, CheckpointStoreError> {
    parse_transaction_with_geometry(record, None, start_offset)
}

pub(super) fn parse_transaction(
    record: &[u8],
    geometry: Geometry,
    start_offset: u64,
) -> Result<ParsedTransaction, CheckpointStoreError> {
    parse_transaction_with_geometry(record, Some(geometry), start_offset)
}

pub(super) fn parse_transaction_with_geometry(
    record: &[u8],
    geometry: Option<Geometry>,
    start_offset: u64,
) -> Result<ParsedTransaction, CheckpointStoreError> {
    if record.len() < TX_HEADER_SIZE + TX_CHECKSUM_SIZE
        || record.get(..4) != Some(TX_MAGIC.as_slice())
    {
        return Err(format_error("invalid T2W1 transaction header"));
    }
    let record_len = usize::try_from(read_u32(record, 4)?)
        .map_err(|_| format_error("transaction length overflow"))?;
    if record_len != record.len() {
        return Err(format_error("transaction length mismatch"));
    }
    if xxh3_64(
        record
            .get(..record.len() - TX_CHECKSUM_SIZE)
            .ok_or_else(|| format_error("transaction checksum input outside record"))?,
    ) != read_u64(record, record.len() - TX_CHECKSUM_SIZE)?
    {
        return Err(format_error("transaction checksum mismatch"));
    }
    let version_start = u64::from(read_u32(record, 8)?);
    let version_count = u64::from(read_u32(record, 12)?);
    let checkpoint_count = u64::from(read_u32(record, 16)?);
    let new_version_count = u64::from(read_u32(record, 20)?);
    let byte_start = read_u64(record, 24)?;
    let byte_delta_len = usize::try_from(read_u64(record, 32)?)
        .map_err(|_| format_error("byte delta length overflow"))?;
    let node_start = read_u64(record, 40)?;
    let compact_node_count = usize::try_from(read_u32(record, 48)?)
        .map_err(|_| format_error("compact node count overflow"))?;
    let wide_node_count = usize::try_from(read_u32(record, 52)?)
        .map_err(|_| format_error("wide node count overflow"))?;
    let wide_start = read_u64(record, 56)?;
    let checkpoint_record_len = usize::try_from(read_u32(record, 64)?)
        .map_err(|_| format_error("checkpoint record length overflow"))?;
    let request_id_len = usize::try_from(read_u32(record, 68)?)
        .map_err(|_| format_error("request id length overflow"))?;
    if request_id_len > MAX_REQUEST_ID_BYTES {
        return Err(format_error("request id exceeds the byte limit"));
    }
    if checkpoint_count == 0 {
        return Err(format_error(
            "transaction checkpoint count must be non-zero",
        ));
    }
    let expected_version_count = version_start
        .checked_add(new_version_count)
        .ok_or_else(|| format_error("version topology overflows u64"))?;
    if version_count != expected_version_count {
        return Err(format_error("transaction version topology is inconsistent"));
    }
    if let Some(geometry) = geometry {
        let expected_checkpoint_count = geometry
            .checkpoint_count
            .checked_add(1)
            .ok_or_else(|| format_error("checkpoint topology overflows u64"))?;
        if version_start != geometry.version_count
            || checkpoint_count != expected_checkpoint_count
            || byte_start != geometry.byte_len
            || node_start != geometry.node_count
            || wide_start != geometry.wide_count
        {
            return Err(format_error("transaction topology/watermark mismatch"));
        }
    }
    let compact_bytes_len = compact_node_count
        .checked_mul(COMPACT_NODE_SIZE)
        .ok_or_else(|| format_error("compact-node byte length overflow"))?;
    let wide_bytes_len = wide_node_count
        .checked_mul(WIDE_RECORD_SIZE)
        .ok_or_else(|| format_error("wide-node byte length overflow"))?;
    let root_bytes_len = usize::try_from(new_version_count)
        .map_err(|_| format_error("new version count overflow"))?
        .checked_mul(ROOT_RECORD_SIZE)
        .ok_or_else(|| format_error("root-record byte length overflow"))?;
    let expected = TX_HEADER_SIZE
        .checked_add(request_id_len)
        .and_then(|n| n.checked_add(byte_delta_len))
        .and_then(|n| n.checked_add(compact_bytes_len))
        .and_then(|n| n.checked_add(wide_bytes_len))
        .and_then(|n| n.checked_add(root_bytes_len))
        .and_then(|n| n.checked_add(checkpoint_record_len))
        .and_then(|n| n.checked_add(TX_CHECKSUM_SIZE))
        .ok_or_else(|| format_error("transaction payload geometry overflow"))?;
    if expected != record.len() {
        return Err(format_error("transaction payload geometry mismatch"));
    }
    let mut cursor = TX_HEADER_SIZE;
    let request_id = if request_id_len == 0 {
        None
    } else {
        let end = cursor
            .checked_add(request_id_len)
            .ok_or_else(|| format_error("request id range overflow"))?;
        let request_id = record
            .get(cursor..end)
            .ok_or_else(|| format_error("request id outside transaction"))?
            .to_vec();
        cursor = end;
        Some(request_id)
    };
    let byte_end_cursor = cursor + byte_delta_len;
    let bytes = record
        .get(cursor..byte_end_cursor)
        .ok_or_else(|| format_error("transaction byte delta outside record"))?
        .to_vec();
    cursor = byte_end_cursor;
    let compact_end_cursor = cursor + compact_bytes_len;
    let compact_nodes = record
        .get(cursor..compact_end_cursor)
        .ok_or_else(|| format_error("compact nodes outside transaction"))?
        .to_vec();
    cursor = compact_end_cursor;
    let wide_end_cursor = cursor + wide_bytes_len;
    let wide_nodes = record
        .get(cursor..wide_end_cursor)
        .ok_or_else(|| format_error("wide nodes outside transaction"))?
        .to_vec();
    cursor = wide_end_cursor;
    let mut roots = Vec::new();
    let mut parents = Vec::new();
    for expected_id in version_start..version_count {
        let root_end = cursor + ROOT_RECORD_SIZE;
        let (root, parent) = parse_root(
            record
                .get(cursor..root_end)
                .ok_or_else(|| format_error("root record outside transaction"))?,
            expected_id,
        )?;
        roots.push(root);
        parents.push(parent);
        cursor = root_end;
    }
    let cp_end = cursor + checkpoint_record_len;
    let ordinal = u32::try_from(checkpoint_count - 1)
        .map_err(|_| format_error("checkpoint ordinal exceeds u32"))?;
    let checkpoint = parse_checkpoint_record(
        record
            .get(cursor..cp_end)
            .ok_or_else(|| format_error("checkpoint record outside transaction"))?,
        version_count,
        ordinal,
    )?;
    cursor = cp_end;
    if cursor != record.len() - TX_CHECKSUM_SIZE {
        return Err(format_error("transaction cursor mismatch"));
    }
    let byte_end = byte_start
        .checked_add(
            u64::try_from(bytes.len()).map_err(|_| format_error("byte delta length overflow"))?,
        )
        .ok_or_else(|| format_error("byte watermark overflow"))?;
    let node_end = node_start
        .checked_add(
            u64::try_from(compact_node_count)
                .map_err(|_| format_error("compact-node count overflow"))?,
        )
        .ok_or_else(|| format_error("node watermark overflow"))?;
    let wide_end = wide_start
        .checked_add(
            u64::try_from(wide_node_count).map_err(|_| format_error("wide-node count overflow"))?,
        )
        .ok_or_else(|| format_error("wide watermark overflow"))?;
    let end_offset = start_offset
        .checked_add(
            u64::try_from(record.len()).map_err(|_| format_error("record length overflow"))?,
        )
        .ok_or_else(|| format_error("transaction file offset overflow"))?;
    Ok(ParsedTransaction {
        end_offset,
        version_start,
        version_count,
        checkpoint_count,
        byte_start,
        byte_end,
        bytes,
        node_start,
        node_end,
        compact_nodes,
        wide_start,
        wide_end,
        wide_nodes,
        roots,
        parents,
        checkpoint,
        request_id,
        operation_digest: Sha256::digest(record).into(),
    })
}

pub(super) fn parse_hot_prefix(
    path: &Path,
    mut geometry: Geometry,
) -> Result<HotParse, CheckpointStoreError> {
    let mut file = File::open(path)?;
    let physical = file.metadata()?.len();
    let mut offset = 0u64;
    let mut transactions = Vec::new();
    let minimum = u64::try_from(TX_HEADER_SIZE + TX_CHECKSUM_SIZE)
        .map_err(|_| format_error("minimum transaction size overflow"))?;
    while offset
        .checked_add(minimum)
        .ok_or_else(|| format_error("hot-WAL scan offset overflow"))?
        <= physical
    {
        file.seek(SeekFrom::Start(offset))?;
        let mut header = [0u8; TX_HEADER_SIZE];
        if file.read_exact(&mut header).is_err() {
            break;
        }
        if header.get(..4) != Some(TX_MAGIC.as_slice()) {
            break;
        }
        let record_len = u64::from(read_u32(&header, 4)?);
        if record_len < minimum {
            break;
        }
        let end = match offset.checked_add(record_len) {
            Some(value) if value <= physical => value,
            _ => break,
        };
        file.seek(SeekFrom::Start(offset))?;
        let mut record = vec![
            0u8;
            usize::try_from(record_len)
                .map_err(|_| format_error("hot transaction length overflow"))?
        ];
        if file.read_exact(&mut record).is_err() {
            break;
        }
        let tx = match parse_transaction(&record, geometry, offset) {
            Ok(value) => value,
            Err(_) => break,
        };
        geometry.advance(&tx);
        transactions.push(tx);
        offset = end;
    }
    Ok(HotParse {
        transactions,
        logical_tail: offset,
    })
}

pub(super) fn file_starts_with_tx_magic(path: &Path) -> Result<bool, CheckpointStoreError> {
    let mut file = File::open(path)?;
    let mut magic = [0u8; 4];
    match file.read_exact(&mut magic) {
        Ok(()) => Ok(magic == TX_MAGIC),
        Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => Ok(false),
        Err(error) => Err(error.into()),
    }
}

pub(super) fn validate_transaction_against_state(
    state: &StoreState,
    tx: &ParsedTransaction,
) -> Result<(), CheckpointStoreError> {
    if let Some(parent) = tx.checkpoint.parent_checkpoint_id.as_ref() {
        let key = (tx.checkpoint.thread_id.clone(), parent.clone());
        let parent_ordinal = state
            .checkpoint_ordinals
            .get(&key)
            .copied()
            .ok_or_else(|| format_error("checkpoint parent is not committed"))?;
        if parent_ordinal >= tx.checkpoint.ordinal {
            return Err(format_error("checkpoint parent is not prior"));
        }
    }
    if state.checkpoint_ordinals.contains_key(&(
        tx.checkpoint.thread_id.clone(),
        tx.checkpoint.checkpoint_id.clone(),
    )) {
        return Err(format_error("duplicate checkpoint key"));
    }
    if state.deleted_checkpoints.contains(&(
        tx.checkpoint.thread_id.clone(),
        tx.checkpoint.checkpoint_id.clone(),
    )) {
        return Err(CheckpointStoreError::CheckpointDeleted);
    }
    if let Some(request_id) = tx.request_id.as_ref() {
        if let Some(existing) = state.request_records.get(request_id) {
            if existing.operation_digest != tx.operation_digest {
                return Err(CheckpointStoreError::RequestIdConflict);
            }
            return Err(format_error("duplicate request id"));
        }
    }
    validate_new_nodes(state, tx)
}

pub(super) fn reject_deleted_checkpoint(
    state: &StoreState,
    tx: &ParsedTransaction,
) -> Result<(), CheckpointStoreError> {
    if state.deleted_checkpoints.contains(&(
        tx.checkpoint.thread_id.clone(),
        tx.checkpoint.checkpoint_id.clone(),
    )) {
        return Err(CheckpointStoreError::CheckpointDeleted);
    }
    Ok(())
}

pub(super) fn validate_new_nodes(
    state: &StoreState,
    tx: &ParsedTransaction,
) -> Result<(), CheckpointStoreError> {
    let mut arena_len = state.geometry()?.byte_len;
    arena_len = arena_len
        .checked_add(
            u64::try_from(tx.bytes.len()).map_err(|_| format_error("delta length overflow"))?,
        )
        .ok_or_else(|| format_error("arena length after delta overflow"))?;
    let base_nodes = state.geometry()?.node_count;
    let base_wide_count = state.lazy_base_geometry().wide_count;
    let combined_wide_len = state
        .geometry()?
        .wide_count
        .checked_mul(
            u64::try_from(WIDE_RECORD_SIZE)
                .map_err(|_| format_error("wide record size overflow"))?,
        )
        .and_then(|length| length.checked_add(u64::try_from(tx.wide_nodes.len()).ok()?))
        .ok_or_else(|| format_error("combined wide-node length overflow"))?;
    for (local, slot) in tx.compact_nodes.chunks_exact(COMPACT_NODE_SIZE).enumerate() {
        let node_id = base_nodes
            .checked_add(u64::try_from(local).map_err(|_| format_error("node index overflow"))?)
            .ok_or_else(|| format_error("global node id overflow"))?;
        let kind = read_u32(slot, 12)?;
        match kind {
            KIND_LEAF => {
                let offset = read_u64(slot, 0)?;
                let length = u64::from(read_u32(slot, 8)?);
                if offset
                    .checked_add(length)
                    .ok_or_else(|| format_error("leaf end overflow"))?
                    > arena_len
                {
                    return Err(format_error("leaf references bytes beyond committed arena"));
                }
            }
            KIND_BINARY => {
                let left_delta = u64::from(read_u32(slot, 0)?);
                let right_delta = u64::from(read_u32(slot, 4)?);
                if left_delta == 0
                    || right_delta == 0
                    || left_delta > node_id
                    || right_delta > node_id
                {
                    return Err(format_error("compact binary delta is invalid"));
                }
            }
            KIND_WIDE => {
                let wide_index = read_u64(slot, 0)?;
                if wide_index < base_wide_count {
                    continue;
                }
                let local_wide_index = wide_index
                    .checked_sub(base_wide_count)
                    .ok_or_else(|| format_error("local wide index underflow"))?;
                let wide_index = usize::try_from(local_wide_index)
                    .map_err(|_| format_error("wide index overflow"))?;
                let start = wide_index
                    .checked_mul(WIDE_RECORD_SIZE)
                    .ok_or_else(|| format_error("wide-record offset overflow"))?;
                let end = start
                    .checked_add(WIDE_RECORD_SIZE)
                    .ok_or_else(|| format_error("wide-record end overflow"))?;
                let record = if end <= state.wide_nodes.len() {
                    state
                        .wide_nodes
                        .get(start..end)
                        .ok_or_else(|| format_error("wide record outside base state"))?
                } else {
                    let relative_start = start.saturating_sub(state.wide_nodes.len());
                    let relative_end = end.saturating_sub(state.wide_nodes.len());
                    tx.wide_nodes
                        .get(relative_start..relative_end)
                        .ok_or_else(|| format_error("wide record outside transaction"))?
                };
                match read_u32(record, 0)? {
                    WIDE_KIND_LEAF => {
                        let offset = read_u64(record, 8)?;
                        let length = read_u64(record, 16)?;
                        if offset
                            .checked_add(length)
                            .ok_or_else(|| format_error("wide leaf end overflow"))?
                            > arena_len
                        {
                            return Err(format_error(
                                "wide leaf references bytes beyond committed arena",
                            ));
                        }
                    }
                    WIDE_KIND_BINARY => {
                        let left = read_u64(record, 8)?;
                        let right = read_u64(record, 16)?;
                        if left >= node_id || right >= node_id {
                            return Err(format_error(
                                "wide binary child is not topologically prior",
                            ));
                        }
                    }
                    _ => return Err(format_error("unknown wide node kind")),
                }
            }
            _ => return Err(format_error("unknown compact node kind")),
        }
    }
    if combined_wide_len
        % u64::try_from(WIDE_RECORD_SIZE).map_err(|_| format_error("wide record size overflow"))?
        != 0
    {
        return Err(format_error("wide-node bytes are not record aligned"));
    }
    for root in tx.roots.iter().flatten() {
        if *root >= tx.node_end {
            return Err(format_error("version root is outside committed node range"));
        }
    }
    Ok(())
}

pub(super) fn apply_transaction(
    state: &mut StoreState,
    tx: &ParsedTransaction,
) -> Result<(), CheckpointStoreError> {
    if Geometry::from_state(state)?
        != (Geometry {
            byte_len: tx.byte_start,
            node_count: tx.node_start,
            wide_count: tx.wide_start,
            version_count: tx.version_start,
            checkpoint_count: tx.checkpoint_count - 1,
        })
    {
        return Err(format_error(
            "transaction does not append to current state geometry",
        ));
    }
    validate_transaction_against_state(state, tx)?;
    state.arena_bytes.extend_from_slice(&tx.bytes);
    state.compact_nodes.extend_from_slice(&tx.compact_nodes);
    state.wide_nodes.extend_from_slice(&tx.wide_nodes);
    state.versions.extend_from_slice(&tx.roots);
    state.parents.extend_from_slice(&tx.parents);
    if !state.thread_ordinals.contains_key(&tx.checkpoint.thread_id) {
        let ordinal = u32::try_from(state.threads.len())
            .map_err(|_| format_error("thread ordinal exceeds u32"))?;
        state
            .thread_ordinals
            .insert(tx.checkpoint.thread_id.clone(), ordinal);
        state.threads.push(tx.checkpoint.thread_id.clone());
    }
    state.checkpoint_ordinals.insert(
        (
            tx.checkpoint.thread_id.clone(),
            tx.checkpoint.checkpoint_id.clone(),
        ),
        tx.checkpoint.ordinal,
    );
    if let Some(request_id) = tx.request_id.as_ref() {
        state.request_records.insert(
            request_id.clone(),
            RequestRecord {
                key: request_id.clone(),
                operation_digest: tx.operation_digest,
                checkpoint_ordinal: tx.checkpoint.ordinal,
            },
        );
    }
    state.checkpoints.push(tx.checkpoint.clone());
    Ok(())
}

#[derive(Debug, Clone, Copy)]
pub(super) struct DecodedNode {
    pub(super) kind: u8,
    pub(super) a: u64,
    pub(super) b: u64,
}

pub(super) fn decode_node(
    state: &StoreState,
    node_id: u64,
) -> Result<DecodedNode, CheckpointStoreError> {
    let local_node_id = node_id
        .checked_sub(state.lazy_base_geometry().node_count)
        .ok_or_else(|| format_error("node id belongs to lazy sealed base"))?;
    let index =
        usize::try_from(local_node_id).map_err(|_| format_error("node id exceeds usize"))?;
    let start = index
        .checked_mul(COMPACT_NODE_SIZE)
        .ok_or_else(|| format_error("compact node offset overflow"))?;
    let end = start
        .checked_add(COMPACT_NODE_SIZE)
        .ok_or_else(|| format_error("compact node end overflow"))?;
    let slot = state
        .compact_nodes
        .get(start..end)
        .ok_or_else(|| format_error("node id outside committed compact nodes"))?;
    match read_u32(slot, 12)? {
        KIND_LEAF => Ok(DecodedNode {
            kind: 0,
            a: read_u64(slot, 0)?,
            b: u64::from(read_u32(slot, 8)?),
        }),
        KIND_BINARY => {
            let left_delta = u64::from(read_u32(slot, 0)?);
            let right_delta = u64::from(read_u32(slot, 4)?);
            if left_delta == 0 || right_delta == 0 || left_delta > node_id || right_delta > node_id
            {
                return Err(format_error("compact binary delta underflow"));
            }
            Ok(DecodedNode {
                kind: 1,
                a: node_id - left_delta,
                b: node_id - right_delta,
            })
        }
        KIND_WIDE => {
            let wide_index = read_u64(slot, 0)?
                .checked_sub(state.lazy_base_geometry().wide_count)
                .ok_or_else(|| format_error("wide index belongs to lazy sealed base"))?;
            let wide_index = usize::try_from(wide_index)
                .map_err(|_| format_error("wide index exceeds usize"))?;
            let wide_start = wide_index
                .checked_mul(WIDE_RECORD_SIZE)
                .ok_or_else(|| format_error("wide offset overflow"))?;
            let wide_end = wide_start
                .checked_add(WIDE_RECORD_SIZE)
                .ok_or_else(|| format_error("wide end overflow"))?;
            let record = state
                .wide_nodes
                .get(wide_start..wide_end)
                .ok_or_else(|| format_error("wide index outside committed records"))?;
            match read_u32(record, 0)? {
                WIDE_KIND_LEAF => Ok(DecodedNode {
                    kind: 0,
                    a: read_u64(record, 8)?,
                    b: read_u64(record, 16)?,
                }),
                WIDE_KIND_BINARY => {
                    let left = read_u64(record, 8)?;
                    let right = read_u64(record, 16)?;
                    if left >= node_id || right >= node_id {
                        return Err(format_error("wide binary child not prior"));
                    }
                    Ok(DecodedNode {
                        kind: 1,
                        a: left,
                        b: right,
                    })
                }
                _ => Err(format_error("unknown wide node kind")),
            }
        }
        _ => Err(format_error("unknown compact node kind")),
    }
}

pub(super) fn extract_root(state: &StoreState, root: u64) -> Result<Vec<u8>, CheckpointStoreError> {
    let mut out = Vec::new();
    let mut stack = vec![root];
    while let Some(node_id) = stack.pop() {
        let node = decode_node(state, node_id)?;
        if node.kind == 0 {
            let start =
                usize::try_from(node.a).map_err(|_| format_error("leaf offset overflow"))?;
            let length =
                usize::try_from(node.b).map_err(|_| format_error("leaf length overflow"))?;
            let end = start
                .checked_add(length)
                .ok_or_else(|| format_error("leaf byte end overflow"))?;
            out.extend_from_slice(
                state
                    .arena_bytes
                    .get(start..end)
                    .ok_or_else(|| format_error("leaf bytes outside committed arena"))?,
            );
        } else {
            stack.push(node.b);
            stack.push(node.a);
        }
    }
    Ok(out)
}

pub(super) fn extract_version(
    state: &StoreState,
    version: u32,
) -> Result<Vec<u8>, CheckpointStoreError> {
    let index = usize::try_from(version).map_err(|_| format_error("version index overflow"))?;
    let root = state
        .versions
        .get(index)
        .copied()
        .flatten()
        .ok_or_else(|| format_error("version has no root"))?;
    extract_root(state, root)
}

pub(super) fn reconstruct_checkpoint(
    state: &StoreState,
    checkpoint: &CheckpointInfo,
) -> Result<Vec<u8>, CheckpointStoreError> {
    let identity = extract_version(state, checkpoint.identity_version)?;
    let messages = checkpoint
        .messages_version
        .map(|version| extract_version(state, version))
        .transpose()?
        .unwrap_or_default();
    let result = checkpoint
        .result_version
        .map(|version| extract_version(state, version))
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
