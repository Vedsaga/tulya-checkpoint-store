use super::*;

pub(super) fn materialize_sealed_state(
    dir: &Path,
    manifest: &Manifest,
    recovery_mode: CheckpointStoreRecoveryMode,
) -> Result<StoreState, CheckpointStoreError> {
    if manifest.generation == 0 {
        let mut state = StoreState::default();
        apply_manifest_lifecycle_metadata(&mut state, manifest)?;
        return Ok(state);
    }
    let mut streams = (0..STREAM_NAMES.len())
        .map(|_| Vec::<u8>::new())
        .collect::<Vec<_>>();
    let mut zstd_decompressor = zstd::bulk::Decompressor::new()?;
    for segment in &manifest.segments {
        let path = dir.join(&segment.file);
        let parsed = read_segment_index_file(&path)?;
        if parsed.header.generation != segment.generation
            || parsed.header.checkpoint_start_count != segment.checkpoint_start_count
            || parsed.header.checkpoint_end_count != segment.checkpoint_end_count
            || parsed.header.version_start_count != segment.version_start_count
            || parsed.header.version_end_count != segment.version_end_count
        {
            return Err(format_error("segment header disagrees with manifest"));
        }
        let mut file = File::open(&path)?;
        for (stream_id, stream_entry) in parsed.streams.iter().enumerate() {
            let current = u64::try_from(
                streams
                    .get(stream_id)
                    .ok_or_else(|| format_error("materialized stream missing"))?
                    .len(),
            )
            .map_err(|_| format_error("materialized stream length overflow"))?;
            if current != stream_entry.global_start {
                return Err(format_error("sealed stream global start is not contiguous"));
            }
            let block_start = usize::try_from(stream_entry.first_block)
                .map_err(|_| format_error("stream first block overflow"))?;
            let block_end = block_start
                .checked_add(
                    usize::try_from(stream_entry.block_count)
                        .map_err(|_| format_error("stream block count overflow"))?,
                )
                .ok_or_else(|| format_error("stream block range overflow"))?;
            let mut segment_raw = Vec::new();
            for block in parsed
                .blocks
                .get(block_start..block_end)
                .ok_or_else(|| format_error("stream block range outside table"))?
            {
                if usize::try_from(block.stream_id)
                    .map_err(|_| format_error("block stream id overflow"))?
                    != stream_id
                {
                    return Err(format_error("block belongs to wrong stream"));
                }
                file.seek(SeekFrom::Start(
                    parsed
                        .header
                        .payload_offset
                        .checked_add(block.encoded_offset)
                        .ok_or_else(|| format_error("encoded block file offset overflow"))?,
                ))?;
                let mut encoded = vec![
                    0u8;
                    usize::try_from(block.encoded_len).map_err(|_| {
                        format_error("encoded block length overflow")
                    })?
                ];
                file.read_exact(&mut encoded)?;
                let raw = zstd_decompressor.decompress(
                    &encoded,
                    usize::try_from(block.raw_len)
                        .map_err(|_| format_error("raw block length overflow"))?,
                )?;
                if raw.len()
                    != usize::try_from(block.raw_len)
                        .map_err(|_| format_error("raw block length overflow"))?
                    || xxh3_64(&raw) != block.raw_xxh3_64
                {
                    return Err(format_error("sealed block decompression/hash mismatch"));
                }
                segment_raw.extend_from_slice(&raw);
            }
            if u64::try_from(segment_raw.len())
                .map_err(|_| format_error("stream raw length overflow"))?
                != stream_entry.raw_len
                || (stream_entry.raw_len > 0 && xxh3_64(&segment_raw) != stream_entry.raw_xxh3_64)
            {
                return Err(format_error("sealed stream raw length/hash mismatch"));
            }
            streams
                .get_mut(stream_id)
                .ok_or_else(|| format_error("materialized stream missing"))?
                .extend_from_slice(&segment_raw);
        }
    }
    for (index, expected) in manifest.stream_sizes.iter().enumerate() {
        let actual = u64::try_from(
            streams
                .get(index)
                .ok_or_else(|| format_error("materialized stream missing"))?
                .len(),
        )
        .map_err(|_| format_error("materialized stream length overflow"))?;
        if actual != *expected {
            return Err(format_error(
                "materialized stream size disagrees with manifest",
            ));
        }
    }
    let mut state = state_from_streams(streams, manifest, recovery_mode)?;
    apply_manifest_lifecycle_metadata(&mut state, manifest)?;
    for route in &manifest.routes {
        for record in route_request_records(&dir.join(&route.file))? {
            if u64::from(record.checkpoint_ordinal) >= manifest.checkpoint_count
                || state
                    .checkpoints
                    .get(
                        usize::try_from(record.checkpoint_ordinal)
                            .map_err(|_| format_error("request checkpoint ordinal overflow"))?,
                    )
                    .is_none()
            {
                return Err(format_error(
                    "request checkpoint ordinal outside sealed state",
                ));
            }
            if state.retired_requests.contains_key(&record.key) {
                return Err(format_error("active request key is also retired"));
            }
            if state
                .request_records
                .insert(record.key.clone(), record)
                .is_some()
            {
                return Err(format_error("duplicate sealed request id"));
            }
        }
    }
    Ok(state)
}

pub(super) fn apply_manifest_lifecycle_metadata(
    state: &mut StoreState,
    manifest: &Manifest,
) -> Result<(), CheckpointStoreError> {
    for tombstone in &manifest.deleted_checkpoints {
        let key = (tombstone.thread_id.clone(), tombstone.checkpoint_id.clone());
        if state.checkpoint_ordinals.contains_key(&key) || !state.deleted_checkpoints.insert(key) {
            return Err(format_error("duplicate or live checkpoint tombstone"));
        }
    }
    for retired in &manifest.retired_requests {
        if state.request_records.contains_key(&retired.key) {
            return Err(format_error("active request key is also retired"));
        }
        if state
            .retired_requests
            .insert(retired.key.clone(), retired.operation_digest)
            .is_some()
        {
            return Err(format_error("duplicate retired request key"));
        }
    }
    Ok(())
}

pub(super) fn state_from_streams(
    mut streams: Vec<Vec<u8>>,
    manifest: &Manifest,
    recovery_mode: CheckpointStoreRecoveryMode,
) -> Result<StoreState, CheckpointStoreError> {
    let arena_bytes = match recovery_mode {
        CheckpointStoreRecoveryMode::ClonePayload => streams
            .get(PAYLOAD)
            .ok_or_else(|| format_error("payload stream missing"))?
            .clone(),
        CheckpointStoreRecoveryMode::ReusePayload => streams
            .get_mut(PAYLOAD)
            .ok_or_else(|| format_error("payload stream missing"))
            .map(std::mem::take)?,
        CheckpointStoreRecoveryMode::Lazy => {
            return Err(format_error(
                "lazy recovery must use the bounded sealed reader",
            ));
        }
    };
    let mut state = StoreState {
        arena_bytes,
        ..StoreState::default()
    };
    let kinds = streams
        .get(NODE_KIND)
        .ok_or_else(|| format_error("node kind stream missing"))?;
    let field0 = streams
        .get(NODE_FIELD0)
        .ok_or_else(|| format_error("node field0 stream missing"))?;
    let field1 = streams
        .get(NODE_FIELD1)
        .ok_or_else(|| format_error("node field1 stream missing"))?;
    if field0.len()
        != kinds
            .len()
            .checked_mul(8)
            .ok_or_else(|| format_error("node field0 size overflow"))?
        || field1.len()
            != kinds
                .len()
                .checked_mul(4)
                .ok_or_else(|| format_error("node field1 size overflow"))?
    {
        return Err(format_error("sealed node column widths disagree"));
    }
    for index in 0..kinds.len() {
        let mut slot = [0u8; COMPACT_NODE_SIZE];
        let f0_start = index * 8;
        let f1_start = index * 4;
        slot.get_mut(..8)
            .ok_or_else(|| format_error("compact slot field0 missing"))?
            .copy_from_slice(
                field0
                    .get(f0_start..f0_start + 8)
                    .ok_or_else(|| format_error("node field0 value missing"))?,
            );
        slot.get_mut(8..12)
            .ok_or_else(|| format_error("compact slot field1 missing"))?
            .copy_from_slice(
                field1
                    .get(f1_start..f1_start + 4)
                    .ok_or_else(|| format_error("node field1 value missing"))?,
            );
        let kind = match *kinds
            .get(index)
            .ok_or_else(|| format_error("node kind value missing"))?
        {
            0 => KIND_LEAF,
            1 => KIND_BINARY,
            2 => KIND_WIDE,
            _ => return Err(format_error("sealed node kind code invalid")),
        };
        slot.get_mut(12..16)
            .ok_or_else(|| format_error("compact slot kind field missing"))?
            .copy_from_slice(&kind.to_le_bytes());
        state.compact_nodes.extend_from_slice(&slot);
    }
    let wide_kinds = streams
        .get(WIDE_KIND)
        .ok_or_else(|| format_error("wide kind stream missing"))?;
    for name in [WIDE_A, WIDE_B, WIDE_C] {
        if streams
            .get(name)
            .ok_or_else(|| format_error("wide field stream missing"))?
            .len()
            != wide_kinds
                .len()
                .checked_mul(8)
                .ok_or_else(|| format_error("wide field size overflow"))?
        {
            return Err(format_error("sealed wide column widths disagree"));
        }
    }
    for index in 0..wide_kinds.len() {
        let mut record = [0u8; WIDE_RECORD_SIZE];
        let kind = match *wide_kinds
            .get(index)
            .ok_or_else(|| format_error("wide kind missing"))?
        {
            0 => WIDE_KIND_LEAF,
            1 => WIDE_KIND_BINARY,
            _ => return Err(format_error("sealed wide kind code invalid")),
        };
        record
            .get_mut(..4)
            .ok_or_else(|| format_error("wide kind field missing"))?
            .copy_from_slice(&kind.to_le_bytes());
        for (stream_id, dest_start) in [(WIDE_A, 8usize), (WIDE_B, 16usize), (WIDE_C, 24usize)] {
            let source_start = index * 8;
            record
                .get_mut(dest_start..dest_start + 8)
                .ok_or_else(|| format_error("wide destination field missing"))?
                .copy_from_slice(
                    streams
                        .get(stream_id)
                        .ok_or_else(|| format_error("wide source stream missing"))?
                        .get(source_start..source_start + 8)
                        .ok_or_else(|| format_error("wide source value missing"))?,
                );
        }
        state.wide_nodes.extend_from_slice(&record);
    }
    let roots = streams
        .get(VERSION_ROOT)
        .ok_or_else(|| format_error("version-root stream missing"))?;
    let parents = streams
        .get(VERSION_PARENT)
        .ok_or_else(|| format_error("version-parent stream missing"))?;
    if roots.len() % 8 != 0 || parents.len() % 4 != 0 || roots.len() / 8 != parents.len() / 4 {
        return Err(format_error("sealed version column widths disagree"));
    }
    for index in 0..roots.len() / 8 {
        let raw_root = read_u64(roots, index * 8)?;
        let raw_parent = read_u32(parents, index * 4)?;
        state.versions.push(if raw_root == NONE_ROOT {
            None
        } else {
            Some(raw_root)
        });
        state.parents.push(if raw_parent == NONE_PARENT {
            None
        } else {
            Some(raw_parent)
        });
    }
    state.threads = decode_string_table(
        streams
            .get(THREAD_OFFSETS)
            .ok_or_else(|| format_error("thread offsets missing"))?,
        streams
            .get(THREAD_BYTES)
            .ok_or_else(|| format_error("thread bytes missing"))?,
    )?;
    for (index, thread) in state.threads.iter().enumerate() {
        state.thread_ordinals.insert(
            thread.clone(),
            u32::try_from(index).map_err(|_| format_error("thread ordinal overflow"))?,
        );
    }
    let checkpoint_ids = decode_string_table(
        streams
            .get(CP_ID_OFFSETS)
            .ok_or_else(|| format_error("checkpoint id offsets missing"))?,
        streams
            .get(CP_ID_BYTES)
            .ok_or_else(|| format_error("checkpoint id bytes missing"))?,
    )?;
    let checkpoint_count = usize::try_from(manifest.checkpoint_count)
        .map_err(|_| format_error("manifest checkpoint count overflow"))?;
    if checkpoint_ids.len() != checkpoint_count {
        return Err(format_error("checkpoint id table count mismatch"));
    }
    let require_width = |stream_id: usize, width: usize| -> Result<(), CheckpointStoreError> {
        let actual = streams
            .get(stream_id)
            .ok_or_else(|| format_error("checkpoint stream missing"))?
            .len();
        let expected = checkpoint_count
            .checked_mul(width)
            .ok_or_else(|| format_error("checkpoint stream expected width overflow"))?;
        if actual != expected {
            Err(format_error("checkpoint stream width mismatch"))
        } else {
            Ok(())
        }
    };
    for stream_id in [
        CP_THREAD,
        CP_NO,
        CP_PARENT_ORDINAL,
        CP_IDENTITY_VERSION,
        CP_MESSAGES_VERSION,
        CP_RESULT_VERSION,
    ] {
        require_width(stream_id, 4)?;
    }
    for stream_id in [CP_LOGICAL_LEN, CP_STATE_HASH] {
        require_width(stream_id, 8)?;
    }
    for index in 0..checkpoint_count {
        let thread_ordinal = read_u32(
            streams
                .get(CP_THREAD)
                .ok_or_else(|| format_error("checkpoint thread stream missing"))?,
            index * 4,
        )?;
        let thread = state
            .threads
            .get(
                usize::try_from(thread_ordinal)
                    .map_err(|_| format_error("thread ordinal overflow"))?,
            )
            .cloned()
            .ok_or_else(|| format_error("checkpoint thread ordinal outside table"))?;
        let parent_ordinal = read_u32(
            streams
                .get(CP_PARENT_ORDINAL)
                .ok_or_else(|| format_error("parent ordinal stream missing"))?,
            index * 4,
        )?;
        let parent_checkpoint_id = if parent_ordinal == NONE_PARENT {
            None
        } else {
            let parent_index = usize::try_from(parent_ordinal)
                .map_err(|_| format_error("parent ordinal overflow"))?;
            if parent_index >= index {
                return Err(format_error("sealed checkpoint parent is not prior"));
            }
            Some(
                checkpoint_ids
                    .get(parent_index)
                    .cloned()
                    .ok_or_else(|| format_error("parent checkpoint id missing"))?,
            )
        };
        let optional = |raw: u32| if raw == NONE_VERSION { None } else { Some(raw) };
        let info = CheckpointInfo {
            ordinal: u32::try_from(index)
                .map_err(|_| format_error("checkpoint ordinal overflow"))?,
            thread_id: thread.clone(),
            checkpoint_no: read_u32(
                streams
                    .get(CP_NO)
                    .ok_or_else(|| format_error("checkpoint number stream missing"))?,
                index * 4,
            )?,
            checkpoint_id: checkpoint_ids
                .get(index)
                .cloned()
                .ok_or_else(|| format_error("checkpoint id missing"))?,
            parent_checkpoint_id,
            identity_version: read_u32(
                streams
                    .get(CP_IDENTITY_VERSION)
                    .ok_or_else(|| format_error("identity version stream missing"))?,
                index * 4,
            )?,
            messages_version: optional(read_u32(
                streams
                    .get(CP_MESSAGES_VERSION)
                    .ok_or_else(|| format_error("messages version stream missing"))?,
                index * 4,
            )?),
            result_version: optional(read_u32(
                streams
                    .get(CP_RESULT_VERSION)
                    .ok_or_else(|| format_error("result version stream missing"))?,
                index * 4,
            )?),
            logical_state_len: read_u64(
                streams
                    .get(CP_LOGICAL_LEN)
                    .ok_or_else(|| format_error("logical length stream missing"))?,
                index * 8,
            )?,
            state_hash: read_u64(
                streams
                    .get(CP_STATE_HASH)
                    .ok_or_else(|| format_error("state hash stream missing"))?,
                index * 8,
            )?,
        };
        state
            .checkpoint_ordinals
            .insert((thread, info.checkpoint_id.clone()), info.ordinal);
        state.checkpoints.push(info);
    }
    if state.versions.len()
        != usize::try_from(manifest.version_count)
            .map_err(|_| format_error("manifest version count overflow"))?
        || state.threads.len()
            != usize::try_from(manifest.thread_count)
                .map_err(|_| format_error("manifest thread count overflow"))?
    {
        return Err(format_error(
            "materialized sealed metadata counts disagree with manifest",
        ));
    }
    validate_materialized_state(&state)?;
    Ok(state)
}

pub(super) fn state_from_lazy_reader(
    dir: &Path,
    manifest: &Manifest,
    reader: &LazyCheckpointStore,
) -> Result<StoreState, CheckpointStoreError> {
    let mut state = StoreState {
        base_geometry: Some(Geometry::from_manifest(manifest)?),
        versions: reader.metadata.versions.clone(),
        parents: reader.metadata.version_parents.clone(),
        threads: reader.metadata.threads.clone(),
        thread_ordinals: reader.metadata.thread_ordinals.clone(),
        checkpoints: reader.metadata.checkpoints.clone(),
        checkpoint_ordinals: reader.metadata.checkpoint_ordinals.clone(),
        ..StoreState::default()
    };
    for route in &manifest.routes {
        for record in route_request_records(&dir.join(&route.file))? {
            if u64::from(record.checkpoint_ordinal) >= manifest.checkpoint_count
                || state
                    .checkpoints
                    .get(
                        usize::try_from(record.checkpoint_ordinal)
                            .map_err(|_| format_error("request checkpoint ordinal overflow"))?,
                    )
                    .is_none()
            {
                return Err(format_error(
                    "request checkpoint ordinal outside lazy state",
                ));
            }
            if state.retired_requests.contains_key(&record.key)
                || state
                    .request_records
                    .insert(record.key.clone(), record)
                    .is_some()
            {
                return Err(format_error("duplicate or retired lazy request id"));
            }
        }
    }
    apply_manifest_lifecycle_metadata(&mut state, manifest)?;
    Ok(state)
}

pub(super) fn decode_string_table(
    offsets: &[u8],
    bytes: &[u8],
) -> Result<Vec<String>, CheckpointStoreError> {
    if offsets.is_empty() {
        if bytes.is_empty() {
            return Ok(Vec::new());
        }
        return Err(format_error("string bytes exist without offsets"));
    }
    if offsets.len() % 4 != 0 || offsets.len() < 4 {
        return Err(format_error("string offset table is not u32 aligned"));
    }
    let count = offsets.len() / 4 - 1;
    let mut result = Vec::with_capacity(count);
    let mut previous = read_u32(offsets, 0)?;
    if previous != 0 {
        return Err(format_error("string offset table does not start at zero"));
    }
    for index in 0..count {
        let next = read_u32(offsets, (index + 1) * 4)?;
        if next < previous {
            return Err(format_error("string offsets regress"));
        }
        let start =
            usize::try_from(previous).map_err(|_| format_error("string offset overflow"))?;
        let end = usize::try_from(next).map_err(|_| format_error("string offset overflow"))?;
        let value = std::str::from_utf8(
            bytes
                .get(start..end)
                .ok_or_else(|| format_error("string bytes outside table"))?,
        )
        .map_err(|_| format_error("string table contains non-UTF8 bytes"))?
        .to_owned();
        result.push(value);
        previous = next;
    }
    if usize::try_from(previous).map_err(|_| format_error("final string offset overflow"))?
        != bytes.len()
    {
        return Err(format_error(
            "final string offset does not match byte table",
        ));
    }
    Ok(result)
}

pub(super) fn validate_materialized_state(state: &StoreState) -> Result<(), CheckpointStoreError> {
    if state.compact_nodes.len() % COMPACT_NODE_SIZE != 0
        || state.wide_nodes.len() % WIDE_RECORD_SIZE != 0
    {
        return Err(format_error(
            "materialized node bytes are not record aligned",
        ));
    }
    let dummy = ParsedTransaction {
        end_offset: 0,
        version_start: 0,
        version_count: u64::try_from(state.versions.len())
            .map_err(|_| format_error("version count overflow"))?,
        checkpoint_count: u64::try_from(state.checkpoints.len())
            .map_err(|_| format_error("checkpoint count overflow"))?,
        byte_start: 0,
        byte_end: u64::try_from(state.arena_bytes.len())
            .map_err(|_| format_error("arena length overflow"))?,
        bytes: state.arena_bytes.clone(),
        node_start: 0,
        node_end: u64::try_from(state.compact_nodes.len() / COMPACT_NODE_SIZE)
            .map_err(|_| format_error("node count overflow"))?,
        compact_nodes: state.compact_nodes.clone(),
        wide_start: 0,
        wide_end: u64::try_from(state.wide_nodes.len() / WIDE_RECORD_SIZE)
            .map_err(|_| format_error("wide count overflow"))?,
        wide_nodes: state.wide_nodes.clone(),
        roots: state.versions.clone(),
        parents: state.parents.clone(),
        checkpoint: state.checkpoints.last().cloned().unwrap_or(CheckpointInfo {
            ordinal: 0,
            thread_id: String::new(),
            checkpoint_no: 0,
            checkpoint_id: String::new(),
            parent_checkpoint_id: None,
            identity_version: 0,
            messages_version: None,
            result_version: None,
            logical_state_len: 0,
            state_hash: 0,
        }),
        request_id: None,
        operation_digest: [0u8; 32],
    };
    let empty = StoreState::default();
    validate_new_nodes(&empty, &dummy)?;
    Ok(())
}

pub(super) struct CompactedState {
    pub(super) state: StoreState,
    pub(super) deleted_checkpoints: Vec<(String, String)>,
    pub(super) retired_requests: Vec<(Vec<u8>, [u8; 32])>,
}

pub(super) struct CompactedGeneration {
    pub(super) finalized: FinalizedSegment,
    pub(super) route_bytes: Vec<u8>,
    pub(super) route_meta: RouteMeta,
}

pub(super) fn append_compacted_leaf(
    source: &StoreState,
    target: &mut StoreState,
    offset: u64,
    length: u64,
) -> Result<u64, CheckpointStoreError> {
    let start =
        usize::try_from(offset).map_err(|_| format_error("source leaf offset exceeds usize"))?;
    let length_usize =
        usize::try_from(length).map_err(|_| format_error("source leaf length exceeds usize"))?;
    let end = start
        .checked_add(length_usize)
        .ok_or_else(|| format_error("source leaf end overflows usize"))?;
    let bytes = source
        .arena_bytes
        .get(start..end)
        .ok_or_else(|| format_error("source leaf lies outside committed payload"))?;
    let new_offset = u64::try_from(target.arena_bytes.len())
        .map_err(|_| format_error("compacted payload exceeds u64"))?;
    target.arena_bytes.extend_from_slice(bytes);
    let new_id = u64::try_from(target.compact_nodes.len() / COMPACT_NODE_SIZE)
        .map_err(|_| format_error("compacted node count exceeds u64"))?;
    let mut slot = [0u8; COMPACT_NODE_SIZE];
    slot[..8].copy_from_slice(&new_offset.to_le_bytes());
    slot[8..12].copy_from_slice(
        &u32::try_from(length)
            .map_err(|_| format_error("compacted leaf length exceeds u32"))?
            .to_le_bytes(),
    );
    slot[12..16].copy_from_slice(&KIND_LEAF.to_le_bytes());
    target.compact_nodes.extend_from_slice(&slot);
    Ok(new_id)
}

pub(super) fn compact_node(
    source: &StoreState,
    target: &mut StoreState,
    node_map: &mut HashMap<u64, u64>,
    old_id: u64,
) -> Result<u64, CheckpointStoreError> {
    if let Some(mapped) = node_map.get(&old_id).copied() {
        return Ok(mapped);
    }
    let decoded = decode_node(source, old_id)?;
    let new_id = match decoded.kind {
        0 => append_compacted_leaf(source, target, decoded.a, decoded.b)?,
        1 => {
            let left = compact_node(source, target, node_map, decoded.a)?;
            let right = compact_node(source, target, node_map, decoded.b)?;
            let candidate = u64::try_from(target.compact_nodes.len() / COMPACT_NODE_SIZE)
                .map_err(|_| format_error("compacted node count exceeds u64"))?;
            let left_delta = candidate
                .checked_sub(left)
                .ok_or_else(|| format_error("compacted left child is not prior"))?;
            let right_delta = candidate
                .checked_sub(right)
                .ok_or_else(|| format_error("compacted right child is not prior"))?;
            let mut slot = [0u8; COMPACT_NODE_SIZE];
            if left_delta > 0
                && right_delta > 0
                && left_delta <= u64::from(u32::MAX)
                && right_delta <= u64::from(u32::MAX)
            {
                slot[..4].copy_from_slice(
                    &u32::try_from(left_delta)
                        .map_err(|_| format_error("left delta exceeds u32"))?
                        .to_le_bytes(),
                );
                slot[4..8].copy_from_slice(
                    &u32::try_from(right_delta)
                        .map_err(|_| format_error("right delta exceeds u32"))?
                        .to_le_bytes(),
                );
                slot[12..16].copy_from_slice(&KIND_BINARY.to_le_bytes());
                target.compact_nodes.extend_from_slice(&slot);
                candidate
            } else {
                let wide_index = u64::try_from(target.wide_nodes.len() / WIDE_RECORD_SIZE)
                    .map_err(|_| format_error("compacted wide-node count exceeds u64"))?;
                slot[..8].copy_from_slice(&wide_index.to_le_bytes());
                slot[12..16].copy_from_slice(&KIND_WIDE.to_le_bytes());
                target.compact_nodes.extend_from_slice(&slot);
                let mut wide = [0u8; WIDE_RECORD_SIZE];
                wide[..4].copy_from_slice(&WIDE_KIND_BINARY.to_le_bytes());
                wide[8..16].copy_from_slice(&left.to_le_bytes());
                wide[16..24].copy_from_slice(&right.to_le_bytes());
                target.wide_nodes.extend_from_slice(&wide);
                candidate
            }
        }
        _ => return Err(format_error("decoded compact node kind is invalid")),
    };
    node_map.insert(old_id, new_id);
    Ok(new_id)
}

pub(super) fn compact_version(
    source: &StoreState,
    target: &mut StoreState,
    node_map: &mut HashMap<u64, u64>,
    version_map: &mut HashMap<u32, u32>,
    old_version: u32,
) -> Result<u32, CheckpointStoreError> {
    if let Some(mapped) = version_map.get(&old_version).copied() {
        return Ok(mapped);
    }
    let old_index =
        usize::try_from(old_version).map_err(|_| format_error("version id exceeds usize"))?;
    let old_parent = source
        .parents
        .get(old_index)
        .copied()
        .ok_or_else(|| format_error("version parent is outside source state"))?;
    let mapped_parent = old_parent.and_then(|parent| version_map.get(&parent).copied());
    let root = source
        .versions
        .get(old_index)
        .copied()
        .flatten()
        .ok_or_else(|| format_error("referenced version has no root"))?;
    let mapped_root = compact_node(source, target, node_map, root)?;
    let new_version = u32::try_from(target.versions.len())
        .map_err(|_| format_error("compacted version count exceeds u32"))?;
    target.versions.push(Some(mapped_root));
    target.parents.push(mapped_parent);
    version_map.insert(old_version, new_version);
    Ok(new_version)
}

pub(super) fn compact_live_state(
    source: &StoreState,
    deleted_keys: &HashSet<(String, String)>,
) -> Result<CompactedState, CheckpointStoreError> {
    let mut target = StoreState {
        deleted_checkpoints: source.deleted_checkpoints.clone(),
        retired_requests: source.retired_requests.clone(),
        ..StoreState::default()
    };
    for key in deleted_keys {
        if source.deleted_checkpoints.contains(key) {
            return Err(format_error(
                "prune set contains an already-deleted checkpoint",
            ));
        }
        target.deleted_checkpoints.insert(key.clone());
    }

    let mut node_map = HashMap::new();
    let mut version_map = HashMap::new();
    let mut old_to_new_checkpoint = HashMap::new();
    for checkpoint in &source.checkpoints {
        let old_key = (
            checkpoint.thread_id.clone(),
            checkpoint.checkpoint_id.clone(),
        );
        if deleted_keys.contains(&old_key) {
            continue;
        }
        if let Some(parent) = checkpoint.parent_checkpoint_id.as_ref() {
            let parent_key = (checkpoint.thread_id.clone(), parent.clone());
            if deleted_keys.contains(&parent_key) {
                return Err(format_error(
                    "prune would leave a live child with a deleted parent",
                ));
            }
        }
        let thread_ordinal =
            if let Some(ordinal) = target.thread_ordinals.get(&checkpoint.thread_id).copied() {
                ordinal
            } else {
                let ordinal = u32::try_from(target.threads.len())
                    .map_err(|_| format_error("compacted thread count exceeds u32"))?;
                target
                    .thread_ordinals
                    .insert(checkpoint.thread_id.clone(), ordinal);
                target.threads.push(checkpoint.thread_id.clone());
                ordinal
            };
        let _ = thread_ordinal;
        let identity_version = compact_version(
            source,
            &mut target,
            &mut node_map,
            &mut version_map,
            checkpoint.identity_version,
        )?;
        let messages_version = checkpoint
            .messages_version
            .map(|version| {
                compact_version(
                    source,
                    &mut target,
                    &mut node_map,
                    &mut version_map,
                    version,
                )
            })
            .transpose()?;
        let result_version = checkpoint
            .result_version
            .map(|version| {
                compact_version(
                    source,
                    &mut target,
                    &mut node_map,
                    &mut version_map,
                    version,
                )
            })
            .transpose()?;
        let ordinal = u32::try_from(target.checkpoints.len())
            .map_err(|_| format_error("compacted checkpoint count exceeds u32"))?;
        let mut info = checkpoint.clone();
        info.ordinal = ordinal;
        info.identity_version = identity_version;
        info.messages_version = messages_version;
        info.result_version = result_version;
        if target
            .checkpoint_ordinals
            .insert(
                (info.thread_id.clone(), info.checkpoint_id.clone()),
                ordinal,
            )
            .is_some()
        {
            return Err(format_error("duplicate live checkpoint during prune"));
        }
        old_to_new_checkpoint.insert(checkpoint.ordinal, ordinal);
        target.checkpoints.push(info);
    }

    for record in source.request_records.values() {
        if let Some(ordinal) = old_to_new_checkpoint
            .get(&record.checkpoint_ordinal)
            .copied()
        {
            target.request_records.insert(
                record.key.clone(),
                RequestRecord {
                    key: record.key.clone(),
                    operation_digest: record.operation_digest,
                    checkpoint_ordinal: ordinal,
                },
            );
        } else if let Some(existing) = target.retired_requests.get(&record.key) {
            if existing != &record.operation_digest {
                return Err(CheckpointStoreError::RequestIdConflict);
            }
        } else {
            target
                .retired_requests
                .insert(record.key.clone(), record.operation_digest);
        }
    }
    validate_materialized_state(&target)?;
    let mut deleted_checkpoints = target
        .deleted_checkpoints
        .iter()
        .cloned()
        .collect::<Vec<_>>();
    deleted_checkpoints.sort();
    let mut retired_requests = target
        .retired_requests
        .iter()
        .map(|(key, digest)| (key.clone(), *digest))
        .collect::<Vec<_>>();
    retired_requests.sort_by(|left, right| left.0.cmp(&right.0));
    Ok(CompactedState {
        state: target,
        deleted_checkpoints,
        retired_requests,
    })
}

pub(super) fn write_compacted_generation(
    dir: &Path,
    generation: u64,
    config: CheckpointStoreConfig,
    compacted: &CompactedState,
) -> Result<CompactedGeneration, CheckpointStoreError> {
    let state = &compacted.state;
    let zero_starts = vec![0u64; STREAM_NAMES.len()];
    let mut writer = StreamingSegmentWriter::new(
        dir,
        generation,
        zero_starts,
        0,
        0,
        config.sealed_block_size,
        config.zstd_level,
    )?;
    writer.push(PAYLOAD, &state.arena_bytes)?;
    for slot in state.compact_nodes.chunks_exact(COMPACT_NODE_SIZE) {
        let code = match read_u32(slot, 12)? {
            KIND_LEAF => 0u8,
            KIND_BINARY => 1u8,
            KIND_WIDE => 2u8,
            _ => return Err(format_error("invalid compact node kind during prune")),
        };
        writer.push(
            NODE_FIELD0,
            slot.get(..8)
                .ok_or_else(|| format_error("prune node field0 missing"))?,
        )?;
        writer.push(
            NODE_FIELD1,
            slot.get(8..12)
                .ok_or_else(|| format_error("prune node field1 missing"))?,
        )?;
        writer.push(NODE_KIND, &[code])?;
    }
    for record in state.wide_nodes.chunks_exact(WIDE_RECORD_SIZE) {
        let code = match read_u32(record, 0)? {
            WIDE_KIND_LEAF => 0u8,
            WIDE_KIND_BINARY => 1u8,
            _ => return Err(format_error("invalid wide node kind during prune")),
        };
        writer.push(WIDE_KIND, &[code])?;
        writer.push(
            WIDE_A,
            record
                .get(8..16)
                .ok_or_else(|| format_error("prune wide field A missing"))?,
        )?;
        writer.push(
            WIDE_B,
            record
                .get(16..24)
                .ok_or_else(|| format_error("prune wide field B missing"))?,
        )?;
        writer.push(
            WIDE_C,
            record
                .get(24..32)
                .ok_or_else(|| format_error("prune wide field C missing"))?,
        )?;
    }
    for (root, parent) in state.versions.iter().zip(&state.parents) {
        writer.push_u64(VERSION_ROOT, root.unwrap_or(NONE_ROOT))?;
        writer.push_u32(VERSION_PARENT, parent.unwrap_or(NONE_PARENT))?;
    }
    if !state.threads.is_empty() {
        writer.push_u32(THREAD_OFFSETS, 0)?;
        for thread in &state.threads {
            writer.push(THREAD_BYTES, thread.as_bytes())?;
            writer.push_u32(
                THREAD_OFFSETS,
                u32::try_from(writer.current_stream_len(THREAD_BYTES)?)
                    .map_err(|_| format_error("prune thread bytes exceed u32"))?,
            )?;
        }
    }
    if !state.checkpoints.is_empty() {
        writer.push_u32(CP_ID_OFFSETS, 0)?;
    }
    let mut route_checkpoints = Vec::with_capacity(state.checkpoints.len());
    for checkpoint in &state.checkpoints {
        let thread_ordinal = state
            .thread_ordinals
            .get(&checkpoint.thread_id)
            .copied()
            .ok_or_else(|| format_error("prune checkpoint thread is missing"))?;
        writer.push_u32(CP_THREAD, thread_ordinal)?;
        writer.push_u32(CP_NO, checkpoint.checkpoint_no)?;
        writer.push(CP_ID_BYTES, checkpoint.checkpoint_id.as_bytes())?;
        writer.push_u32(
            CP_ID_OFFSETS,
            u32::try_from(writer.current_stream_len(CP_ID_BYTES)?)
                .map_err(|_| format_error("prune checkpoint ids exceed u32"))?,
        )?;
        let parent_ordinal = checkpoint
            .parent_checkpoint_id
            .as_ref()
            .map(|parent| {
                state
                    .checkpoint_ordinals
                    .get(&(checkpoint.thread_id.clone(), parent.clone()))
                    .copied()
                    .ok_or_else(|| format_error("prune checkpoint parent is missing"))
            })
            .transpose()?
            .unwrap_or(NONE_PARENT);
        writer.push_u32(CP_PARENT_ORDINAL, parent_ordinal)?;
        writer.push_u32(CP_IDENTITY_VERSION, checkpoint.identity_version)?;
        writer.push_u32(
            CP_MESSAGES_VERSION,
            checkpoint.messages_version.unwrap_or(NONE_VERSION),
        )?;
        writer.push_u32(
            CP_RESULT_VERSION,
            checkpoint.result_version.unwrap_or(NONE_VERSION),
        )?;
        writer.push_u64(CP_LOGICAL_LEN, checkpoint.logical_state_len)?;
        writer.push_u64(CP_STATE_HASH, checkpoint.state_hash)?;
        route_checkpoints.push((
            checkpoint.thread_id.clone(),
            checkpoint.checkpoint_id.clone(),
            checkpoint.ordinal,
        ));
    }
    let finalized = writer.finalize(
        0,
        0,
        u64::try_from(state.checkpoints.len())
            .map_err(|_| format_error("prune checkpoint count overflow"))?,
        u64::try_from(state.versions.len())
            .map_err(|_| format_error("prune version count overflow"))?,
    )?;
    let mut route_threads = state
        .threads
        .iter()
        .enumerate()
        .map(|(ordinal, thread)| {
            Ok::<_, CheckpointStoreError>((
                thread.clone(),
                u32::try_from(ordinal)
                    .map_err(|_| format_error("prune thread ordinal overflow"))?,
            ))
        })
        .collect::<Result<Vec<_>, _>>()?;
    route_threads.sort_by(|left, right| left.0.cmp(&right.0));
    let mut requests = state.request_records.values().cloned().collect::<Vec<_>>();
    requests.sort_by(|left, right| left.key.cmp(&right.key));
    let (route_bytes, route_hash) =
        build_route_file(generation, &route_threads, &route_checkpoints, &requests)?;
    let route_name = format!("route-g{generation:06}.t3r");
    let route_meta = RouteMeta {
        generation,
        file: route_name,
        thread_entry_count: u64::try_from(route_threads.len())
            .map_err(|_| format_error("prune route thread count overflow"))?,
        checkpoint_entry_count: u64::try_from(route_checkpoints.len())
            .map_err(|_| format_error("prune route checkpoint count overflow"))?,
        route_file_bytes: u64::try_from(route_bytes.len())
            .map_err(|_| format_error("prune route file length overflow"))?,
        route_index_xxh3_64: route_hash,
    };
    Ok(CompactedGeneration {
        finalized,
        route_bytes,
        route_meta,
    })
}
