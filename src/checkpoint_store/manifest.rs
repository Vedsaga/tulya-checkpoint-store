use super::*;

pub(super) fn build_route_file(
    generation: u64,
    new_threads: &[(String, u32)],
    new_checkpoints: &[(String, String, u32)],
    new_requests: &[RequestRecord],
) -> Result<(Vec<u8>, u64), CheckpointStoreError> {
    #[derive(Clone)]
    struct RouteEntry {
        hash: u64,
        ordinal: u32,
        key: Vec<u8>,
    }
    fn route_key(thread: &str, checkpoint: Option<&str>) -> Result<Vec<u8>, CheckpointStoreError> {
        let thread_len = u32::try_from(thread.len())
            .map_err(|_| format_error("route thread length overflow"))?;
        let checkpoint_len = checkpoint
            .map(|value| {
                u32::try_from(value.len())
                    .map_err(|_| format_error("route checkpoint length overflow"))
            })
            .transpose()?
            .unwrap_or(0);
        let mut key = Vec::new();
        key.push(if checkpoint.is_some() { 1 } else { 0 });
        key.extend_from_slice(&thread_len.to_le_bytes());
        key.extend_from_slice(thread.as_bytes());
        key.extend_from_slice(&checkpoint_len.to_le_bytes());
        if let Some(checkpoint) = checkpoint {
            key.extend_from_slice(checkpoint.as_bytes());
        }
        Ok(key)
    }
    let mut threads = Vec::new();
    for (thread, ordinal) in new_threads {
        let key = route_key(thread, None)?;
        threads.push(RouteEntry {
            hash: xxh3_64(&key),
            ordinal: *ordinal,
            key,
        });
    }
    let mut checkpoints = Vec::new();
    for (thread, checkpoint, ordinal) in new_checkpoints {
        let key = route_key(thread, Some(checkpoint))?;
        checkpoints.push(RouteEntry {
            hash: xxh3_64(&key),
            ordinal: *ordinal,
            key,
        });
    }
    threads.sort_by(|left, right| {
        left.hash
            .cmp(&right.hash)
            .then_with(|| left.key.cmp(&right.key))
    });
    checkpoints.sort_by(|left, right| {
        left.hash
            .cmp(&right.hash)
            .then_with(|| left.key.cmp(&right.key))
    });
    let thread_table_offset =
        u64::try_from(ROUTE_HEADER_SIZE).map_err(|_| format_error("route header size overflow"))?;
    let thread_table_bytes = threads
        .len()
        .checked_mul(ROUTE_ENTRY_SIZE)
        .ok_or_else(|| format_error("route thread table overflow"))?;
    let checkpoint_table_bytes = checkpoints
        .len()
        .checked_mul(ROUTE_ENTRY_SIZE)
        .ok_or_else(|| format_error("route checkpoint table overflow"))?;
    let checkpoint_table_offset = thread_table_offset
        .checked_add(
            u64::try_from(thread_table_bytes)
                .map_err(|_| format_error("route thread table size overflow"))?,
        )
        .ok_or_else(|| format_error("route checkpoint offset overflow"))?;
    let key_blob_offset = checkpoint_table_offset
        .checked_add(
            u64::try_from(checkpoint_table_bytes)
                .map_err(|_| format_error("route checkpoint table size overflow"))?,
        )
        .ok_or_else(|| format_error("route key blob offset overflow"))?;
    let mut table = Vec::new();
    let mut blob = Vec::new();
    for entry in threads.iter().chain(checkpoints.iter()) {
        let key_offset =
            u32::try_from(blob.len()).map_err(|_| format_error("route key offset overflow"))?;
        let key_len = u32::try_from(entry.key.len())
            .map_err(|_| format_error("route key length overflow"))?;
        blob.extend_from_slice(&entry.key);
        table.extend_from_slice(&entry.hash.to_le_bytes());
        table.extend_from_slice(&entry.ordinal.to_le_bytes());
        table.extend_from_slice(&key_offset.to_le_bytes());
        table.extend_from_slice(&key_len.to_le_bytes());
        table.extend_from_slice(&0u32.to_le_bytes());
    }
    let mut tail = table;
    tail.extend_from_slice(&blob);
    tail.extend_from_slice(&request_section_bytes(new_requests)?);
    let index_hash = if tail.is_empty() { 0 } else { xxh3_64(&tail) };
    let mut out = Vec::new();
    out.extend_from_slice(ROUTE_MAGIC);
    out.extend_from_slice(&ROUTE_FORMAT_VERSION.to_le_bytes());
    out.extend_from_slice(&0u32.to_le_bytes());
    out.extend_from_slice(&generation.to_le_bytes());
    out.extend_from_slice(
        &u32::try_from(threads.len())
            .map_err(|_| format_error("route thread count overflow"))?
            .to_le_bytes(),
    );
    out.extend_from_slice(
        &u32::try_from(checkpoints.len())
            .map_err(|_| format_error("route checkpoint count overflow"))?
            .to_le_bytes(),
    );
    out.extend_from_slice(&thread_table_offset.to_le_bytes());
    out.extend_from_slice(&checkpoint_table_offset.to_le_bytes());
    out.extend_from_slice(&key_blob_offset.to_le_bytes());
    out.extend_from_slice(&index_hash.to_le_bytes());
    if out.len() != ROUTE_HEADER_SIZE {
        return Err(format_error("route header width mismatch"));
    }
    out.extend_from_slice(&tail);
    Ok((out, index_hash))
}

pub(super) fn request_section_bytes(
    records: &[RequestRecord],
) -> Result<Vec<u8>, CheckpointStoreError> {
    if records.is_empty() {
        return Ok(Vec::new());
    }
    let mut section = Vec::new();
    section.extend_from_slice(REQUEST_SECTION_MAGIC);
    section.extend_from_slice(&REQUEST_SECTION_VERSION.to_le_bytes());
    section.extend_from_slice(&0u32.to_le_bytes());
    section.extend_from_slice(
        &u32::try_from(records.len())
            .map_err(|_| format_error("request section record count exceeds u32"))?
            .to_le_bytes(),
    );
    for record in records {
        validate_request_id(&record.key)?;
        section.extend_from_slice(
            &u32::try_from(record.key.len())
                .map_err(|_| format_error("request key length exceeds u32"))?
                .to_le_bytes(),
        );
        section.extend_from_slice(&record.checkpoint_ordinal.to_le_bytes());
        section.extend_from_slice(&record.operation_digest);
        section.extend_from_slice(&record.key);
    }
    let section_bytes = u64::try_from(section.len())
        .map_err(|_| format_error("request section length exceeds u64"))?;
    section.extend_from_slice(&section_bytes.to_le_bytes());
    section.extend_from_slice(REQUEST_FOOTER_MAGIC);
    Ok(section)
}

pub(super) fn route_request_records(
    path: &Path,
) -> Result<Vec<RequestRecord>, CheckpointStoreError> {
    let bytes = fs::read(path)?;
    if bytes.len() < ROUTE_HEADER_SIZE {
        return Err(format_error("route file is shorter than its header"));
    }
    let thread_count = usize::try_from(read_u32(&bytes, 24)?)
        .map_err(|_| format_error("route thread count overflow"))?;
    let checkpoint_count = usize::try_from(read_u32(&bytes, 28)?)
        .map_err(|_| format_error("route checkpoint count overflow"))?;
    let thread_table_offset = usize::try_from(read_u64(&bytes, 32)?)
        .map_err(|_| format_error("route thread table offset overflow"))?;
    let checkpoint_table_offset = usize::try_from(read_u64(&bytes, 40)?)
        .map_err(|_| format_error("route checkpoint table offset overflow"))?;
    let key_blob_offset = usize::try_from(read_u64(&bytes, 48)?)
        .map_err(|_| format_error("route key blob offset overflow"))?;
    let entry_bytes = ROUTE_ENTRY_SIZE;
    let mut key_blob_len = 0usize;
    for (table_offset, count) in [
        (thread_table_offset, thread_count),
        (checkpoint_table_offset, checkpoint_count),
    ] {
        for index in 0..count {
            let base = table_offset
                .checked_add(
                    index
                        .checked_mul(entry_bytes)
                        .ok_or_else(|| format_error("route entry offset overflow"))?,
                )
                .ok_or_else(|| format_error("route entry offset overflow"))?;
            let key_offset = usize::try_from(read_u32(&bytes, base + 12)?)
                .map_err(|_| format_error("route key offset overflow"))?;
            let key_len = usize::try_from(read_u32(&bytes, base + 16)?)
                .map_err(|_| format_error("route key length overflow"))?;
            key_blob_len = key_blob_len.max(
                key_offset
                    .checked_add(key_len)
                    .ok_or_else(|| format_error("route key end overflow"))?,
            );
        }
    }
    let key_blob_end = key_blob_offset
        .checked_add(key_blob_len)
        .ok_or_else(|| format_error("route key blob end overflow"))?;
    if key_blob_end > bytes.len() {
        return Err(format_error("route key blob exceeds file"));
    }
    if bytes.len() < REQUEST_SECTION_FOOTER_BYTES {
        return Ok(Vec::new());
    }
    let footer_start = bytes.len() - REQUEST_SECTION_FOOTER_BYTES;
    if bytes.get(footer_start + 8..footer_start + 16) != Some(REQUEST_FOOTER_MAGIC.as_slice()) {
        return Ok(Vec::new());
    }
    let section_bytes = usize::try_from(read_u64(&bytes, footer_start)?)
        .map_err(|_| format_error("request section length overflow"))?;
    let section_start = footer_start
        .checked_sub(section_bytes)
        .ok_or_else(|| format_error("request section starts before file"))?;
    if section_start != key_blob_end {
        return Err(format_error(
            "request section is not contiguous with route keys",
        ));
    }
    if section_bytes < REQUEST_SECTION_HEADER_BYTES
        || read_u64(&bytes, footer_start)?
            != u64::try_from(footer_start - section_start)
                .map_err(|_| format_error("request section size overflow"))?
        || bytes.get(section_start..section_start + 8) != Some(REQUEST_SECTION_MAGIC.as_slice())
        || read_u32(&bytes, section_start + 8)? != REQUEST_SECTION_VERSION
        || read_u32(&bytes, section_start + 12)? != 0
    {
        return Err(format_error("request section header is invalid"));
    }
    let record_count = usize::try_from(read_u32(&bytes, section_start + 16)?)
        .map_err(|_| format_error("request section record count overflow"))?;
    let section_end = section_start
        .checked_add(section_bytes)
        .ok_or_else(|| format_error("request section end overflow"))?;
    let mut cursor = section_start + REQUEST_SECTION_HEADER_BYTES;
    let mut records = Vec::new();
    for _ in 0..record_count {
        let key_len = usize::try_from(read_u32(&bytes, cursor)?)
            .map_err(|_| format_error("request key length overflow"))?;
        let checkpoint_ordinal = read_u32(&bytes, cursor + 4)?;
        let digest_start = cursor
            .checked_add(8)
            .ok_or_else(|| format_error("request digest offset overflow"))?;
        let digest_end = digest_start
            .checked_add(32)
            .ok_or_else(|| format_error("request digest end overflow"))?;
        let key_start = digest_end;
        let key_end = key_start
            .checked_add(key_len)
            .ok_or_else(|| format_error("request key end overflow"))?;
        if key_end > section_end {
            return Err(format_error("request record exceeds section"));
        }
        let key = bytes
            .get(key_start..key_end)
            .ok_or_else(|| format_error("request key outside section"))?
            .to_vec();
        validate_request_id(&key)?;
        let operation_digest: [u8; 32] = bytes
            .get(digest_start..digest_end)
            .ok_or_else(|| format_error("request digest outside section"))?
            .try_into()
            .map_err(|_| format_error("request digest width mismatch"))?;
        records.push(RequestRecord {
            key,
            operation_digest,
            checkpoint_ordinal,
        });
        cursor = key_end;
    }
    if cursor != section_end {
        return Err(format_error("request section has trailing record bytes"));
    }
    Ok(records)
}

pub(super) fn segment_to_value(meta: &SegmentMeta) -> Value {
    json!({
        "generation": meta.generation,
        "file": meta.file,
        "wal_start_bytes": meta.wal_start_bytes,
        "wal_end_bytes": meta.wal_end_bytes,
        "checkpoint_start_count": meta.checkpoint_start_count,
        "checkpoint_end_count": meta.checkpoint_end_count,
        "version_start_count": meta.version_start_count,
        "version_end_count": meta.version_end_count,
        "raw_stream_bytes": meta.raw_stream_bytes,
        "stream_starts": meta.stream_starts,
        "stream_ends": meta.stream_ends,
        "segment_file_bytes": meta.segment_file_bytes,
        "segment_file_xxh3_64": meta.segment_file_xxh3_64,
        "index_bytes": meta.index_bytes,
        "index_xxh3_64": meta.index_xxh3_64,
        "block_size": meta.block_size,
        "block_count": meta.block_count,
        "codec": "zstd",
        "zstd_level": meta.zstd_level
    })
}

pub(super) fn route_to_value(meta: &RouteMeta) -> Value {
    json!({
        "generation": meta.generation,
        "file": meta.file,
        "thread_entry_count": meta.thread_entry_count,
        "checkpoint_entry_count": meta.checkpoint_entry_count,
        "route_file_bytes": meta.route_file_bytes,
        "route_index_xxh3_64": meta.route_index_xxh3_64
    })
}

pub(super) fn tombstone_to_value(tombstone: &CheckpointTombstone) -> Value {
    json!({
        "thread_id": tombstone.thread_id,
        "checkpoint_id": tombstone.checkpoint_id,
    })
}

pub(super) fn tombstone_from_value(
    value: &Value,
) -> Result<CheckpointTombstone, CheckpointStoreError> {
    let thread_id = value
        .get("thread_id")
        .and_then(Value::as_str)
        .ok_or_else(|| format_error("checkpoint tombstone thread id is missing"))?
        .to_owned();
    let checkpoint_id = value
        .get("checkpoint_id")
        .and_then(Value::as_str)
        .ok_or_else(|| format_error("checkpoint tombstone id is missing"))?
        .to_owned();
    validate_checkpoint_identifier(&thread_id, "thread id")?;
    validate_checkpoint_identifier(&checkpoint_id, "checkpoint id")?;
    Ok(CheckpointTombstone {
        thread_id,
        checkpoint_id,
    })
}

pub(super) fn byte_array_from_value(
    value: &Value,
    key: &str,
) -> Result<Vec<u8>, CheckpointStoreError> {
    value
        .get(key)
        .and_then(Value::as_array)
        .ok_or_else(|| format_error(format!("retired request {key} is missing")))?
        .iter()
        .map(|item| {
            let value = item
                .as_u64()
                .ok_or_else(|| format_error(format!("retired request {key} contains non-byte")))?;
            u8::try_from(value)
                .map_err(|_| format_error(format!("retired request {key} byte overflows u8")))
        })
        .collect()
}

pub(super) fn retired_request_to_value(record: &RetiredRequestRecord) -> Value {
    json!({
        "key": record.key,
        "operation_digest": record.operation_digest,
    })
}

pub(super) fn retired_request_from_value(
    value: &Value,
) -> Result<RetiredRequestRecord, CheckpointStoreError> {
    let key = byte_array_from_value(value, "key")?;
    validate_request_id(&key)?;
    let digest = byte_array_from_value(value, "operation_digest")?;
    let operation_digest: [u8; 32] = digest
        .try_into()
        .map_err(|_| format_error("retired request digest must contain 32 bytes"))?;
    Ok(RetiredRequestRecord {
        key,
        operation_digest,
    })
}

pub(super) fn manifest_bytes(manifest: &Manifest) -> Result<Vec<u8>, CheckpointStoreError> {
    let store_id = manifest
        .store_id
        .ok_or_else(|| format_error("public manifest requires a StoreId"))?;
    let value = json!({
        "format": MANIFEST_FORMAT,
        "format_version": MANIFEST_FORMAT_VERSION,
        "store_id": store_id.to_hex(),
        "generation": manifest.generation,
        "sealed_end_wal_bytes": manifest.sealed_end_wal_bytes,
        "checkpoint_count": manifest.checkpoint_count,
        "version_count": manifest.version_count,
        "stream_names": STREAM_NAMES,
        "stream_sizes": manifest.stream_sizes,
        "stream_prefix_xxh3_64": vec![0u64; STREAM_NAMES.len()],
        "stream_prefix_hashes_authoritative": false,
        "segments": manifest.segments.iter().map(segment_to_value).collect::<Vec<_>>(),
        "thread_count": manifest.thread_count,
        "routes": manifest.routes.iter().map(route_to_value).collect::<Vec<_>>(),
        "deleted_checkpoints": manifest.deleted_checkpoints.iter().map(tombstone_to_value).collect::<Vec<_>>(),
        "retired_requests": manifest.retired_requests.iter().map(retired_request_to_value).collect::<Vec<_>>(),
        "payload": "incremental suffixes of the canonical 22-stream materialization",
        "codec": "zstd",
        "format_variable_geometry_capable": true,
        "segment_file_hash_authoritative": false,
        "routing": "immutable segment-local route indexes"
    });
    let mut bytes = serde_json::to_vec_pretty(&value)?;
    bytes.push(b'\n');
    Ok(bytes)
}

pub(super) fn ensure_format_v1_manifest(
    dir: &Path,
    mut manifest: Manifest,
) -> Result<Manifest, CheckpointStoreError> {
    let path = dir.join(MANIFEST_FILE);
    if path.exists() {
        let persisted: Value = serde_json::from_slice(&fs::read(&path)?)?;
        let already_v1 = persisted.get("format").and_then(Value::as_str) == Some(MANIFEST_FORMAT)
            && persisted.get("format_version").and_then(Value::as_u64)
                == Some(MANIFEST_FORMAT_VERSION)
            && manifest.store_id.is_some();
        if already_v1 {
            return Ok(manifest);
        }
    }
    if manifest.store_id.is_none() {
        manifest.store_id = Some(StoreId::generate()?);
    }
    staged_write_new(&path, &manifest_bytes(&manifest)?)?;
    Ok(manifest)
}

pub(super) fn required_u64(value: &Value, key: &str) -> Result<u64, CheckpointStoreError> {
    value
        .get(key)
        .and_then(Value::as_u64)
        .ok_or_else(|| format_error(format!("manifest missing/invalid {key}")))
}

pub(super) fn required_u64_array(
    value: &Value,
    key: &str,
) -> Result<Vec<u64>, CheckpointStoreError> {
    value
        .get(key)
        .and_then(Value::as_array)
        .ok_or_else(|| format_error(format!("manifest missing/invalid {key}")))?
        .iter()
        .map(|item| {
            item.as_u64()
                .ok_or_else(|| format_error(format!("non-u64 in manifest {key}")))
        })
        .collect()
}

pub(super) fn segment_from_value(value: &Value) -> Result<SegmentMeta, CheckpointStoreError> {
    Ok(SegmentMeta {
        generation: required_u64(value, "generation")?,
        file: value
            .get("file")
            .and_then(Value::as_str)
            .ok_or_else(|| format_error("segment missing file"))?
            .to_owned(),
        wal_start_bytes: required_u64(value, "wal_start_bytes")?,
        wal_end_bytes: required_u64(value, "wal_end_bytes")?,
        checkpoint_start_count: required_u64(value, "checkpoint_start_count")?,
        checkpoint_end_count: required_u64(value, "checkpoint_end_count")?,
        version_start_count: required_u64(value, "version_start_count")?,
        version_end_count: required_u64(value, "version_end_count")?,
        raw_stream_bytes: required_u64(value, "raw_stream_bytes")?,
        stream_starts: required_u64_array(value, "stream_starts")?,
        stream_ends: required_u64_array(value, "stream_ends")?,
        segment_file_bytes: required_u64(value, "segment_file_bytes")?,
        segment_file_xxh3_64: required_u64(value, "segment_file_xxh3_64")?,
        index_bytes: required_u64(value, "index_bytes")?,
        index_xxh3_64: required_u64(value, "index_xxh3_64")?,
        block_size: u32::try_from(required_u64(value, "block_size")?)
            .map_err(|_| format_error("segment block size overflow"))?,
        block_count: u32::try_from(required_u64(value, "block_count")?)
            .map_err(|_| format_error("segment block count overflow"))?,
        zstd_level: i32::try_from(
            value
                .get("zstd_level")
                .and_then(Value::as_i64)
                .ok_or_else(|| format_error("segment missing zstd level"))?,
        )
        .map_err(|_| format_error("segment zstd level overflow"))?,
    })
}

pub(super) fn route_from_value(value: &Value) -> Result<RouteMeta, CheckpointStoreError> {
    Ok(RouteMeta {
        generation: required_u64(value, "generation")?,
        file: value
            .get("file")
            .and_then(Value::as_str)
            .ok_or_else(|| format_error("route missing file"))?
            .to_owned(),
        thread_entry_count: required_u64(value, "thread_entry_count")?,
        checkpoint_entry_count: required_u64(value, "checkpoint_entry_count")?,
        route_file_bytes: required_u64(value, "route_file_bytes")?,
        route_index_xxh3_64: required_u64(value, "route_index_xxh3_64")?,
    })
}

pub(super) fn load_manifest(dir: &Path) -> Result<Manifest, CheckpointStoreError> {
    let path = dir.join(MANIFEST_FILE);
    if !path.exists() {
        return Ok(Manifest::default());
    }
    let value: Value = serde_json::from_slice(&fs::read(&path)?)?;
    let format = value
        .get("format")
        .and_then(Value::as_str)
        .ok_or_else(|| format_error("manifest format is missing"))?;
    let format_version = value
        .get("format_version")
        .and_then(Value::as_u64)
        .ok_or_else(|| format_error("manifest format version is missing"))?;
    let pre_release = format == PRE_RELEASE_MANIFEST_FORMAT;
    if format != MANIFEST_FORMAT && !pre_release {
        return Err(format_error("checkpoint-store manifest format mismatch"));
    }
    let store_id = match (pre_release, format_version) {
        (false, MANIFEST_FORMAT_VERSION) => {
            let encoded = value
                .get("store_id")
                .and_then(Value::as_str)
                .ok_or_else(|| format_error("public-format StoreId is missing"))?;
            Some(StoreId::from_hex(encoded)?)
        }
        (true, PRE_RELEASE_REVISION_INITIAL) => {
            if value.get("store_id").is_some_and(|value| !value.is_null()) {
                return Err(format_error(
                    "pre-release manifest contains StoreId before its revision",
                ));
            }
            None
        }
        (true, PRE_RELEASE_REVISION_STORE_ID | PRE_RELEASE_REVISION_PRUNE) => {
            let encoded = value
                .get("store_id")
                .and_then(Value::as_str)
                .ok_or_else(|| format_error("pre-release StoreId is missing"))?;
            Some(StoreId::from_hex(encoded)?)
        }
        _ => {
            return Err(format_error(
                "unsupported checkpoint-store manifest version",
            ))
        }
    };
    let segments = value
        .get("segments")
        .and_then(Value::as_array)
        .ok_or_else(|| format_error("manifest missing segments"))?
        .iter()
        .map(segment_from_value)
        .collect::<Result<Vec<_>, _>>()?;
    let routes = value
        .get("routes")
        .and_then(Value::as_array)
        .ok_or_else(|| format_error("manifest missing routes"))?
        .iter()
        .map(route_from_value)
        .collect::<Result<Vec<_>, _>>()?;
    let has_prune_metadata = !pre_release || format_version >= PRE_RELEASE_REVISION_PRUNE;
    let deleted_checkpoints = if has_prune_metadata {
        value
            .get("deleted_checkpoints")
            .and_then(Value::as_array)
            .ok_or_else(|| format_error("manifest missing deleted checkpoints"))?
            .iter()
            .map(tombstone_from_value)
            .collect::<Result<Vec<_>, _>>()?
    } else {
        Vec::new()
    };
    let retired_requests = if has_prune_metadata {
        value
            .get("retired_requests")
            .and_then(Value::as_array)
            .ok_or_else(|| format_error("manifest missing retired requests"))?
            .iter()
            .map(retired_request_from_value)
            .collect::<Result<Vec<_>, _>>()?
    } else {
        Vec::new()
    };
    let manifest = Manifest {
        generation: required_u64(&value, "generation")?,
        sealed_end_wal_bytes: required_u64(&value, "sealed_end_wal_bytes")?,
        checkpoint_count: required_u64(&value, "checkpoint_count")?,
        version_count: required_u64(&value, "version_count")?,
        thread_count: value
            .get("thread_count")
            .and_then(Value::as_u64)
            .unwrap_or(0),
        stream_sizes: required_u64_array(&value, "stream_sizes")?,
        segments,
        routes,
        store_id,
        deleted_checkpoints,
        retired_requests,
    };
    validate_manifest(dir, &manifest)?;
    Ok(manifest)
}

pub(super) fn validate_route_file(
    dir: &Path,
    route: &RouteMeta,
) -> Result<(), CheckpointStoreError> {
    let bytes = fs::read(dir.join(&route.file))?;
    if bytes.len() < ROUTE_HEADER_SIZE {
        return Err(format_error("route header truncated"));
    }
    if bytes.get(..ROUTE_MAGIC.len()) != Some(ROUTE_MAGIC.as_slice()) {
        return Err(format_error("route magic mismatch"));
    }
    if read_u32(&bytes, 8)? != ROUTE_FORMAT_VERSION {
        return Err(format_error("route format version mismatch"));
    }
    if read_u32(&bytes, 12)? != 0 {
        return Err(format_error("route reserved header field is nonzero"));
    }
    if read_u64(&bytes, 16)? != route.generation {
        return Err(format_error("route generation mismatch"));
    }
    if u64::from(read_u32(&bytes, 24)?) != route.thread_entry_count
        || u64::from(read_u32(&bytes, 28)?) != route.checkpoint_entry_count
    {
        return Err(format_error("route entry counts mismatch manifest"));
    }

    let thread_table_offset = read_u64(&bytes, 32)?;
    let checkpoint_table_offset = read_u64(&bytes, 40)?;
    let key_blob_offset = read_u64(&bytes, 48)?;
    let stored_index_hash = read_u64(&bytes, 56)?;
    let header_bytes =
        u64::try_from(ROUTE_HEADER_SIZE).map_err(|_| format_error("route header size overflow"))?;
    let entry_bytes =
        u64::try_from(ROUTE_ENTRY_SIZE).map_err(|_| format_error("route entry size overflow"))?;
    let expected_checkpoint_table_offset = header_bytes
        .checked_add(
            route
                .thread_entry_count
                .checked_mul(entry_bytes)
                .ok_or_else(|| format_error("route thread table size overflow"))?,
        )
        .ok_or_else(|| format_error("route checkpoint table offset overflow"))?;
    let expected_key_blob_offset = expected_checkpoint_table_offset
        .checked_add(
            route
                .checkpoint_entry_count
                .checked_mul(entry_bytes)
                .ok_or_else(|| format_error("route checkpoint table size overflow"))?,
        )
        .ok_or_else(|| format_error("route key blob offset overflow"))?;
    let file_len =
        u64::try_from(bytes.len()).map_err(|_| format_error("route file length overflow"))?;

    if thread_table_offset != header_bytes
        || checkpoint_table_offset != expected_checkpoint_table_offset
        || key_blob_offset != expected_key_blob_offset
        || key_blob_offset > file_len
    {
        return Err(format_error(
            "route table offsets mismatch deterministic layout",
        ));
    }

    let tail = bytes
        .get(ROUTE_HEADER_SIZE..)
        .ok_or_else(|| format_error("route tail outside file"))?;
    let actual_index_hash = if tail.is_empty() { 0 } else { xxh3_64(tail) };
    if stored_index_hash != route.route_index_xxh3_64 || actual_index_hash != stored_index_hash {
        return Err(format_error("route index checksum mismatch"));
    }
    Ok(())
}

pub(super) fn validate_manifest(
    dir: &Path,
    manifest: &Manifest,
) -> Result<(), CheckpointStoreError> {
    if manifest.stream_sizes.len() != STREAM_NAMES.len()
        || manifest.segments.len() != manifest.routes.len()
    {
        return Err(format_error(
            "manifest stream or segment/route width mismatch",
        ));
    }
    let mut expected_streams = vec![0u64; STREAM_NAMES.len()];
    let mut wal = 0u64;
    let mut checkpoints = 0u64;
    let mut versions = 0u64;
    let mut generation = 0u64;
    for (segment, route) in manifest.segments.iter().zip(manifest.routes.iter()) {
        if segment.generation <= generation || segment.generation != route.generation {
            return Err(format_error("manifest generation ordering mismatch"));
        }
        if segment.wal_start_bytes != wal
            || segment.checkpoint_start_count != checkpoints
            || segment.version_start_count != versions
            || segment.stream_starts != expected_streams
            || segment.stream_ends.len() != STREAM_NAMES.len()
        {
            return Err(format_error("manifest authority ranges are not contiguous"));
        }
        if fs::metadata(dir.join(&segment.file))?.len() != segment.segment_file_bytes
            || fs::metadata(dir.join(&route.file))?.len() != route.route_file_bytes
        {
            return Err(format_error("manifest referenced file size mismatch"));
        }
        validate_route_file(dir, route)?;
        expected_streams = segment.stream_ends.clone();
        wal = segment.wal_end_bytes;
        checkpoints = segment.checkpoint_end_count;
        versions = segment.version_end_count;
        generation = segment.generation;
    }
    let route_checkpoints = manifest.routes.iter().try_fold(0u64, |acc, route| {
        acc.checked_add(route.checkpoint_entry_count)
            .ok_or_else(|| format_error("route checkpoint total overflow"))
    })?;
    let route_threads = manifest.routes.iter().try_fold(0u64, |acc, route| {
        acc.checked_add(route.thread_entry_count)
            .ok_or_else(|| format_error("route thread total overflow"))
    })?;
    if manifest.generation != generation
        || manifest.sealed_end_wal_bytes != wal
        || manifest.checkpoint_count != checkpoints
        || manifest.version_count != versions
        || manifest.stream_sizes != expected_streams
        || manifest.checkpoint_count != route_checkpoints
        || manifest.thread_count != route_threads
    {
        return Err(format_error("manifest top-level authority mismatch"));
    }
    Ok(())
}
