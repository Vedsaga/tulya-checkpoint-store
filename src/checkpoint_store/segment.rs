use super::*;

#[derive(Debug, Clone, Copy)]
pub(super) struct SegmentHeader {
    pub(super) generation: u64,
    pub(super) checkpoint_start_count: u64,
    pub(super) checkpoint_end_count: u64,
    pub(super) version_start_count: u64,
    pub(super) version_end_count: u64,
    pub(super) block_size: u32,
    pub(super) block_count: u32,
    pub(super) payload_offset: u64,
    pub(super) payload_bytes: u64,
    pub(super) index_offset: u64,
    pub(super) stream_table_bytes: u64,
    pub(super) block_table_bytes: u64,
    pub(super) index_xxh3_64: u64,
    pub(super) header_xxh3_64: u64,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct StreamEntry {
    pub(super) global_start: u64,
    pub(super) global_end: u64,
    pub(super) raw_len: u64,
    pub(super) first_block: u32,
    pub(super) block_count: u32,
    pub(super) raw_xxh3_64: u64,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct BlockEntry {
    pub(super) stream_id: u32,
    pub(super) raw_offset: u64,
    pub(super) encoded_offset: u64,
    pub(super) raw_len: u32,
    pub(super) encoded_len: u32,
    pub(super) raw_xxh3_64: u64,
}

#[derive(Debug, Clone)]
pub(super) struct SegmentMeta {
    pub(super) generation: u64,
    pub(super) file: String,
    pub(super) wal_start_bytes: u64,
    pub(super) wal_end_bytes: u64,
    pub(super) checkpoint_start_count: u64,
    pub(super) checkpoint_end_count: u64,
    pub(super) version_start_count: u64,
    pub(super) version_end_count: u64,
    pub(super) raw_stream_bytes: u64,
    pub(super) stream_starts: Vec<u64>,
    pub(super) stream_ends: Vec<u64>,
    pub(super) segment_file_bytes: u64,
    pub(super) segment_file_xxh3_64: u64,
    pub(super) index_bytes: u64,
    pub(super) index_xxh3_64: u64,
    pub(super) block_size: u32,
    pub(super) block_count: u32,
    pub(super) zstd_level: i32,
}

#[derive(Debug, Clone)]
pub(super) struct RouteMeta {
    pub(super) generation: u64,
    pub(super) file: String,
    pub(super) thread_entry_count: u64,
    pub(super) checkpoint_entry_count: u64,
    pub(super) route_file_bytes: u64,
    pub(super) route_index_xxh3_64: u64,
}

#[derive(Debug, Clone)]
pub(super) struct Manifest {
    pub(super) generation: u64,
    pub(super) sealed_end_wal_bytes: u64,
    pub(super) checkpoint_count: u64,
    pub(super) version_count: u64,
    pub(super) thread_count: u64,
    pub(super) stream_sizes: Vec<u64>,
    pub(super) segments: Vec<SegmentMeta>,
    pub(super) routes: Vec<RouteMeta>,
    pub(super) store_id: Option<StoreId>,
    pub(super) deleted_checkpoints: Vec<CheckpointTombstone>,
    pub(super) retired_requests: Vec<RetiredRequestRecord>,
}

impl Default for Manifest {
    fn default() -> Self {
        Self {
            generation: 0,
            sealed_end_wal_bytes: 0,
            checkpoint_count: 0,
            version_count: 0,
            thread_count: 0,
            stream_sizes: vec![0; STREAM_NAMES.len()],
            segments: Vec::new(),
            routes: Vec::new(),
            store_id: None,
            deleted_checkpoints: Vec::new(),
            retired_requests: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) struct LocalBlock {
    pub(super) raw_offset: u64,
    pub(super) encoded_offset: u64,
    pub(super) raw_len: u32,
    pub(super) encoded_len: u32,
    pub(super) raw_xxh3_64: u64,
}

pub(super) struct StreamState {
    pub(super) raw_buf: Vec<u8>,
    pub(super) raw_len: u64,
    pub(super) flushed_raw_len: u64,
    pub(super) blocks: Vec<LocalBlock>,
    pub(super) hasher: Xxh3,
}

impl StreamState {
    pub(super) fn new(block_size: usize) -> Self {
        Self {
            raw_buf: Vec::with_capacity(block_size),
            raw_len: 0,
            flushed_raw_len: 0,
            blocks: Vec::new(),
            hasher: Xxh3::new(),
        }
    }

    pub(super) fn raw_hash(&self) -> u64 {
        if self.raw_len == 0 {
            0
        } else {
            self.hasher.digest()
        }
    }
}

pub(super) struct StreamingSegmentWriter {
    pub(super) file: File,
    pub(super) tmp_path: PathBuf,
    pub(super) states: Vec<StreamState>,
    pub(super) stream_starts: Vec<u64>,
    pub(super) block_size: u32,
    pub(super) zstd_level: i32,
    pub(super) generation: u64,
    pub(super) checkpoint_start: u64,
    pub(super) version_start: u64,
    pub(super) payload_bytes: u64,
}

pub(super) struct FinalizedSegment {
    pub(super) tmp_path: PathBuf,
    pub(super) meta: SegmentMeta,
}

impl StreamingSegmentWriter {
    pub(super) fn new(
        live_dir: &Path,
        generation: u64,
        stream_starts: Vec<u64>,
        checkpoint_start: u64,
        version_start: u64,
        block_size: u32,
        zstd_level: i32,
    ) -> Result<Self, CheckpointStoreError> {
        if stream_starts.len() != STREAM_NAMES.len() {
            return Err(format_error("stream start width mismatch"));
        }
        let block_size_usize =
            usize::try_from(block_size).map_err(|_| format_error("sealed block size overflow"))?;
        let final_path = live_dir.join(format!("structured-g{generation:06}.t3s"));
        let tmp_path = tmp_path_for(&final_path)?;
        if tmp_path.exists() {
            fs::remove_file(&tmp_path)?;
        }
        let mut file = File::create(&tmp_path)?;
        file.write_all(&[0u8; SEGMENT_HEADER_SIZE])?;
        Ok(Self {
            file,
            tmp_path,
            states: (0..STREAM_NAMES.len())
                .map(|_| StreamState::new(block_size_usize))
                .collect(),
            stream_starts,
            block_size,
            zstd_level,
            generation,
            checkpoint_start,
            version_start,
            payload_bytes: 0,
        })
    }

    pub(super) fn current_stream_len(&self, id: usize) -> Result<u64, CheckpointStoreError> {
        let state = self
            .states
            .get(id)
            .ok_or_else(|| format_error("stream id outside writer"))?;
        self.stream_starts
            .get(id)
            .copied()
            .ok_or_else(|| format_error("stream start id outside writer"))?
            .checked_add(state.raw_len)
            .ok_or_else(|| format_error("stream length overflow"))
    }

    pub(super) fn push(&mut self, id: usize, mut data: &[u8]) -> Result<(), CheckpointStoreError> {
        if id >= self.states.len() {
            return Err(format_error("stream id outside writer"));
        }
        {
            let state = self
                .states
                .get_mut(id)
                .ok_or_else(|| format_error("stream state missing"))?;
            state.hasher.update(data);
            state.raw_len = state
                .raw_len
                .checked_add(
                    u64::try_from(data.len())
                        .map_err(|_| format_error("stream push length overflow"))?,
                )
                .ok_or_else(|| format_error("stream raw length overflow"))?;
        }
        let block_size =
            usize::try_from(self.block_size).map_err(|_| format_error("block size overflow"))?;
        while !data.is_empty() {
            let current = self
                .states
                .get(id)
                .ok_or_else(|| format_error("stream state missing"))?
                .raw_buf
                .len();
            let take = (block_size - current).min(data.len());
            self.states
                .get_mut(id)
                .ok_or_else(|| format_error("stream state missing"))?
                .raw_buf
                .extend_from_slice(
                    data.get(..take)
                        .ok_or_else(|| format_error("stream push slice outside data"))?,
                );
            data = data
                .get(take..)
                .ok_or_else(|| format_error("stream remainder outside data"))?;
            if self
                .states
                .get(id)
                .ok_or_else(|| format_error("stream state missing"))?
                .raw_buf
                .len()
                == block_size
            {
                self.flush_stream_block(id)?;
            }
        }
        Ok(())
    }

    pub(super) fn push_u32(&mut self, id: usize, value: u32) -> Result<(), CheckpointStoreError> {
        self.push(id, &value.to_le_bytes())
    }

    pub(super) fn push_u64(&mut self, id: usize, value: u64) -> Result<(), CheckpointStoreError> {
        self.push(id, &value.to_le_bytes())
    }

    pub(super) fn flush_stream_block(&mut self, id: usize) -> Result<(), CheckpointStoreError> {
        if self
            .states
            .get(id)
            .ok_or_else(|| format_error("stream state missing"))?
            .raw_buf
            .is_empty()
        {
            return Ok(());
        }
        let raw = std::mem::take(
            &mut self
                .states
                .get_mut(id)
                .ok_or_else(|| format_error("stream state missing"))?
                .raw_buf,
        );
        let encoded = zstd::bulk::compress(&raw, self.zstd_level)?;
        let state = self
            .states
            .get_mut(id)
            .ok_or_else(|| format_error("stream state missing"))?;
        let raw_offset = state.flushed_raw_len;
        let encoded_offset = self.payload_bytes;
        self.file.write_all(&encoded)?;
        let raw_len =
            u32::try_from(raw.len()).map_err(|_| format_error("raw block length overflow"))?;
        let encoded_len = u32::try_from(encoded.len())
            .map_err(|_| format_error("encoded block length overflow"))?;
        state.blocks.push(LocalBlock {
            raw_offset,
            encoded_offset,
            raw_len,
            encoded_len,
            raw_xxh3_64: xxh3_64(&raw),
        });
        state.flushed_raw_len = state
            .flushed_raw_len
            .checked_add(u64::from(raw_len))
            .ok_or_else(|| format_error("flushed raw length overflow"))?;
        self.payload_bytes = self
            .payload_bytes
            .checked_add(u64::from(encoded_len))
            .ok_or_else(|| format_error("encoded payload length overflow"))?;
        state.raw_buf = Vec::with_capacity(
            usize::try_from(self.block_size).map_err(|_| format_error("block size overflow"))?,
        );
        Ok(())
    }

    pub(super) fn finalize(
        mut self,
        wal_start: u64,
        wal_end: u64,
        checkpoint_end: u64,
        version_end: u64,
    ) -> Result<FinalizedSegment, CheckpointStoreError> {
        for id in 0..self.states.len() {
            self.flush_stream_block(id)?;
        }
        let mut streams = Vec::with_capacity(STREAM_NAMES.len());
        let mut blocks = Vec::new();
        for (sid, state) in self.states.iter().enumerate() {
            let first =
                u32::try_from(blocks.len()).map_err(|_| format_error("block index overflow"))?;
            for block in &state.blocks {
                blocks.push(BlockEntry {
                    stream_id: u32::try_from(sid)
                        .map_err(|_| format_error("stream id overflow"))?,
                    raw_offset: block.raw_offset,
                    encoded_offset: block.encoded_offset,
                    raw_len: block.raw_len,
                    encoded_len: block.encoded_len,
                    raw_xxh3_64: block.raw_xxh3_64,
                });
            }
            let start = *self
                .stream_starts
                .get(sid)
                .ok_or_else(|| format_error("stream start missing"))?;
            let end = start
                .checked_add(state.raw_len)
                .ok_or_else(|| format_error("stream end overflow"))?;
            streams.push(StreamEntry {
                global_start: start,
                global_end: end,
                raw_len: state.raw_len,
                first_block: first,
                block_count: u32::try_from(state.blocks.len())
                    .map_err(|_| format_error("stream block count overflow"))?,
                raw_xxh3_64: state.raw_hash(),
            });
        }
        let mut index = Vec::new();
        for entry in &streams {
            write_stream_entry(&mut index, entry);
        }
        for block in &blocks {
            write_block_entry(&mut index, block);
        }
        let index_hash = if index.is_empty() { 0 } else { xxh3_64(&index) };
        let index_offset = u64::try_from(SEGMENT_HEADER_SIZE)
            .map_err(|_| format_error("segment header size overflow"))?
            .checked_add(self.payload_bytes)
            .ok_or_else(|| format_error("segment index offset overflow"))?;
        self.file.write_all(&index)?;
        let header = SegmentHeader {
            generation: self.generation,
            checkpoint_start_count: self.checkpoint_start,
            checkpoint_end_count: checkpoint_end,
            version_start_count: self.version_start,
            version_end_count: version_end,
            block_size: self.block_size,
            block_count: u32::try_from(blocks.len())
                .map_err(|_| format_error("block count overflow"))?,
            payload_offset: u64::try_from(SEGMENT_HEADER_SIZE)
                .map_err(|_| format_error("header size overflow"))?,
            payload_bytes: self.payload_bytes,
            index_offset,
            stream_table_bytes: u64::try_from(STREAM_NAMES.len() * STREAM_ENTRY_SIZE)
                .map_err(|_| format_error("stream table length overflow"))?,
            block_table_bytes: u64::try_from(
                blocks
                    .len()
                    .checked_mul(BLOCK_ENTRY_SIZE)
                    .ok_or_else(|| format_error("block table length overflow"))?,
            )
            .map_err(|_| format_error("block table length overflow"))?,
            index_xxh3_64: index_hash,
            header_xxh3_64: 0,
        };
        let header_bytes = segment_header_bytes(header)?;
        self.file.seek(SeekFrom::Start(0))?;
        self.file.write_all(&header_bytes)?;
        self.file.flush()?;
        let file_bytes = fs::metadata(&self.tmp_path)?.len();
        let raw_total = streams.iter().try_fold(0u64, |acc, stream| {
            acc.checked_add(stream.raw_len)
                .ok_or_else(|| format_error("raw segment total overflow"))
        })?;
        let ends = streams
            .iter()
            .map(|stream| stream.global_end)
            .collect::<Vec<_>>();
        let meta = SegmentMeta {
            generation: self.generation,
            file: format!("structured-g{:06}.t3s", self.generation),
            wal_start_bytes: wal_start,
            wal_end_bytes: wal_end,
            checkpoint_start_count: self.checkpoint_start,
            checkpoint_end_count: checkpoint_end,
            version_start_count: self.version_start,
            version_end_count: version_end,
            raw_stream_bytes: raw_total,
            stream_starts: self.stream_starts.clone(),
            stream_ends: ends,
            segment_file_bytes: file_bytes,
            segment_file_xxh3_64: 0,
            index_bytes: u64::try_from(index.len())
                .map_err(|_| format_error("segment index length overflow"))?,
            index_xxh3_64: index_hash,
            block_size: self.block_size,
            block_count: u32::try_from(blocks.len())
                .map_err(|_| format_error("block count overflow"))?,
            zstd_level: self.zstd_level,
        };
        Ok(FinalizedSegment {
            tmp_path: self.tmp_path,
            meta,
        })
    }
}

pub(super) fn write_transaction_to_segment(
    writer: &mut StreamingSegmentWriter,
    tx: &ParsedTransaction,
    state: &StoreState,
    next_thread_ordinal: &mut u32,
    new_threads: &mut Vec<(String, u32)>,
    new_checkpoints: &mut Vec<(String, String, u32)>,
    new_requests: &mut Vec<RequestRecord>,
) -> Result<(), CheckpointStoreError> {
    writer.push(PAYLOAD, &tx.bytes)?;
    for slot in tx.compact_nodes.chunks_exact(COMPACT_NODE_SIZE) {
        let code = match read_u32(slot, 12)? {
            KIND_LEAF => 0u8,
            KIND_BINARY => 1u8,
            KIND_WIDE => 2u8,
            _ => return Err(format_error("unknown compact node kind during sealing")),
        };
        writer.push(
            NODE_FIELD0,
            slot.get(..8)
                .ok_or_else(|| format_error("node field0 slice missing"))?,
        )?;
        writer.push(
            NODE_FIELD1,
            slot.get(8..12)
                .ok_or_else(|| format_error("node field1 slice missing"))?,
        )?;
        writer.push(NODE_KIND, &[code])?;
    }
    for record in tx.wide_nodes.chunks_exact(WIDE_RECORD_SIZE) {
        let code = match read_u32(record, 0)? {
            WIDE_KIND_LEAF => 0u8,
            WIDE_KIND_BINARY => 1u8,
            _ => return Err(format_error("unknown wide node kind during sealing")),
        };
        writer.push(WIDE_KIND, &[code])?;
        writer.push(
            WIDE_A,
            record
                .get(8..16)
                .ok_or_else(|| format_error("wide field A missing"))?,
        )?;
        writer.push(
            WIDE_B,
            record
                .get(16..24)
                .ok_or_else(|| format_error("wide field B missing"))?,
        )?;
        writer.push(
            WIDE_C,
            record
                .get(24..32)
                .ok_or_else(|| format_error("wide field C missing"))?,
        )?;
    }
    for (root, parent) in tx.roots.iter().zip(tx.parents.iter()) {
        writer.push_u64(VERSION_ROOT, root.unwrap_or(NONE_ROOT))?;
        writer.push_u32(VERSION_PARENT, parent.unwrap_or(NONE_PARENT))?;
    }
    let thread_ordinal = state
        .thread_ordinals
        .get(&tx.checkpoint.thread_id)
        .copied()
        .ok_or_else(|| format_error("checkpoint thread missing from committed state"))?;
    match thread_ordinal.cmp(&*next_thread_ordinal) {
        std::cmp::Ordering::Equal => {
            if writer.current_stream_len(THREAD_OFFSETS)? == 0 {
                writer.push_u32(THREAD_OFFSETS, 0)?;
            }
            writer.push(THREAD_BYTES, tx.checkpoint.thread_id.as_bytes())?;
            let end = u32::try_from(writer.current_stream_len(THREAD_BYTES)?)
                .map_err(|_| format_error("thread byte offset exceeds u32"))?;
            writer.push_u32(THREAD_OFFSETS, end)?;
            new_threads.push((tx.checkpoint.thread_id.clone(), thread_ordinal));
            *next_thread_ordinal = next_thread_ordinal
                .checked_add(1)
                .ok_or_else(|| format_error("thread ordinal overflow"))?;
        }
        std::cmp::Ordering::Greater => {
            return Err(format_error("thread ordinal skipped during sealing"));
        }
        std::cmp::Ordering::Less => {}
    }
    writer.push_u32(CP_THREAD, thread_ordinal)?;
    writer.push_u32(CP_NO, tx.checkpoint.checkpoint_no)?;
    if writer.current_stream_len(CP_ID_OFFSETS)? == 0 {
        writer.push_u32(CP_ID_OFFSETS, 0)?;
    }
    writer.push(CP_ID_BYTES, tx.checkpoint.checkpoint_id.as_bytes())?;
    let cp_id_end = u32::try_from(writer.current_stream_len(CP_ID_BYTES)?)
        .map_err(|_| format_error("checkpoint id bytes exceed u32"))?;
    writer.push_u32(CP_ID_OFFSETS, cp_id_end)?;
    let parent_ordinal = match tx.checkpoint.parent_checkpoint_id.as_ref() {
        None => NONE_PARENT,
        Some(parent) => state
            .checkpoint_ordinals
            .get(&(tx.checkpoint.thread_id.clone(), parent.clone()))
            .copied()
            .ok_or_else(|| format_error("checkpoint parent missing during sealing"))?,
    };
    writer.push_u32(CP_PARENT_ORDINAL, parent_ordinal)?;
    writer.push_u32(CP_IDENTITY_VERSION, tx.checkpoint.identity_version)?;
    writer.push_u32(
        CP_MESSAGES_VERSION,
        tx.checkpoint.messages_version.unwrap_or(NONE_VERSION),
    )?;
    writer.push_u32(
        CP_RESULT_VERSION,
        tx.checkpoint.result_version.unwrap_or(NONE_VERSION),
    )?;
    writer.push_u64(CP_LOGICAL_LEN, tx.checkpoint.logical_state_len)?;
    writer.push_u64(CP_STATE_HASH, tx.checkpoint.state_hash)?;
    new_checkpoints.push((
        tx.checkpoint.thread_id.clone(),
        tx.checkpoint.checkpoint_id.clone(),
        tx.checkpoint.ordinal,
    ));
    if let Some(request_id) = tx.request_id.as_ref() {
        new_requests.push(RequestRecord {
            key: request_id.clone(),
            operation_digest: tx.operation_digest,
            checkpoint_ordinal: tx.checkpoint.ordinal,
        });
    }
    Ok(())
}

pub(super) fn segment_header_bytes(
    mut header: SegmentHeader,
) -> Result<Vec<u8>, CheckpointStoreError> {
    let mut out = Vec::with_capacity(SEGMENT_HEADER_SIZE);
    out.extend_from_slice(SEGMENT_MAGIC);
    out.extend_from_slice(&SEGMENT_FORMAT_VERSION.to_le_bytes());
    out.extend_from_slice(
        &u32::try_from(STREAM_NAMES.len())
            .map_err(|_| format_error("stream count overflow"))?
            .to_le_bytes(),
    );
    out.extend_from_slice(&header.generation.to_le_bytes());
    out.extend_from_slice(&header.checkpoint_start_count.to_le_bytes());
    out.extend_from_slice(&header.checkpoint_end_count.to_le_bytes());
    out.extend_from_slice(&header.version_start_count.to_le_bytes());
    out.extend_from_slice(&header.version_end_count.to_le_bytes());
    out.extend_from_slice(&header.block_size.to_le_bytes());
    out.extend_from_slice(&header.block_count.to_le_bytes());
    out.extend_from_slice(&header.payload_offset.to_le_bytes());
    out.extend_from_slice(&header.payload_bytes.to_le_bytes());
    out.extend_from_slice(&header.index_offset.to_le_bytes());
    out.extend_from_slice(&header.stream_table_bytes.to_le_bytes());
    out.extend_from_slice(&header.block_table_bytes.to_le_bytes());
    out.extend_from_slice(&header.index_xxh3_64.to_le_bytes());
    out.extend_from_slice(&0u64.to_le_bytes());
    if out.len() != SEGMENT_HEADER_SIZE {
        return Err(format_error("segment header width mismatch"));
    }
    header.header_xxh3_64 = xxh3_64(
        out.get(..112)
            .ok_or_else(|| format_error("segment header checksum slice missing"))?,
    );
    out.get_mut(112..120)
        .ok_or_else(|| format_error("segment header hash field missing"))?
        .copy_from_slice(&header.header_xxh3_64.to_le_bytes());
    Ok(out)
}

pub(super) fn write_stream_entry(out: &mut Vec<u8>, entry: &StreamEntry) {
    out.extend_from_slice(&entry.global_start.to_le_bytes());
    out.extend_from_slice(&entry.global_end.to_le_bytes());
    out.extend_from_slice(&entry.raw_len.to_le_bytes());
    out.extend_from_slice(&entry.first_block.to_le_bytes());
    out.extend_from_slice(&entry.block_count.to_le_bytes());
    out.extend_from_slice(&entry.raw_xxh3_64.to_le_bytes());
}

pub(super) fn write_block_entry(out: &mut Vec<u8>, entry: &BlockEntry) {
    out.extend_from_slice(&entry.stream_id.to_le_bytes());
    out.extend_from_slice(&0u32.to_le_bytes());
    out.extend_from_slice(&entry.raw_offset.to_le_bytes());
    out.extend_from_slice(&entry.encoded_offset.to_le_bytes());
    out.extend_from_slice(&entry.raw_len.to_le_bytes());
    out.extend_from_slice(&entry.encoded_len.to_le_bytes());
    out.extend_from_slice(&entry.raw_xxh3_64.to_le_bytes());
}

pub(super) fn parse_segment_header(data: &[u8]) -> Result<SegmentHeader, CheckpointStoreError> {
    if data.len() < SEGMENT_HEADER_SIZE || data.get(..8) != Some(SEGMENT_MAGIC.as_slice()) {
        return Err(format_error("segment magic/header mismatch"));
    }
    if read_u32(data, 8)? != SEGMENT_FORMAT_VERSION {
        return Err(format_error("segment format version mismatch"));
    }
    if usize::try_from(read_u32(data, 12)?)
        .map_err(|_| format_error("segment stream count overflow"))?
        != STREAM_NAMES.len()
    {
        return Err(format_error("segment stream count mismatch"));
    }
    let header = SegmentHeader {
        generation: read_u64(data, 16)?,
        checkpoint_start_count: read_u64(data, 24)?,
        checkpoint_end_count: read_u64(data, 32)?,
        version_start_count: read_u64(data, 40)?,
        version_end_count: read_u64(data, 48)?,
        block_size: read_u32(data, 56)?,
        block_count: read_u32(data, 60)?,
        payload_offset: read_u64(data, 64)?,
        payload_bytes: read_u64(data, 72)?,
        index_offset: read_u64(data, 80)?,
        stream_table_bytes: read_u64(data, 88)?,
        block_table_bytes: read_u64(data, 96)?,
        index_xxh3_64: read_u64(data, 104)?,
        header_xxh3_64: read_u64(data, 112)?,
    };
    if xxh3_64(
        data.get(..112)
            .ok_or_else(|| format_error("segment header checksum slice missing"))?,
    ) != header.header_xxh3_64
    {
        return Err(format_error("segment header checksum mismatch"));
    }
    if header.block_size == 0
        || header.payload_offset
            != u64::try_from(SEGMENT_HEADER_SIZE)
                .map_err(|_| format_error("header size overflow"))?
        || header.index_offset
            != header
                .payload_offset
                .checked_add(header.payload_bytes)
                .ok_or_else(|| format_error("segment index offset overflow"))?
        || header.stream_table_bytes
            != u64::try_from(STREAM_NAMES.len() * STREAM_ENTRY_SIZE)
                .map_err(|_| format_error("stream table size overflow"))?
        || header.block_table_bytes
            != u64::from(header.block_count)
                .checked_mul(
                    u64::try_from(BLOCK_ENTRY_SIZE)
                        .map_err(|_| format_error("block entry size overflow"))?,
                )
                .ok_or_else(|| format_error("block table size overflow"))?
    {
        return Err(format_error("segment header geometry mismatch"));
    }
    Ok(header)
}

pub(super) struct ParsedSegmentIndex {
    pub(super) header: SegmentHeader,
    pub(super) streams: Vec<StreamEntry>,
    pub(super) blocks: Vec<BlockEntry>,
}

pub(super) fn read_lazy_segment_index_file(
    path: &Path,
) -> Result<LazySegmentIndex, CheckpointStoreError> {
    let mut file = File::open(path)?;
    let file_bytes = file.metadata()?.len();
    let mut header_bytes = vec![0u8; SEGMENT_HEADER_SIZE];
    file.read_exact(&mut header_bytes)?;
    let header = parse_segment_header(&header_bytes)?;
    let index_bytes_len = header
        .stream_table_bytes
        .checked_add(header.block_table_bytes)
        .ok_or_else(|| format_error("lazy segment index length overflow"))?;
    let index_end = header
        .index_offset
        .checked_add(index_bytes_len)
        .ok_or_else(|| format_error("lazy segment index end overflow"))?;
    if index_end != file_bytes {
        return Err(format_error(
            "lazy segment index does not end at file boundary",
        ));
    }
    let stream_table_len = usize::try_from(header.stream_table_bytes)
        .map_err(|_| format_error("lazy segment stream table length exceeds usize"))?;
    let mut stream_bytes = vec![0u8; stream_table_len];
    file.seek(SeekFrom::Start(header.index_offset))?;
    file.read_exact(&mut stream_bytes)?;
    let mut streams = Vec::with_capacity(STREAM_NAMES.len());
    for index in 0..STREAM_NAMES.len() {
        let base = index
            .checked_mul(STREAM_ENTRY_SIZE)
            .ok_or_else(|| format_error("lazy stream entry offset overflow"))?;
        let entry = StreamEntry {
            global_start: read_u64(&stream_bytes, base)?,
            global_end: read_u64(&stream_bytes, base + 8)?,
            raw_len: read_u64(&stream_bytes, base + 16)?,
            first_block: read_u32(&stream_bytes, base + 24)?,
            block_count: read_u32(&stream_bytes, base + 28)?,
            raw_xxh3_64: read_u64(&stream_bytes, base + 32)?,
        };
        if entry.global_end < entry.global_start
            || entry.raw_len != entry.global_end - entry.global_start
            || u64::from(entry.first_block) + u64::from(entry.block_count)
                > u64::from(header.block_count)
        {
            return Err(format_error("lazy segment stream entry invalid"));
        }
        streams.push(entry);
    }
    Ok(LazySegmentIndex { header, streams })
}

pub(super) fn read_lazy_block_entry(
    file: &File,
    index: &LazySegmentIndex,
    block_index: u32,
) -> Result<BlockEntry, CheckpointStoreError> {
    if block_index >= index.header.block_count {
        return Err(format_error("lazy block index outside segment"));
    }
    let block_offset = u64::from(block_index)
        .checked_mul(
            u64::try_from(BLOCK_ENTRY_SIZE)
                .map_err(|_| format_error("block entry size overflow"))?,
        )
        .and_then(|offset| {
            index
                .header
                .index_offset
                .checked_add(index.header.stream_table_bytes)?
                .checked_add(offset)
        })
        .ok_or_else(|| format_error("lazy block table offset overflow"))?;
    let mut bytes = [0u8; BLOCK_ENTRY_SIZE];
    read_file_exact_at(file, &mut bytes, block_offset)?;
    let entry = BlockEntry {
        stream_id: read_u32(&bytes, 0)?,
        raw_offset: read_u64(&bytes, 8)?,
        encoded_offset: read_u64(&bytes, 16)?,
        raw_len: read_u32(&bytes, 24)?,
        encoded_len: read_u32(&bytes, 28)?,
        raw_xxh3_64: read_u64(&bytes, 32)?,
    };
    if usize::try_from(entry.stream_id)
        .map_err(|_| format_error("lazy block stream id overflow"))?
        >= STREAM_NAMES.len()
        || entry
            .encoded_offset
            .checked_add(u64::from(entry.encoded_len))
            .ok_or_else(|| format_error("lazy encoded block end overflow"))?
            > index.header.payload_bytes
    {
        return Err(format_error("lazy segment block entry invalid"));
    }
    Ok(entry)
}

pub(super) fn read_segment_index_file(
    path: &Path,
) -> Result<ParsedSegmentIndex, CheckpointStoreError> {
    let mut file = File::open(path)?;
    let file_bytes = file.metadata()?.len();
    let mut header_bytes = vec![0u8; SEGMENT_HEADER_SIZE];
    file.read_exact(&mut header_bytes)?;
    let header = parse_segment_header(&header_bytes)?;
    let index_bytes_len = header
        .stream_table_bytes
        .checked_add(header.block_table_bytes)
        .ok_or_else(|| format_error("segment index length overflow"))?;
    let index_end = header
        .index_offset
        .checked_add(index_bytes_len)
        .ok_or_else(|| format_error("segment index end overflow"))?;
    if index_end != file_bytes {
        return Err(format_error("segment index does not end at file boundary"));
    }
    file.seek(SeekFrom::Start(header.index_offset))?;
    let mut index_bytes = vec![
        0u8;
        usize::try_from(index_bytes_len).map_err(|_| format_error(
            "segment index length exceeds usize"
        ))?
    ];
    file.read_exact(&mut index_bytes)?;
    if xxh3_64(&index_bytes) != header.index_xxh3_64 {
        return Err(format_error("segment index checksum mismatch"));
    }
    let mut streams = Vec::with_capacity(STREAM_NAMES.len());
    for index in 0..STREAM_NAMES.len() {
        let base = index
            .checked_mul(STREAM_ENTRY_SIZE)
            .ok_or_else(|| format_error("stream entry offset overflow"))?;
        let entry = StreamEntry {
            global_start: read_u64(&index_bytes, base)?,
            global_end: read_u64(&index_bytes, base + 8)?,
            raw_len: read_u64(&index_bytes, base + 16)?,
            first_block: read_u32(&index_bytes, base + 24)?,
            block_count: read_u32(&index_bytes, base + 28)?,
            raw_xxh3_64: read_u64(&index_bytes, base + 32)?,
        };
        if entry.global_end < entry.global_start
            || entry.raw_len != entry.global_end - entry.global_start
            || u64::from(entry.first_block) + u64::from(entry.block_count)
                > u64::from(header.block_count)
        {
            return Err(format_error("segment stream entry invalid"));
        }
        streams.push(entry);
    }
    let block_base = usize::try_from(header.stream_table_bytes)
        .map_err(|_| format_error("block table base overflow"))?;
    let mut blocks = Vec::with_capacity(
        usize::try_from(header.block_count).map_err(|_| format_error("block count overflow"))?,
    );
    for index in
        0..usize::try_from(header.block_count).map_err(|_| format_error("block count overflow"))?
    {
        let base = block_base
            .checked_add(
                index
                    .checked_mul(BLOCK_ENTRY_SIZE)
                    .ok_or_else(|| format_error("block entry offset overflow"))?,
            )
            .ok_or_else(|| format_error("block table offset overflow"))?;
        let entry = BlockEntry {
            stream_id: read_u32(&index_bytes, base)?,
            raw_offset: read_u64(&index_bytes, base + 8)?,
            encoded_offset: read_u64(&index_bytes, base + 16)?,
            raw_len: read_u32(&index_bytes, base + 24)?,
            encoded_len: read_u32(&index_bytes, base + 28)?,
            raw_xxh3_64: read_u64(&index_bytes, base + 32)?,
        };
        if usize::try_from(entry.stream_id).map_err(|_| format_error("block stream id overflow"))?
            >= STREAM_NAMES.len()
            || entry
                .encoded_offset
                .checked_add(u64::from(entry.encoded_len))
                .ok_or_else(|| format_error("encoded block end overflow"))?
                > header.payload_bytes
        {
            return Err(format_error("segment block entry invalid"));
        }
        blocks.push(entry);
    }
    Ok(ParsedSegmentIndex {
        header,
        streams,
        blocks,
    })
}
