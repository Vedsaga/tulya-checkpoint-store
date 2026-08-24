use super::*;

pub(super) const TX_MAGIC: [u8; 4] = *b"T2W1";
pub(super) const TX_HEADER_SIZE: usize = 72;
pub(super) const TX_CHECKSUM_SIZE: usize = 8;
pub(super) const ROOT_MAGIC: [u8; 4] = *b"T2R1";
pub(super) const CHECKPOINT_MAGIC: [u8; 4] = *b"T2P1";
pub(super) const ROOT_RECORD_SIZE: usize = 32;
pub(super) const CHECKPOINT_PREFIX_SIZE: usize = 52;
pub(super) const NONE_ROOT: u64 = u64::MAX;
pub(super) const NONE_VERSION: u32 = u32::MAX;
pub(super) const NONE_PARENT: u32 = u32::MAX;
pub(super) const COMPACT_NODE_SIZE: usize = 16;
pub(super) const WIDE_RECORD_SIZE: usize = 32;
#[allow(dead_code)] // Used by the reserved zero-copy identity adapter path.
pub(super) const CANONICAL_STATE_PREFIX_BYTES: u64 = 12;
#[allow(dead_code)] // Used by the reserved zero-copy identity adapter path.
pub(super) const CANONICAL_STATE_SUFFIX_BYTES: u64 = 15;
pub(super) const KIND_LEAF: u32 = 0xD100_0001;
pub(super) const KIND_BINARY: u32 = 0xD100_0002;
pub(super) const KIND_WIDE: u32 = 0xD100_0003;
pub(super) const WIDE_KIND_LEAF: u32 = 1;
pub(super) const WIDE_KIND_BINARY: u32 = 2;

pub(super) const MANIFEST_FORMAT: &str = crate::format::NAME;
pub(super) const MANIFEST_FORMAT_VERSION: u64 = crate::format::VERSION as u64;
// Read-only compatibility with data produced during private development.
// These are implementation revisions, not public Tulya format versions.
pub(super) const PRE_RELEASE_MANIFEST_FORMAT: &str = "tulya-r3-structured-segment-manifest-r3";
pub(super) const PRE_RELEASE_REVISION_INITIAL: u64 = 3;
pub(super) const PRE_RELEASE_REVISION_STORE_ID: u64 = 4;
pub(super) const PRE_RELEASE_REVISION_PRUNE: u64 = 5;
pub(super) const MANIFEST_FILE: &str = "structured-segment-manifest.json";
pub(super) const HOT_WAL_FILE: &str = "hot.wal";
pub(super) const SEGMENT_MAGIC: &[u8; 8] = b"T3STRS02";
pub(super) const SEGMENT_FORMAT_VERSION: u32 = 2;
pub(super) const SEGMENT_HEADER_SIZE: usize = 120;
pub(super) const STREAM_ENTRY_SIZE: usize = 40;
pub(super) const BLOCK_ENTRY_SIZE: usize = 40;
pub(super) const ROUTE_MAGIC: &[u8; 8] = b"T3ROUT01";
pub(super) const ROUTE_FORMAT_VERSION: u32 = 1;
pub(super) const ROUTE_HEADER_SIZE: usize = 64;
pub(super) const ROUTE_ENTRY_SIZE: usize = 24;
pub(super) const WRITER_LOCK_FILE: &str = ".tulya-writer.lock";
pub(super) const READER_RECLAIM_LOCK_FILE: &str = ".tulya-reader-reclaim.lock";
pub(super) const RECLAIM_WORKER_LOCK_FILE: &str = ".tulya-reclaim-worker.lock";
pub(super) const MAX_CHECKPOINT_IDENTIFIER_BYTES: usize = 4096;
pub(super) const MAX_REQUEST_ID_BYTES: usize = 4096;
pub(super) const REQUEST_SECTION_MAGIC: &[u8; 8] = b"T2REQ01\0";
pub(super) const REQUEST_FOOTER_MAGIC: &[u8; 8] = b"T2REQF1\0";
pub(super) const REQUEST_SECTION_VERSION: u32 = 1;
pub(super) const REQUEST_SECTION_HEADER_BYTES: usize = 20;
pub(super) const REQUEST_SECTION_FOOTER_BYTES: usize = 16;
pub(super) const LAZY_BLOCK_CACHE_CAPACITY: usize = 128;
pub(super) const LAZY_DIAGNOSTIC_MAX_CACHE_CAPACITY: usize = 4096;
pub(super) const LAZY_EAGER_PAYLOAD_CAP_BYTES: u64 = 8 * 1024 * 1024;

pub(super) fn lazy_payload_fast_path_allowed(payload_len: u64) -> bool {
    payload_len <= LAZY_EAGER_PAYLOAD_CAP_BYTES
}

pub(super) fn lazy_block_cache_capacity() -> Result<usize, CheckpointStoreError> {
    let Some(value) = std::env::var_os("TULYA_LAZY_BLOCK_CACHE_CAPACITY_DIAGNOSTIC") else {
        return Ok(LAZY_BLOCK_CACHE_CAPACITY);
    };
    let value = value
        .to_str()
        .ok_or_else(|| format_error("lazy diagnostic cache capacity is not UTF-8"))?;
    let capacity = value
        .parse::<usize>()
        .map_err(|_| format_error("lazy diagnostic cache capacity is not a number"))?;
    if capacity == 0 || capacity > LAZY_DIAGNOSTIC_MAX_CACHE_CAPACITY {
        return Err(format_error(
            "lazy diagnostic cache capacity is outside bounds",
        ));
    }
    Ok(capacity)
}

pub(super) const STREAM_NAMES: [&str; 22] = [
    "payload.bin",
    "node_field0_u64.bin",
    "node_field1_u32.bin",
    "node_kind_u8.bin",
    "wide_kind_u8.bin",
    "wide_a_u64.bin",
    "wide_b_u64.bin",
    "wide_c_u64.bin",
    "version_root_u64.bin",
    "version_parent_u32.bin",
    "thread_offsets_u32.bin",
    "thread_bytes.bin",
    "checkpoint_thread_u32.bin",
    "checkpoint_no_u32.bin",
    "checkpoint_id_offsets_u32.bin",
    "checkpoint_id_bytes.bin",
    "checkpoint_parent_ordinal_u32.bin",
    "checkpoint_identity_version_u32.bin",
    "checkpoint_messages_version_u32.bin",
    "checkpoint_result_version_u32.bin",
    "checkpoint_logical_state_len_u64.bin",
    "checkpoint_state_hash_u64.bin",
];
pub(super) const PAYLOAD: usize = 0;
pub(super) const NODE_FIELD0: usize = 1;
pub(super) const NODE_FIELD1: usize = 2;
pub(super) const NODE_KIND: usize = 3;
pub(super) const WIDE_KIND: usize = 4;
pub(super) const WIDE_A: usize = 5;
pub(super) const WIDE_B: usize = 6;
pub(super) const WIDE_C: usize = 7;
pub(super) const VERSION_ROOT: usize = 8;
pub(super) const VERSION_PARENT: usize = 9;
pub(super) const THREAD_OFFSETS: usize = 10;
pub(super) const THREAD_BYTES: usize = 11;
pub(super) const CP_THREAD: usize = 12;
pub(super) const CP_NO: usize = 13;
pub(super) const CP_ID_OFFSETS: usize = 14;
pub(super) const CP_ID_BYTES: usize = 15;
pub(super) const CP_PARENT_ORDINAL: usize = 16;
pub(super) const CP_IDENTITY_VERSION: usize = 17;
pub(super) const CP_MESSAGES_VERSION: usize = 18;
pub(super) const CP_RESULT_VERSION: usize = 19;
pub(super) const CP_LOGICAL_LEN: usize = 20;
pub(super) const CP_STATE_HASH: usize = 21;
