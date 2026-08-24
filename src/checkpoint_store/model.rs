use super::*;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct Geometry {
    pub(super) byte_len: u64,
    pub(super) node_count: u64,
    pub(super) wide_count: u64,
    pub(super) version_count: u64,
    pub(super) checkpoint_count: u64,
}

impl Geometry {
    pub(super) fn from_manifest(manifest: &Manifest) -> Result<Self, CheckpointStoreError> {
        if manifest.stream_sizes.len() != STREAM_NAMES.len() {
            return Err(format_error("manifest stream width mismatch"));
        }
        Ok(Self {
            byte_len: manifest.stream_sizes[PAYLOAD],
            node_count: manifest.stream_sizes[NODE_KIND],
            wide_count: manifest.stream_sizes[WIDE_KIND],
            version_count: manifest.version_count,
            checkpoint_count: manifest.checkpoint_count,
        })
    }

    pub(super) fn previous_generation(
        manifest: &Manifest,
    ) -> Result<Option<Self>, CheckpointStoreError> {
        let Some(last) = manifest.segments.last() else {
            return Ok(None);
        };
        if last.stream_starts.len() != STREAM_NAMES.len() {
            return Err(format_error("last segment stream-start width mismatch"));
        }
        Ok(Some(Self {
            byte_len: last.stream_starts[PAYLOAD],
            node_count: last.stream_starts[NODE_KIND],
            wide_count: last.stream_starts[WIDE_KIND],
            version_count: last.version_start_count,
            checkpoint_count: last.checkpoint_start_count,
        }))
    }

    pub(super) fn from_state(state: &StoreState) -> Result<Self, CheckpointStoreError> {
        state.geometry()
    }

    pub(super) fn advance(&mut self, tx: &ParsedTransaction) {
        self.byte_len = tx.byte_end;
        self.node_count = tx.node_end;
        self.wide_count = tx.wide_end;
        self.version_count = tx.version_count;
        self.checkpoint_count = tx.checkpoint_count;
    }
}

#[derive(Debug, Clone)]
pub(super) struct ParsedTransaction {
    pub(super) end_offset: u64,
    pub(super) version_start: u64,
    pub(super) version_count: u64,
    pub(super) checkpoint_count: u64,
    pub(super) byte_start: u64,
    pub(super) byte_end: u64,
    pub(super) bytes: Vec<u8>,
    pub(super) node_start: u64,
    pub(super) node_end: u64,
    pub(super) compact_nodes: Vec<u8>,
    pub(super) wide_start: u64,
    pub(super) wide_end: u64,
    pub(super) wide_nodes: Vec<u8>,
    pub(super) roots: Vec<Option<u64>>,
    pub(super) parents: Vec<Option<u32>>,
    pub(super) checkpoint: CheckpointInfo,
    pub(super) request_id: Option<Vec<u8>>,
    pub(super) operation_digest: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct RequestRecord {
    pub(super) key: Vec<u8>,
    pub(super) operation_digest: [u8; 32],
    pub(super) checkpoint_ordinal: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct CheckpointTombstone {
    pub(super) thread_id: String,
    pub(super) checkpoint_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct RetiredRequestRecord {
    pub(super) key: Vec<u8>,
    pub(super) operation_digest: [u8; 32],
}

#[derive(Default)]
pub(super) struct StoreState {
    /// Geometry already represented by the lazy sealed base. The byte/node
    /// vectors below contain only the writable overlay when this is present.
    pub(super) base_geometry: Option<Geometry>,
    pub(super) arena_bytes: Vec<u8>,
    pub(super) compact_nodes: Vec<u8>,
    pub(super) wide_nodes: Vec<u8>,
    pub(super) versions: Vec<Option<u64>>,
    pub(super) parents: Vec<Option<u32>>,
    pub(super) checkpoints: Vec<CheckpointInfo>,
    pub(super) threads: Vec<String>,
    pub(super) thread_ordinals: HashMap<String, u32>,
    pub(super) checkpoint_ordinals: HashMap<(String, String), u32>,
    pub(super) request_records: HashMap<Vec<u8>, RequestRecord>,
    pub(super) deleted_checkpoints: HashSet<(String, String)>,
    pub(super) retired_requests: HashMap<Vec<u8>, [u8; 32]>,
}

impl StoreState {
    pub(super) fn geometry(&self) -> Result<Geometry, CheckpointStoreError> {
        let base = self.base_geometry.unwrap_or_default();
        Ok(Geometry {
            byte_len: base
                .byte_len
                .checked_add(
                    u64::try_from(self.arena_bytes.len())
                        .map_err(|_| format_error("overlay arena byte length overflow"))?,
                )
                .ok_or_else(|| format_error("combined arena byte length overflow"))?,
            node_count: base
                .node_count
                .checked_add(
                    u64::try_from(self.compact_nodes.len() / COMPACT_NODE_SIZE)
                        .map_err(|_| format_error("overlay node count overflow"))?,
                )
                .ok_or_else(|| format_error("combined node count overflow"))?,
            wide_count: base
                .wide_count
                .checked_add(
                    u64::try_from(self.wide_nodes.len() / WIDE_RECORD_SIZE)
                        .map_err(|_| format_error("overlay wide count overflow"))?,
                )
                .ok_or_else(|| format_error("combined wide count overflow"))?,
            version_count: u64::try_from(self.versions.len())
                .map_err(|_| format_error("version count overflow"))?,
            checkpoint_count: u64::try_from(self.checkpoints.len())
                .map_err(|_| format_error("checkpoint count overflow"))?,
        })
    }

    pub(super) fn lazy_base_geometry(&self) -> Geometry {
        self.base_geometry.unwrap_or_default()
    }
}

#[derive(Default)]
pub(super) struct LazyMetadata {
    pub(super) versions: Vec<Option<u64>>,
    pub(super) version_parents: Vec<Option<u32>>,
    pub(super) threads: Vec<String>,
    pub(super) thread_ordinals: HashMap<String, u32>,
    pub(super) checkpoints: Vec<CheckpointInfo>,
    pub(super) checkpoint_ordinals: HashMap<(String, String), u32>,
}

pub(super) struct LazySegment {
    pub(super) index: LazySegmentIndex,
    pub(super) file: File,
    pub(super) decompressor: zstd::bulk::Decompressor<'static>,
}

pub(super) struct LazySegmentIndex {
    pub(super) header: SegmentHeader,
    pub(super) streams: Vec<StreamEntry>,
}
