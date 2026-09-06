use super::*;

/// Result of an independent, read-only integrity scan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FsckReport {
    /// Public format version verified by this scan.
    pub format_version: u32,
    /// Persistent identity of the scanned store.
    pub store_id: StoreId,
    /// Latest immutable generation referenced by the manifest.
    pub sealed_generation: u64,
    /// Number of immutable segment files checked.
    pub sealed_segment_count: u64,
    /// Total committed checkpoints reconstructed and hashed.
    pub checkpoint_count: u64,
    /// Total committed versions checked.
    pub version_count: u64,
    /// Valid committed bytes found at the start of the hot WAL.
    pub hot_logical_bytes: u64,
    /// Physical size of the preinitialized hot WAL reserve.
    pub hot_physical_bytes: u64,
    /// Non-zero bytes after the valid hot prefix. These bytes are ignored by
    /// recovery and can result from a torn, unacknowledged append.
    pub trailing_nonzero_bytes: u64,
    /// Reconstructed checkpoints whose stored length or hash did not match.
    pub failures: u64,
}

/// Checks a Tulya store without opening a writer, creating lock files,
/// repairing data, recycling the WAL, or changing directory contents.
///
/// Run this against an offline store or a stable filesystem snapshot. A
/// concurrent writer can legitimately publish a new manifest while the scan
/// is in progress.
pub fn fsck(dir: impl AsRef<Path>) -> Result<FsckReport, CheckpointStoreError> {
    let dir = dir.as_ref();
    let metadata = fs::metadata(dir).map_err(|error| {
        if error.kind() == io::ErrorKind::NotFound {
            format_error("fsck store directory does not exist")
        } else {
            error.into()
        }
    })?;
    if !metadata.is_dir() {
        return Err(format_error("fsck path is not a directory"));
    }
    if !dir.join(MANIFEST_FILE).is_file() {
        return Err(format_error(
            "fsck did not find a Tulya checkpoint manifest",
        ));
    }

    let manifest = load_manifest(dir)?;
    let store_id = manifest
        .store_id
        .ok_or_else(|| format_error("store predates the public Tulya format contract"))?;
    let mut state =
        materialize_sealed_state(dir, &manifest, CheckpointStoreRecoveryMode::ReusePayload)?;

    let hot_path = dir.join(HOT_WAL_FILE);
    let hot_physical_bytes = fs::metadata(&hot_path)
        .map_err(|error| {
            if error.kind() == io::ErrorKind::NotFound {
                format_error("fsck did not find the hot WAL")
            } else {
                error.into()
            }
        })?
        .len();
    let hot = parse_hot_prefix(&hot_path, Geometry::from_manifest(&manifest)?)?;
    for transaction in hot.transactions {
        let prepared = prepare_transaction_apply(&mut state, transaction)?;
        apply_prepared_transaction(&mut state, prepared);
    }
    validate_materialized_state(&state)?;

    let mut failures = 0u64;
    for checkpoint in &state.checkpoints {
        let bytes = reconstruct_checkpoint(&state, checkpoint)?;
        let len = u64::try_from(bytes.len())
            .map_err(|_| format_error("fsck reconstructed state length overflow"))?;
        if len != checkpoint.logical_state_len || xxh3_64(&bytes) != checkpoint.state_hash {
            failures = failures.saturating_add(1);
        }
    }

    let mut trailing_nonzero_bytes = 0u64;
    if hot.logical_tail < hot_physical_bytes {
        let mut file = File::open(&hot_path)?;
        file.seek(SeekFrom::Start(hot.logical_tail))?;
        let mut buffer = [0u8; 64 * 1024];
        loop {
            let read = file.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            trailing_nonzero_bytes = trailing_nonzero_bytes.saturating_add(
                u64::try_from(buffer[..read].iter().filter(|byte| **byte != 0).count())
                    .map_err(|_| format_error("fsck trailing-byte count overflow"))?,
            );
        }
    }

    Ok(FsckReport {
        format_version: crate::format::VERSION,
        store_id,
        sealed_generation: manifest.generation,
        sealed_segment_count: u64::try_from(manifest.segments.len())
            .map_err(|_| format_error("fsck segment count overflow"))?,
        checkpoint_count: u64::try_from(state.checkpoints.len())
            .map_err(|_| format_error("fsck checkpoint count overflow"))?,
        version_count: u64::try_from(state.versions.len())
            .map_err(|_| format_error("fsck version count overflow"))?,
        hot_logical_bytes: hot.logical_tail,
        hot_physical_bytes,
        trailing_nonzero_bytes,
        failures,
    })
}
