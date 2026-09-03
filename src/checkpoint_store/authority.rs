use super::*;
use crate::format_authority::{
    probe_hot_wal_prefix, probe_public_manifest, recovery_dispatch, HotWalAuthority,
    PublicStoreFormat, RecoveryDispatch,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StoreRecoveryAuthority {
    V1,
    V2,
}

pub(super) fn probe_store_recovery_authority(
    dir: &Path,
) -> Result<StoreRecoveryAuthority, CheckpointStoreError> {
    let manifest_path = dir.join(MANIFEST_FILE);
    let hot_path = dir.join(HOT_WAL_FILE);
    let wal = probe_hot_wal_prefix(&read_hot_authority_prefix(&hot_path)?)
        .map_err(|error| format_error(error.to_string()))?;

    if !manifest_path.exists() {
        return match wal {
            HotWalAuthority::Empty | HotWalAuthority::V1Transaction => {
                Ok(StoreRecoveryAuthority::V1)
            }
            HotWalAuthority::V2Commit => Err(format_error(
                "Format v2 hot WAL has no authoritative public-v2 manifest",
            )),
        };
    }

    let manifest_bytes = fs::read(&manifest_path)?;
    let value: Value = serde_json::from_slice(&manifest_bytes)?;
    let format = value
        .get("format")
        .and_then(Value::as_str)
        .ok_or_else(|| format_error("manifest format is missing"))?;

    let public_format = if format == PRE_RELEASE_MANIFEST_FORMAT {
        PublicStoreFormat::V1
    } else {
        probe_public_manifest(&manifest_bytes).map_err(|error| format_error(error.to_string()))?
    };
    let dispatch = recovery_dispatch(public_format, wal)
        .map_err(|error| format_error(error.to_string()))?;
    match dispatch {
        RecoveryDispatch::V1TransactionWal => Ok(StoreRecoveryAuthority::V1),
        RecoveryDispatch::V2CommitWal => Ok(StoreRecoveryAuthority::V2),
    }
}

fn read_hot_authority_prefix(path: &Path) -> Result<Vec<u8>, CheckpointStoreError> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let mut file = File::open(path)?;
    let byte_count = usize::try_from(file.metadata()?.len().min(4))
        .map_err(|_| format_error("hot WAL authority prefix length exceeds usize"))?;
    let mut prefix = vec![0u8; byte_count];
    file.read_exact(&mut prefix)?;
    Ok(prefix)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tempfile::tempdir;

    fn write_manifest(dir: &Path, format: &str, version: u64) {
        fs::write(
            dir.join(MANIFEST_FILE),
            serde_json::to_vec(&json!({
                "format": format,
                "format_version": version,
            }))
            .unwrap(),
        )
        .unwrap();
    }

    fn write_hot(dir: &Path, magic: &[u8; 4]) {
        fs::write(dir.join(HOT_WAL_FILE), magic).unwrap();
    }

    #[test]
    fn empty_store_and_pre_release_manifest_stay_on_v1_compatibility_path() {
        let empty = tempdir().unwrap();
        assert_eq!(
            probe_store_recovery_authority(empty.path()).unwrap(),
            StoreRecoveryAuthority::V1
        );

        let pre_release = tempdir().unwrap();
        write_manifest(
            pre_release.path(),
            PRE_RELEASE_MANIFEST_FORMAT,
            PRE_RELEASE_REVISION_PRUNE,
        );
        write_hot(pre_release.path(), b"T2W1");
        assert_eq!(
            probe_store_recovery_authority(pre_release.path()).unwrap(),
            StoreRecoveryAuthority::V1
        );
    }

    #[test]
    fn public_v1_and_v2_dispatch_only_to_matching_outer_wal() {
        let v1 = tempdir().unwrap();
        write_manifest(v1.path(), crate::format::NAME, 1);
        write_hot(v1.path(), b"T2W1");
        assert_eq!(
            probe_store_recovery_authority(v1.path()).unwrap(),
            StoreRecoveryAuthority::V1
        );

        let v2 = tempdir().unwrap();
        write_manifest(v2.path(), crate::format::NAME, 2);
        write_hot(v2.path(), b"T2C2");
        assert_eq!(
            probe_store_recovery_authority(v2.path()).unwrap(),
            StoreRecoveryAuthority::V2
        );
    }

    #[test]
    fn mismatched_or_unenveloped_wal_fails_before_representation_parsing() {
        let v1_with_v2 = tempdir().unwrap();
        write_manifest(v1_with_v2.path(), crate::format::NAME, 1);
        write_hot(v1_with_v2.path(), b"T2C2");
        assert!(probe_store_recovery_authority(v1_with_v2.path()).is_err());

        let v2_with_v1 = tempdir().unwrap();
        write_manifest(v2_with_v1.path(), crate::format::NAME, 2);
        write_hot(v2_with_v1.path(), b"T2W1");
        assert!(probe_store_recovery_authority(v2_with_v1.path()).is_err());

        let bare_structural = tempdir().unwrap();
        write_manifest(bare_structural.path(), crate::format::NAME, 2);
        write_hot(bare_structural.path(), b"T2W2");
        let error = probe_store_recovery_authority(bare_structural.path()).unwrap_err();
        assert!(error
            .to_string()
            .contains("bare T2W2 structural transaction cannot be authoritative"));
    }

    #[test]
    fn v2_wal_without_v2_manifest_has_no_authority() {
        let dir = tempdir().unwrap();
        write_hot(dir.path(), b"T2C2");
        let error = probe_store_recovery_authority(dir.path()).unwrap_err();
        assert!(error
            .to_string()
            .contains("Format v2 hot WAL has no authoritative public-v2 manifest"));
    }
}
