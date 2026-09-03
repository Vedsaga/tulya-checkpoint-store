//! Pure public-format authority probe for staged Format-v2 recovery dispatch.
//!
//! This module does not read or write files and does not change the current
//! writable public format. It only classifies already-read manifest/WAL bytes
//! so a later production recovery layer can dispatch without reinterpreting
//! Format-v1 records.

use serde_json::Value;
use std::fmt;

const PUBLIC_FORMAT_V1: u64 = 1;
const PUBLIC_FORMAT_V2: u64 = 2;
const WAL_V1_MAGIC: [u8; 4] = *b"T2W1";
const WAL_V2_COMMIT_MAGIC: [u8; 4] = *b"T2C2";
const WAL_V2_STRUCTURAL_MAGIC: [u8; 4] = *b"T2W2";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PublicStoreFormat {
    V1,
    V2,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HotWalAuthority {
    Empty,
    V1Transaction,
    V2Commit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RecoveryDispatch {
    V1TransactionWal,
    V2CommitWal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FormatAuthorityError {
    Invalid(&'static str),
    UnsupportedManifestVersion(u64),
    ManifestWalMismatch {
        manifest: PublicStoreFormat,
        wal: HotWalAuthority,
    },
}

impl fmt::Display for FormatAuthorityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(message) => formatter.write_str(message),
            Self::UnsupportedManifestVersion(version) => {
                write!(formatter, "unsupported checkpoint-store manifest version {version}")
            }
            Self::ManifestWalMismatch { manifest, wal } => write!(
                formatter,
                "checkpoint-store manifest/WAL authority mismatch: {manifest:?} manifest with {wal:?} WAL"
            ),
        }
    }
}

impl std::error::Error for FormatAuthorityError {}

pub(crate) fn probe_public_manifest(
    bytes: &[u8],
) -> Result<PublicStoreFormat, FormatAuthorityError> {
    let value: Value = serde_json::from_slice(bytes)
        .map_err(|_| FormatAuthorityError::Invalid("checkpoint-store manifest JSON is invalid"))?;
    probe_public_manifest_value(&value)
}

fn probe_public_manifest_value(
    value: &Value,
) -> Result<PublicStoreFormat, FormatAuthorityError> {
    let format = value
        .get("format")
        .and_then(Value::as_str)
        .ok_or(FormatAuthorityError::Invalid(
            "checkpoint-store manifest format is missing",
        ))?;
    if format != crate::format::NAME {
        return Err(FormatAuthorityError::Invalid(
            "checkpoint-store manifest format mismatch",
        ));
    }

    let version = value
        .get("format_version")
        .and_then(Value::as_u64)
        .ok_or(FormatAuthorityError::Invalid(
            "checkpoint-store manifest format version is missing",
        ))?;
    match version {
        PUBLIC_FORMAT_V1 => Ok(PublicStoreFormat::V1),
        PUBLIC_FORMAT_V2 => Ok(PublicStoreFormat::V2),
        other => Err(FormatAuthorityError::UnsupportedManifestVersion(other)),
    }
}

pub(crate) fn probe_hot_wal_prefix(
    bytes: &[u8],
) -> Result<HotWalAuthority, FormatAuthorityError> {
    if bytes.is_empty() || bytes.iter().take(4).all(|byte| *byte == 0) {
        return Ok(HotWalAuthority::Empty);
    }
    if bytes.len() < 4 {
        return Err(FormatAuthorityError::Invalid(
            "checkpoint-store WAL prefix is truncated",
        ));
    }

    let magic: [u8; 4] = bytes
        .get(..4)
        .ok_or(FormatAuthorityError::Invalid(
            "checkpoint-store WAL prefix is truncated",
        ))?
        .try_into()
        .map_err(|_| {
            FormatAuthorityError::Invalid("checkpoint-store WAL magic width mismatch")
        })?;
    match magic {
        WAL_V1_MAGIC => Ok(HotWalAuthority::V1Transaction),
        WAL_V2_COMMIT_MAGIC => Ok(HotWalAuthority::V2Commit),
        WAL_V2_STRUCTURAL_MAGIC => Err(FormatAuthorityError::Invalid(
            "bare T2W2 structural transaction cannot be authoritative",
        )),
        _ => Err(FormatAuthorityError::Invalid(
            "checkpoint-store WAL magic is unsupported",
        )),
    }
}

pub(crate) fn recovery_dispatch(
    manifest: PublicStoreFormat,
    wal: HotWalAuthority,
) -> Result<RecoveryDispatch, FormatAuthorityError> {
    match (manifest, wal) {
        (PublicStoreFormat::V1, HotWalAuthority::Empty | HotWalAuthority::V1Transaction) => {
            Ok(RecoveryDispatch::V1TransactionWal)
        }
        (PublicStoreFormat::V2, HotWalAuthority::Empty | HotWalAuthority::V2Commit) => {
            Ok(RecoveryDispatch::V2CommitWal)
        }
        (manifest, wal) => Err(FormatAuthorityError::ManifestWalMismatch { manifest, wal }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn manifest(version: u64) -> Vec<u8> {
        serde_json::to_vec(&json!({
            "format": crate::format::NAME,
            "format_version": version,
        }))
        .unwrap()
    }

    #[test]
    fn manifest_probe_distinguishes_v1_v2_and_rejects_unknown_versions() {
        assert_eq!(
            probe_public_manifest(&manifest(1)),
            Ok(PublicStoreFormat::V1)
        );
        assert_eq!(
            probe_public_manifest(&manifest(2)),
            Ok(PublicStoreFormat::V2)
        );
        assert_eq!(
            probe_public_manifest(&manifest(3)),
            Err(FormatAuthorityError::UnsupportedManifestVersion(3))
        );

        let wrong_name = serde_json::to_vec(&json!({
            "format": "other-format",
            "format_version": 1,
        }))
        .unwrap();
        assert_eq!(
            probe_public_manifest(&wrong_name),
            Err(FormatAuthorityError::Invalid(
                "checkpoint-store manifest format mismatch"
            ))
        );
    }

    #[test]
    fn wal_probe_recognizes_only_outer_authority_records() {
        assert_eq!(probe_hot_wal_prefix(&[]), Ok(HotWalAuthority::Empty));
        assert_eq!(
            probe_hot_wal_prefix(&[0, 0, 0, 0, 7, 8]),
            Ok(HotWalAuthority::Empty)
        );
        assert_eq!(
            probe_hot_wal_prefix(b"T2W1payload"),
            Ok(HotWalAuthority::V1Transaction)
        );
        assert_eq!(
            probe_hot_wal_prefix(b"T2C2payload"),
            Ok(HotWalAuthority::V2Commit)
        );
        assert_eq!(
            probe_hot_wal_prefix(b"T2W2payload"),
            Err(FormatAuthorityError::Invalid(
                "bare T2W2 structural transaction cannot be authoritative"
            ))
        );
        assert_eq!(
            probe_hot_wal_prefix(b"T2X9payload"),
            Err(FormatAuthorityError::Invalid(
                "checkpoint-store WAL magic is unsupported"
            ))
        );
        assert_eq!(
            probe_hot_wal_prefix(b"T2"),
            Err(FormatAuthorityError::Invalid(
                "checkpoint-store WAL prefix is truncated"
            ))
        );
    }

    #[test]
    fn recovery_dispatch_rejects_cross_version_manifest_wal_pairs() {
        assert_eq!(
            recovery_dispatch(PublicStoreFormat::V1, HotWalAuthority::Empty),
            Ok(RecoveryDispatch::V1TransactionWal)
        );
        assert_eq!(
            recovery_dispatch(PublicStoreFormat::V1, HotWalAuthority::V1Transaction),
            Ok(RecoveryDispatch::V1TransactionWal)
        );
        assert_eq!(
            recovery_dispatch(PublicStoreFormat::V2, HotWalAuthority::Empty),
            Ok(RecoveryDispatch::V2CommitWal)
        );
        assert_eq!(
            recovery_dispatch(PublicStoreFormat::V2, HotWalAuthority::V2Commit),
            Ok(RecoveryDispatch::V2CommitWal)
        );
        assert_eq!(
            recovery_dispatch(PublicStoreFormat::V1, HotWalAuthority::V2Commit),
            Err(FormatAuthorityError::ManifestWalMismatch {
                manifest: PublicStoreFormat::V1,
                wal: HotWalAuthority::V2Commit,
            })
        );
        assert_eq!(
            recovery_dispatch(PublicStoreFormat::V2, HotWalAuthority::V1Transaction),
            Err(FormatAuthorityError::ManifestWalMismatch {
                manifest: PublicStoreFormat::V2,
                wal: HotWalAuthority::V1Transaction,
            })
        );
    }

    #[test]
    fn current_writer_version_stays_v1_until_migration_is_authoritative() {
        assert_eq!(crate::format::VERSION, 1);
    }
}
