//! Generation manifest for the domain-neutral history authority.
//!
//! One manifest identifies exactly one authoritative generation: an optional
//! immutable sealed snapshot plus the mutable hot log that continues it.
//! File names derive from the generation and never appear in the manifest,
//! so manifest bytes cannot introduce path authority:
//!
//! ```text
//! history-manifest.json
//! history-<20-digit-generation>.wal
//! history-snap-<20-digit-generation>.ths
//! history.lock                       (writer rendezvous, per store dir)
//! ```
//!
//! Media is compact JSON with one trailing newline. Parsing is strict and
//! hand-rolled over `serde_json::Value` (no derive dependency): unknown or
//! missing fields, mistyped values, bad hex, and trailing bytes all fail
//! closed. Duplicate JSON keys collapse in `Value` parsing exactly like the
//! existing format-authority probe; manifest publication writes atomically
//! via temporary file plus rename, so crash-torn duplicates cannot occur and
//! only a hand-forged manifest could smuggle one in (its bytes would still
//! have to survive the sealed digest binding to matter).

use super::HistoryError;
use sha2::{Digest, Sha256};
use std::fmt;

pub(crate) const HISTORY_MANIFEST_FILE: &str = "history-manifest.json";
pub(crate) const HISTORY_LOCK_FILE: &str = "history.lock";
pub(crate) const HISTORY_FORMAT_NAME: &str = "tulya-history-store";
pub(crate) const HISTORY_MANIFEST_SCHEMA: u64 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ManifestSealed {
    byte_len: u64,
    sha256: [u8; 32],
}

impl ManifestSealed {
    pub(crate) const fn byte_len(self) -> u64 {
        self.byte_len
    }

    pub(crate) const fn sha256(self) -> [u8; 32] {
        self.sha256
    }

    pub(crate) fn for_snapshot(byte_len: u64, snapshot_bytes: &[u8]) -> Self {
        Self {
            byte_len,
            sha256: sealed_artifact_digest(snapshot_bytes),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct HistoryManifest {
    generation: u64,
    sealed: Option<ManifestSealed>,
}

impl HistoryManifest {
    pub(crate) const fn generation(self) -> u64 {
        self.generation
    }

    pub(crate) const fn sealed(self) -> Option<ManifestSealed> {
        self.sealed
    }

    pub(crate) fn for_generation(generation: u64, sealed: Option<ManifestSealed>) -> Self {
        Self { generation, sealed }
    }
}

impl fmt::Display for HistoryManifest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.sealed {
            Some(sealed) => write!(
                formatter,
                "history generation {} sealed {} bytes",
                self.generation, sealed.byte_len
            ),
            None => write!(
                formatter,
                "history generation {} without sealed base",
                self.generation
            ),
        }
    }
}

/// Canonical hot-log filename for a generation: fixed 20-digit zero padding
/// covers the full `u64` range.
pub(crate) fn history_wal_filename(generation: u64) -> String {
    format!("history-{generation:020}.wal")
}

/// Canonical sealed-snapshot filename for a generation.
pub(crate) fn history_snapshot_filename(generation: u64) -> String {
    format!("history-snap-{generation:020}.ths")
}

pub(crate) fn encode_history_manifest(manifest: &HistoryManifest) -> Vec<u8> {
    let mut output = String::from("{\"format\":\"");
    output.push_str(HISTORY_FORMAT_NAME);
    output.push_str("\",\"manifest_schema\":");
    output.push_str(&HISTORY_MANIFEST_SCHEMA.to_string());
    output.push_str(",\"generation\":");
    output.push_str(&manifest.generation.to_string());
    output.push_str(",\"sealed\":");
    match manifest.sealed {
        None => output.push_str("null"),
        Some(sealed) => {
            output.push_str("{\"byte_len\":");
            output.push_str(&sealed.byte_len.to_string());
            output.push_str(",\"sha256\":\"");
            output.push_str(&hex_encode(sealed.sha256));
            output.push_str("\"}");
        }
    }
    output.push_str("}\n");
    output.into_bytes()
}

pub(crate) fn decode_history_manifest(bytes: &[u8]) -> Result<HistoryManifest, HistoryError> {
    let value: serde_json::Value = serde_json::from_slice(bytes)
        .map_err(|_| HistoryError::Invalid("history manifest JSON is malformed"))?;
    let object = value.as_object().ok_or(HistoryError::Invalid(
        "history manifest must be a JSON object",
    ))?;
    for key in object.keys() {
        match key.as_str() {
            "format" | "manifest_schema" | "generation" | "sealed" => {}
            _ => {
                return Err(HistoryError::Invalid(
                    "history manifest contains an unknown field",
                ));
            }
        }
    }
    let format = object
        .get("format")
        .and_then(serde_json::Value::as_str)
        .ok_or(HistoryError::Invalid("history manifest format is missing"))?;
    if format != HISTORY_FORMAT_NAME {
        return Err(HistoryError::Invalid(
            "history manifest format name mismatch",
        ));
    }
    match object
        .get("manifest_schema")
        .and_then(serde_json::Value::as_u64)
    {
        Some(schema) if schema == HISTORY_MANIFEST_SCHEMA => {}
        Some(_) => {
            return Err(HistoryError::Invalid(
                "history manifest schema is unsupported",
            ));
        }
        None => {
            return Err(HistoryError::Invalid("history manifest schema is missing"));
        }
    }
    let generation = object
        .get("generation")
        .and_then(serde_json::Value::as_u64)
        .ok_or(HistoryError::Invalid(
            "history manifest generation is missing",
        ))?;
    let sealed = match object.get("sealed") {
        None => {
            return Err(HistoryError::Invalid(
                "history manifest sealed authority is missing",
            ));
        }
        Some(serde_json::Value::Null) => None,
        Some(value) => {
            let sealed_object = value.as_object().ok_or(HistoryError::Invalid(
                "history manifest sealed authority must be an object",
            ))?;
            for key in sealed_object.keys() {
                match key.as_str() {
                    "byte_len" | "sha256" => {}
                    _ => {
                        return Err(HistoryError::Invalid(
                            "history manifest sealed authority contains an unknown field",
                        ));
                    }
                }
            }
            let byte_len = sealed_object
                .get("byte_len")
                .and_then(serde_json::Value::as_u64)
                .ok_or(HistoryError::Invalid(
                    "history manifest sealed byte length is missing",
                ))?;
            if byte_len == 0 {
                return Err(HistoryError::Invalid(
                    "history manifest sealed byte length must be positive",
                ));
            }
            let sha256 = sealed_object
                .get("sha256")
                .and_then(serde_json::Value::as_str)
                .ok_or(HistoryError::Invalid(
                    "history manifest sealed digest is missing",
                ))?;
            Some(ManifestSealed {
                byte_len,
                sha256: decode_sha256(sha256)?,
            })
        }
    };
    Ok(HistoryManifest { generation, sealed })
}

/// Binds sealed snapshot bytes to a manifest: plain SHA-256 over the exact
/// file bytes, deliberately under a different domain role than the snapshot
/// internal digest so the two bindings cannot be confused.
pub(crate) fn sealed_artifact_digest(bytes: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"tulya-history/v1/manifest-sealed\0");
    hasher.update(bytes);
    let digest = hasher.finalize();
    let mut output = [0u8; 32];
    output.copy_from_slice(&digest);
    output
}

fn decode_sha256(hex: &str) -> Result<[u8; 32], HistoryError> {
    let bytes = hex.as_bytes();
    if bytes.len() != 64 {
        return Err(HistoryError::Invalid(
            "history manifest sealed digest must be 64 hex characters",
        ));
    }
    let mut output = [0u8; 32];
    for (index, slot) in output.iter_mut().enumerate() {
        let high = hex_value(bytes[index * 2])?;
        let low = hex_value(bytes[index * 2 + 1])?;
        *slot = (high << 4) | low;
    }
    Ok(output)
}

fn hex_value(byte: u8) -> Result<u8, HistoryError> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        _ => Err(HistoryError::Invalid(
            "history manifest sealed digest must be lowercase hex",
        )),
    }
}

fn hex_encode(digest: [u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(64);
    for byte in digest {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sealed_manifest() -> HistoryManifest {
        HistoryManifest::for_generation(
            7,
            Some(ManifestSealed {
                byte_len: 12345,
                sha256: [0xAB; 32],
            }),
        )
    }

    #[test]
    fn manifest_golden_bytes_round_trip() {
        let empty = HistoryManifest::for_generation(0, None);
        let encoded = encode_history_manifest(&empty);
        assert_eq!(
            encoded,
            b"{\"format\":\"tulya-history-store\",\"manifest_schema\":1,\
              \"generation\":0,\"sealed\":null}\n"
        );
        assert_eq!(decode_history_manifest(&encoded).unwrap(), empty);

        let sealed = sealed_manifest();
        let encoded = encode_history_manifest(&sealed);
        let digest_hex = "ab".repeat(32);
        let expected = format!(
            "{{\"format\":\"tulya-history-store\",\"manifest_schema\":1,\
             \"generation\":7,\"sealed\":{{\"byte_len\":12345,\
             \"sha256\":\"{digest_hex}\"}}}}\n"
        );
        assert_eq!(encoded, expected.as_bytes());
        assert_eq!(decode_history_manifest(&encoded).unwrap(), sealed);
    }

    #[test]
    fn manifest_filenames_derive_from_generation() {
        assert_eq!(HISTORY_MANIFEST_FILE, "history-manifest.json");
        assert_eq!(history_wal_filename(0), "history-00000000000000000000.wal");
        assert_eq!(
            history_snapshot_filename(7),
            "history-snap-00000000000000000007.ths"
        );
        assert_eq!(
            history_wal_filename(u64::MAX),
            "history-18446744073709551615.wal"
        );
    }

    #[test]
    fn manifest_parser_fails_closed() {
        // Unknown field.
        assert!(decode_history_manifest(
            b"{\"format\":\"tulya-history-store\",\"manifest_schema\":1,\
              \"generation\":0,\"sealed\":null,\"extra\":1}"
        )
        .is_err());
        // Missing sealed.
        assert!(decode_history_manifest(
            b"{\"format\":\"tulya-history-store\",\"manifest_schema\":1,\
              \"generation\":0}"
        )
        .is_err());
        // Wrong name, schema, types.
        assert!(decode_history_manifest(
            b"{\"format\":\"other\",\"manifest_schema\":1,\
              \"generation\":0,\"sealed\":null}"
        )
        .is_err());
        assert!(decode_history_manifest(
            b"{\"format\":\"tulya-history-store\",\"manifest_schema\":2,\
              \"generation\":0,\"sealed\":null}"
        )
        .is_err());
        assert!(decode_history_manifest(
            b"{\"format\":\"tulya-history-store\",\"manifest_schema\":1,\
              \"generation\":\"7\",\"sealed\":null}"
        )
        .is_err());
        // Zero sealed length, bad digests.
        assert!(decode_history_manifest(
            b"{\"format\":\"tulya-history-store\",\"manifest_schema\":1,\
              \"generation\":7,\"sealed\":{\"byte_len\":0,\"sha256\":\"00\"}}"
        )
        .is_err());
        assert!(decode_history_manifest(
            b"{\"format\":\"tulya-history-store\",\"manifest_schema\":1,\
              \"generation\":7,\"sealed\":{\"byte_len\":9,\"sha256\":\"ABCD\"}}"
        )
        .is_err());
        // Malformed and trailing bytes.
        assert!(decode_history_manifest(b"{\"format\":").is_err());
        assert!(decode_history_manifest(b"").is_err());
        let mut trailed = encode_history_manifest(&sealed_manifest());
        trailed.extend_from_slice(b"{}");
        assert!(decode_history_manifest(&trailed).is_err());
        // Wrong top-level type.
        assert!(decode_history_manifest(b"[1,2,3]").is_err());
    }
}
