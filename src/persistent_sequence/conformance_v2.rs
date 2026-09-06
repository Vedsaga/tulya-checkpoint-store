use super::apply_v2::{V2ApplyError, V2CommittedState, V2RequestRecord, V2RequestStatus};
use super::commit_v2::checkpoint_operation_digest;
use super::format_v2::{V2NodeRecord, V2RootRecord};
use super::publication_v2::{checkpoint_state_metadata, V2CheckpointRecord};
use serde_json::Value;

const FIXTURE: &str = include_str!("format_v2_conformance.json");
const SCHEMA: &str = "tulya-format-v2-conformance-v1";

fn fixture() -> Value {
    serde_json::from_str(FIXTURE).expect("Format-v2 conformance fixture must be valid JSON")
}

fn object<'a>(value: &'a Value, field: &str) -> &'a Value {
    value
        .get(field)
        .unwrap_or_else(|| panic!("missing conformance field {field}"))
}

fn array<'a>(value: &'a Value, field: &str) -> &'a [Value] {
    object(value, field)
        .as_array()
        .unwrap_or_else(|| panic!("conformance field {field} must be an array"))
}

fn string<'a>(value: &'a Value, field: &str) -> &'a str {
    object(value, field)
        .as_str()
        .unwrap_or_else(|| panic!("conformance field {field} must be a string"))
}

fn number(value: &Value, field: &str) -> u64 {
    object(value, field)
        .as_u64()
        .unwrap_or_else(|| panic!("conformance field {field} must be an unsigned number"))
}

fn optional_string(value: &Value, field: &str) -> Option<String> {
    match object(value, field) {
        Value::Null => None,
        value => Some(
            value
                .as_str()
                .unwrap_or_else(|| panic!("conformance field {field} must be a string or null"))
                .to_owned(),
        ),
    }
}

fn decode_hex(value: &str) -> Vec<u8> {
    assert!(
        value.len() % 2 == 0,
        "conformance hex value must have even length"
    );
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let high = (pair[0] as char)
                .to_digit(16)
                .unwrap_or_else(|| panic!("invalid conformance hex digit"))
                as u8;
            let low = (pair[1] as char)
                .to_digit(16)
                .unwrap_or_else(|| panic!("invalid conformance hex digit"))
                as u8;
            (high << 4) | low
        })
        .collect()
}

fn decode_digest(value: &str) -> [u8; 32] {
    let bytes = decode_hex(value);
    bytes
        .try_into()
        .unwrap_or_else(|_| panic!("conformance operation digest must be 32 bytes"))
}

#[test]
fn operation_digest_vectors_match_the_canonical_rust_digest() {
    let fixture = fixture();
    assert_eq!(string(&fixture, "schema"), SCHEMA);

    for vector in array(&fixture, "operation_digest_vectors") {
        let payload = decode_hex(string(vector, "state_payload_hex"));
        let node = V2NodeRecord::leaf(0, &payload).unwrap();
        let root = V2RootRecord::from_node(0, node).unwrap();
        let state = checkpoint_state_metadata(root, None, None).unwrap();
        let checkpoint = V2CheckpointRecord {
            checkpoint_no: u32::try_from(number(vector, "checkpoint_no")).unwrap(),
            thread_id: string(vector, "thread_id").to_owned(),
            checkpoint_id: string(vector, "checkpoint_id").to_owned(),
            parent_checkpoint_id: optional_string(vector, "parent_checkpoint_id"),
            identity_version: u32::try_from(number(vector, "identity_version")).unwrap(),
            messages_version: None,
            result_version: None,
            state,
        };
        let actual = checkpoint_operation_digest(&checkpoint).unwrap();
        assert_eq!(
            actual,
            decode_digest(string(vector, "expected_operation_digest_hex")),
            "operation digest vector {} disagrees",
            string(vector, "name")
        );
    }
}

#[test]
fn request_lifecycle_vectors_match_the_durable_ledger_contract() {
    let fixture = fixture();
    assert_eq!(string(&fixture, "schema"), SCHEMA);
    let mut seen = std::collections::HashSet::new();

    for vector in array(&fixture, "request_lifecycle_vectors") {
        let name = string(vector, "name");
        assert!(seen.insert(name), "duplicate conformance vector {name}");
        let mut state = V2CommittedState::default();

        for record in array(vector, "active") {
            let request_id = decode_hex(string(record, "request_id_hex"));
            let previous = state.request_records.insert(
                request_id,
                V2RequestRecord {
                    operation_digest: decode_digest(string(record, "operation_digest_hex")),
                    checkpoint_ordinal: number(record, "checkpoint_ordinal"),
                },
            );
            assert!(
                previous.is_none(),
                "duplicate active request in vector {name}"
            );
        }
        for record in array(vector, "retired") {
            let request_id = decode_hex(string(record, "request_id_hex"));
            let previous = state.retired_requests.insert(
                request_id,
                decode_digest(string(record, "operation_digest_hex")),
            );
            assert!(
                previous.is_none(),
                "duplicate retired request in vector {name}"
            );
        }
        for request_id in state.request_records.keys() {
            assert!(
                !state.retired_requests.contains_key(request_id),
                "active/retired overlap in vector {name}"
            );
        }

        let active_before = state.request_records.clone();
        let retired_before = state.retired_requests.clone();
        let query = object(vector, "query");
        let result = state.classify_request(
            &decode_hex(string(query, "request_id_hex")),
            decode_digest(string(query, "operation_digest_hex")),
        );
        match string(vector, "expected") {
            "new" => assert_eq!(result, Ok(V2RequestStatus::New), "vector {name}"),
            "replay" => {
                let ordinal = number(vector, "expected_checkpoint_ordinal");
                assert_eq!(
                    result,
                    Ok(V2RequestStatus::Replay {
                        checkpoint_ordinal: ordinal
                    }),
                    "vector {name}"
                );
            }
            "retired" => assert_eq!(result, Ok(V2RequestStatus::Retired), "vector {name}"),
            "conflict" => assert_eq!(result, Err(V2ApplyError::RequestConflict), "vector {name}"),
            expected => panic!("unknown conformance outcome {expected}"),
        }
        assert!(!object(vector, "mutates").as_bool().unwrap());
        assert_eq!(state.request_records, active_before, "vector {name}");
        assert_eq!(state.retired_requests, retired_before, "vector {name}");
    }
}
