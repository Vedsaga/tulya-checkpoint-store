use super::apply_v2::{
    apply_v2_commit, V2ApplyError, V2CommittedState, V2RequestRecord, V2RequestStatus,
};
use super::avl::V2AvlSequence;
use super::commit_v2::{checkpoint_operation_digest, decode_v2_commit, encode_v2_commit};
use super::compaction_v2::prepare_v2_compaction;
use super::format_v2::{
    decode_v2_node, decode_v2_root, encode_v2_node, encode_v2_root, V2NodeRecord, V2RootRecord,
};
use super::hot_frame_v2::encode_v2_hot_frame;
use super::publication_v2::{
    checkpoint_state_metadata, V2CheckpointRecord, V2StateMetadata, V2VersionRecord,
};
use super::recovery_v2::{recover_v2_hot_wal, V2RecoveryStop};
use super::snapshot_v2::{
    decode_v2_sealed_snapshot, encode_v2_sealed_snapshot, snapshot_digest_input,
    V2ActiveRequestRecord, V2DeletedCheckpointRecord, V2RetiredRequestRecord, V2SealedSnapshot,
};
use super::transaction_v2::{V2WalGeometry, V2WalTransaction};
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

fn encode_hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        output.push(char::from(DIGITS[usize::from(*byte >> 4)]));
        output.push(char::from(DIGITS[usize::from(*byte & 0x0f)]));
    }
    output
}

fn decode_digest(value: &str) -> [u8; 32] {
    let bytes = decode_hex(value);
    bytes
        .try_into()
        .unwrap_or_else(|_| panic!("conformance operation digest must be 32 bytes"))
}

fn string_pair(value: &Value) -> (&str, &str) {
    let values = value
        .as_array()
        .unwrap_or_else(|| panic!("conformance identity must be a two-item array"));
    assert_eq!(values.len(), 2, "conformance identity must have two items");
    (
        values[0]
            .as_str()
            .unwrap_or_else(|| panic!("conformance identity thread must be a string")),
        values[1]
            .as_str()
            .unwrap_or_else(|| panic!("conformance identity checkpoint must be a string")),
    )
}

fn deletion_checkpoint(record: &Value, state: V2StateMetadata) -> V2CheckpointRecord {
    V2CheckpointRecord {
        checkpoint_no: u32::try_from(number(record, "checkpoint_no")).unwrap(),
        thread_id: string(record, "thread_id").to_owned(),
        checkpoint_id: string(record, "checkpoint_id").to_owned(),
        parent_checkpoint_id: optional_string(record, "parent_checkpoint_id"),
        identity_version: u32::try_from(number(record, "identity_version")).unwrap(),
        messages_version: None,
        result_version: None,
        state,
    }
}

fn one_checkpoint_snapshot() -> V2SealedSnapshot {
    let mut sequence = V2AvlSequence::default();
    let root = sequence.append(None, b"abc").unwrap().root();
    let image = sequence.export_image(&[root]).unwrap();
    let version = V2VersionRecord::new(0, None, root).unwrap();
    let checkpoint = V2CheckpointRecord {
        checkpoint_no: 1,
        thread_id: "thread".to_owned(),
        checkpoint_id: "cp-1".to_owned(),
        parent_checkpoint_id: None,
        identity_version: 0,
        messages_version: None,
        result_version: None,
        state: checkpoint_state_metadata(root, None, None).unwrap(),
    };
    let operation_digest = checkpoint_operation_digest(&checkpoint).unwrap();
    V2SealedSnapshot {
        image,
        versions: vec![version],
        checkpoints: vec![checkpoint],
        active_requests: vec![
            V2ActiveRequestRecord::new(b"req-1".to_vec(), operation_digest, 0).unwrap(),
        ],
        retired_requests: Vec::new(),
        deleted_checkpoints: Vec::new(),
    }
}

fn recovery_first_transaction() -> V2WalTransaction {
    let node = V2NodeRecord::leaf(0, b"abc").unwrap();
    let root = V2RootRecord::from_node(0, node).unwrap();
    V2WalTransaction {
        payload: b"abc".to_vec(),
        nodes: vec![node],
        versions: vec![V2VersionRecord::new(0, None, root).unwrap()],
        checkpoint: V2CheckpointRecord {
            checkpoint_no: 1,
            thread_id: "thread".to_owned(),
            checkpoint_id: "cp-1".to_owned(),
            parent_checkpoint_id: None,
            identity_version: 0,
            messages_version: None,
            result_version: None,
            state: checkpoint_state_metadata(root, None, None).unwrap(),
        },
    }
}

fn recovery_second_transaction(base: V2WalGeometry) -> V2WalTransaction {
    let old = V2NodeRecord::leaf(0, b"abc").unwrap();
    let old_root = V2RootRecord::from_node(0, old).unwrap();
    let leaf = V2NodeRecord::leaf(base.payload_len, b"XYZ").unwrap();
    let leaf_root = V2RootRecord::from_node(base.node_count, leaf).unwrap();
    let branch = V2NodeRecord::branch(old_root, leaf_root).unwrap();
    let branch_root = V2RootRecord::from_node(base.node_count + 1, branch).unwrap();
    let version_id = u32::try_from(base.version_count).unwrap();
    V2WalTransaction {
        payload: b"XYZ".to_vec(),
        nodes: vec![leaf, branch],
        versions: vec![V2VersionRecord::new(version_id, Some(0), branch_root).unwrap()],
        checkpoint: V2CheckpointRecord {
            checkpoint_no: 2,
            thread_id: "thread".to_owned(),
            checkpoint_id: "cp-2".to_owned(),
            parent_checkpoint_id: Some("cp-1".to_owned()),
            identity_version: version_id,
            messages_version: None,
            result_version: None,
            state: checkpoint_state_metadata(branch_root, None, None).unwrap(),
        },
    }
}

fn recovery_frame(
    base: V2WalGeometry,
    transaction: &V2WalTransaction,
    request_id: &[u8],
) -> Vec<u8> {
    let commit = encode_v2_commit(base, transaction, Some(request_id)).unwrap();
    encode_v2_hot_frame(&commit).unwrap()
}

#[test]
fn structural_commitment_vectors_match_canonical_node_and_root_codecs() {
    let fixture = fixture();
    assert_eq!(string(&fixture, "schema"), SCHEMA);

    for vector in array(&fixture, "structural_commitment_vectors") {
        let kind = string(vector, "kind");
        let (node, root) = match kind {
            "leaf" => {
                let node = V2NodeRecord::leaf(
                    number(vector, "payload_offset"),
                    &decode_hex(string(vector, "payload_hex")),
                )
                .unwrap();
                let root = V2RootRecord::from_node(number(vector, "node_id"), node).unwrap();
                (node, root)
            }
            "branch" => {
                let left = object(vector, "left");
                let left_node = V2NodeRecord::leaf(
                    number(left, "payload_offset"),
                    &decode_hex(string(left, "payload_hex")),
                )
                .unwrap();
                let left_root =
                    V2RootRecord::from_node(number(left, "node_id"), left_node).unwrap();
                let right = object(vector, "right");
                let right_node = V2NodeRecord::leaf(
                    number(right, "payload_offset"),
                    &decode_hex(string(right, "payload_hex")),
                )
                .unwrap();
                let right_root =
                    V2RootRecord::from_node(number(right, "node_id"), right_node).unwrap();
                let node = V2NodeRecord::branch(left_root, right_root).unwrap();
                let root = V2RootRecord::from_node(number(vector, "root_node_id"), node).unwrap();
                (node, root)
            }
            other => panic!("unknown structural vector kind {other}"),
        };
        assert_eq!(
            encode_hex(&encode_v2_node(node)),
            string(vector, "expected_node_hex"),
            "node encoding vector {} disagrees",
            string(vector, "name")
        );
        assert_eq!(
            encode_hex(&encode_v2_root(root)),
            string(vector, "expected_root_hex"),
            "root encoding vector {} disagrees",
            string(vector, "name")
        );
    }
}

#[test]
fn structural_history_vectors_preserve_old_roots_and_sibling_independence() {
    let fixture = fixture();
    assert_eq!(string(&fixture, "schema"), SCHEMA);

    for vector in array(&fixture, "structural_history_vectors") {
        let mut sequence = V2AvlSequence::default();
        let base = sequence
            .append(None, &decode_hex(string(vector, "base_payload_hex")))
            .unwrap()
            .root();
        let base_encoding = encode_hex(&encode_v2_root(base));
        let left = sequence
            .append(Some(base), &decode_hex(string(vector, "left_append_hex")))
            .unwrap()
            .root();
        let right = sequence
            .append(Some(base), &decode_hex(string(vector, "right_append_hex")))
            .unwrap()
            .root();

        assert_eq!(
            sequence.read_range(base, 0, base.logical_len()).unwrap(),
            decode_hex(string(vector, "expected_parent_hex")),
            "parent payload vector {} disagrees",
            string(vector, "name")
        );
        assert_eq!(
            sequence.read_range(left, 0, left.logical_len()).unwrap(),
            decode_hex(string(vector, "expected_left_hex")),
            "left payload vector {} disagrees",
            string(vector, "name")
        );
        assert_eq!(
            sequence.read_range(right, 0, right.logical_len()).unwrap(),
            decode_hex(string(vector, "expected_right_hex")),
            "right payload vector {} disagrees",
            string(vector, "name")
        );
        assert_eq!(
            encode_hex(&encode_v2_root(base)),
            base_encoding,
            "historical parent root changed for vector {}",
            string(vector, "name")
        );
        assert_ne!(
            left.commitment(),
            right.commitment(),
            "sibling roots unexpectedly share a commitment for vector {}",
            string(vector, "name")
        );
        sequence.verify_root(base).unwrap();
        sequence.verify_root(left).unwrap();
        sequence.verify_root(right).unwrap();
    }
}

#[test]
fn structural_invalid_vectors_fail_closed() {
    let fixture = fixture();
    assert_eq!(string(&fixture, "schema"), SCHEMA);
    for vector in array(&fixture, "structural_invalid_vectors") {
        match string(vector, "kind") {
            "node-height" => {
                let node = V2NodeRecord::leaf(0, b"abc").unwrap();
                let mut encoded = encode_v2_node(node).to_vec();
                encoded[6] = 2;
                assert!(decode_v2_node(&encoded).is_err());
            }
            "root-flags" => {
                let node = V2NodeRecord::leaf(0, b"abc").unwrap();
                let root = V2RootRecord::from_node(0, node).unwrap();
                let mut encoded = encode_v2_root(root).to_vec();
                encoded[5] = 1;
                assert!(decode_v2_root(&encoded).is_err());
            }
            kind => panic!("unknown invalid structural vector kind {kind}"),
        }
        assert_eq!(string(vector, "expected"), "reject");
    }
}

fn compaction_genesis_transaction(checkpoint_id: &str, payload: &[u8]) -> V2WalTransaction {
    let node = V2NodeRecord::leaf(0, payload).unwrap();
    let root = V2RootRecord::from_node(0, node).unwrap();
    V2WalTransaction {
        payload: payload.to_vec(),
        nodes: vec![node],
        versions: vec![V2VersionRecord::new(0, None, root).unwrap()],
        checkpoint: V2CheckpointRecord {
            checkpoint_no: 1,
            thread_id: "thread".to_owned(),
            checkpoint_id: checkpoint_id.to_owned(),
            parent_checkpoint_id: None,
            identity_version: 0,
            messages_version: None,
            result_version: None,
            state: checkpoint_state_metadata(root, None, None).unwrap(),
        },
    }
}

fn compaction_branch_child_transaction(
    base: V2WalGeometry,
    checkpoint_no: u32,
    thread_id: &str,
    checkpoint_id: &str,
    parent_checkpoint_id: Option<&str>,
    parent_version: u32,
    parent_root: V2RootRecord,
    payload: &[u8],
) -> V2WalTransaction {
    let leaf = V2NodeRecord::leaf(base.payload_len, payload).unwrap();
    let leaf_root = V2RootRecord::from_node(base.node_count, leaf).unwrap();
    let branch = V2NodeRecord::branch(parent_root, leaf_root).unwrap();
    let branch_root = V2RootRecord::from_node(base.node_count + 1, branch).unwrap();
    let version_id = u32::try_from(base.version_count).unwrap();
    V2WalTransaction {
        payload: payload.to_vec(),
        nodes: vec![leaf, branch],
        versions: vec![
            V2VersionRecord::new(version_id, Some(parent_version), branch_root).unwrap(),
        ],
        checkpoint: V2CheckpointRecord {
            checkpoint_no,
            thread_id: thread_id.to_owned(),
            checkpoint_id: checkpoint_id.to_owned(),
            parent_checkpoint_id: parent_checkpoint_id.map(str::to_owned),
            identity_version: version_id,
            messages_version: None,
            result_version: None,
            state: checkpoint_state_metadata(branch_root, None, None).unwrap(),
        },
    }
}

fn compaction_sparse_state() -> V2CommittedState {
    let mut state = V2CommittedState::default();
    let genesis = compaction_genesis_transaction("cp-1", b"aaa");
    let base = state.geometry().unwrap();
    let encoded = encode_v2_commit(base, &genesis, Some(b"req-1")).unwrap();
    apply_v2_commit(&mut state, &encoded).unwrap();
    let genesis_root = state.versions[0].root();

    let base = state.geometry().unwrap();
    let second = compaction_branch_child_transaction(
        base,
        2,
        "thread",
        "cp-2",
        Some("cp-1"),
        0,
        genesis_root,
        b"bbb",
    );
    let encoded = encode_v2_commit(base, &second, Some(b"req-2")).unwrap();
    apply_v2_commit(&mut state, &encoded).unwrap();
    let second_root = state.versions[1].root();

    let base = state.geometry().unwrap();
    let third = compaction_branch_child_transaction(
        base,
        3,
        "thread",
        "cp-3",
        Some("cp-2"),
        1,
        second_root,
        b"ccc",
    );
    let encoded = encode_v2_commit(base, &third, Some(b"req-3")).unwrap();
    apply_v2_commit(&mut state, &encoded).unwrap();

    let base = state.geometry().unwrap();
    let sibling = compaction_branch_child_transaction(
        base,
        4,
        "thread",
        "cp-sibling",
        Some("cp-1"),
        0,
        genesis_root,
        b"ddd",
    );
    let encoded = encode_v2_commit(base, &sibling, Some(b"req-4")).unwrap();
    apply_v2_commit(&mut state, &encoded).unwrap();

    let base = state.geometry().unwrap();
    let other = compaction_branch_child_transaction(
        base,
        5,
        "other-thread",
        "other-root",
        None,
        0,
        genesis_root,
        b"eee",
    );
    let encoded = encode_v2_commit(base, &other, Some(b"req-5")).unwrap();
    apply_v2_commit(&mut state, &encoded).unwrap();

    let prepared_delete = state
        .prepare_delete_checkpoint_subtree("thread", "cp-2")
        .unwrap();
    state.apply_prepared_delete_checkpoint_subtree(prepared_delete);
    state
}

#[test]
fn compaction_vectors_preserve_logical_checkpoints_and_digests() {
    let fixture = fixture();
    assert_eq!(string(&fixture, "schema"), SCHEMA);

    for vector in array(&fixture, "compaction_vectors") {
        assert_eq!(string(vector, "kind"), "deleted-middle-subtree");
        let state = compaction_sparse_state();
        let source_digests = state
            .checkpoints
            .iter()
            .map(checkpoint_operation_digest)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let prepared = prepare_v2_compaction(&state).unwrap();

        assert_eq!(
            prepared.payload(),
            decode_hex(string(vector, "expected_compact_payload_hex"))
        );
        assert_eq!(
            prepared.nodes().len(),
            number(vector, "expected_compact_node_count") as usize
        );
        assert_eq!(
            prepared.versions().len(),
            number(vector, "expected_compact_version_count") as usize
        );
        assert_eq!(
            prepared.checkpoints().len(),
            number(vector, "expected_compact_checkpoint_count") as usize
        );
        for (index, expected) in array(vector, "expected_live").iter().enumerate() {
            let (thread_id, checkpoint_id) = string_pair(expected);
            let checkpoint = &prepared.checkpoints()[index];
            assert_eq!(checkpoint.thread_id, thread_id);
            assert_eq!(checkpoint.checkpoint_id, checkpoint_id);
            assert_eq!(
                checkpoint.identity_version,
                array(vector, "expected_identity_versions")[index]
                    .as_u64()
                    .unwrap() as u32
            );
        }
        let compact_digests = prepared
            .checkpoints()
            .iter()
            .map(checkpoint_operation_digest)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(source_digests, compact_digests);
    }
}

#[test]
fn snapshot_vectors_match_canonical_schema2_bytes_and_reopen() {
    let fixture = fixture();
    assert_eq!(string(&fixture, "schema"), SCHEMA);

    for vector in array(&fixture, "snapshot_vectors") {
        let snapshot = match string(vector, "kind") {
            "one-checkpoint" => one_checkpoint_snapshot(),
            "tombstone-only" => V2SealedSnapshot {
                image: Vec::new(),
                versions: Vec::new(),
                checkpoints: Vec::new(),
                active_requests: Vec::new(),
                retired_requests: Vec::new(),
                deleted_checkpoints: vec![V2DeletedCheckpointRecord::new(
                    string(vector, "thread_id").to_owned(),
                    string(vector, "checkpoint_id").to_owned(),
                )
                .unwrap()],
            },
            kind => panic!("unknown snapshot vector kind {kind}"),
        };
        let encoded = encode_v2_sealed_snapshot(&snapshot).unwrap();
        assert_eq!(
            encoded.len(),
            number(vector, "expected_length") as usize,
            "snapshot length vector {} disagrees",
            string(vector, "name")
        );
        assert_eq!(
            encode_hex(&encoded[64..96]),
            string(vector, "expected_digest_hex"),
            "snapshot digest vector {} disagrees",
            string(vector, "name")
        );
        if let Some(expected_input) = vector
            .get("expected_digest_input_hex")
            .and_then(Value::as_str)
        {
            let input = snapshot_digest_input(&encoded[..64], &encoded[96..]);
            assert_eq!(
                input.len(),
                number(vector, "expected_digest_input_length") as usize,
                "snapshot digest-input length vector {} disagrees",
                string(vector, "name")
            );
            assert_eq!(
                encode_hex(&input),
                expected_input,
                "snapshot digest-input vector {} disagrees",
                string(vector, "name")
            );
        }
        assert_eq!(
            decode_v2_sealed_snapshot(&encoded).unwrap(),
            snapshot,
            "snapshot reopen vector {} disagrees",
            string(vector, "name")
        );
    }
}

#[test]
fn recovery_vectors_match_old_or_new_hot_wal_stops() {
    let fixture = fixture();
    assert_eq!(string(&fixture, "schema"), SCHEMA);
    let first = recovery_first_transaction();
    let first_frame = recovery_frame(V2WalGeometry::default(), &first, b"req-1");
    let base = V2WalGeometry {
        payload_len: 3,
        node_count: 1,
        version_count: 1,
        checkpoint_count: 1,
    };
    let second = recovery_second_transaction(base);
    let second_frame = recovery_frame(base, &second, b"req-2");

    for vector in array(&fixture, "recovery_vectors") {
        let mut wal = first_frame.clone();
        match string(vector, "kind") {
            "two-frames-zero-reserve" => {
                wal.extend_from_slice(&second_frame);
                wal.resize(wal.len() + 1024, 0);
            }
            "torn-second-frame" => {
                let second_start = wal.len();
                wal.resize(second_start + second_frame.len() + 512, 0);
                let written = second_frame.len() / 2;
                wal[second_start..second_start + written].copy_from_slice(&second_frame[..written]);
            }
            kind => panic!("unknown recovery vector kind {kind}"),
        }

        let recovered = recover_v2_hot_wal(&wal, V2CommittedState::default()).unwrap();
        let expected_stop = match string(vector, "expected_stop") {
            "zero_reserve" => V2RecoveryStop::ZeroReserve,
            "torn_final_commit" => V2RecoveryStop::TornFinalCommit,
            stop => panic!("unknown recovery stop {stop}"),
        };
        assert_eq!(
            recovered.commit_count,
            number(vector, "expected_commit_count"),
            "recovery commit count vector {} disagrees",
            string(vector, "name")
        );
        assert_eq!(
            recovered.stop,
            expected_stop,
            "recovery stop vector {} disagrees",
            string(vector, "name")
        );
        assert_eq!(
            recovered.state.geometry().unwrap().checkpoint_count,
            number(vector, "expected_checkpoint_count"),
            "recovery semantic state vector {} disagrees",
            string(vector, "name")
        );
    }
}

#[test]
fn recovery_invalid_vectors_fail_closed() {
    let fixture = fixture();
    assert_eq!(string(&fixture, "schema"), SCHEMA);
    let first = recovery_first_transaction();
    let first_frame = recovery_frame(V2WalGeometry::default(), &first, b"req-1");
    let base = V2WalGeometry {
        payload_len: 3,
        node_count: 1,
        version_count: 1,
        checkpoint_count: 1,
    };
    let second = recovery_second_transaction(base);
    let second_frame = recovery_frame(base, &second, b"req-2");

    for vector in array(&fixture, "recovery_invalid_vectors") {
        let wal = match string(vector, "kind") {
            "reserve-garbage" => {
                let mut wal = first_frame.clone();
                wal.extend_from_slice(&[0, 0, 0, 0, 9]);
                wal
            }
            "bare-structural" => b"T2W2payload".to_vec(),
            "corrupt-complete-frame" => {
                let mut corrupt_second = second_frame.clone();
                let commit_last = corrupt_second.len() - 40 - 1;
                corrupt_second[commit_last] ^= 1;
                let mut wal = first_frame.clone();
                wal.extend_from_slice(&corrupt_second);
                wal
            }
            "duplicate-physical-retry" => {
                let mut wal = first_frame.clone();
                wal.extend_from_slice(&first_frame);
                wal
            }
            kind => panic!("unknown invalid recovery vector kind {kind}"),
        };
        assert_eq!(string(vector, "expected"), "reject");
        assert!(
            recover_v2_hot_wal(&wal, V2CommittedState::default()).is_err(),
            "invalid recovery vector {} was accepted",
            string(vector, "name")
        );
    }
}

#[test]
fn commit_envelope_vectors_match_requestless_and_requestful_rules() {
    let fixture = fixture();
    assert_eq!(string(&fixture, "schema"), SCHEMA);
    let transaction = recovery_first_transaction();

    for vector in array(&fixture, "commit_envelope_vectors") {
        let request_id = optional_string(vector, "request_id_hex").map(|value| decode_hex(&value));
        let encoded = encode_v2_commit(
            V2WalGeometry::default(),
            &transaction,
            request_id.as_deref(),
        )
        .unwrap();
        let decoded = decode_v2_commit(&encoded).unwrap();
        assert_eq!(
            decoded.request_id,
            request_id,
            "request identity vector {} disagrees",
            string(vector, "name")
        );
        assert_eq!(
            decoded.operation_digest,
            decode_digest(string(vector, "expected_operation_digest_hex")),
            "commit operation-digest vector {} disagrees",
            string(vector, "name")
        );
    }
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

#[test]
fn invalid_snapshot_vectors_fail_closed_without_persisting_overlap() {
    let fixture = fixture();
    assert_eq!(string(&fixture, "schema"), SCHEMA);

    for vector in array(&fixture, "invalid_snapshot_vectors") {
        match string(vector, "kind") {
            "active-retired-overlap" => {
                let mut snapshot = one_checkpoint_snapshot();
                let digest = snapshot.active_requests[0].operation_digest();
                snapshot
                    .retired_requests
                    .push(V2RetiredRequestRecord::new(b"req-1".to_vec(), digest).unwrap());
                assert!(
                    encode_v2_sealed_snapshot(&snapshot).is_err(),
                    "invalid snapshot vector {} was accepted",
                    string(vector, "name")
                );
            }
            kind => panic!("unknown invalid snapshot vector kind {kind}"),
        }
        assert_eq!(string(vector, "expected"), "reject");
    }
}

#[test]
fn deletion_vectors_match_tombstone_retirement_and_ordinal_remapping() {
    let fixture = fixture();
    assert_eq!(string(&fixture, "schema"), SCHEMA);

    for vector in array(&fixture, "deletion_vectors") {
        let name = string(vector, "name");
        let payload = decode_hex(string(vector, "base_state_payload_hex"));
        let node = V2NodeRecord::leaf(0, &payload).unwrap();
        let root = V2RootRecord::from_node(0, node).unwrap();
        let state_metadata = checkpoint_state_metadata(root, None, None).unwrap();
        let mut state = V2CommittedState::default();

        for (index, record) in array(vector, "checkpoints").iter().enumerate() {
            let checkpoint = deletion_checkpoint(record, state_metadata);
            let request_id = decode_hex(string(record, "request_id_hex"));
            let transaction = if index == 0 {
                V2WalTransaction {
                    payload: payload.clone(),
                    nodes: vec![node],
                    versions: vec![V2VersionRecord::new(0, None, root).unwrap()],
                    checkpoint,
                }
            } else {
                V2WalTransaction {
                    payload: Vec::new(),
                    nodes: Vec::new(),
                    versions: Vec::new(),
                    checkpoint,
                }
            };
            let base = state.geometry().unwrap();
            let encoded = encode_v2_commit(base, &transaction, Some(&request_id)).unwrap();
            assert_eq!(
                super::apply_v2::apply_v2_commit(&mut state, &encoded),
                Ok(super::apply_v2::V2ApplyOutcome::Applied {
                    checkpoint_ordinal: index as u64
                }),
                "setup commit for deletion vector {name}"
            );
        }

        let before = state.geometry().unwrap();
        let delete = object(vector, "delete");
        let prepared = state
            .prepare_delete_checkpoint_subtree(
                string(delete, "thread_id"),
                string(delete, "checkpoint_id"),
            )
            .unwrap();
        assert_eq!(
            prepared.deleted_checkpoint_count(),
            array(vector, "expected_deleted").len() as u64,
            "deleted count for vector {name}"
        );
        state.apply_prepared_delete_checkpoint_subtree(prepared);

        let expected_live = array(vector, "expected_live");
        assert_eq!(
            state.checkpoints.len(),
            expected_live.len(),
            "live count for {name}"
        );
        for (checkpoint, expected) in state.checkpoints.iter().zip(expected_live) {
            let (thread_id, checkpoint_id) = string_pair(expected);
            assert_eq!(checkpoint.thread_id, thread_id, "live thread for {name}");
            assert_eq!(
                checkpoint.checkpoint_id, checkpoint_id,
                "live checkpoint for {name}"
            );
        }

        let expected_retired = array(vector, "expected_retired_request_ids_hex")
            .iter()
            .map(|value| decode_hex(value.as_str().unwrap()))
            .collect::<std::collections::HashSet<_>>();
        assert_eq!(
            state
                .retired_requests
                .keys()
                .cloned()
                .collect::<std::collections::HashSet<_>>(),
            expected_retired,
            "retired request ledger for {name}"
        );

        let expected_deleted = array(vector, "expected_deleted")
            .iter()
            .map(|value| {
                let (thread_id, checkpoint_id) = string_pair(value);
                (thread_id.to_owned(), checkpoint_id.to_owned())
            })
            .collect::<std::collections::HashSet<_>>();
        assert_eq!(
            state.deleted_checkpoints, expected_deleted,
            "deleted checkpoint ledger for {name}"
        );

        for record in array(vector, "expected_active_request_ordinals") {
            let request_id = decode_hex(string(record, "request_id_hex"));
            let active = state
                .request_records
                .get(&request_id)
                .unwrap_or_else(|| panic!("missing active request in vector {name}"));
            assert_eq!(
                active.checkpoint_ordinal,
                number(record, "checkpoint_ordinal"),
                "active ordinal for {name}"
            );
        }
        assert_eq!(
            state.request_records.len(),
            array(vector, "expected_active_request_ordinals").len(),
            "active request count for {name}"
        );

        if object(vector, "clear_sequence_geometry").as_bool().unwrap() {
            assert!(state.payload.is_empty());
            assert!(state.nodes.is_empty());
            assert!(state.versions.is_empty());
        } else {
            assert_eq!(state.geometry().unwrap().payload_len, before.payload_len);
            assert_eq!(state.geometry().unwrap().node_count, before.node_count);
            assert_eq!(
                state.geometry().unwrap().version_count,
                before.version_count
            );
        }
    }
}
