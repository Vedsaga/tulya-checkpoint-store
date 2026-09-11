from pathlib import Path


def replace_once(path: str, old: str, new: str) -> None:
    p = Path(path)
    text = p.read_text()
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{path}: expected old block exactly once, found {count}")
    p.write_text(text.replace(old, new, 1))


def replace_count(path: str, old: str, new: str, expected: int) -> None:
    p = Path(path)
    text = p.read_text()
    count = text.count(old)
    if count != expected:
        raise SystemExit(f"{path}: expected block {expected} times, found {count}")
    p.write_text(text.replace(old, new))


AUTHORITY = "crates/tulya-core/src/persistent_history/authority.rs"
COMPACTION = "crates/tulya-core/src/persistent_sequence/compaction_v2.rs"

# A zero-copy split still hashes aliased boundary bytes to construct and verify
# canonical leaf commitments. The current durable path therefore performs one
# extra bounded integrity read versus the old copy-on-split implementation.
# Keep the key invariant: the bound is constant and independent of parent size.
replace_count(
    AUTHORITY,
    '''            foreground.payload_bytes_read <= 2 * 16384,
            "payload read {} exceeds two boundary leaves",
''',
    '''            foreground.payload_bytes_read <= 3 * 16384,
            "payload read {} exceeds three integrity-checked boundary-leaf reads",
''',
    3,
)

# The old compactor treated every overlap as structurally malformed. With
# immutable payload aliases, overlap itself is legal; an overlap whose stored
# leaf commitment disagrees with the referenced bytes remains corruption and
# must still fail closed.
replace_once(
    COMPACTION,
    '''    #[test]
    fn overlapping_retained_payload_ranges_fail_closed() {
        // Each range is individually in-bounds, so reachability retains both
        // leaves; preparation must reject the overlap instead of merging it.
        let overlapping = overlapping_range_state();
        assert!(plan_v2_compaction(&overlapping).is_ok());
        assert_eq!(
            prepare_v2_compaction(&overlapping),
            Err(V2CompactionError::Invalid(
                "v2 compaction retained payload ranges overlap or duplicate"
            ))
        );

        let payload = b"aaa";
        let leaf = V2NodeRecord::leaf(0, payload).unwrap();
        let first_root = V2RootRecord::from_node(0, leaf).unwrap();
        let second_root = V2RootRecord::from_node(1, leaf).unwrap();
        let duplicate = V2CommittedState {
            payload: payload.to_vec(),
            nodes: vec![leaf, leaf],
            versions: vec![
                V2VersionRecord::new(0, None, first_root).unwrap(),
                V2VersionRecord::new(1, Some(0), second_root).unwrap(),
            ],
            checkpoints: vec![V2CheckpointRecord {
                checkpoint_no: 1,
                thread_id: "thread".to_owned(),
                checkpoint_id: "cp-1".to_owned(),
                parent_checkpoint_id: None,
                identity_version: 1,
                messages_version: None,
                result_version: None,
                state: checkpoint_state_metadata(second_root, None, None).unwrap(),
            }],
            ..Default::default()
        };
        assert!(plan_v2_compaction(&duplicate).is_ok());
        assert_eq!(
            prepare_v2_compaction(&duplicate),
            Err(V2CompactionError::Invalid(
                "v2 compaction retained payload ranges overlap or duplicate"
            ))
        );
    }
''',
    '''    #[test]
    fn payload_aliases_compact_once_and_inconsistent_aliases_fail_closed() {
        // This fixture stores an overlapping leaf whose commitment was built
        // from different bytes than the range it names. Overlap is now legal,
        // but the commitment mismatch remains corruption and fails closed.
        let inconsistent = overlapping_range_state();
        assert!(plan_v2_compaction(&inconsistent).is_ok());
        assert_eq!(
            prepare_v2_compaction(&inconsistent),
            Err(V2CompactionError::Invalid(
                "v2 compaction rebuilt leaf commitment disagrees with source"
            ))
        );

        // Exact duplicate immutable views are valid aliases. Compaction keeps
        // one copy of the payload union while preserving both leaf mappings.
        let payload = b"aaa";
        let leaf = V2NodeRecord::leaf(0, payload).unwrap();
        let first_root = V2RootRecord::from_node(0, leaf).unwrap();
        let second_root = V2RootRecord::from_node(1, leaf).unwrap();
        let duplicate = V2CommittedState {
            payload: payload.to_vec(),
            nodes: vec![leaf, leaf],
            versions: vec![
                V2VersionRecord::new(0, None, first_root).unwrap(),
                V2VersionRecord::new(1, Some(0), second_root).unwrap(),
            ],
            checkpoints: vec![V2CheckpointRecord {
                checkpoint_no: 1,
                thread_id: "thread".to_owned(),
                checkpoint_id: "cp-1".to_owned(),
                parent_checkpoint_id: None,
                identity_version: 1,
                messages_version: None,
                result_version: None,
                state: checkpoint_state_metadata(second_root, None, None).unwrap(),
            }],
            ..Default::default()
        };
        assert!(plan_v2_compaction(&duplicate).is_ok());
        let prepared = prepare_v2_compaction(&duplicate).unwrap();
        assert_eq!(prepared.payload(), payload);
        assert_eq!(prepared.payload_mapping().len(), 2);
        assert_eq!(prepared.nodes().len(), 2);
    }
''',
)

replace_count(
    COMPACTION,
    '''            Err(V2CompactionError::Invalid(
                "v2 compaction retained payload ranges overlap or duplicate"
            ))
''',
    '''            Err(V2CompactionError::Invalid(
                "v2 compaction rebuilt leaf commitment disagrees with source"
            ))
''',
    2,
)

# Keep the top-level compaction contract honest about the new representation.
replace_once(
    COMPACTION,
    '''/// Validates retained source ranges and repacks their exact bytes densely.
///
/// Ranges arrive ordered by `(offset, length)`. Gaps (deleted leaves) are
/// valid and simply disappear; any overlap, duplicate, backward move, overflow,
/// or out-of-bounds reference fails closed instead of being silently merged.
''',
    '''/// Validates retained source ranges and repacks their byte union densely.
///
/// Ranges arrive ordered by `(offset, length)`. Gaps (deleted leaves) simply
/// disappear. Overlap and duplicates are valid immutable aliases; backward
/// order, overflow, out-of-bounds references, and commitment disagreement
/// still fail closed.
''',
)

print("zero-copy follow-up test and invariant updates applied exactly")
