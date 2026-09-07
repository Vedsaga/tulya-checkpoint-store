//! External API-surface proof for `tulya-core`.
//!
//! This test links against `tulya-core` exactly like a downstream adapter
//! would: it may only touch the public surface. The complete durable
//! lifecycle — open, create, commit, retire, seal, reopen, exact reads —
//! must be expressible through [`WritableHistoryAuthority`] alone, with no
//! writable log handle, durable store method, or free seal publisher in
//! reach. If any step below stops compiling, the encapsulation boundary has
//! regressed.

use tulya_core::persistent_history::authority::{open_history_authority, WritableHistoryAuthority};
use tulya_core::persistent_history::durable_log::DurableError;
use tulya_core::persistent_history::CommitOutcome;

fn committed(
    authority: &mut WritableHistoryAuthority,
    history: tulya_core::persistent_history::HistoryId,
    parent: Option<tulya_core::persistent_history::VersionId>,
    payload: &[u8],
) -> tulya_core::persistent_history::Version {
    match authority
        .append(history, parent, payload, None, None)
        .unwrap()
    {
        CommitOutcome::Committed(version) => version,
        CommitOutcome::Replayed(_) | CommitOutcome::Retired => {
            panic!("surface commit must create")
        }
    }
}

#[test]
fn public_surface_covers_the_full_durable_lifecycle() {
    let temp = tempfile::tempdir().unwrap();
    let mut first = WritableHistoryAuthority::open(temp.path()).unwrap();
    assert_eq!(first.generation(), 0);
    assert!(matches!(
        WritableHistoryAuthority::open(temp.path()),
        Err(DurableError::AlreadyOpen)
    ));

    let history = first.create_history(Some(b"thread-a")).unwrap();
    let v0 = committed(&mut first, history, None, b"aaa");
    // Request-scoped commit plus retirement through the authority.
    let v1 = match first
        .append(history, Some(v0.id()), b"bbb", Some(b"req-1"), None)
        .unwrap()
    {
        CommitOutcome::Committed(version) => version,
        CommitOutcome::Replayed(_) | CommitOutcome::Retired => {
            panic!("request commit must create")
        }
    };
    first.retire(b"req-1").unwrap();

    let summary = first.seal().unwrap();
    assert_eq!(summary.generation, 1);
    assert!(summary.recycled_hot);
    assert_eq!(first.generation(), 1);
    assert!(matches!(
        WritableHistoryAuthority::open(temp.path()),
        Err(DurableError::AlreadyOpen)
    ));
    let v2 = committed(&mut first, history, Some(v1.id()), b"ccc");
    drop(first);

    let second = WritableHistoryAuthority::open(temp.path()).unwrap();
    assert_eq!(second.generation(), 1);
    assert_eq!(second.stats().snapshot_versions, 2);
    assert!(second.stats().suffix_bytes > 0);
    for version in [v0, v1, v2] {
        let got = second.store().lookup_version(version.id()).unwrap();
        assert_eq!(got, version);
    }
    let mut output = Vec::new();
    let v2 = second.store().lookup_version(v2.id()).unwrap();
    second
        .store()
        .read(v2, 0, v2.root().logical_len().get(), &mut output)
        .unwrap();
    assert_eq!(output, b"aaabbbccc");
    drop(second);

    // Lock-free read-only loading observes the same authority.
    let read_only = open_history_authority(temp.path()).unwrap();
    assert_eq!(read_only.generation, 1);
    assert_eq!(read_only.store.version_count(), 3);
}
