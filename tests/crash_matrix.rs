#![cfg(feature = "fault-injection")]

use std::error::Error;
use std::fs;
use std::path::Path;
use std::process::Command;

use serde_json::json;
use tulya_checkpoint_store::{CheckpointStore, CheckpointStoreConfig};

const CRASH_ENV: &str = "TULYA_CHECKPOINT_STORE_CRASH_POINT";
const CRASH_EXIT_CODE: i32 = 86;
const CRASH_POINTS: [&str; 16] = [
    "after-segment-write",
    "after-segment-sync",
    "after-segment-rename",
    "after-segment-dir-sync",
    "after-route-write",
    "after-route-sync",
    "after-route-rename",
    "after-route-dir-sync",
    "after-manifest-write",
    "after-manifest-sync",
    "after-manifest-rename",
    "after-manifest-dir-sync",
    "after-wal-write",
    "after-wal-sync",
    "after-wal-rename",
    "after-wal-dir-sync",
];

fn copy_store(source: &Path, destination: &Path) -> Result<(), Box<dyn Error>> {
    fs::create_dir(destination)?;
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        if entry.file_type()?.is_file() {
            fs::copy(entry.path(), destination.join(entry.file_name()))?;
        }
    }
    Ok(())
}

fn assert_exact_and_resume(
    path: &Path,
    allowed_before: &[u64],
    target: u64,
    expected: &[(&str, Vec<u8>)],
) -> Result<(), Box<dyn Error>> {
    let mut store = CheckpointStore::open(path, CheckpointStoreConfig::default())?;
    assert!(allowed_before.contains(&store.sealed_checkpoint_count()));
    assert_eq!(store.checkpoint_count(), expected.len());
    assert_eq!(store.verify_all()?.failures, 0);
    for (checkpoint_id, bytes) in expected {
        assert_eq!(
            store.read_checkpoint("crash-thread", checkpoint_id)?,
            *bytes
        );
    }
    if store.sealed_checkpoint_count() < target {
        store.seal_through(target)?;
    }
    assert_eq!(store.sealed_checkpoint_count(), target);
    assert_eq!(store.verify_all()?.failures, 0);
    drop(store);

    let reopened = CheckpointStore::open(path, CheckpointStoreConfig::default())?;
    assert_eq!(reopened.sealed_checkpoint_count(), target);
    assert_eq!(reopened.verify_all()?.failures, 0);
    Ok(())
}

fn run_campaign(
    binary: &Path,
    baseline: &Path,
    cases: &Path,
    allowed_before: &[u64],
    target: u64,
    expected: &[(&str, Vec<u8>)],
) -> Result<(), Box<dyn Error>> {
    fs::create_dir(cases)?;
    for point in CRASH_POINTS {
        let case = cases.join(point);
        copy_store(baseline, &case)?;
        let status = Command::new(binary)
            .args([
                "--db",
                case.to_str().ok_or("case path is not UTF-8")?,
                "seal",
                "--through",
                &target.to_string(),
            ])
            .env(CRASH_ENV, point)
            .status()?;
        assert_eq!(status.code(), Some(CRASH_EXIT_CODE), "failpoint {point}");
        assert_exact_and_resume(&case, allowed_before, target, expected)?;
    }
    Ok(())
}

#[test]
fn all_32_seal_handoff_crashes_recover_exactly() -> Result<(), Box<dyn Error>> {
    let temp = tempfile::tempdir()?;
    let baseline = temp.path().join("baseline-hot");
    let mut store = CheckpointStore::open(&baseline, CheckpointStoreConfig::default())?;
    store.append_messages_checkpoint("crash-thread", "root", 0, None, &[json!("root")])?;
    store.append_messages_checkpoint("crash-thread", "left", 1, Some("root"), &[json!("left")])?;
    store.append_messages_checkpoint(
        "crash-thread",
        "right",
        1,
        Some("root"),
        &[json!("right")],
    )?;
    let expected = [
        ("root", store.read_checkpoint("crash-thread", "root")?),
        ("left", store.read_checkpoint("crash-thread", "left")?),
        ("right", store.read_checkpoint("crash-thread", "right")?),
    ];
    drop(store);

    let binary = Path::new(env!("CARGO_BIN_EXE_tulya-checkpoint"));
    run_campaign(
        binary,
        &baseline,
        &temp.path().join("first-generation"),
        &[0, 2],
        2,
        &expected,
    )?;

    let later = temp.path().join("later-baseline");
    copy_store(&baseline, &later)?;
    let mut later_store = CheckpointStore::open(&later, CheckpointStoreConfig::default())?;
    later_store.seal_through(2)?;
    drop(later_store);
    run_campaign(
        binary,
        &later,
        &temp.path().join("later-generation"),
        &[2, 3],
        3,
        &expected,
    )?;
    Ok(())
}
