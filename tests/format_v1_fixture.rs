use std::error::Error;
use std::fs;
use std::path::Path;

use tulya_checkpoint_store::{format, fsck, CheckpointStore, CheckpointStoreConfig};

fn copy_fixture(source: &Path, destination: &Path) -> Result<(), Box<dyn Error>> {
    fs::create_dir(destination)?;
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        if entry.file_type()?.is_file() {
            fs::copy(entry.path(), destination.join(entry.file_name()))?;
        }
    }
    Ok(())
}

#[test]
fn committed_format_v1_fixture_remains_readable() -> Result<(), Box<dyn Error>> {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/format-v1");
    let report = fsck(&fixture)?;
    assert_eq!(report.format_version, format::VERSION);
    assert_eq!(report.checkpoint_count, 3);
    assert_eq!(report.sealed_segment_count, 1);
    assert_eq!(report.failures, 0);

    let temp = tempfile::tempdir()?;
    let store_path = temp.path().join("store");
    copy_fixture(&fixture, &store_path)?;
    let store = CheckpointStore::open(&store_path, CheckpointStoreConfig::default())?;
    assert_eq!(store.checkpoint_count(), 3);
    let left = store.read_checkpoint("support-case-42", "increase-timeout")?;
    let right = store.read_checkpoint("support-case-42", "fix-query")?;
    assert_ne!(left, right);
    assert!(String::from_utf8(left)?.contains("Try a longer timeout"));
    assert!(String::from_utf8(right)?.contains("Optimize the query"));
    Ok(())
}
