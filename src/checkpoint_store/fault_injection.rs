use super::*;

#[cfg(feature = "fault-injection")]
pub(super) const CRASH_ENV: &str = "TULYA_CHECKPOINT_STORE_CRASH_POINT";
#[cfg(feature = "fault-injection")]
pub(super) const CRASH_EXIT_CODE: i32 = 86;

#[cfg(feature = "fault-injection")]
pub(super) fn maybe_crash(point: &str) {
    if std::env::var(CRASH_ENV).ok().as_deref() == Some(point) {
        std::process::exit(CRASH_EXIT_CODE);
    }
}

#[cfg(not(feature = "fault-injection"))]
#[inline]
pub(super) fn maybe_crash(_point: &str) {}

pub(super) fn maybe_file_crash(path: &Path, stage: &str) {
    let Some(name) = path.file_name().and_then(|value| value.to_str()) else {
        return;
    };
    let kind = if name.starts_with("structured-g") && name.ends_with(".t3s") {
        Some("segment")
    } else if name.starts_with("route-g") && name.ends_with(".t3r") {
        Some("route")
    } else if name == MANIFEST_FILE {
        Some("manifest")
    } else {
        None
    };
    if let Some(kind) = kind {
        maybe_crash(&format!("after-{kind}-{stage}"));
    }
}
