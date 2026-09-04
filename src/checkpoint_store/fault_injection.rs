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

#[cfg(feature = "fault-injection")]
pub(crate) const WAL_IO_FAULT_ENV: &str = "TULYA_CHECKPOINT_STORE_WAL_IO_FAULT";

#[cfg(feature = "fault-injection")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WalIoFault {
    ShortWrite(usize),
    WriteEnospcAfter(usize),
    FlushEioAfter,
    SyncEioAfter,
    ReserveEnospcAfterSetLen,
}

#[cfg(feature = "fault-injection")]
pub(crate) fn configured_wal_io_fault() -> io::Result<Option<WalIoFault>> {
    let Some(raw) = std::env::var_os(WAL_IO_FAULT_ENV) else {
        return Ok(None);
    };
    let raw = raw.into_string().map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "hot WAL fault-injection value is not UTF-8",
        )
    })?;
    let fault = if let Some(value) = raw.strip_prefix("short-write=") {
        let limit = parse_fault_usize(value, "short-write")?;
        if limit == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "short-write fault limit must be positive",
            ));
        }
        WalIoFault::ShortWrite(limit)
    } else if let Some(value) = raw.strip_prefix("write-enospc-after=") {
        WalIoFault::WriteEnospcAfter(parse_fault_usize(value, "write-enospc-after")?)
    } else {
        match raw.as_str() {
            "flush-eio-after" => WalIoFault::FlushEioAfter,
            "sync-eio-after" => WalIoFault::SyncEioAfter,
            "reserve-enospc-after-set-len" => WalIoFault::ReserveEnospcAfterSetLen,
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "unknown hot WAL fault-injection value",
                ));
            }
        }
    };
    Ok(Some(fault))
}

#[cfg(feature = "fault-injection")]
fn parse_fault_usize(value: &str, label: &str) -> io::Result<usize> {
    value.parse::<usize>().map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid {label} hot WAL fault-injection limit"),
        )
    })
}

#[cfg(all(feature = "fault-injection", unix))]
pub(crate) fn injected_disk_full_error() -> io::Error {
    io::Error::from_raw_os_error(28)
}

#[cfg(all(feature = "fault-injection", windows))]
pub(crate) fn injected_disk_full_error() -> io::Error {
    io::Error::from_raw_os_error(112)
}

#[cfg(all(feature = "fault-injection", not(any(unix, windows))))]
pub(crate) fn injected_disk_full_error() -> io::Error {
    io::Error::other("injected hot WAL storage-full failure")
}

#[cfg(all(feature = "fault-injection", unix))]
pub(crate) fn injected_io_error() -> io::Error {
    io::Error::from_raw_os_error(5)
}

#[cfg(all(feature = "fault-injection", windows))]
pub(crate) fn injected_io_error() -> io::Error {
    io::Error::from_raw_os_error(1117)
}

#[cfg(all(feature = "fault-injection", not(any(unix, windows))))]
pub(crate) fn injected_io_error() -> io::Error {
    io::Error::other("injected hot WAL I/O failure")
}

#[cfg(feature = "fault-injection")]
pub(crate) const PUBLICATION_IO_FAULT_ENV: &str = "TULYA_CHECKPOINT_STORE_PUBLICATION_IO_FAULT";

#[cfg(feature = "fault-injection")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PublicationIoFault {
    SegmentSyncEioAfter,
    SegmentRenameEioBefore,
    SegmentRenameEioAfter,
    SegmentDirSyncEioAfter,
    RouteSyncEioAfter,
    RouteRenameEioBefore,
    RouteRenameEioAfter,
    RouteDirSyncEioAfter,
    ManifestSyncEioAfter,
    ManifestRenameEioBefore,
    ManifestRenameEioAfter,
    ManifestDirSyncEioAfter,
    WalRecycleSyncEioAfter,
    WalRecycleRenameEioBefore,
    WalRecycleRenameEioAfter,
    WalRecycleDirSyncEioAfter,
}

#[cfg(feature = "fault-injection")]
pub(crate) fn configured_publication_io_fault() -> io::Result<Option<PublicationIoFault>> {
    let Some(raw) = std::env::var_os(PUBLICATION_IO_FAULT_ENV) else {
        return Ok(None);
    };
    let raw = raw.into_string().map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "publication fault-injection value is not UTF-8",
        )
    })?;
    let fault = match raw.as_str() {
        "segment-sync-eio-after" => PublicationIoFault::SegmentSyncEioAfter,
        "segment-rename-eio-before" => PublicationIoFault::SegmentRenameEioBefore,
        "segment-rename-eio-after" => PublicationIoFault::SegmentRenameEioAfter,
        "segment-dir-sync-eio-after" => PublicationIoFault::SegmentDirSyncEioAfter,
        "route-sync-eio-after" => PublicationIoFault::RouteSyncEioAfter,
        "route-rename-eio-before" => PublicationIoFault::RouteRenameEioBefore,
        "route-rename-eio-after" => PublicationIoFault::RouteRenameEioAfter,
        "route-dir-sync-eio-after" => PublicationIoFault::RouteDirSyncEioAfter,
        "manifest-sync-eio-after" => PublicationIoFault::ManifestSyncEioAfter,
        "manifest-rename-eio-before" => PublicationIoFault::ManifestRenameEioBefore,
        "manifest-rename-eio-after" => PublicationIoFault::ManifestRenameEioAfter,
        "manifest-dir-sync-eio-after" => PublicationIoFault::ManifestDirSyncEioAfter,
        "wal-recycle-sync-eio-after" => PublicationIoFault::WalRecycleSyncEioAfter,
        "wal-recycle-rename-eio-before" => PublicationIoFault::WalRecycleRenameEioBefore,
        "wal-recycle-rename-eio-after" => PublicationIoFault::WalRecycleRenameEioAfter,
        "wal-recycle-dir-sync-eio-after" => PublicationIoFault::WalRecycleDirSyncEioAfter,
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "unknown publication fault-injection value",
            ));
        }
    };
    Ok(Some(fault))
}
