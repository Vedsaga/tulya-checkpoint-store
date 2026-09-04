//! Internal hot-WAL commit I/O state machine.
//!
//! This module separates the foreground commit protocol from `std::fs::File`
//! so short writes and durability failures can be tested deterministically
//! before the abstraction is wired into the production `HotWal` handle.

#[cfg(feature = "fault-injection")]
use crate::checkpoint_store::fault_injection::{
    configured_wal_io_fault, injected_disk_full_error, injected_io_error, WalIoFault,
};
use crate::checkpoint_store::CheckpointStoreError;
use crate::error_classification::{
    durability_indeterminate_error, recovery_required_error, DurabilityOperation,
};
use std::fs::File;
use std::io::{self, Write};
use std::path::Path;
use std::time::Instant;

/// Minimal syscall surface required to publish one complete WAL record.
pub(crate) trait HotWalCommitIo {
    /// Writes some prefix of `bytes`, following normal `Write::write` semantics.
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize>;

    /// Flushes userspace buffering after the complete record was written.
    fn flush(&mut self) -> io::Result<()>;

    /// Issues the data durability barrier for the complete record.
    fn sync_data(&mut self) -> io::Result<()>;
}

/// Production adapter over one already-positioned WAL file.
pub(crate) struct FileHotWalCommitIo<'a> {
    file: &'a mut File,
    #[cfg(feature = "fault-injection")]
    fault: Option<WalIoFault>,
    #[cfg(feature = "fault-injection")]
    fault_written: usize,
}

impl<'a> FileHotWalCommitIo<'a> {
    pub(crate) fn new(file: &'a mut File) -> Result<Self, CheckpointStoreError> {
        Ok(Self {
            file,
            #[cfg(feature = "fault-injection")]
            fault: configured_wal_io_fault()?,
            #[cfg(feature = "fault-injection")]
            fault_written: 0,
        })
    }

    #[cfg(feature = "fault-injection")]
    fn record_fault_write(&mut self, count: usize) -> io::Result<()> {
        self.fault_written = self.fault_written.checked_add(count).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "hot WAL fault-injection write counter overflow",
            )
        })?;
        Ok(())
    }
}

impl HotWalCommitIo for FileHotWalCommitIo<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        #[cfg(feature = "fault-injection")]
        {
            let count = match self.fault {
                Some(WalIoFault::ShortWrite(limit)) => {
                    let take = bytes.len().min(limit);
                    self.file.write(bytes.get(..take).ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            "hot WAL short-write fault slice is outside input",
                        )
                    })?)?
                }
                Some(WalIoFault::WriteEnospcAfter(limit)) => {
                    if self.fault_written >= limit {
                        return Err(injected_disk_full_error());
                    }
                    let take = bytes.len().min(limit - self.fault_written);
                    self.file.write(bytes.get(..take).ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            "hot WAL ENOSPC fault slice is outside input",
                        )
                    })?)?
                }
                _ => self.file.write(bytes)?,
            };
            self.record_fault_write(count)?;
            Ok(count)
        }

        #[cfg(not(feature = "fault-injection"))]
        {
            self.file.write(bytes)
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()?;
        #[cfg(feature = "fault-injection")]
        if self.fault == Some(WalIoFault::FlushEioAfter) {
            return Err(injected_io_error());
        }
        Ok(())
    }

    fn sync_data(&mut self) -> io::Result<()> {
        self.file.sync_data()?;
        #[cfg(feature = "fault-injection")]
        if self.fault == Some(WalIoFault::SyncEioAfter) {
            return Err(injected_io_error());
        }
        Ok(())
    }
}

/// Timings preserved by the existing foreground append report.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct HotWalCommitTimings {
    pub(crate) write_ns: u128,
    pub(crate) sync_data_ns: u128,
}

/// Mutable commit state for one open writer handle.
///
/// Once WAL bytes may have changed without a fully acknowledged commit, this
/// object refuses every later commit. Reopen/recovery must establish the next
/// authoritative logical tail before a new committer is created.
#[derive(Debug, Default)]
pub(crate) struct HotWalCommitter {
    recovery_required: bool,
}

impl HotWalCommitter {
    /// Writes exactly one record and performs exactly one successful
    /// `sync_data` before returning success.
    pub(crate) fn commit(
        &mut self,
        path: &Path,
        io: &mut dyn HotWalCommitIo,
        record: &[u8],
    ) -> Result<HotWalCommitTimings, CheckpointStoreError> {
        self.ensure_writable(path)?;
        if record.is_empty() {
            return Err(CheckpointStoreError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                "hot WAL commit record is empty",
            )));
        }

        let write_started = Instant::now();
        let mut written = 0usize;
        while written < record.len() {
            let remaining = record.get(written..).ok_or_else(|| {
                CheckpointStoreError::Io(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "hot WAL write cursor exceeded record length",
                ))
            })?;
            match io.write(remaining) {
                Ok(0) => {
                    return self.write_failure(
                        path,
                        written,
                        io::Error::new(io::ErrorKind::WriteZero, "hot WAL write made no progress"),
                    );
                }
                Ok(count) if count <= remaining.len() => {
                    written = written.checked_add(count).ok_or_else(|| {
                        CheckpointStoreError::Io(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "hot WAL write cursor overflow",
                        ))
                    })?;
                }
                Ok(_) => {
                    return Err(self.recovery_required_error(
                        path,
                        Some(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "hot WAL writer reported more bytes than supplied",
                        )),
                    ));
                }
                Err(source) => return self.write_failure(path, written, source),
            }
        }

        if let Err(source) = io.flush() {
            self.recovery_required = true;
            return Err(durability_indeterminate_error(
                DurabilityOperation::WalFlush,
                path,
                source,
            ));
        }
        let write_ns = write_started.elapsed().as_nanos();

        let sync_started = Instant::now();
        if let Err(source) = io.sync_data() {
            self.recovery_required = true;
            return Err(durability_indeterminate_error(
                DurabilityOperation::WalSyncData,
                path,
                source,
            ));
        }
        let sync_data_ns = sync_started.elapsed().as_nanos();

        Ok(HotWalCommitTimings {
            write_ns,
            sync_data_ns,
        })
    }

    pub(crate) fn ensure_writable(&self, path: &Path) -> Result<(), CheckpointStoreError> {
        if self.recovery_required {
            Err(recovery_required_error(path, None))
        } else {
            Ok(())
        }
    }

    pub(crate) fn mark_recovery_required(&mut self) {
        self.recovery_required = true;
    }

    pub(crate) fn recovery_required_error(
        &mut self,
        path: &Path,
        source: Option<io::Error>,
    ) -> CheckpointStoreError {
        self.recovery_required = true;
        recovery_required_error(path, source)
    }

    fn write_failure(
        &mut self,
        path: &Path,
        bytes_written: usize,
        source: io::Error,
    ) -> Result<HotWalCommitTimings, CheckpointStoreError> {
        if bytes_written == 0 {
            return Err(CheckpointStoreError::Io(source));
        }
        Err(self.recovery_required_error(path, Some(source)))
    }

    #[cfg(test)]
    fn requires_recovery(&self) -> bool {
        self.recovery_required
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CheckpointStoreFailureKind;
    use std::path::PathBuf;

    #[derive(Debug)]
    struct ScriptedIo {
        bytes: Vec<u8>,
        max_write: usize,
        fail_write_after: Option<usize>,
        fail_flush: bool,
        fail_sync: bool,
        overreport_write: bool,
        write_calls: usize,
        flush_calls: usize,
        sync_calls: usize,
    }

    impl ScriptedIo {
        fn healthy(max_write: usize) -> Self {
            Self {
                bytes: Vec::new(),
                max_write,
                fail_write_after: None,
                fail_flush: false,
                fail_sync: false,
                overreport_write: false,
                write_calls: 0,
                flush_calls: 0,
                sync_calls: 0,
            }
        }
    }

    impl HotWalCommitIo for ScriptedIo {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.write_calls = self.write_calls.saturating_add(1);
            if self.overreport_write {
                return Ok(bytes.len().saturating_add(1));
            }
            if let Some(limit) = self.fail_write_after {
                if self.bytes.len() >= limit {
                    return Err(io::Error::from_raw_os_error(28));
                }
            }
            let until_failure = self
                .fail_write_after
                .map_or(bytes.len(), |limit| limit.saturating_sub(self.bytes.len()));
            let take = bytes.len().min(self.max_write).min(until_failure);
            if take == 0 {
                return Err(io::Error::from_raw_os_error(28));
            }
            let chunk = bytes.get(..take).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "scripted write slice overflow")
            })?;
            self.bytes.extend_from_slice(chunk);
            Ok(take)
        }

        fn flush(&mut self) -> io::Result<()> {
            self.flush_calls = self.flush_calls.saturating_add(1);
            if self.fail_flush {
                Err(io::Error::from_raw_os_error(5))
            } else {
                Ok(())
            }
        }

        fn sync_data(&mut self) -> io::Result<()> {
            self.sync_calls = self.sync_calls.saturating_add(1);
            if self.fail_sync {
                Err(io::Error::from_raw_os_error(5))
            } else {
                Ok(())
            }
        }
    }

    #[test]
    fn short_writes_complete_exact_record_before_one_sync() {
        let path = PathBuf::from("hot.wal");
        let mut committer = HotWalCommitter::default();
        let mut io = ScriptedIo::healthy(2);
        let record = b"abcdefgh";

        committer.commit(&path, &mut io, record).unwrap();

        assert_eq!(io.bytes, record);
        assert_eq!(io.write_calls, 4);
        assert_eq!(io.flush_calls, 1);
        assert_eq!(io.sync_calls, 1);
        assert!(!committer.requires_recovery());
    }

    #[test]
    fn zero_progress_write_failure_is_definite_and_retryable_in_place() {
        let path = PathBuf::from("hot.wal");
        let mut committer = HotWalCommitter::default();
        let mut io = ScriptedIo::healthy(8);
        io.fail_write_after = Some(0);

        let error = committer.commit(&path, &mut io, b"record").unwrap_err();
        assert_eq!(error.failure_kind(), CheckpointStoreFailureKind::Capacity);
        assert!(!committer.requires_recovery());

        io.fail_write_after = None;
        committer.commit(&path, &mut io, b"record").unwrap();
        assert_eq!(io.bytes, b"record");
    }

    #[test]
    fn partial_write_failure_poisoning_blocks_every_later_commit() {
        let path = PathBuf::from("hot.wal");
        let mut committer = HotWalCommitter::default();
        let mut io = ScriptedIo::healthy(8);
        io.fail_write_after = Some(3);

        let error = committer.commit(&path, &mut io, b"record").unwrap_err();
        assert_eq!(
            error.failure_kind(),
            CheckpointStoreFailureKind::RecoveryRequired
        );
        assert_eq!(io.bytes, b"rec");
        assert!(committer.requires_recovery());
        let write_calls = io.write_calls;

        io.fail_write_after = None;
        let blocked = committer.commit(&path, &mut io, b"second").unwrap_err();
        assert_eq!(
            blocked.failure_kind(),
            CheckpointStoreFailureKind::RecoveryRequired
        );
        assert_eq!(io.write_calls, write_calls);
        assert_eq!(io.bytes, b"rec");
    }

    #[test]
    fn invalid_overreported_write_poisoning_requires_recovery() {
        let path = PathBuf::from("hot.wal");
        let mut committer = HotWalCommitter::default();
        let mut io = ScriptedIo::healthy(32);
        io.overreport_write = true;

        let error = committer.commit(&path, &mut io, b"record").unwrap_err();
        assert_eq!(
            error.failure_kind(),
            CheckpointStoreFailureKind::RecoveryRequired
        );
        assert!(committer.requires_recovery());
    }

    #[test]
    fn flush_failure_is_indeterminate_and_poisoning() {
        let path = PathBuf::from("hot.wal");
        let mut committer = HotWalCommitter::default();
        let mut io = ScriptedIo::healthy(32);
        io.fail_flush = true;

        let error = committer.commit(&path, &mut io, b"record").unwrap_err();
        assert_eq!(
            error.failure_kind(),
            CheckpointStoreFailureKind::DurabilityIndeterminate
        );
        let context = error.durability_indeterminate().unwrap();
        assert_eq!(context.operation(), DurabilityOperation::WalFlush);
        assert_eq!(io.bytes, b"record");
        assert_eq!(io.sync_calls, 0);
        assert!(committer.requires_recovery());
    }

    #[test]
    fn sync_failure_is_indeterminate_and_poisoning() {
        let path = PathBuf::from("hot.wal");
        let mut committer = HotWalCommitter::default();
        let mut io = ScriptedIo::healthy(32);
        io.fail_sync = true;

        let error = committer.commit(&path, &mut io, b"record").unwrap_err();
        assert_eq!(
            error.failure_kind(),
            CheckpointStoreFailureKind::DurabilityIndeterminate
        );
        let context = error.durability_indeterminate().unwrap();
        assert_eq!(context.operation(), DurabilityOperation::WalSyncData);
        assert_eq!(io.bytes, b"record");
        assert_eq!(io.flush_calls, 1);
        assert_eq!(io.sync_calls, 1);
        assert!(committer.requires_recovery());
    }
}
