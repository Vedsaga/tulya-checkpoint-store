# Tulya checkpoint format

Tulya has one public on-disk format. Users do not choose among format
versions. Every manifest carries `"format":"tulya-checkpoint-store"` and
`"format_version":1` so readers can reject incompatible bytes safely; that
integer is compatibility metadata, not a separately marketed product or mode.
The crate version and internal component magic numbers are implementation
details; they do not create additional user-visible format versions.

Every Tulya store contains:

- `structured-segment-manifest.json`: the single authority record. It says
  `"format": "tulya-checkpoint-store"` and `"format_version": 1`;
- `hot.wal`: a preinitialized reserve whose valid prefix contains committed
  transactions and whose remaining bytes are ignored reserve/torn suffix;
- immutable compressed segments and route indexes named by the manifest;
- lock files used for the single writer and generation-safe reclaim.

The compatibility promise for the 1.x crate line is that stores created by a
released writer remain readable by later compatible readers. A future
incompatible format would require a deliberate migration tool and a new public
format number; an internal codec or manifest implementation revision does not.

The committed golden store is at `tests/fixtures/format-v1`. Its integration
test checks independent `fsck`, reopen, root reconstruction, and two exact
sibling branches. Do not regenerate this fixture as part of ordinary tests.

Private development builds produced manifests with internal revision labels.
The release reader can upgrade those local pre-release manifests, but those
labels were never public compatibility contracts and are not emitted by the
released writer.
