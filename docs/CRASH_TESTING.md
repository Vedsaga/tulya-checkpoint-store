# Deterministic process-crash matrix

The `fault-injection` Cargo feature enables process exit code 86 when
`TULYA_CHECKPOINT_STORE_CRASH_POINT` names a compiled boundary. Normal builds
compile the same calls to no-ops.

Run the complete matrix:

```bash
cargo test --locked --features fault-injection \
  --test crash_matrix -- --test-threads=1
```

The integration test runs all 16 boundaries twice: once while publishing the
first sealed generation and once while advancing a later generation. Every one
of the 32 cases must exit at the requested point, reopen to exactly the old or
target authority, reconstruct all sibling checkpoints, resume idempotently,
and reopen again at the target.

The boundaries cover segment, route, manifest, and WAL replacement after each
write, file sync, rename, and parent-directory sync. An additional append
boundary exists after hot-WAL sync and before in-memory publication.

This is intentionally feature-gated test instrumentation. Do not ship a
production binary compiled with `fault-injection`.
