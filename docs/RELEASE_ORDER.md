# Release order: `tulya-core` first, adapter second

The workspace holds two releasable packages with a one-way dependency:

```text
tulya-checkpoint-store (adapter, root package)
    depends on
tulya-core (crates/tulya-core, versioned path dependency)
```

## Order

1. Publish `tulya-core` at version `X.Y.Z`.
2. Only then publish `tulya-checkpoint-store` at a version whose
   `tulya-core = { version = "...", path = "crates/tulya-core" }`
   requirement is satisfied by the published core.

`cargo package` rewrites a path dependency into a plain version requirement,
so packaging or publishing the adapter is impossible while the required core
version exists only as a workspace path. That failure is the release order
talking, not a bug: do NOT publish a placeholder core, vendor a fake
registry, or loosen the version requirement merely to make adapter packaging
green.

## What CI enforces (pre-publication)

- `cargo package -p tulya-core --locked` must pass: the core is always
  publishable on its own (self-contained manifest, complete file list,
  verification build).
- The `packaged-artifact` job stages a git-free `git archive` export of the
  revision, overlays the exact `.crate` bits produced for `tulya-core`, and
  runs both libraries' test suites plus the benchmark/bench binaries there.
  Every cargo command in that stage therefore builds and tests the adapter
  against precisely what would be released as `tulya-core`, without
  pretending crates.io already hosts it.
- The adapter package itself is NOT `cargo package`-validated until the core
  version it requires is published; the workspace `cargo test`/`clippy`/`fmt`
  gates cover the adapter sources on every revision.

## Version policy

- Core and adapter versions advance in lockstep while pre-1.0: any core
  change that alters persisted bytes, the public API, or crash semantics
  bumps both crates together in one slice.
- The root `Cargo.toml` `include` list deliberately excludes `crates/**`:
  the adapter crate must never bundle core sources; it resolves the core
  from the registry at publish time and from the workspace path in-tree.
