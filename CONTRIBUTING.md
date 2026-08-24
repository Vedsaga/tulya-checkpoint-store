# Contributing

Please open an issue before changing public format bytes, durability ordering, or
benchmark semantics. A pull request should keep the worktree clean and pass:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --locked -- -D warnings
cargo test --all-targets --locked -- --test-threads=1
cargo test --locked --features fault-injection --test crash_matrix -- --test-threads=1
```

Format changes require a new golden-fixture compatibility test and an explicit
migration decision. Benchmark changes must preserve old results and losses;
never rewrite a failed or weaker result into a new claim.
