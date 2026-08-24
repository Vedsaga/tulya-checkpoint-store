# Provenance

This focused repository was extracted on 2026-08-23 from:

- source repository: `/home/vedsaga/rust_projects/tulya-engine`;
- source commit: `4ab176c1dd4bbca143eefe1bcc0b6dac0a79ef24`;
- production store body SHA-256:
  `a4963cd00a73c12d492c2a56065519f0556c58f2f145abbd6fb15db1bb520d04`.

The initial copied production store body was byte-identical to that source
commit. Before the first public release it was mechanically decomposed into a
normal Rust module tree, then hardened in this repository with the single
public manifest, golden fixture, independent fsck, first-class crash matrix,
smaller public facade, restart-safe LangGraph shadow, CI and release evidence.

The `tulya-checkpoint-store-0.1.0.crate` artifact is built and verified by
`cargo package --locked`. Its digest is intentionally not embedded here:
because this file is packaged, doing so would create a self-referential hash.

The full historical raw manifests remain in the source repository.
`benchmarks/frozen_evidence.json` copies the claim ledger, digests, measured
summary, and explicit losses. The standalone executable protocol is under
`benchmarks/branch_forest/`; `docs/BENCHMARKS.md` preserves the permitted
interpretation.
