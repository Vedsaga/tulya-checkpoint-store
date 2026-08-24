# Production checkpoint-store crash r2 — result

Date: 2026-08-23

Decision: **PASS on the current production checkpoint-store body.**

Source body SHA-256:
`a4963cd00a73c12d492c2a56065519f0556c58f2f145abbd6fb15db1bb520d04`.

The benchmark-only mechanically derived diagnostic build forced exit code 86
without unwinding at 16 named segment, route, manifest and WAL write/sync/
rename/directory-sync boundaries. It exercised those boundaries across two
advancing seal generations: 32 cases total.

Every case satisfied:

- fresh-process reopen observed only the complete old or complete target
  authority;
- exact checkpoint reconstruction and full verification;
- exact resume to the target generation;
- append after reclaim and append after reopen;
- clean source head/status before and after.

Raw summary SHA-256:
`cdffadf51753741d278cecca1664c59c8cfa45026b4970b8e125eb1dd73ffe5a`.

Claim boundary: process crash/restart exactness at named userspace durability
boundaries on this tested Linux/filesystem stack. No sudden-power-loss,
controller-cache, torn-sector or portability claim is authorized.
