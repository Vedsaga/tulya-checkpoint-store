# LangGraph verified shadow

`TulyaShadowSaver` leaves an existing LangGraph saver authoritative and mirrors
one append-only sequence channel (default: `messages`) into Tulya. This gives a
low-risk deployment path: reads, pending writes, DeltaChannel behavior,
deletion, copying, and pruning remain with the primary; Tulya retains a
separately verifiable branch history.

The adapter now covers LangGraph's sync and async methods, serializes concurrent
CLI writes, rebuilds its mapping from durable Tulya records on process restart,
continues from an old branch after restart, and exposes verify, read-only fsck,
stats, and seal operations. The real `StateGraph` smoke tests cover sibling
branches, adapter restart, branch continuation, seal/reopen, and async graph
execution.

It is deliberately not advertised as a drop-in saver. Tulya does not store
LangGraph pending writes or serve primary reads, and primary-side deletion or
pruning does not erase the Tulya audit shadow. The current adapter starts a CLI
process for each mirrored checkpoint, so its end-to-end write latency is not a
product performance claim.

```bash
cargo build --release --locked --bin tulya-checkpoint
python -m pip install -r integrations/langgraph/requirements.txt
PYTHONPATH=integrations/langgraph python -m unittest -v \
  integrations/langgraph/test_shadow_smoke.py
```
