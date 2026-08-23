# LangGraph shadow adapter

`TulyaShadowSaver` keeps an existing LangGraph saver authoritative and mirrors
an append-only sequence channel (default: `messages`) into Tulya.

This is a low-risk design-partner probe, not a drop-in replacement. Reads,
pending writes and graph execution remain on the primary saver. Mirroring is
fail-open by default and exposes explicit verification, stats and seal calls.

Build the CLI, install LangGraph plus its SQLite checkpoint package in a Python
environment, then run:

```bash
PYTHONPATH=integrations/langgraph python -m unittest -v \
  integrations/langgraph/test_shadow_smoke.py
```
