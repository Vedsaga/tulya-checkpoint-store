"""Small real-LangGraph branch/reopen smoke test for ``TulyaShadowSaver``."""

from __future__ import annotations

import operator
import sqlite3
import tempfile
import unittest
from pathlib import Path
from typing import Annotated, TypedDict

from langgraph.checkpoint.sqlite import SqliteSaver
from langgraph.graph import END, START, StateGraph

from tulya_shadow import TulyaShadowSaver


class State(TypedDict):
    """Minimal append-only graph state used by the smoke test."""

    messages: Annotated[list[str], operator.add]


class TulyaShadowSmokeTest(unittest.TestCase):
    """Exercise one root and two sibling invocations through public APIs."""

    def test_real_langgraph_sibling_branches_verify_after_seal(self) -> None:
        repo = Path(__file__).resolve().parents[2]
        binary = repo / "target" / "release" / "tulya-checkpoint"
        self.assertTrue(binary.is_file(), f"build the CLI first: {binary}")

        with tempfile.TemporaryDirectory() as raw_root:
            root = Path(raw_root)
            connection = sqlite3.connect(
                root / "primary.sqlite", check_same_thread=False
            )
            try:
                primary = SqliteSaver(connection)
                shadow = TulyaShadowSaver(
                    primary,
                    root / "tulya",
                    binary=binary,
                    fail_open=False,
                )
                builder = StateGraph(State)
                builder.add_edge(START, END)
                graph = builder.compile(checkpointer=shadow)
                base = {
                    "configurable": {"thread_id": "demo", "checkpoint_ns": ""}
                }
                graph.invoke({"messages": ["root"]}, base)
                root_config = graph.get_state(base).config
                graph.invoke({"messages": ["left"]}, root_config)
                graph.invoke({"messages": ["right"]}, root_config)

                self.assertEqual(shadow.shadow_failures, ())
                self.assertEqual(shadow.shadow_stats()["checkpoint_count"], 3)
                self.assertEqual(shadow.verify_shadow()["failures"], 0)
                shadow.seal_shadow()
                self.assertEqual(shadow.verify_shadow()["failures"], 0)
                self.assertEqual(
                    shadow.shadow_stats()["sealed_checkpoint_count"], 3
                )
            finally:
                connection.close()


if __name__ == "__main__":
    unittest.main()
