"""Small real-LangGraph branch/reopen smoke test for ``TulyaShadowSaver``."""

from __future__ import annotations

import operator
import tempfile
import unittest
from pathlib import Path
from typing import Annotated, TypedDict

from langgraph.checkpoint.memory import InMemorySaver
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
            primary = InMemorySaver()
            shadow = TulyaShadowSaver(
                primary,
                root / "tulya",
                binary=binary,
                fail_open=False,
            )
            builder = StateGraph(State)
            builder.add_edge(START, END)
            graph = builder.compile(checkpointer=shadow)
            base = {"configurable": {"thread_id": "demo", "checkpoint_ns": ""}}
            graph.invoke({"messages": ["root"]}, base)
            root_config = graph.get_state(base).config
            graph.invoke({"messages": ["left"]}, root_config)
            graph.invoke({"messages": ["right"]}, root_config)

            self.assertEqual(shadow.shadow_failures, ())
            self.assertEqual(shadow.shadow_stats()["checkpoint_count"], 3)

            # Recreate both adapter and graph, then continue from an old
            # branch point. The parent mapping must come from durable Tulya
            # state, not process-local dictionaries.
            restarted = TulyaShadowSaver(
                primary,
                root / "tulya",
                binary=binary,
                fail_open=False,
            )
            restarted_graph = builder.compile(checkpointer=restarted)
            restarted_graph.invoke({"messages": ["after-restart"]}, root_config)
            self.assertEqual(restarted.shadow_failures, ())
            self.assertEqual(restarted.shadow_stats()["checkpoint_count"], 4)
            self.assertEqual(restarted.verify_shadow()["failures"], 0)
            restarted.seal_shadow()
            self.assertEqual(restarted.verify_shadow()["failures"], 0)
            self.assertEqual(restarted.shadow_stats()["sealed_checkpoint_count"], 4)
            self.assertEqual(restarted.fsck_shadow()["failures"], 0)


class TulyaShadowAsyncSmokeTest(unittest.IsolatedAsyncioTestCase):
    """Exercise LangGraph's async saver surface through a real graph."""

    async def test_async_graph_mirrors_and_verifies(self) -> None:
        repo = Path(__file__).resolve().parents[2]
        binary = repo / "target" / "release" / "tulya-checkpoint"
        self.assertTrue(binary.is_file(), f"build the CLI first: {binary}")

        with tempfile.TemporaryDirectory() as raw_root:
            primary = InMemorySaver()
            shadow = TulyaShadowSaver(
                primary,
                Path(raw_root) / "tulya",
                binary=binary,
                fail_open=False,
            )
            builder = StateGraph(State)
            builder.add_edge(START, END)
            graph = builder.compile(checkpointer=shadow)
            config = {"configurable": {"thread_id": "async-demo"}}
            await graph.ainvoke({"messages": ["async-root"]}, config)
            self.assertEqual(shadow.shadow_failures, ())
            self.assertEqual(shadow.shadow_stats()["checkpoint_count"], 1)
            self.assertEqual(shadow.verify_shadow()["failures"], 0)


if __name__ == "__main__":
    unittest.main()
