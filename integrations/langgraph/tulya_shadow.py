"""Fail-open LangGraph shadow writer for Tulya's append-only message store.

This adapter deliberately leaves the configured LangGraph saver authoritative.
It mirrors checkpoints whose selected channel is a growing sequence into Tulya,
then exposes explicit verification/seal calls. It is a product-integration
probe, not yet a complete replacement checkpointer: pending writes, deletes,
pruning, encryption policy, and async methods remain owned by the primary saver.
"""

from __future__ import annotations

import base64
import json
import os
import shutil
import subprocess
from collections.abc import Iterator, Sequence
from pathlib import Path
from typing import Any

from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.base import (
    BaseCheckpointSaver,
    ChannelVersions,
    Checkpoint,
    CheckpointMetadata,
    CheckpointTuple,
)


class TulyaShadowError(RuntimeError):
    """Raised when a strict Tulya shadow operation cannot be completed."""


class TulyaShadowSaver(BaseCheckpointSaver[Any]):
    """Delegate to a primary saver while mirroring an append-only channel.

    Args:
        primary: The authoritative LangGraph checkpoint saver.
        store_dir: Tulya store directory.
        binary: Path to the ``tulya-checkpoint`` binary. If omitted, use
            ``TULYA_CHECKPOINT_BIN`` or search ``PATH``.
        channel: Sequence-valued checkpoint channel to mirror.
        fail_open: Keep the primary saver available after a mirror failure.

    Each channel item is encoded through the primary saver's serializer and
    stored as a JSON envelope. This supports values that are not natively JSON
    serializable without asking Tulya to reinterpret LangGraph object types.
    """

    def __init__(
        self,
        primary: BaseCheckpointSaver[Any],
        store_dir: str | os.PathLike[str],
        *,
        binary: str | os.PathLike[str] | None = None,
        channel: str = "messages",
        fail_open: bool = True,
    ) -> None:
        super().__init__(serde=primary.serde)
        selected = (
            Path(binary)
            if binary is not None
            else Path(os.environ["TULYA_CHECKPOINT_BIN"])
            if "TULYA_CHECKPOINT_BIN" in os.environ
            else Path(shutil.which("tulya-checkpoint") or "")
        )
        if not str(selected) or not selected.is_file():
            raise TulyaShadowError(
                "tulya-checkpoint binary not found; pass binary= or set "
                "TULYA_CHECKPOINT_BIN"
            )
        self.primary = primary
        self.store_dir = Path(store_dir).resolve()
        self.binary = selected.resolve()
        self.channel = channel
        self.fail_open = fail_open
        self._primary_to_tulya: dict[tuple[str, str, str], str | None] = {}
        self._messages_by_tulya_id: dict[tuple[str, str], list[dict[str, str]]] = {}
        self._failures: list[dict[str, str]] = []
        self._next_checkpoint_no = int(self._run("stats")["checkpoint_count"])

    @property
    def config_specs(self) -> list[Any]:
        """Return the authoritative saver's configuration fields."""

        return self.primary.config_specs

    @property
    def shadow_failures(self) -> tuple[dict[str, str], ...]:
        """Return immutable descriptions of fail-open mirror failures."""

        return tuple(self._failures)

    def _run(
        self,
        command: str,
        *arguments: str,
        stdin_value: Any | None = None,
    ) -> dict[str, Any]:
        argv = [str(self.binary), "--db", str(self.store_dir), command, *arguments]
        completed = subprocess.run(
            argv,
            input=(
                json.dumps(
                    stdin_value,
                    ensure_ascii=False,
                    separators=(",", ":"),
                    sort_keys=True,
                ).encode("utf-8")
                if stdin_value is not None
                else None
            ),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
        try:
            result = json.loads(completed.stdout)
        except json.JSONDecodeError as error:
            raise TulyaShadowError(
                f"Tulya command produced invalid JSON: {completed.stdout!r}; "
                f"stderr={completed.stderr.decode(errors='replace')!r}"
            ) from error
        if completed.returncode != 0 or result.get("ok") is not True:
            raise TulyaShadowError(
                f"Tulya command failed: {result}; "
                f"stderr={completed.stderr.decode(errors='replace')!r}"
            )
        return result

    def _thread_key(self, thread_id: str, checkpoint_ns: str) -> str:
        return json.dumps(
            [thread_id, checkpoint_ns],
            ensure_ascii=False,
            separators=(",", ":"),
        )

    def _encoded_messages(self, checkpoint: Checkpoint) -> list[dict[str, str]] | None:
        if self.channel not in checkpoint["channel_values"]:
            return None
        value = checkpoint["channel_values"][self.channel]
        values = value if isinstance(value, list) else [value]
        encoded: list[dict[str, str]] = []
        for item in values:
            type_name, payload = self.serde.dumps_typed(item)
            encoded.append(
                {
                    "serde_type": type_name,
                    "payload_base64": base64.b64encode(payload).decode("ascii"),
                }
            )
        return encoded

    def _mirror_put(
        self,
        config: RunnableConfig,
        saved: RunnableConfig,
        checkpoint: Checkpoint,
    ) -> None:
        configurable = config.get("configurable", {})
        saved_configurable = saved.get("configurable", {})
        thread_id = str(configurable["thread_id"])
        checkpoint_ns = str(configurable.get("checkpoint_ns", ""))
        primary_checkpoint_id = str(saved_configurable["checkpoint_id"])
        primary_parent_id = configurable.get("checkpoint_id")
        primary_parent_key = (
            thread_id,
            checkpoint_ns,
            str(primary_parent_id),
        )
        parent_tulya_id = (
            self._primary_to_tulya.get(primary_parent_key)
            if primary_parent_id is not None
            else None
        )
        current = self._encoded_messages(checkpoint)
        current_key = (thread_id, checkpoint_ns, primary_checkpoint_id)
        if current is None:
            self._primary_to_tulya[current_key] = parent_tulya_id
            return

        thread_key = self._thread_key(thread_id, checkpoint_ns)
        prior = (
            self._messages_by_tulya_id.get((thread_key, parent_tulya_id), [])
            if parent_tulya_id is not None
            else []
        )
        if len(current) < len(prior) or current[: len(prior)] != prior:
            raise TulyaShadowError(
                f"channel {self.channel!r} is not append-only at "
                f"checkpoint {primary_checkpoint_id}"
            )
        delta = current[len(prior) :]
        if not delta:
            self._primary_to_tulya[current_key] = parent_tulya_id
            return

        arguments = [
            "--thread-id",
            thread_key,
            "--checkpoint-id",
            primary_checkpoint_id,
            "--checkpoint-no",
            str(self._next_checkpoint_no),
        ]
        if parent_tulya_id is not None:
            arguments.extend(["--parent-checkpoint-id", parent_tulya_id])
        self._run("put", *arguments, stdin_value=delta)
        self._primary_to_tulya[current_key] = primary_checkpoint_id
        self._messages_by_tulya_id[(thread_key, primary_checkpoint_id)] = current
        self._next_checkpoint_no += 1

    def get_tuple(self, config: RunnableConfig) -> CheckpointTuple | None:
        """Read from the authoritative saver."""

        return self.primary.get_tuple(config)

    def list(
        self,
        config: RunnableConfig | None,
        *,
        filter: dict[str, Any] | None = None,
        before: RunnableConfig | None = None,
        limit: int | None = None,
    ) -> Iterator[CheckpointTuple]:
        """List from the authoritative saver."""

        return self.primary.list(config, filter=filter, before=before, limit=limit)

    def put(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        """Commit to the primary saver and then mirror the selected channel."""

        saved = self.primary.put(config, checkpoint, metadata, new_versions)
        try:
            self._mirror_put(config, saved, checkpoint)
        except Exception as error:
            self._failures.append(
                {
                    "checkpoint_id": str(checkpoint.get("id", "unknown")),
                    "error": str(error),
                }
            )
            if not self.fail_open:
                raise
        return saved

    def put_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        """Persist pending writes only in the authoritative saver."""

        self.primary.put_writes(config, writes, task_id, task_path)

    def get_delta_channel_history(
        self, config: RunnableConfig, channel: str
    ) -> Any:
        """Delegate beta DeltaChannel history reads to the primary saver."""

        return self.primary.get_delta_channel_history(config, channel)

    def verify_shadow(self) -> dict[str, Any]:
        """Run Tulya's complete logical-state verifier."""

        return self._run("verify")

    def shadow_stats(self) -> dict[str, Any]:
        """Return Tulya physical storage and lifecycle counters."""

        return self._run("stats")

    def seal_shadow(self) -> dict[str, Any]:
        """Seal every mirrored checkpoint and reclaim the hot WAL prefix."""

        return self._run("seal")

