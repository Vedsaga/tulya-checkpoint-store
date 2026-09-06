"""Fail-open LangGraph shadow writer for Tulya's branch-aware history store.

The configured LangGraph saver remains authoritative. Tulya mirrors one
append-only sequence channel and provides independently verifiable history.
Pending writes, reads, deletes, copies, pruning, and DeltaChannel behavior are
delegated to the primary saver. Tulya deliberately retains mirrored records as
an audit shadow when the primary deletes or prunes them.
"""

from __future__ import annotations

import asyncio
import base64
import json
import os
import shutil
import subprocess
import threading
import time
from collections.abc import AsyncIterator, Iterator, Mapping, Sequence
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


_MIRROR_LATENCY_SAMPLE_LIMIT = 4096


class TulyaShadowSaver(BaseCheckpointSaver[Any]):
    """Delegate to a primary saver while mirroring an append-only channel.

    Args:
        primary: Authoritative LangGraph checkpoint saver.
        store_dir: Tulya store directory.
        binary: Path to ``tulya-checkpoint``. When omitted, use
            ``TULYA_CHECKPOINT_BIN`` or search ``PATH``.
        channel: Sequence-valued checkpoint channel to mirror.
        fail_open: Keep the primary saver available after a mirror failure.

    Channel values are encoded through the primary saver's serializer and
    stored as JSON envelopes. Adapter indexes are rebuilt from Tulya at startup
    so a restarted process can continue an existing branch.
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
        self._lock = threading.RLock()
        self._async_mirror_lock: asyncio.Lock | None = None
        self._primary_to_tulya: dict[tuple[str, str, str], str | None] = {}
        self._messages_by_tulya_id: dict[tuple[str, str], list[dict[str, str]]] = {}
        self._failures: list[dict[str, str]] = []
        self._mirror_callbacks_total = 0
        self._mirror_callback_successes = 0
        self._mirror_callback_failures = 0
        self._mirror_callback_latency_ns_total = 0
        self._mirror_callback_latency_ns_min: int | None = None
        self._mirror_callback_latency_ns_max: int | None = None
        self._mirror_callback_latency_samples: list[int] = []
        self._next_checkpoint_no = int(self._run("stats")["checkpoint_count"])
        self._restore_shadow_index()

    @property
    def config_specs(self) -> list[Any]:
        """Return the authoritative saver's configuration fields."""

        return self.primary.config_specs

    @property
    def shadow_failures(self) -> tuple[dict[str, str], ...]:
        """Return immutable descriptions of fail-open mirror failures."""

        with self._lock:
            return tuple(self._failures)

    def _run(
        self,
        command: str,
        *arguments: str,
        stdin_value: Any | None = None,
    ) -> dict[str, Any]:
        with self._lock:
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
        return self._decode_command_result(
            completed.stdout, completed.stderr, completed.returncode
        )

    @staticmethod
    def _decode_command_result(
        stdout: bytes, stderr: bytes, returncode: int
    ) -> dict[str, Any]:
        try:
            result = json.loads(stdout)
        except json.JSONDecodeError as error:
            raise TulyaShadowError(
                f"Tulya command produced invalid JSON: {stdout!r}; "
                f"stderr={stderr.decode(errors='replace')!r}"
            ) from error
        if returncode != 0 or result.get("ok") is not True:
            raise TulyaShadowError(
                f"Tulya command failed: {result}; "
                f"stderr={stderr.decode(errors='replace')!r}"
            )
        return result

    async def _run_async(
        self,
        command: str,
        *arguments: str,
        stdin_value: Any | None = None,
    ) -> dict[str, Any]:
        argv = [str(self.binary), "--db", str(self.store_dir), command, *arguments]
        encoded_input = (
            json.dumps(
                stdin_value,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8")
            if stdin_value is not None
            else None
        )
        process = await asyncio.create_subprocess_exec(
            *argv,
            stdin=(
                asyncio.subprocess.PIPE
                if encoded_input is not None
                else asyncio.subprocess.DEVNULL
            ),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await process.communicate(encoded_input)
        return self._decode_command_result(stdout, stderr, process.returncode)

    def _async_mirror_lock_for_loop(self) -> asyncio.Lock:
        if self._async_mirror_lock is None:
            self._async_mirror_lock = asyncio.Lock()
        return self._async_mirror_lock

    @staticmethod
    def _decode_thread_key(thread_key: str) -> tuple[str, str]:
        try:
            decoded = json.loads(thread_key)
        except json.JSONDecodeError as error:
            raise TulyaShadowError(
                f"Tulya thread key is not a LangGraph shadow key: {thread_key!r}"
            ) from error
        if not isinstance(decoded, list) or len(decoded) != 2:
            raise TulyaShadowError(
                f"Tulya thread key is not a two-part LangGraph key: {thread_key!r}"
            )
        return str(decoded[0]), str(decoded[1])

    def _restore_shadow_index(self) -> None:
        """Rebuild in-memory mappings solely from durable Tulya records."""

        for checkpoint in self._run("list").get("checkpoints", []):
            thread_key = str(checkpoint["thread_id"])
            thread_id, checkpoint_ns = self._decode_thread_key(thread_key)
            checkpoint_id = str(checkpoint["checkpoint_id"])
            state = self._run(
                "get",
                "--thread-id",
                thread_key,
                "--checkpoint-id",
                checkpoint_id,
            )["state"]
            messages = state.get("messages") if isinstance(state, dict) else None
            if not isinstance(messages, list):
                raise TulyaShadowError(
                    f"Tulya checkpoint {checkpoint_id!r} has no envelope list"
                )
            self._primary_to_tulya[(thread_id, checkpoint_ns, checkpoint_id)] = (
                checkpoint_id
            )
            self._messages_by_tulya_id[(thread_key, checkpoint_id)] = messages

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

    def _resolve_parent_tulya(
        self, thread_id: str, checkpoint_ns: str, primary_checkpoint_id: str
    ) -> str | None:
        """Walk through primary checkpoints that emitted no mirrored delta."""

        visited: list[tuple[str, str, str]] = []
        cursor: str | None = primary_checkpoint_id
        resolved: str | None = None
        while cursor is not None:
            key = (thread_id, checkpoint_ns, cursor)
            if key in self._primary_to_tulya:
                resolved = self._primary_to_tulya[key]
                break
            visited.append(key)
            item = self.primary.get_tuple(
                {
                    "configurable": {
                        "thread_id": thread_id,
                        "checkpoint_ns": checkpoint_ns,
                        "checkpoint_id": cursor,
                    }
                }
            )
            if item is None or item.parent_config is None:
                cursor = None
            else:
                parent_id = item.parent_config.get("configurable", {}).get(
                    "checkpoint_id"
                )
                cursor = str(parent_id) if parent_id is not None else None
        for key in visited:
            self._primary_to_tulya[key] = resolved
        return resolved

    def _mirror_put(
        self,
        config: RunnableConfig,
        saved: RunnableConfig,
        checkpoint: Checkpoint,
    ) -> None:
        with self._lock:
            configurable = config.get("configurable", {})
            saved_configurable = saved.get("configurable", {})
            thread_id = str(configurable["thread_id"])
            checkpoint_ns = str(configurable.get("checkpoint_ns", ""))
            primary_checkpoint_id = str(saved_configurable["checkpoint_id"])
            primary_parent_id = configurable.get("checkpoint_id")
            parent_tulya_id = (
                self._resolve_parent_tulya(
                    thread_id, checkpoint_ns, str(primary_parent_id)
                )
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
            existing = self._primary_to_tulya.get(current_key)
            if existing is not None and self._messages_by_tulya_id.get(
                (thread_key, existing)
            ) == current:
                return
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

    async def _mirror_put_async(
        self,
        config: RunnableConfig,
        saved: RunnableConfig,
        checkpoint: Checkpoint,
    ) -> None:
        """Mirror one checkpoint without blocking the active asyncio loop."""

        with self._lock:
            configurable = config.get("configurable", {})
            saved_configurable = saved.get("configurable", {})
            thread_id = str(configurable["thread_id"])
            checkpoint_ns = str(configurable.get("checkpoint_ns", ""))
            primary_checkpoint_id = str(saved_configurable["checkpoint_id"])
            primary_parent_id = configurable.get("checkpoint_id")
            parent_tulya_id = (
                self._resolve_parent_tulya(
                    thread_id, checkpoint_ns, str(primary_parent_id)
                )
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
            existing = self._primary_to_tulya.get(current_key)
            if existing is not None and self._messages_by_tulya_id.get(
                (thread_key, existing)
            ) == current:
                return
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
            await self._run_async("put", *arguments, stdin_value=delta)
            self._primary_to_tulya[current_key] = primary_checkpoint_id
            self._messages_by_tulya_id[(thread_key, primary_checkpoint_id)] = current
            self._next_checkpoint_no += 1

    def _record_mirror_failure(self, checkpoint: Checkpoint, error: Exception) -> None:
        with self._lock:
            self._failures.append(
                {
                    "checkpoint_id": str(checkpoint.get("id", "unknown")),
                    "error": str(error),
                }
            )

    def _record_mirror_observation(self, elapsed_ns: int, succeeded: bool) -> None:
        """Record bounded per-checkpoint shadow overhead for pilot accounting."""

        with self._lock:
            self._mirror_callbacks_total += 1
            if succeeded:
                self._mirror_callback_successes += 1
            else:
                self._mirror_callback_failures += 1
            self._mirror_callback_latency_ns_total += elapsed_ns
            if (
                self._mirror_callback_latency_ns_min is None
                or elapsed_ns < self._mirror_callback_latency_ns_min
            ):
                self._mirror_callback_latency_ns_min = elapsed_ns
            if (
                self._mirror_callback_latency_ns_max is None
                or elapsed_ns > self._mirror_callback_latency_ns_max
            ):
                self._mirror_callback_latency_ns_max = elapsed_ns
            if len(self._mirror_callback_latency_samples) < _MIRROR_LATENCY_SAMPLE_LIMIT:
                self._mirror_callback_latency_samples.append(elapsed_ns)
            else:
                slot = (self._mirror_callbacks_total - 1) % _MIRROR_LATENCY_SAMPLE_LIMIT
                self._mirror_callback_latency_samples[slot] = elapsed_ns

    def shadow_metrics(self) -> dict[str, Any]:
        """Return bounded mirror-overhead metrics for a pilot report.

        Latency percentiles are computed from at most the most recent cyclic
        sample of callbacks. The primary saver remains authoritative; these
        metrics describe only the adapter's mirror callback, including the
        per-checkpoint CLI process used by this alpha implementation.
        """

        with self._lock:
            sample = sorted(self._mirror_callback_latency_samples)
            if sample:
                p50 = sample[(len(sample) - 1) // 2]
                p95 = sample[min(len(sample) - 1, int((len(sample) - 1) * 0.95))]
            else:
                p50 = None
                p95 = None
            return {
                "fail_open": self.fail_open,
                "mirror_callbacks_total": self._mirror_callbacks_total,
                "mirror_callback_successes": self._mirror_callback_successes,
                "mirror_callback_failures": self._mirror_callback_failures,
                "shadow_failures": len(self._failures),
                "mirror_callback_latency_ns": {
                    "count": self._mirror_callbacks_total,
                    "sample_count": len(sample),
                    "sample_limit": _MIRROR_LATENCY_SAMPLE_LIMIT,
                    "sample_scope": "cyclic bounded callback sample",
                    "total": self._mirror_callback_latency_ns_total,
                    "min": self._mirror_callback_latency_ns_min,
                    "p50": p50,
                    "p95": p95,
                    "max": self._mirror_callback_latency_ns_max,
                },
            }

    def get_tuple(self, config: RunnableConfig) -> CheckpointTuple | None:
        return self.primary.get_tuple(config)

    def list(
        self,
        config: RunnableConfig | None,
        *,
        filter: dict[str, Any] | None = None,
        before: RunnableConfig | None = None,
        limit: int | None = None,
    ) -> Iterator[CheckpointTuple]:
        return self.primary.list(config, filter=filter, before=before, limit=limit)

    def put(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        saved = self.primary.put(config, checkpoint, metadata, new_versions)
        started_ns = time.perf_counter_ns()
        succeeded = False
        try:
            self._mirror_put(config, saved, checkpoint)
            succeeded = True
        except Exception as error:
            self._record_mirror_failure(checkpoint, error)
            if not self.fail_open:
                raise
        finally:
            self._record_mirror_observation(time.perf_counter_ns() - started_ns, succeeded)
        return saved

    def put_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        self.primary.put_writes(config, writes, task_id, task_path)

    def get_delta_channel_history(
        self, *, config: RunnableConfig, channels: Sequence[str]
    ) -> Mapping[str, Any]:
        return self.primary.get_delta_channel_history(
            config=config, channels=channels
        )

    def delete_thread(self, thread_id: str) -> None:
        self.primary.delete_thread(thread_id)

    def delete_for_runs(self, run_ids: Sequence[str]) -> None:
        self.primary.delete_for_runs(run_ids)

    def copy_thread(self, source_thread_id: str, target_thread_id: str) -> None:
        self.primary.copy_thread(source_thread_id, target_thread_id)

    def prune(
        self, thread_ids: Sequence[str], *, strategy: str = "keep_latest"
    ) -> None:
        self.primary.prune(thread_ids, strategy=strategy)

    def get_next_version(self, current: Any | None, channel: None) -> Any:
        return self.primary.get_next_version(current, channel)

    async def aget_tuple(self, config: RunnableConfig) -> CheckpointTuple | None:
        return await self.primary.aget_tuple(config)

    async def alist(
        self,
        config: RunnableConfig | None,
        *,
        filter: dict[str, Any] | None = None,
        before: RunnableConfig | None = None,
        limit: int | None = None,
    ) -> AsyncIterator[CheckpointTuple]:
        async for item in self.primary.alist(
            config, filter=filter, before=before, limit=limit
        ):
            yield item

    async def aput(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        saved = await self.primary.aput(config, checkpoint, metadata, new_versions)
        started_ns = time.perf_counter_ns()
        succeeded = False
        try:
            async with self._async_mirror_lock_for_loop():
                await self._mirror_put_async(config, saved, checkpoint)
            succeeded = True
        except Exception as error:
            self._record_mirror_failure(checkpoint, error)
            if not self.fail_open:
                raise
        finally:
            self._record_mirror_observation(time.perf_counter_ns() - started_ns, succeeded)
        return saved

    async def aput_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        await self.primary.aput_writes(config, writes, task_id, task_path)

    async def aget_delta_channel_history(
        self, *, config: RunnableConfig, channels: Sequence[str]
    ) -> Mapping[str, Any]:
        return await self.primary.aget_delta_channel_history(
            config=config, channels=channels
        )

    async def adelete_thread(self, thread_id: str) -> None:
        await self.primary.adelete_thread(thread_id)

    async def adelete_for_runs(self, run_ids: Sequence[str]) -> None:
        await self.primary.adelete_for_runs(run_ids)

    async def acopy_thread(
        self, source_thread_id: str, target_thread_id: str
    ) -> None:
        await self.primary.acopy_thread(source_thread_id, target_thread_id)

    async def aprune(
        self, thread_ids: Sequence[str], *, strategy: str = "keep_latest"
    ) -> None:
        await self.primary.aprune(thread_ids, strategy=strategy)

    def verify_shadow(self) -> dict[str, Any]:
        return self._run("verify")

    def fsck_shadow(self) -> dict[str, Any]:
        return self._run("fsck")

    def shadow_stats(self) -> dict[str, Any]:
        return self._run("stats")

    def seal_shadow(self) -> dict[str, Any]:
        return self._run("seal")
