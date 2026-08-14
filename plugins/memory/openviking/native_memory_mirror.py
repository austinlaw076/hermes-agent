"""Stable mirroring for Hermes native MEMORY.md / USER.md entries.

Hermes' built-in memory tool identifies entries by unique text substrings rather
than durable IDs. OpenViking, by contrast, needs an exact ``viking://`` file URI
to update or delete a memory safely. This module keeps the missing identity map
in a small profile-scoped registry and serializes mirror operations through one
FIFO worker.

The registry is intentionally narrow: it tracks only memories created through
the built-in-memory mirror. Session-extracted memories and explicit
``viking_remember`` writes are outside its ownership and are never guessed at or
deleted by similarity.
"""

from __future__ import annotations

import json
import logging
import os
import queue
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from utils import atomic_json_write

logger = logging.getLogger("plugins.memory.openviking")

_LEGACY_REGISTRY_VERSION = 1
_REGISTRY_VERSION = 2
_REGISTRY_RELATIVE_PATH = Path("openviking") / "memory_mirror_registry.json"
_OUTBOX_RELATIVE_PATH = Path("openviking") / "memory_mirror_outbox.jsonl"
_SUPPORTED_ACTIONS = frozenset({"add", "replace", "remove"})
_REGISTRY_STATES = frozenset({"active", "pending_create", "pending_replace", "pending_delete"})
_POLL_SECONDS = 0.05
_JOURNAL_VERSION = 1
_ACK_OUTCOMES = frozenset({"applied", "already_applied", "already_absent"})
_REPAIR_REASON_TORN_TAIL = "torn_tail"


class _MappingError(RuntimeError):
    """A destructive mirror operation could not resolve one exact URI."""


class _RemoteConflict(RuntimeError):
    """Remote state conflicts with a durable deterministic mirror event."""


class NativeMemoryMirror:
    """FIFO, profile-scoped mirror of Hermes native memory into OpenViking."""

    def __init__(self, provider: Any):
        self._provider = provider
        self._queue: queue.Queue[Dict[str, Any]] = queue.Queue()
        self._state_lock = threading.Lock()
        self._journal_lock = threading.Lock()
        self._worker: Optional[threading.Thread] = None
        self._shutting_down = False
        self._stop_event = threading.Event()
        self._started = False
        self._recovery_error = ""
        self._delivery_blocked = False
        self._delivery_error = ""

    def _ensure_worker_locked(self) -> None:
        if self._worker is not None and self._worker.is_alive():
            return
        if self._queue.empty() or self._shutting_down or self._delivery_blocked:
            return
        self._worker = threading.Thread(
            target=self._run,
            daemon=True,
            name="openviking-memory-mirror",
        )
        self._worker.start()

    def _repair_registry_from_pending(
        self,
        registry: Dict[str, Any],
        pending: list[Dict[str, Any]],
    ) -> None:
        """Rebuild latest intended registry state from exact durable events."""
        for event in pending:
            matches = [
                (index, entry)
                for index, entry in enumerate(registry["entries"])
                if entry["uri"] == event["uri"]
            ]
            if len(matches) > 1:
                raise RuntimeError(
                    f"mirror registry has duplicate URI mappings for {event['uri']}"
                )
            if matches and matches[0][1]["target"] != event["target"]:
                raise RuntimeError(
                    f"mirror registry target conflicts with durable event {event['event_id']}"
                )

            if matches:
                mapping = matches[0][1]
            else:
                baseline = event["after"] if event["action"] == "add" else event["before"]
                if not isinstance(baseline, str):
                    raise RuntimeError(
                        f"durable event {event['event_id']} cannot reconstruct registry state"
                    )
                mapping = {
                    "target": event["target"],
                    "uri": event["uri"],
                    "content": baseline,
                    "state": "active",
                    "pending_event_id": "",
                }
                registry["entries"].append(mapping)

            if event["action"] == "add":
                if not isinstance(event["after"], str):
                    raise RuntimeError(
                        f"durable add event {event['event_id']} has no content"
                    )
                mapping["content"] = event["after"]
                mapping["state"] = "pending_create"
            elif event["action"] == "replace":
                if not isinstance(event["after"], str):
                    raise RuntimeError(
                        f"durable replace event {event['event_id']} has no content"
                    )
                mapping["content"] = event["after"]
                mapping["state"] = "pending_replace"
            else:
                if not isinstance(event["before"], str):
                    raise RuntimeError(
                        f"durable remove event {event['event_id']} has no prior content"
                    )
                mapping["content"] = event["before"]
                mapping["state"] = "pending_delete"
            mapping["pending_event_id"] = event["event_id"]

    def start(self) -> bool:
        """Recover durable pending work once, then start ordered replay."""
        with self._state_lock:
            if self._started:
                return not self._recovery_error
            try:
                events, acked = self._scan_journal()
                pending = [event for event in events if event["event_id"] not in acked]
                registry = self._load_registry()
                if pending:
                    self._repair_registry_from_pending(registry, pending)
                    self._save_registry(registry)
                    for event in pending:
                        self._queue.put(event)
                self._started = True
                self._ensure_worker_locked()
                return True
            except Exception as exc:
                self._started = True
                self._recovery_error = str(exc)
                logger.error(
                    "OpenViking memory mirror recovery failed; durable replay disabled: %s",
                    exc,
                )
                return False

    def enqueue(
        self,
        action: str,
        target: str,
        content: str,
        *,
        metadata: Optional[Dict[str, Any]] = None,
        subdir: str,
    ) -> None:
        """Durably record one native-memory mutation, then queue delivery."""
        if action not in _SUPPORTED_ACTIONS:
            return
        if action in {"add", "replace"} and not content:
            return

        target = str(target or "memory")
        content = str(content or "")
        metadata = dict(metadata or {})
        subdir = str(subdir)

        if not self.start():
            logger.error(
                "OpenViking memory mirror skipped %s because durable recovery is blocked: %s",
                action,
                self._recovery_error,
            )
            return

        with self._state_lock:
            if self._shutting_down:
                logger.warning(
                    "OpenViking memory mirror skipped %s during provider shutdown",
                    action,
                )
                return

            registry = self._load_registry()
            try:
                event = self._build_event(
                    registry,
                    action=action,
                    target=target,
                    content=content,
                    metadata=metadata,
                    subdir=subdir,
                )
            except _MappingError as exc:
                logger.warning("OpenViking memory mirror skipped: %s", exc)
                return
            if event is None:
                return

            # Journal is the durability boundary. Registry intent is advanced
            # only after the exact event has reached stable storage.
            self._append_record(event)
            self._apply_registry_intent(registry, event)
            self._save_registry(registry)

            self._queue.put(event)
            if self._delivery_blocked:
                logger.warning(
                    "OpenViking memory mirror event %s journaled behind unresolved "
                    "deterministic conflict: %s",
                    event["event_id"],
                    self._delivery_error,
                )
            self._ensure_worker_locked()

    def shutdown(self, timeout: float = 5.0) -> None:
        """Drain queued work when possible, then interrupt retries safely."""
        with self._state_lock:
            self._shutting_down = True
            worker = self._worker

        if worker is None:
            return

        deadline = time.monotonic() + max(0.0, timeout)
        while self._queue.unfinished_tasks and time.monotonic() < deadline:
            time.sleep(0.01)

        if self._queue.unfinished_tasks:
            self._stop_event.set()

        remaining = max(0.0, deadline - time.monotonic())
        if worker.is_alive() and remaining:
            worker.join(timeout=remaining)
        if worker.is_alive():
            self._stop_event.set()
            worker.join(timeout=0.1)
        if worker.is_alive():
            logger.warning(
                "OpenViking memory mirror worker did not drain before shutdown"
            )

    @staticmethod
    def _retry_delay(attempt: int) -> float:
        return float(min(30, 2 ** max(0, attempt - 1)))

    def _discard_queued_events(self) -> None:
        while True:
            try:
                self._queue.get_nowait()
            except queue.Empty:
                return
            else:
                self._queue.task_done()

    def _run(self) -> None:
        while True:
            with self._state_lock:
                stopping = self._shutting_down
            if stopping and self._queue.empty():
                return

            try:
                event = self._queue.get(timeout=_POLL_SECONDS)
            except queue.Empty:
                continue

            attempt = 0
            while True:
                try:
                    outcome = self._apply_remote(event)
                    self._ack_event(event, outcome)
                except _RemoteConflict as exc:
                    logger.error(
                        "OpenViking memory mirror deterministic conflict: %s", exc
                    )
                    with self._state_lock:
                        self._delivery_blocked = True
                        self._delivery_error = str(exc)
                    self._queue.task_done()
                    self._discard_queued_events()
                    return
                except Exception as exc:
                    attempt += 1
                    if attempt == 1 or attempt in {2, 4, 8}:
                        logger.warning(
                            "OpenViking memory mirror delivery failed; retrying "
                            "event %s (attempt %d): %s",
                            event.get("event_id", ""),
                            attempt,
                            exc,
                        )
                    if self._stop_event.wait(self._retry_delay(attempt)):
                        self._queue.task_done()
                        self._discard_queued_events()
                        return
                    continue
                else:
                    self._queue.task_done()
                    break

    def _hermes_root(self) -> Path:
        root = str(getattr(self._provider, "_hermes_home", "") or "").strip()
        if not root:
            from hermes_constants import get_hermes_home

            root = str(get_hermes_home())
        return Path(root)

    def _outbox_path(self) -> Path:
        return self._hermes_root() / _OUTBOX_RELATIVE_PATH

    @staticmethod
    def _fsync_directory(path: Path) -> None:
        if os.name == "nt":
            return
        flags = os.O_RDONLY
        if hasattr(os, "O_DIRECTORY"):
            flags |= os.O_DIRECTORY
        try:
            fd = os.open(path, flags)
        except OSError:
            return
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    def _append_record(self, record: Dict[str, Any]) -> None:
        """Durably append one JSONL record in process-local FIFO order."""
        with self._journal_lock:
            self._append_record_locked(record)

    def _append_record_locked(self, record: Dict[str, Any]) -> None:
        """Append one record while the process-local journal lock is held."""
        path = self._outbox_path()
        parent_existed = path.parent.exists()
        path.parent.mkdir(parents=True, exist_ok=True)
        existed = path.exists()
        needs_separator = False
        if existed and path.stat().st_size:
            with path.open("rb") as existing:
                existing.seek(-1, os.SEEK_END)
                needs_separator = existing.read(1) != b"\n"

        payload = json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n"
        repair_payload = ""
        if needs_separator:
            repair_payload = json.dumps(
                {
                    "version": _JOURNAL_VERSION,
                    "type": "repair",
                    "reason": _REPAIR_REASON_TORN_TAIL,
                    "recovered_at": self._created_at(),
                },
                sort_keys=True,
                separators=(",", ":"),
            ) + "\n"
        fd = os.open(path, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o600)
        try:
            with os.fdopen(fd, "a", encoding="utf-8", closefd=False) as handle:
                if needs_separator:
                    handle.write("\n")
                    handle.write(repair_payload)
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
        finally:
            os.close(fd)

        if not existed or not parent_existed:
            self._fsync_directory(path.parent)

    @staticmethod
    def _validate_event(record: Dict[str, Any]) -> None:
        required = {
            "version",
            "type",
            "event_id",
            "created_at",
            "action",
            "target",
            "uri",
            "before",
            "after",
        }
        if set(record) != required:
            raise RuntimeError("journal corruption: invalid event fields")
        if record.get("version") != _JOURNAL_VERSION or record.get("type") != "event":
            raise RuntimeError("journal corruption: invalid event header")
        if record.get("action") not in _SUPPORTED_ACTIONS:
            raise RuntimeError("journal corruption: invalid event action")
        for key in ("event_id", "created_at", "target", "uri"):
            if not isinstance(record.get(key), str) or not record[key]:
                raise RuntimeError(f"journal corruption: invalid event {key}")
        if record.get("before") is not None and not isinstance(record.get("before"), str):
            raise RuntimeError("journal corruption: invalid event before")
        if record.get("after") is not None and not isinstance(record.get("after"), str):
            raise RuntimeError("journal corruption: invalid event after")

    @staticmethod
    def _validate_repair(record: Dict[str, Any]) -> None:
        required = {"version", "type", "reason", "recovered_at"}
        if set(record) != required:
            raise RuntimeError("journal corruption: invalid repair fields")
        if record.get("version") != _JOURNAL_VERSION or record.get("type") != "repair":
            raise RuntimeError("journal corruption: invalid repair header")
        if record.get("reason") != _REPAIR_REASON_TORN_TAIL:
            raise RuntimeError("journal corruption: invalid repair reason")
        if not isinstance(record.get("recovered_at"), str) or not record["recovered_at"]:
            raise RuntimeError("journal corruption: invalid repair timestamp")

    @staticmethod
    def _validate_ack(record: Dict[str, Any]) -> None:
        required = {"version", "type", "event_id", "completed_at", "outcome"}
        if set(record) != required:
            raise RuntimeError("journal corruption: invalid ack fields")
        if record.get("version") != _JOURNAL_VERSION or record.get("type") != "ack":
            raise RuntimeError("journal corruption: invalid ack header")
        if not isinstance(record.get("event_id"), str) or not record["event_id"]:
            raise RuntimeError("journal corruption: invalid ack event_id")
        if not isinstance(record.get("completed_at"), str) or not record["completed_at"]:
            raise RuntimeError("journal corruption: invalid ack completed_at")
        if record.get("outcome") not in _ACK_OUTCOMES:
            raise RuntimeError("journal corruption: invalid ack outcome")

    def _scan_journal(self) -> tuple[list[Dict[str, Any]], set[str]]:
        """Return ordered events and compatible acknowledgements from JSONL."""
        path = self._outbox_path()
        if not path.exists():
            return [], set()

        raw = path.read_text(encoding="utf-8")
        if not raw:
            return [], set()
        lines = raw.splitlines(keepends=True)
        events: list[Dict[str, Any]] = []
        seen_events: Dict[str, Dict[str, Any]] = {}
        ack_outcomes: Dict[str, str] = {}

        for index, line in enumerate(lines):
            text = line.rstrip("\r\n")
            if not text:
                continue
            final = index == len(lines) - 1
            try:
                record = json.loads(text)
            except json.JSONDecodeError as exc:
                if final and not line.endswith(("\n", "\r")):
                    logger.warning(
                        "OpenViking memory mirror journal has a torn trailing record; ignoring it"
                    )
                    break
                repaired_torn_tail = False
                if index + 1 < len(lines):
                    next_text = lines[index + 1].rstrip("\r\n")
                    try:
                        next_record = json.loads(next_text)
                    except json.JSONDecodeError:
                        next_record = None
                    if isinstance(next_record, dict) and next_record.get("type") == "repair":
                        self._validate_repair(next_record)
                        repaired_torn_tail = next_record.get("reason") == _REPAIR_REASON_TORN_TAIL
                if repaired_torn_tail:
                    logger.warning(
                        "OpenViking memory mirror journal preserved a recovered torn "
                        "tail at line %d; skipping damaged bytes",
                        index + 1,
                    )
                    continue
                raise RuntimeError(
                    f"journal corruption at line {index + 1}: invalid JSON"
                ) from exc
            if not isinstance(record, dict):
                raise RuntimeError(f"journal corruption at line {index + 1}: record is not an object")

            record_type = record.get("type")
            if record_type == "event":
                self._validate_event(record)
                event_id = record["event_id"]
                if event_id in seen_events:
                    raise RuntimeError(f"journal corruption: duplicate event_id {event_id!r}")
                seen_events[event_id] = record
                events.append(record)
                continue
            if record_type == "repair":
                self._validate_repair(record)
                continue
            if record_type == "ack":
                self._validate_ack(record)
                event_id = record["event_id"]
                outcome = record["outcome"]
                previous = ack_outcomes.get(event_id)
                if previous is not None and previous != outcome:
                    raise RuntimeError(
                        f"journal corruption: incompatible acknowledgements for {event_id!r}"
                    )
                ack_outcomes[event_id] = outcome
                continue
            raise RuntimeError(f"journal corruption at line {index + 1}: unknown record type")

        return events, set(ack_outcomes)

    def _registry_path(self) -> Path:
        return self._hermes_root() / _REGISTRY_RELATIVE_PATH

    @staticmethod
    def _validate_registry_entry(entry: Any, path: Path) -> None:
        if not isinstance(entry, dict):
            raise RuntimeError(f"invalid mirror registry entry: {path}")
        if not all(
            isinstance(entry.get(key), str)
            for key in ("target", "uri", "content", "state", "pending_event_id")
        ):
            raise RuntimeError(f"invalid mirror registry entry fields: {path}")
        if entry["state"] not in _REGISTRY_STATES:
            raise RuntimeError(f"invalid mirror registry entry state: {path}")
        if entry["state"] == "active" and entry["pending_event_id"]:
            raise RuntimeError(f"invalid active mirror registry entry: {path}")
        if entry["state"] != "active" and not entry["pending_event_id"]:
            raise RuntimeError(f"invalid pending mirror registry entry: {path}")

    def _load_registry(self) -> Dict[str, Any]:
        path = self._registry_path()
        if not path.exists():
            return {"version": _REGISTRY_VERSION, "entries": []}

        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            raise RuntimeError(f"cannot read mirror registry {path}: {exc}") from exc

        if not isinstance(payload, dict) or not isinstance(payload.get("entries"), list):
            raise RuntimeError(f"unsupported or invalid mirror registry: {path}")

        version = payload.get("version")
        if version == _LEGACY_REGISTRY_VERSION:
            migrated_entries = []
            for entry in payload["entries"]:
                if not isinstance(entry, dict) or not all(
                    isinstance(entry.get(key), str) for key in ("target", "uri", "content")
                ):
                    raise RuntimeError(f"invalid mirror registry entry fields: {path}")
                migrated_entries.append(
                    {
                        "target": entry["target"],
                        "uri": entry["uri"],
                        "content": entry["content"],
                        "state": "active",
                        "pending_event_id": "",
                    }
                )
            return {"version": _REGISTRY_VERSION, "entries": migrated_entries}

        if version != _REGISTRY_VERSION:
            raise RuntimeError(f"unsupported or invalid mirror registry: {path}")
        for entry in payload["entries"]:
            self._validate_registry_entry(entry, path)
        return payload

    def _save_registry(self, registry: Dict[str, Any]) -> None:
        path = self._registry_path()
        if registry.get("version") != _REGISTRY_VERSION:
            raise RuntimeError("cannot save unsupported mirror registry version")
        for entry in registry.get("entries", []):
            self._validate_registry_entry(entry, path)
        path.parent.mkdir(parents=True, exist_ok=True)
        atomic_json_write(path, registry, mode=0o600)

    @staticmethod
    def _resolve_mapping(
        registry: Dict[str, Any],
        *,
        target: str,
        old_text: str,
        action: str,
    ) -> tuple[int, Dict[str, str]]:
        old_text = str(old_text or "").strip()
        if not old_text:
            raise _MappingError(f"{action} requires old_text for stable URI resolution")

        matches = [
            (index, entry)
            for index, entry in enumerate(registry["entries"])
            if entry["target"] == target and old_text in entry["content"]
        ]
        if not matches:
            raise _MappingError(
                f"{action} has no stable OpenViking URI mapping for target={target!r}; "
                "leaving OpenViking unchanged"
            )
        if len(matches) != 1:
            raise _MappingError(
                f"{action} matched {len(matches)} OpenViking URI mappings for "
                f"target={target!r}; leaving OpenViking unchanged"
            )
        return matches[0]

    @staticmethod
    def _created_at() -> str:
        return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")

    def _build_event_uri(self, event_id: str, subdir: str) -> str:
        agent = str(getattr(self._provider, "_agent", "") or "hermes")
        return (
            f"viking://user/peers/{agent}/memories/{subdir}/"
            f"mem_evt_{event_id}.md"
        )

    def _build_event(
        self,
        registry: Dict[str, Any],
        *,
        action: str,
        target: str,
        content: str,
        metadata: Dict[str, Any],
        subdir: str,
    ) -> Optional[Dict[str, Any]]:
        if action == "add":
            if any(
                entry["target"] == target and entry["content"] == content
                for entry in registry["entries"]
            ):
                return None
            event_id = uuid.uuid4().hex
            return {
                "version": _JOURNAL_VERSION,
                "type": "event",
                "event_id": event_id,
                "created_at": self._created_at(),
                "action": action,
                "target": target,
                "uri": self._build_event_uri(event_id, subdir),
                "before": None,
                "after": content,
            }

        index, mapping = self._resolve_mapping(
            registry,
            target=target,
            old_text=str(metadata.get("old_text") or ""),
            action=action,
        )
        del index
        return {
            "version": _JOURNAL_VERSION,
            "type": "event",
            "event_id": uuid.uuid4().hex,
            "created_at": self._created_at(),
            "action": action,
            "target": target,
            "uri": mapping["uri"],
            "before": mapping["content"],
            "after": content if action == "replace" else None,
        }

    def _apply_registry_intent(self, registry: Dict[str, Any], event: Dict[str, Any]) -> None:
        action = event["action"]
        if action == "add":
            registry["entries"].append(
                {
                    "target": event["target"],
                    "uri": event["uri"],
                    "content": event["after"],
                    "state": "pending_create",
                    "pending_event_id": event["event_id"],
                }
            )
            return

        index = next(
            (
                index
                for index, entry in enumerate(registry["entries"])
                if entry["target"] == event["target"] and entry["uri"] == event["uri"]
            ),
            None,
        )
        if index is None:
            raise _MappingError(
                f"{action} lost its stable OpenViking URI mapping before registry update"
            )
        mapping = registry["entries"][index]
        mapping["content"] = event["after"] if action == "replace" else event["before"]
        mapping["state"] = "pending_replace" if action == "replace" else "pending_delete"
        mapping["pending_event_id"] = event["event_id"]

    def _apply_registry_ack(self, event: Dict[str, Any]) -> None:
        """Acknowledge only if this event is still the registry's newest intent."""
        # This transition shares the same lock as enqueue's load→intent→save
        # sequence. Without it, an older remote ACK can save a stale registry
        # snapshot over a newer local pending intent.
        with self._state_lock:
            registry = self._load_registry()
            index = next(
                (
                    index
                    for index, entry in enumerate(registry["entries"])
                    if entry["target"] == event["target"] and entry["uri"] == event["uri"]
                ),
                None,
            )
            if index is None:
                return
            mapping = registry["entries"][index]
            if mapping["pending_event_id"] != event["event_id"]:
                return

            if event["action"] == "remove":
                registry["entries"].pop(index)
            else:
                mapping["content"] = str(event["after"] or "")
                mapping["state"] = "active"
                mapping["pending_event_id"] = ""
            self._save_registry(registry)

    @staticmethod
    def _status_code(error: Exception) -> Optional[int]:
        value = getattr(error, "status_code", None)
        if isinstance(value, int):
            return value
        response = getattr(error, "response", None)
        value = getattr(response, "status_code", None)
        return value if isinstance(value, int) else None

    @staticmethod
    def _read_content(response: Any) -> str:
        result = response.get("result", response) if isinstance(response, dict) else response
        if isinstance(result, str):
            return result
        if isinstance(result, dict):
            for key in ("content", "text"):
                value = result.get(key)
                if isinstance(value, str):
                    return value
        raise RuntimeError("OpenViking exact read returned no text content")

    def _apply_remote(self, event: Dict[str, Any]) -> str:
        """Apply one exact durable event and return its acknowledgement outcome."""
        ensure_client = getattr(self._provider, "_ensure_client", None)
        if callable(ensure_client) and not ensure_client():
            raise RuntimeError("OpenViking server not connected")
        client = self._provider._new_client()
        action = event["action"]
        uri = event["uri"]

        if action == "add":
            try:
                client.post(
                    "/api/v1/content/write",
                    {"uri": uri, "content": event["after"], "mode": "create"},
                )
                return "applied"
            except Exception as exc:
                if self._status_code(exc) != 409:
                    raise
            response = client.get("/api/v1/content/read", params={"uri": uri})
            if self._read_content(response) == event["after"]:
                return "already_applied"
            raise _RemoteConflict(
                f"add event {event['event_id']} found different content at {uri}"
            )

        if action == "replace":
            try:
                client.post(
                    "/api/v1/content/write",
                    {
                        "uri": uri,
                        "content": event["after"],
                        "mode": "replace",
                        "wait": True,
                    },
                )
            except Exception as exc:
                if self._status_code(exc) == 404:
                    raise _RemoteConflict(
                        f"replace event {event['event_id']} target is absent: {uri}"
                    ) from exc
                raise
            return "applied"

        try:
            client.delete(
                "/api/v1/fs",
                params={"uri": uri, "recursive": False, "wait": True},
            )
        except Exception as exc:
            if self._status_code(exc) == 404:
                return "already_absent"
            raise
        return "applied"

    def _ack_event(self, event: Dict[str, Any], outcome: str) -> None:
        if outcome not in _ACK_OUTCOMES:
            raise RuntimeError(f"invalid mirror acknowledgement outcome: {outcome}")
        # Registry is durably transitioned first; the fsynced ACK is last and
        # therefore proves that remote verification plus local transition ran.
        self._apply_registry_ack(event)
        self._append_record(
            {
                "version": _JOURNAL_VERSION,
                "type": "ack",
                "event_id": event["event_id"],
                "completed_at": self._created_at(),
                "outcome": outcome,
            }
        )

    def _apply(self, event: Dict[str, Any]) -> None:
        """Compatibility wrapper for applying and durably acknowledging an event."""
        outcome = self._apply_remote(event)
        self._ack_event(event, outcome)


_MIRROR_ATTR = "_native_memory_mirror"


def start_native_memory_mirror(provider: Any) -> bool:
    """Recover and start the provider's durable native-memory outbox."""
    mirror = getattr(provider, _MIRROR_ATTR, None)
    if mirror is None:
        mirror = NativeMemoryMirror(provider)
        setattr(provider, _MIRROR_ATTR, mirror)
    return mirror.start()


def enqueue_native_memory_write(
    provider: Any,
    action: str,
    target: str,
    content: str,
    *,
    metadata: Optional[Dict[str, Any]] = None,
    subdir: str,
) -> None:
    """Lazily create the provider's FIFO mirror and enqueue one operation."""
    mirror = getattr(provider, _MIRROR_ATTR, None)
    if mirror is None:
        mirror = NativeMemoryMirror(provider)
        setattr(provider, _MIRROR_ATTR, mirror)
    mirror.enqueue(
        action,
        target,
        content,
        metadata=metadata,
        subdir=subdir,
    )


def shutdown_native_memory_mirror(provider: Any, timeout: float = 5.0) -> None:
    """Drain the provider's native-memory mirror if it was ever used."""
    mirror = getattr(provider, _MIRROR_ATTR, None)
    if mirror is not None:
        mirror.shutdown(timeout=timeout)
