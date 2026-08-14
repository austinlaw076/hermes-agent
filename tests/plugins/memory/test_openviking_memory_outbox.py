import json
import os
import threading
import time

import pytest

from plugins.memory.openviking.native_memory_mirror import NativeMemoryMirror


class _Provider:
    def __init__(self, tmp_path):
        self._hermes_home = str(tmp_path)
        self._agent = "hermes"


class _BlockingClient:
    def __init__(self):
        self.started = threading.Event()
        self.release = threading.Event()
        self.calls = []

    def post(self, path, payload=None, **kwargs):
        self.calls.append(("post", path, dict(payload or {}), dict(kwargs)))
        self.started.set()
        self.release.wait(timeout=2)
        return {"status": "ok", "result": {"uri": (payload or {}).get("uri", "")}}

    def delete(self, path, **kwargs):
        self.calls.append(("delete", path, None, dict(kwargs)))
        self.started.set()
        self.release.wait(timeout=2)
        return {"status": "ok", "result": {}}


class _QueueProvider(_Provider):
    def __init__(self, tmp_path, client):
        super().__init__(tmp_path)
        self._client = client

    def _new_client(self):
        return self._client

    def _build_memory_uri(self, subdir):
        return f"viking://user/peers/hermes/memories/{subdir}/legacy-random.md"


class _Response:
    def __init__(self, status_code):
        self.status_code = status_code


class _HTTPError(RuntimeError):
    def __init__(self, status_code, message="http error"):
        super().__init__(message)
        self.response = _Response(status_code)


class _ReplayClient:
    def __init__(self, initial=None):
        self.files = dict(initial or {})
        self.calls = []
        self.failures = []

    def fail_next(self, method, status_code=503):
        self.failures.append((method, status_code))

    def _maybe_fail(self, method):
        if self.failures and self.failures[0][0] == method:
            _, status = self.failures.pop(0)
            raise _HTTPError(status)

    def post(self, path, payload=None, **kwargs):
        payload = dict(payload or {})
        self.calls.append(("post", path, payload, dict(kwargs)))
        self._maybe_fail("post")
        assert path == "/api/v1/content/write"
        uri = payload["uri"]
        if payload["mode"] == "create":
            if uri in self.files:
                raise _HTTPError(409, "already exists")
            self.files[uri] = payload["content"]
        else:
            if uri not in self.files:
                raise _HTTPError(404, "missing replace target")
            self.files[uri] = payload["content"]
        return {"status": "ok", "result": {"uri": uri}}

    def get(self, path, **kwargs):
        self.calls.append(("get", path, None, dict(kwargs)))
        self._maybe_fail("get")
        assert path == "/api/v1/content/read"
        uri = kwargs["params"]["uri"]
        if uri not in self.files:
            raise _HTTPError(404, "missing read target")
        return {"status": "ok", "result": self.files[uri]}

    def delete(self, path, **kwargs):
        self.calls.append(("delete", path, None, dict(kwargs)))
        self._maybe_fail("delete")
        uri = kwargs["params"]["uri"]
        if uri not in self.files:
            raise _HTTPError(404, "already absent")
        del self.files[uri]
        return {"status": "ok", "result": None}



def _mirror(tmp_path):
    return NativeMemoryMirror(_Provider(tmp_path))



def _event(
    event_id="evt-1",
    *,
    action="add",
    uri="viking://user/mem_evt_evt-1.md",
    before=None,
    after="A",
):
    return {
        "version": 1,
        "type": "event",
        "event_id": event_id,
        "created_at": "2026-08-14T08:00:00Z",
        "action": action,
        "target": "user",
        "uri": uri,
        "before": before,
        "after": after,
    }



def _ack(event_id="evt-1", outcome="applied"):
    return {
        "version": 1,
        "type": "ack",
        "event_id": event_id,
        "completed_at": "2026-08-14T08:00:01Z",
        "outcome": outcome,
    }



def _outbox_records(tmp_path):
    path = tmp_path / "openviking" / "memory_mirror_outbox.jsonl"
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]



def _registry(tmp_path):
    path = tmp_path / "openviking" / "memory_mirror_registry.json"
    return json.loads(path.read_text(encoding="utf-8"))



def test_append_record_creates_private_jsonl_and_fsyncs(tmp_path, monkeypatch):
    mirror = _mirror(tmp_path)
    fsync_calls = []
    real_fsync = os.fsync

    def recording_fsync(fd):
        fsync_calls.append(fd)
        return real_fsync(fd)

    monkeypatch.setattr(os, "fsync", recording_fsync)
    mirror._append_record(_event())

    path = tmp_path / "openviking" / "memory_mirror_outbox.jsonl"
    assert path.exists()
    assert json.loads(path.read_text(encoding="utf-8")) == _event()
    assert fsync_calls, "journal append must fsync before returning"
    if os.name != "nt":
        assert path.stat().st_mode & 0o777 == 0o600




def test_single_process_journal_appends_are_serialized(tmp_path, monkeypatch):
    path = tmp_path / "openviking" / "memory_mirror_outbox.jsonl"
    path.parent.mkdir(parents=True)
    path.write_text("", encoding="utf-8")
    mirror = _mirror(tmp_path)

    first_in_fsync = threading.Event()
    second_in_fsync = threading.Event()
    release_first = threading.Event()
    counter_lock = threading.Lock()
    counter = 0
    real_fsync = os.fsync

    def blocking_fsync(fd):
        nonlocal counter
        with counter_lock:
            ordinal = counter
            counter += 1
        if ordinal == 0:
            first_in_fsync.set()
            release_first.wait(timeout=1)
        elif ordinal == 1:
            second_in_fsync.set()
        return real_fsync(fd)

    monkeypatch.setattr(os, "fsync", blocking_fsync)
    first = threading.Thread(target=mirror._append_record, args=(_event("evt-a"),))
    second = threading.Thread(target=mirror._append_record, args=(_event("evt-b", uri="viking://user/b.md", after="B"),))

    first.start()
    assert first_in_fsync.wait(timeout=1)
    second.start()
    assert not second_in_fsync.wait(timeout=0.1), "second append entered fsync before first append completed"
    release_first.set()
    first.join(timeout=1)
    second.join(timeout=1)
    assert second_in_fsync.is_set()

    events, _ = mirror._scan_journal()
    assert [event["event_id"] for event in events] == ["evt-a", "evt-b"]


def test_event_and_ack_records_round_trip_to_pending_state(tmp_path):
    mirror = _mirror(tmp_path)
    mirror._append_record(_event("evt-a", uri="viking://user/a.md"))
    mirror._append_record(_event("evt-b", uri="viking://user/b.md", after="B"))
    mirror._append_record(_ack("evt-a"))

    events, acked = mirror._scan_journal()

    assert [event["event_id"] for event in events] == ["evt-a", "evt-b"]
    assert acked == {"evt-a"}
    assert [event["event_id"] for event in events if event["event_id"] not in acked] == ["evt-b"]



def test_torn_trailing_record_is_warned_and_ignored(tmp_path, caplog):
    path = tmp_path / "openviking" / "memory_mirror_outbox.jsonl"
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps(_event()) + "\n" + '{"version":1,"type":"event"', encoding="utf-8")

    mirror = _mirror(tmp_path)
    with caplog.at_level("WARNING", logger="plugins.memory.openviking"):
        events, acked = mirror._scan_journal()

    assert [event["event_id"] for event in events] == ["evt-1"]
    assert acked == set()
    assert any("trailing" in record.message.lower() for record in caplog.records)



def test_append_after_torn_tail_starts_on_new_line_without_overwriting_damage(tmp_path):
    path = tmp_path / "openviking" / "memory_mirror_outbox.jsonl"
    path.parent.mkdir(parents=True)
    torn = '{"version":1,"type":"event"'
    path.write_text(json.dumps(_event()) + "\n" + torn, encoding="utf-8")

    mirror = _mirror(tmp_path)
    mirror._append_record(_ack())

    raw = path.read_text(encoding="utf-8")
    assert torn in raw
    assert raw.endswith(json.dumps(_ack(), sort_keys=True, separators=(",", ":")) + "\n")
    assert "\n" + json.dumps(_ack(), sort_keys=True, separators=(",", ":")) in raw



def test_scan_remains_replayable_after_appending_behind_a_torn_tail(tmp_path):
    path = tmp_path / "openviking" / "memory_mirror_outbox.jsonl"
    path.parent.mkdir(parents=True)
    torn = '{"version":1,"type":"event"'
    path.write_text(json.dumps(_event()) + "\n" + torn, encoding="utf-8")

    mirror = _mirror(tmp_path)
    mirror._append_record(_ack())

    events, acked = mirror._scan_journal()
    assert [event["event_id"] for event in events] == ["evt-1"]
    assert acked == {"evt-1"}
    assert torn in path.read_text(encoding="utf-8")


def test_malformed_non_final_record_fails_closed(tmp_path):
    path = tmp_path / "openviking" / "memory_mirror_outbox.jsonl"
    path.parent.mkdir(parents=True)
    path.write_text(
        json.dumps(_event("evt-a")) + "\n" + "not-json\n" + json.dumps(_event("evt-b")) + "\n",
        encoding="utf-8",
    )

    mirror = _mirror(tmp_path)
    with pytest.raises(RuntimeError, match="journal corruption"):
        mirror._scan_journal()



def test_duplicate_incompatible_event_id_is_journal_corruption(tmp_path):
    path = tmp_path / "openviking" / "memory_mirror_outbox.jsonl"
    path.parent.mkdir(parents=True)
    first = _event("evt-a", uri="viking://user/a.md", after="A")
    second = _event("evt-a", uri="viking://user/a.md", after="different")
    path.write_text(json.dumps(first) + "\n" + json.dumps(second) + "\n", encoding="utf-8")

    mirror = _mirror(tmp_path)
    with pytest.raises(RuntimeError, match="duplicate event_id"):
        mirror._scan_journal()



def test_v1_registry_loads_as_v2_active_without_losing_identity(tmp_path):
    path = tmp_path / "openviking" / "memory_mirror_registry.json"
    path.parent.mkdir(parents=True)
    path.write_text(
        json.dumps(
            {
                "version": 1,
                "entries": [
                    {
                        "target": "user",
                        "uri": "viking://user/peers/hermes/memories/preferences/legacy.md",
                        "content": "Legacy fact",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    registry = _mirror(tmp_path)._load_registry()

    assert registry == {
        "version": 2,
        "entries": [
            {
                "target": "user",
                "uri": "viking://user/peers/hermes/memories/preferences/legacy.md",
                "content": "Legacy fact",
                "state": "active",
                "pending_event_id": "",
            }
        ],
    }



def test_add_is_journaled_with_event_derived_uri_before_remote_ack(tmp_path):
    client = _BlockingClient()
    mirror = NativeMemoryMirror(_QueueProvider(tmp_path, client))

    mirror.enqueue("add", "user", "Preferred provider is A", subdir="preferences")
    assert client.started.wait(timeout=1)

    records = _outbox_records(tmp_path)
    event = records[0]
    assert event["type"] == "event"
    assert event["action"] == "add"
    assert event["before"] is None
    assert event["after"] == "Preferred provider is A"
    assert event["uri"].endswith(f"/mem_evt_{event['event_id']}.md")
    assert event["uri"].startswith("viking://user/peers/hermes/memories/preferences/")
    assert _registry(tmp_path) == {
        "version": 2,
        "entries": [
            {
                "target": "user",
                "uri": event["uri"],
                "content": "Preferred provider is A",
                "state": "pending_create",
                "pending_event_id": event["event_id"],
            }
        ],
    }

    client.release.set()
    mirror.shutdown()



def test_replace_resolves_exact_uri_before_journal_and_advances_intended_state(tmp_path):
    registry_path = tmp_path / "openviking" / "memory_mirror_registry.json"
    registry_path.parent.mkdir(parents=True)
    uri = "viking://user/peers/hermes/memories/preferences/fact.md"
    registry_path.write_text(
        json.dumps(
            {
                "version": 2,
                "entries": [
                    {
                        "target": "user",
                        "uri": uri,
                        "content": "Provider is A",
                        "state": "active",
                        "pending_event_id": "",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    client = _BlockingClient()
    mirror = NativeMemoryMirror(_QueueProvider(tmp_path, client))

    mirror.enqueue(
        "replace",
        "user",
        "Provider is B",
        metadata={"old_text": "Provider is A"},
        subdir="preferences",
    )
    assert client.started.wait(timeout=1)

    event = _outbox_records(tmp_path)[0]
    assert event["action"] == "replace"
    assert event["uri"] == uri
    assert event["before"] == "Provider is A"
    assert event["after"] == "Provider is B"
    assert _registry(tmp_path)["entries"][0] == {
        "target": "user",
        "uri": uri,
        "content": "Provider is B",
        "state": "pending_replace",
        "pending_event_id": event["event_id"],
    }

    client.release.set()
    mirror.shutdown()



def test_add_replace_remove_can_all_journal_before_first_remote_ack(tmp_path):
    client = _BlockingClient()
    mirror = NativeMemoryMirror(_QueueProvider(tmp_path, client))

    mirror.enqueue("add", "user", "Device: A", subdir="preferences")
    assert client.started.wait(timeout=1)
    mirror.enqueue(
        "replace",
        "user",
        "Device: B",
        metadata={"old_text": "Device: A"},
        subdir="preferences",
    )
    mirror.enqueue(
        "remove",
        "user",
        "",
        metadata={"old_text": "Device: B"},
        subdir="preferences",
    )

    events = [record for record in _outbox_records(tmp_path) if record["type"] == "event"]
    assert [event["action"] for event in events] == ["add", "replace", "remove"]
    assert len({event["uri"] for event in events}) == 1
    assert events[1]["before"] == "Device: A"
    assert events[1]["after"] == "Device: B"
    assert events[2]["before"] == "Device: B"
    assert events[2]["after"] is None
    assert _registry(tmp_path)["entries"][0]["state"] == "pending_delete"
    assert _registry(tmp_path)["entries"][0]["pending_event_id"] == events[2]["event_id"]

    client.release.set()
    mirror.shutdown()



def test_ack_for_older_event_does_not_regress_newer_pending_intent(tmp_path):
    mirror = _mirror(tmp_path)
    uri = "viking://user/peers/hermes/memories/preferences/fact.md"
    registry = {
        "version": 2,
        "entries": [
            {
                "target": "user",
                "uri": uri,
                "content": "B",
                "state": "pending_replace",
                "pending_event_id": "evt-2",
            }
        ],
    }
    mirror._save_registry(registry)
    older = _event("evt-1", action="add", uri=uri, after="A")

    mirror._apply_registry_ack(older)

    assert _registry(tmp_path) == registry


# Task 3: remote idempotency and ACK-last durability.


def test_replayed_add_with_same_exact_uri_and_content_is_already_applied(tmp_path):
    uri = "viking://user/peers/hermes/memories/preferences/mem_evt_evt-a.md"
    client = _ReplayClient({uri: "A"})
    mirror = NativeMemoryMirror(_QueueProvider(tmp_path, client))

    outcome = mirror._apply_remote(_event("evt-a", uri=uri, after="A"))

    assert outcome == "already_applied"
    assert client.files == {uri: "A"}
    assert [(call[0], call[1]) for call in client.calls] == [
        ("post", "/api/v1/content/write"),
        ("get", "/api/v1/content/read"),
    ]
    assert client.calls[1][3]["params"] == {"uri": uri}



def test_replayed_add_with_conflicting_exact_uri_blocks_without_guessing(tmp_path):
    uri = "viking://user/peers/hermes/memories/preferences/mem_evt_evt-a.md"
    client = _ReplayClient({uri: "different"})
    mirror = NativeMemoryMirror(_QueueProvider(tmp_path, client))

    with pytest.raises(RuntimeError, match="different content"):
        mirror._apply_remote(_event("evt-a", uri=uri, after="A"))

    assert client.files == {uri: "different"}
    assert all(call[1] != "/api/v1/search/recall" for call in client.calls)



def test_replayed_replace_uses_exact_uri_and_wait_true(tmp_path):
    uri = "viking://user/peers/hermes/memories/preferences/fact.md"
    client = _ReplayClient({uri: "A"})
    mirror = NativeMemoryMirror(_QueueProvider(tmp_path, client))
    event = _event("evt-r", action="replace", uri=uri, before="A", after="B")

    outcome = mirror._apply_remote(event)

    assert outcome == "applied"
    assert client.files[uri] == "B"
    call = client.calls[0]
    assert call[0:2] == ("post", "/api/v1/content/write")
    assert call[2] == {"uri": uri, "content": "B", "mode": "replace", "wait": True}



def test_replayed_remove_of_already_absent_exact_uri_is_acknowledgeable(tmp_path):
    uri = "viking://user/peers/hermes/memories/preferences/fact.md"
    client = _ReplayClient()
    mirror = NativeMemoryMirror(_QueueProvider(tmp_path, client))
    event = _event("evt-d", action="remove", uri=uri, before="A", after=None)

    outcome = mirror._apply_remote(event)

    assert outcome == "already_absent"
    call = client.calls[0]
    assert call[0:2] == ("delete", "/api/v1/fs")
    assert call[3]["params"] == {"uri": uri, "recursive": False, "wait": True}



def test_ack_event_updates_registry_before_durable_ack_record(tmp_path, monkeypatch):
    mirror = _mirror(tmp_path)
    uri = "viking://user/peers/hermes/memories/preferences/fact.md"
    event = _event("evt-a", uri=uri, after="A")
    mirror._save_registry(
        {
            "version": 2,
            "entries": [
                {
                    "target": "user",
                    "uri": uri,
                    "content": "A",
                    "state": "pending_create",
                    "pending_event_id": "evt-a",
                }
            ],
        }
    )
    order = []
    original_registry_ack = mirror._apply_registry_ack
    original_append = mirror._append_record

    def registry_ack(record):
        order.append("registry")
        original_registry_ack(record)

    def append(record):
        order.append(record["type"])
        original_append(record)

    monkeypatch.setattr(mirror, "_apply_registry_ack", registry_ack)
    monkeypatch.setattr(mirror, "_append_record", append)

    mirror._ack_event(event, "applied")

    assert order == ["registry", "ack"]
    records = _outbox_records(tmp_path)
    assert records[-1]["type"] == "ack"
    assert records[-1]["event_id"] == "evt-a"
    assert records[-1]["outcome"] == "applied"
    assert _registry(tmp_path)["entries"][0]["state"] == "active"



def test_worker_retries_same_head_before_later_event(tmp_path, monkeypatch):
    uri1 = "viking://user/peers/hermes/memories/preferences/a.md"
    uri2 = "viking://user/peers/hermes/memories/preferences/b.md"
    client = _ReplayClient()
    client.fail_next("post", 503)
    mirror = NativeMemoryMirror(_QueueProvider(tmp_path, client))
    monkeypatch.setattr(mirror, "_retry_delay", lambda attempt: 0.01)

    mirror.enqueue("add", "user", "A", subdir="preferences")
    mirror.enqueue("add", "user", "B", subdir="preferences")
    mirror.shutdown(timeout=2)

    post_contents = [call[2]["content"] for call in client.calls if call[0] == "post"]
    assert post_contents == ["A", "A", "B"]
    records = _outbox_records(tmp_path)
    assert [record["type"] for record in records].count("ack") == 2



def test_deterministic_conflict_stops_later_fifo_delivery(tmp_path, monkeypatch):
    client = _ReplayClient()
    mirror = NativeMemoryMirror(_QueueProvider(tmp_path, client))
    monkeypatch.setattr(mirror, "_retry_delay", lambda attempt: 0.01)

    # Pre-seed the deterministic first URI by forcing a known event id.
    ids = iter(["conflict", "later"])
    monkeypatch.setattr("plugins.memory.openviking.native_memory_mirror.uuid.uuid4", lambda: type("U", (), {"hex": next(ids)})())
    conflict_uri = "viking://user/peers/hermes/memories/preferences/mem_evt_conflict.md"
    client.files[conflict_uri] = "other"

    mirror.enqueue("add", "user", "A", subdir="preferences")
    mirror.enqueue("add", "user", "B", subdir="preferences")
    mirror.shutdown(timeout=1)

    assert "B" not in client.files.values()
    records = _outbox_records(tmp_path)
    assert not any(record["type"] == "ack" for record in records)


def test_new_mutation_after_deterministic_conflict_is_journaled_but_not_delivered(tmp_path, monkeypatch):
    client = _ReplayClient()
    mirror = NativeMemoryMirror(_QueueProvider(tmp_path, client))
    ids = iter(["conflict", "after-conflict"])
    monkeypatch.setattr(
        "plugins.memory.openviking.native_memory_mirror.uuid.uuid4",
        lambda: type("U", (), {"hex": next(ids)})(),
    )
    conflict_uri = "viking://user/peers/hermes/memories/preferences/mem_evt_conflict.md"
    client.files[conflict_uri] = "other"

    mirror.enqueue("add", "user", "A", subdir="preferences")
    deadline = time.monotonic() + 1.0
    while time.monotonic() < deadline:
        worker = mirror._worker
        if worker is not None and not worker.is_alive():
            break
        time.sleep(0.01)
    else:
        raise AssertionError("deterministic conflict did not stop the worker")

    mirror.enqueue("add", "user", "C", subdir="preferences")
    time.sleep(0.05)

    records = _outbox_records(tmp_path)
    events = [record for record in records if record["type"] == "event"]
    assert [event["after"] for event in events] == ["A", "C"]
    assert "C" not in client.files.values()
    assert not any(record["type"] == "ack" for record in records)
    mirror.shutdown(timeout=0.1)
