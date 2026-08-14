import json
import threading
import time

from plugins.memory.openviking import OpenVikingMemoryProvider
from plugins.memory.openviking.native_memory_mirror import NativeMemoryMirror


class _Response:
    def __init__(self, status_code):
        self.status_code = status_code


class _HTTPError(RuntimeError):
    def __init__(self, status_code, message="http error"):
        super().__init__(message)
        self.response = _Response(status_code)


class _Client:
    def __init__(self, *, block=False):
        self.files = {}
        self.calls = []
        self.block = block
        self.started = threading.Event()
        self.release = threading.Event()

    def post(self, path, payload=None, **kwargs):
        payload = dict(payload or {})
        self.calls.append(("post", path, payload, dict(kwargs)))
        self.started.set()
        if self.block:
            self.release.wait(timeout=2)
        uri = payload["uri"]
        if payload["mode"] == "create":
            if uri in self.files:
                raise _HTTPError(409, "already exists")
            self.files[uri] = payload["content"]
        else:
            if uri not in self.files:
                raise _HTTPError(404, "missing")
            self.files[uri] = payload["content"]
        return {"status": "ok", "result": {"uri": uri}}

    def get(self, path, **kwargs):
        self.calls.append(("get", path, None, dict(kwargs)))
        uri = kwargs["params"]["uri"]
        if uri not in self.files:
            raise _HTTPError(404, "missing")
        return {"status": "ok", "result": self.files[uri]}

    def delete(self, path, **kwargs):
        self.calls.append(("delete", path, None, dict(kwargs)))
        uri = kwargs["params"]["uri"]
        if uri not in self.files:
            raise _HTTPError(404, "absent")
        del self.files[uri]
        return {"status": "ok", "result": None}


class _Provider:
    def __init__(self, tmp_path, client, *, available=True):
        self._hermes_home = str(tmp_path)
        self._agent = "hermes"
        self._client = client
        self.available = available

    def _ensure_client(self):
        return self._client if self.available else None

    def _new_client(self):
        return self._client


def _event(event_id, action, uri, before, after):
    return {
        "version": 1,
        "type": "event",
        "event_id": event_id,
        "created_at": "2026-08-14T08:40:00Z",
        "action": action,
        "target": "user",
        "uri": uri,
        "before": before,
        "after": after,
    }


def _ack(event_id, outcome="applied"):
    return {
        "version": 1,
        "type": "ack",
        "event_id": event_id,
        "completed_at": "2026-08-14T08:40:01Z",
        "outcome": outcome,
    }


def _registry(tmp_path):
    path = tmp_path / "openviking" / "memory_mirror_registry.json"
    return json.loads(path.read_text(encoding="utf-8"))


def _records(tmp_path):
    path = tmp_path / "openviking" / "memory_mirror_outbox.jsonl"
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]


def _wait_for(predicate, timeout=2.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    raise AssertionError("timed out waiting for mirror recovery")


def test_provider_hook_journals_even_when_openviking_is_unavailable(tmp_path):
    provider = OpenVikingMemoryProvider()
    provider._hermes_home = str(tmp_path)
    provider._agent = "hermes"
    provider._ensure_client = lambda: None

    provider.on_memory_write("add", "user", "Offline durable fact")

    records = _records(tmp_path)
    assert len(records) == 1
    assert records[0]["type"] == "event"
    assert records[0]["action"] == "add"
    assert records[0]["after"] == "Offline durable fact"
    provider.shutdown()


def test_start_replays_unacked_event_without_a_new_memory_write(tmp_path):
    client = _Client()
    provider = _Provider(tmp_path, client)
    mirror = NativeMemoryMirror(provider)
    uri = "viking://user/peers/hermes/memories/preferences/mem_evt_evt-a.md"
    event = _event("evt-a", "add", uri, None, "Recovered A")
    mirror._append_record(event)

    from plugins.memory.openviking.native_memory_mirror import start_native_memory_mirror

    start_native_memory_mirror(provider)
    _wait_for(lambda: any(record.get("type") == "ack" for record in _records(tmp_path)))

    assert client.files == {uri: "Recovered A"}
    assert [call[0:2] for call in client.calls] == [("post", "/api/v1/content/write")]
    assert _registry(tmp_path)["entries"][0]["state"] == "active"
    provider._native_memory_mirror.shutdown()


def test_start_repairs_latest_registry_intent_before_remote_delivery(tmp_path):
    client = _Client(block=True)
    provider = _Provider(tmp_path, client)
    mirror = NativeMemoryMirror(provider)
    uri = "viking://user/peers/hermes/memories/preferences/mem_evt_evt-a.md"
    add = _event("evt-a", "add", uri, None, "A")
    replace = _event("evt-b", "replace", uri, "A", "B")
    mirror._append_record(add)
    mirror._append_record(replace)

    from plugins.memory.openviking.native_memory_mirror import start_native_memory_mirror

    start_native_memory_mirror(provider)
    assert client.started.wait(timeout=1)

    assert _registry(tmp_path) == {
        "version": 2,
        "entries": [
            {
                "target": "user",
                "uri": uri,
                "content": "B",
                "state": "pending_replace",
                "pending_event_id": "evt-b",
            }
        ],
    }

    client.release.set()
    provider._native_memory_mirror.shutdown()


def test_start_does_not_replay_already_acked_event(tmp_path):
    client = _Client()
    provider = _Provider(tmp_path, client)
    mirror = NativeMemoryMirror(provider)
    uri = "viking://user/peers/hermes/memories/preferences/mem_evt_evt-a.md"
    event = _event("evt-a", "add", uri, None, "A")
    mirror._append_record(event)
    mirror._append_record(_ack("evt-a"))

    from plugins.memory.openviking.native_memory_mirror import start_native_memory_mirror

    start_native_memory_mirror(provider)
    time.sleep(0.05)

    assert client.calls == []
    provider._native_memory_mirror.shutdown()


def test_offline_start_repairs_registry_and_leaves_event_pending(tmp_path):
    client = _Client()
    provider = _Provider(tmp_path, client, available=False)
    mirror = NativeMemoryMirror(provider)
    uri = "viking://user/peers/hermes/memories/preferences/mem_evt_evt-a.md"
    event = _event("evt-a", "add", uri, None, "A")
    mirror._append_record(event)

    from plugins.memory.openviking.native_memory_mirror import start_native_memory_mirror

    start_native_memory_mirror(provider)
    _wait_for(lambda: _registry(tmp_path)["entries"][0]["state"] == "pending_create")

    assert not any(record.get("type") == "ack" for record in _records(tmp_path))
    provider._native_memory_mirror.shutdown(timeout=0.05)
