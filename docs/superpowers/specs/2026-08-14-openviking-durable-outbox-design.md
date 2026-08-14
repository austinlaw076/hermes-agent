# OpenViking Durable Native-Memory Outbox Design

Date: 2026-08-14
Status: Design approved in conversation; implementation not started
Depends on: PR #85860 (`fix/openviking): mirror native memory replace/remove with stable URIs`)

## 1. Purpose

PR #85860 fixes OpenViking native-memory correctness inside one live Hermes process by introducing stable URI mappings, exact `replace`/`remove`, and FIFO delivery. Its remaining reliability gap is that the FIFO queue is in memory. A process or machine crash can therefore lose a mirror mutation after `MEMORY.md` / `USER.md` has changed but before OpenViking has durably observed the change.

This PR adds a plugin-scoped, append-only durable outbox for Hermes built-in memory mirroring. The outbox makes OpenViking delivery restart-safe without turning the OpenViking plugin into a distributed transaction coordinator.

The central invariant is:

> Once an outbox event append returns successfully, the exact remote mutation intent survives process restart and can be replayed without creating a second OpenViking memory or guessing a target.

## 2. Scope

### Goals

1. Persist every built-in-memory mirror mutation in an append-only JSONL journal before asynchronous OpenViking delivery.
2. `flush()` and `fsync()` every event and acknowledgement record.
3. Give every event a stable `event_id` and an exact OpenViking URI before delivery.
4. Make `add` replay idempotent by deriving its URI deterministically from the event identity.
5. Make `replace` and `remove` events self-contained by resolving their exact URI before journaling.
6. Replay unacknowledged events automatically on provider startup, even if no new memory write occurs.
7. Journal while OpenViking is unavailable; backend availability must affect delivery, not durability.
8. Keep registry state usable by subsequent local mutations while earlier remote mutations are still pending.
9. Preserve strict event order within one Hermes process.
10. Leave failed events unacknowledged so restart can retry them.

### Explicit non-goals

This PR does not implement:

- the local-memory-write-before-outbox-hook gap; PR 3 reconciliation/backfill covers that residual window;
- pre-registry reconciliation or migration;
- cross-process file locking or distributed serialization; PR 4 covers that;
- journal compaction or retention pruning;
- semantic-search-based recovery or destructive guessing;
- durable mirroring for session-extracted OpenViking memories or explicit `viking_remember` calls.

## 3. Alternatives considered

### A. Append-only JSONL event + ack journal — selected

Advantages:

- human-readable and easy to audit with ordinary tools;
- no database migration or daemon dependency;
- append-only write pattern has a small crash surface;
- pending work can be reconstructed from the journal alone;
- keeps PR 2 independent from PR 4 cross-process locking.

Trade-off: the file grows monotonically. Memory mutations are expected to be low frequency, so compaction is deliberately deferred.

### B. SQLite outbox — rejected for this PR

SQLite provides excellent transactional and concurrency semantics, but it reduces direct human readability and prematurely couples PR 2 to cross-process behavior that belongs in PR 4.

### C. In-memory queue plus periodic checkpoint — rejected

A periodic checkpoint leaves a time window where Hermes has changed local memory but the pending remote mutation exists only in RAM. It therefore does not satisfy the durability requirement.

## 4. Files and ownership

The OpenViking plugin owns two local persistence files under the active Hermes home:

```text
$HERMES_HOME/openviking/
├── memory_mirror_registry.json
└── memory_mirror_outbox.jsonl
```

Both are private user-state files and must be created with mode `0600` where the platform permits it.

The registry remains the fast local representation of intended stable identities. The outbox is the durable audit/replay log.

Neither file claims ownership over session-extracted memories or explicit `viking_remember` memories.

## 5. Journal format

The journal is newline-delimited UTF-8 JSON. Every line is one independent record.

### Event record

```json
{
  "version": 1,
  "type": "event",
  "event_id": "6b9c...",
  "created_at": "2026-08-14T07:45:00.000000Z",
  "action": "replace",
  "target": "user",
  "uri": "viking://user/default/peers/hermes/memories/preferences/mem_evt_6b9c....md",
  "before": "User prefers provider A",
  "after": "User prefers provider B"
}
```

Semantics:

- `event_id` is a freshly generated stable UUID value.
- `uri` is final and never recomputed during replay.
- `before` is `null` for `add`.
- `after` is `null` for `remove`.
- `replace` stores both full registry contents, not merely Hermes' substring `old_text`, so the journal is human-auditable and self-contained.
- replay never uses `old_text` to discover a target.

### Ack record

```json
{
  "version": 1,
  "type": "ack",
  "event_id": "6b9c...",
  "completed_at": "2026-08-14T07:45:02.000000Z",
  "outcome": "applied"
}
```

Allowed outcomes are intentionally small:

- `applied` — the requested operation was applied now;
- `already_applied` — replay discovered the deterministic target already had the requested content;
- `already_absent` — replay of `remove` found the exact URI already absent.

An event is pending if the journal contains an event record without any valid ack record for that `event_id`.

## 6. Durability boundary

Every event and ack append uses this sequence:

```text
encode one JSON record + newline
        ↓
append to outbox
        ↓
flush userspace buffer
        ↓
fsync file descriptor
        ↓
return success
```

When the outbox file is created for the first time, the implementation should also fsync the containing directory on platforms that support directory fsync so the new directory entry survives a sudden power loss.

No periodic/batched fsync is used.

### Torn final record

A crash can theoretically leave one partially written final JSON line.

Startup parsing must:

- accept every complete valid line before it;
- tolerate and warn about one malformed trailing partial line;
- never treat that malformed trailing fragment as an event or ack;
- ensure the next append begins on a new line so the damaged bytes remain auditable rather than being silently overwritten.

A malformed non-final record is treated as journal corruption. Replay stops and logs an error rather than guessing across a possible missing intent boundary.

## 7. Deterministic add identity

For `add`, the event identity is allocated before journaling. Its requested OpenViking filename is derived from that event identity, for example:

```text
mem_evt_<event_id>.md
```

The full namespace still uses the configured OpenViking peer and native-memory target subdirectory.

Therefore the same event always addresses the same URI across any replay.

A replayed create follows this rule:

1. attempt create at the exact event URI;
2. if it succeeds, continue normally;
3. if OpenViking reports that the exact URI already exists, read that exact URI;
4. if its content equals `after`, treat the event as `already_applied`;
5. if its content differs, stop on a deterministic conflict, leave the event unacked, and log an error.

No semantic search or nearest-match lookup is permitted.

## 8. Resolve-before-journal for replace/remove

`replace` and `remove` must resolve a single exact registry mapping before the event is written.

Flow:

```text
Hermes local mutation succeeded
        ↓
resolve target + old_text against local registry
        ↓ exactly one URI
build self-contained event with exact URI and full before/after
        ↓
append + fsync event
        ↓
update registry intended state
        ↓
FIFO delivery
```

If mapping resolution is missing or ambiguous, no destructive outbox event is written. The plugin fails closed and warns, matching PR #85860's safety model.

Once journaled, replay uses only the event's exact URI and payload.

## 9. Registry semantics

PR #85860's registry currently represents successfully mirrored content. PR 2 changes its meaning slightly: the registry becomes the local record of the latest **intended stable identity/state**, which may be ahead of remote acknowledgement.

Registry entries gain lifecycle fields conceptually equivalent to:

```json
{
  "target": "user",
  "uri": "viking://.../mem_evt_....md",
  "content": "latest intended content",
  "state": "pending_replace",
  "pending_event_id": "..."
}
```

States:

- `pending_create`
- `active`
- `pending_replace`
- `pending_delete`

The registry schema is versioned and PR 2 must load PR #85860's version-1 entries as `active` entries during an explicit in-memory migration, then persist the upgraded representation on the next registry write. Existing stable URI mappings are never discarded.

### Why registry intent moves before remote ACK

Consider three local operations occurring before OpenViking responds:

```text
ADD A
REPLACE A → B
REMOVE B
```

After the ADD event is fsynced, the registry must immediately know the deterministic URI for A. After REPLACE is fsynced, its content must immediately become B so REMOVE can resolve the new local state. Delivery then occurs FIFO against one stable URI.

### Ack must not regress a newer intent

An older event may finish after a newer event has already updated registry intent. Therefore an ack may alter registry lifecycle state only when the entry's `pending_event_id` still equals the acked `event_id`.

Example:

```text
ADD event E1 journaled → registry pending_create(E1)
REPLACE E2 journaled   → registry pending_replace(E2)
remote ADD E1 ACK      → must NOT overwrite pending_replace(E2) with active
remote REPLACE E2 ACK  → registry becomes active
```

For a final `remove` ack, the entry is physically removed only when its current `pending_event_id` equals that remove event.

## 10. Ordering and delivery worker

The existing one-process FIFO design remains. The durable journal replaces the RAM queue as the authoritative pending-work source.

Within one process:

- event append order defines delivery order;
- the worker handles the oldest pending event first;
- later events do not overtake a failed earlier event;
- this preserves state-machine semantics when multiple operations address the same URI.

Cross-process ordering is explicitly deferred to PR 4.

## 11. Backend availability and retry

`on_memory_write()` must no longer require `_ensure_client()` before it journals an event.

Correct behavior is:

```text
local memory mutation succeeds
        ↓
durable event fsync
        ↓
OpenViking available?
   ├── yes → deliver now
   └── no  → retain pending; retry later
```

The worker uses bounded exponential retry delay for transient delivery failures, capped at 30 seconds. A suitable default sequence is approximately:

```text
1s → 2s → 4s → 8s → 16s → 30s → 30s ...
```

The event remains the head of the FIFO while transient failure persists.

Shutdown interrupts the retry loop. No pending event is lost because it remains unacked in the journal.

Deterministic content conflicts or journal corruption are not treated as transient. They stop safe forward progress and require operator attention rather than allowing later dependent events to overtake them.

## 12. Replay on startup

Replay must start during OpenViking provider initialization, not lazily on the next memory mutation.

Startup sequence:

1. read and validate the journal;
2. collect event records in append order;
3. collect acked event IDs;
4. identify unacked events;
5. repair registry intended state from durable unacked events where a crash occurred after event fsync but before the registry write;
6. start the FIFO worker with those pending events;
7. accept new local mirror events after recovery state is initialized.

OpenViking does not need to be reachable for steps 1–5. If it is offline, replay simply remains pending and the retry loop handles eventual delivery.

This ensures a restart performs recovery even if the user never writes another memory afterwards.

## 13. Remote replay semantics

### Add

Remote create is idempotent by deterministic URI plus exact-content verification.

Crash windows handled:

- event fsynced, remote never called → replay creates it;
- remote create succeeded, crash before registry update → replay sees same URI/content and repairs locally;
- remote create succeeded, registry updated, crash before ack → replay sees same URI/content and appends `already_applied` ack.

### Replace

`replace` always writes the exact URI with `mode=replace` and `wait=true`.

Repeating the same replacement against the same URI is idempotent. Remote ACK is not recorded until semantic/vector refresh completes.

### Remove

`remove` always deletes the exact URI with `wait=true`.

If replay finds the exact URI already absent, the event is considered `already_absent` and is acknowledged. It does not search for a substitute target.

## 14. Commit ordering between remote, registry and ack

For every delivered event:

```text
event already fsynced
        ↓
perform/verify remote operation
        ↓
apply registry acknowledgement transition
        ↓ atomic registry write
append ack
        ↓ flush + fsync
```

The ack is deliberately last.

This means an ack in the journal is proof that the remote operation was verified and the corresponding local registry transition was durably attempted first.

If the process crashes after remote success but before ack, replay is safe because the remote operation is idempotent.

If the process crashes after registry update but before ack, replay may repeat the remote operation but must not create a duplicate or regress registry intent.

## 15. Residual crash window deliberately accepted

Hermes core invokes the provider hook after the native `MEMORY.md` / `USER.md` operation succeeds. Therefore this PR still has a narrow boundary:

```text
native file mutation committed
        ↓
process/machine crashes before plugin event fsync
```

A plugin-only outbox cannot close this gap without changing Hermes core into a write-ahead transaction protocol.

PR 2 explicitly accepts this residual risk to keep scope small. PR 3 reconciliation/backfill will detect and repair native entries that exist without a corresponding mirror registry/outbox identity.

This limitation must be documented in the plugin README and PR description.

## 16. Error handling and observability

Logging policy:

- transient OpenViking delivery failure: warning on first failure, then rate-limited/retry-aware logging;
- deterministic URI/content conflict: error and stop FIFO progress;
- malformed trailing journal fragment: warning;
- malformed non-final journal record: error and disable replay;
- registry reconstruction from an unacked event: informational/debug audit message;
- shutdown with pending events: informational message including pending count, not data loss warning.

The implementation should expose enough small helper methods that tests can inspect journal state, pending count and registry transitions without depending on sleeps.

A user-facing reconcile/status command is not part of PR 2; that belongs with PR 3 hygiene tooling.

## 17. Journal growth

No compaction is implemented in this PR.

Reasons:

- keeping the entire event/ack stream maximizes auditability;
- typical personal-memory mutation volume is low;
- compaction interacts with cross-process locking and should not be introduced before PR 4 defines file ownership/locking semantics.

A later compactor may preserve pending records plus a configurable audit horizon, but that is outside this design.

## 18. Test strategy

Implementation follows RED → GREEN.

### Durability / restart tests

1. Event is present and parseable after append returns.
2. Event append calls flush/fsync before delivery is attempted.
3. Ack append calls flush/fsync.
4. Restart replays an event written before any remote call.
5. Startup replay happens without a subsequent `on_memory_write()` call.
6. OpenViking unavailable at mutation time still produces a durable event.

### Add idempotency tests

7. `add` URI is determined by the event identity and is stable across restart.
8. Crash after remote create but before ack does not create a second URI.
9. Existing exact URI with identical content becomes `already_applied`.
10. Existing exact URI with different content remains unacked and blocks safe progress.

### Replace/remove replay tests

11. `replace` event contains the exact URI before journaling and replays on that URI only.
12. Replayed replace uses `wait=true` and does not create a new memory.
13. Replayed remove uses the exact URI and `wait=true`.
14. Replayed remove against an already absent exact URI becomes `already_absent`.

### Chained intent tests

15. ADD → REPLACE → REMOVE can all be journaled before remote delivery and use one stable URI.
16. Registry intended content advances after event fsync so the next local substring mutation resolves correctly.
17. Ack for an older event cannot overwrite a newer pending registry state.
18. Final remove ack deletes the mapping only when it is still the current pending event.

### Journal recovery tests

19. One torn trailing JSON fragment is warned and ignored.
20. A malformed non-final record stops replay.
21. Acked events are not replayed after restart.
22. Event replay order follows original journal order.
23. Duplicate/differing records for the same event ID are rejected as corruption; duplicate compatible ack records are harmless.

### Compatibility tests

24. PR #85860 registry version-1 entries load as active mappings without losing URI/content.
25. Existing OpenViking provider/plugin/memory bridge regression suites remain green.
26. Windows/macOS path and file-permission behavior does not introduce platform-specific failures.

## 19. Implementation boundaries

The preferred code organization is to evolve `plugins/memory/openviking/native_memory_mirror.py` rather than move provider-specific durability into Hermes core.

Small internal units should separate:

- journal codec/scanner;
- durable append helper;
- registry migration/state transitions;
- event construction/resolution;
- remote idempotent application;
- replay/retry worker lifecycle.

`plugins/memory/openviking/__init__.py` should remain a thin integration layer responsible for provider lifecycle hooks and delegating native-memory mutations to the mirror component.

No unrelated OpenViking session, extraction, resource, or tool behavior should be refactored in this PR.

## 20. Acceptance criteria

PR 2 is complete only when all of the following hold:

1. After event fsync, a process restart cannot lose that mirror intent.
2. A replayed `add` can never produce a second URI for the same event.
3. Replay never uses semantic similarity to choose a mutation target.
4. Backend downtime does not prevent durable journaling.
5. Startup automatically replays unacknowledged events.
6. `add`, `replace` and `remove` preserve FIFO semantics through crash/restart.
7. `replace`/`remove` acknowledgement still implies OpenViking index cleanup (`wait=true`).
8. Registry intended state remains usable while earlier operations are pending.
9. Journal and registry corruption fail closed rather than silently discarding memory operations.
10. All new RED tests pass after implementation, and the existing affected Hermes test suites remain green.

## 21. Follow-up sequence

After PR 2:

- **PR 3 — pre-registry reconciliation/backfill**: recover legacy mappings and the accepted native-write-before-outbox gap using deterministic/exact matching first and human review for ambiguous cases.
- **PR 4 — cross-process serialization**: add profile-scoped inter-process locking/lease semantics around outbox append, registry transitions, replay and future compaction.

This ordering keeps durability, migration hygiene and multi-process coordination as separately reviewable failure domains.