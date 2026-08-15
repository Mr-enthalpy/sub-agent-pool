# Local Agent Scheduler

Local Agent Scheduler is a single-machine, local-first, restart-safe control
plane for durable agent work. It owns Tasks, Attempts, Leases, Results,
LogicalAgents, Batches, pool topology, recovery, escalation, and durable Root
notifications. Physical execution belongs to replaceable adapters.

V0.1.2 uses SQLite WAL and ships a Codex `app-server` adapter. Codex, CCR,
TokenRhythm, DeepSeek, credentials, and provider routing are not Scheduler Core
semantics.

The long-term architecture is in [ARCHITECTURE.md](ARCHITECTURE.md). The frozen
V0.1 contracts and V0.1.2 correctness closure are in
[docs/V0.1_SPEC.md](docs/V0.1_SPEC.md).

## Development

```powershell
python -m pip install -e .
python -m unittest discover -s tests -v
local-agent-scheduler --config config/scheduler.example.toml init
local-agent-scheduler --config config/scheduler.example.toml status
```

The CLI is diagnostic and control-oriented. It is not the authoritative state;
SQLite is.

## Minimal operation

Review `config/scheduler.example.toml`, then initialize the database, submit a
Task graph, recover persisted activity, and run the daemon:

```powershell
local-agent-scheduler --config config/scheduler.example.toml init
local-agent-scheduler --config config/scheduler.example.toml batch submit --file examples/batch.json
local-agent-scheduler --config config/scheduler.example.toml recover
local-agent-scheduler --config config/scheduler.example.toml daemon
```

Diagnostics are available through `status` and each entity's `list`/`show`
commands. Explicit controls include Task/Batch cancellation, Result ACK,
Escalation resolution, Execution interrupt/terminate, pool reconciliation,
partition creation/idempotent structural upsert, resize/capacity movement,
MERGE, and guarded partition retirement. Existing partition structure
(`retention`, target, profile, and tags) is immutable in V0.1; use `resize` for
capacity and MOVE/MERGE for classification changes. `pool move-agent` remains
an explicit diagnostic/admin primitive; normal Root orchestration uses the
partition-level topology surface. Busy topology changes track current and
desired membership separately; reconciliation counts pending inbound capacity,
and cutover adopts the destination retention policy.

The example configuration uses the filesystem RootBridge so local testing does
not wake a real Codex task. Set `root_bridge.kind = "codex_app_server"` and a
`root_thread_id` to use the Codex wakeup bridge. Notifications carry durable
event IDs and entity indexes only; Results remain in SQLite.
The bridge may reconcile its own exact notification turn against persisted
Codex state when a live terminal event is missed. Root remains notification-
driven and does not poll Scheduler state.
Outbox delivery runs in a narrow notifier thread independently of execution
supervision and Lease heartbeat renewal. Failed delivery backoff is measured
from delivery completion, so a slow timeout cannot trigger an immediate retry.
Heartbeat renewal is restricted to Executions positively admitted as RUNNING by
the current daemon; adapter presence or UNKNOWN database state is insufficient.
Restart recovery fences unstarted claims, and ambiguous reconciliation cannot
roll a Lease forward forever.
Schema v7 also rejects ambiguous pre-closure MERGE/RETIRE residue with
`LEGACY_TOPOLOGY_REPAIR_REQUIRED`; it never guesses lost declared capacity or
silently moves nonterminal Tasks from an inactive partition.
Positive RUNNING confirmation atomically renews the Lease before daemon-owned
supervision admission. Read-only or frozen-isolated expired work can detach
logically across a target cutover while its old Execution remains available for
stale physical reconciliation; unsafe writers remain suspended.
The cutover safety proof is caller-independent and therefore does not depend on
whether expiry or topology mutation happened first. Late start observations
retain their runtime locator through physical-only history, while Core heartbeat
accepts RUNNING Executions only.
Each successful Codex start stage captures its newly earned session/thread/turn
locator before the next deadline gate. Semantic retirement fences every reusable
Incarnation, and unavailable adapters leave unconfirmed physical work UNKNOWN
and reconcilable rather than declaring it physically failed.
Persisted unavailable partitions do not abort daemon startup: unhealthy work is
normalized while healthy work and notification delivery continue. Every
ExecutionAdapter call is contractually bounded by the adapter's configured
operation deadline; the Codex adapter uses one shared method budget across its
multi-stage start sequence.

See [docs/V0.1_COMPLETION_REPORT.md](docs/V0.1_COMPLETION_REPORT.md) for the
implemented boundaries, recovery procedure, test evidence, and limitations.
