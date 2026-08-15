from __future__ import annotations

import contextlib
import json
import sqlite3
import time
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any


SCHEMA_VERSION = 7


def new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex}"


def utc_now() -> float:
    return time.time()


def json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def json_loads(value: str | None, default: Any = None) -> Any:
    if value is None:
        return default
    return json.loads(value)


SCHEMA = r"""
CREATE TABLE IF NOT EXISTS schema_migrations (
    version INTEGER PRIMARY KEY,
    applied_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS scheduler_meta (
    key TEXT PRIMARY KEY,
    value_json TEXT NOT NULL,
    updated_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS batches (
    id TEXT PRIMARY KEY,
    state TEXT NOT NULL CHECK (state IN ('OPEN','ACTIVE','SUSPENDED','COMPLETED','CANCELLED')),
    metadata_json TEXT NOT NULL DEFAULT '{}',
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS workstreams (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    project_state_ref TEXT,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS tasks (
    id TEXT PRIMARY KEY,
    batch_id TEXT NOT NULL REFERENCES batches(id) ON DELETE RESTRICT,
    name TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    acceptance_json TEXT NOT NULL DEFAULT '{}',
    partition_name TEXT NOT NULL REFERENCES pool_partitions(name) ON DELETE RESTRICT,
    workstream_id TEXT REFERENCES workstreams(id) ON DELETE SET NULL,
    continuity TEXT NOT NULL CHECK (continuity IN ('required','preferred','none')),
    affinity_tags_json TEXT NOT NULL DEFAULT '[]',
    workspace_mode TEXT NOT NULL CHECK (workspace_mode IN ('read_only','write')),
    required INTEGER NOT NULL CHECK (required = 1),
    priority INTEGER NOT NULL DEFAULT 0,
    state TEXT NOT NULL CHECK (state IN ('BLOCKED','QUEUED','LEASED','RUNNING','RETRY_WAIT','SUSPENDED','COMPLETED','CANCELLED')),
    max_attempts INTEGER NOT NULL CHECK (max_attempts >= 1),
    retry_classes_json TEXT NOT NULL,
    base_backoff_seconds REAL NOT NULL CHECK (base_backoff_seconds >= 0),
    max_backoff_seconds REAL NOT NULL CHECK (max_backoff_seconds >= base_backoff_seconds),
    next_eligible_at REAL,
    current_attempt_id TEXT,
    fencing_epoch INTEGER NOT NULL DEFAULT 0 CHECK (fencing_epoch >= 0),
    supersedes_task_id TEXT REFERENCES tasks(id) ON DELETE SET NULL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS task_dependencies (
    task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    depends_on_task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE RESTRICT,
    PRIMARY KEY (task_id, depends_on_task_id),
    CHECK (task_id <> depends_on_task_id)
);

CREATE TABLE IF NOT EXISTS pool_partitions (
    name TEXT PRIMARY KEY,
    desired_capacity INTEGER NOT NULL CHECK (desired_capacity >= 0),
    retention TEXT NOT NULL CHECK (retention IN ('resident','ephemeral')),
    execution_target TEXT NOT NULL,
    execution_profile TEXT NOT NULL,
    tags_json TEXT NOT NULL DEFAULT '[]',
    active INTEGER NOT NULL DEFAULT 1 CHECK (active IN (0,1)),
    merged_into TEXT REFERENCES pool_partitions(name) ON DELETE SET NULL,
    topology_revision INTEGER NOT NULL DEFAULT 0,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS pool_topology_revisions (
    revision INTEGER PRIMARY KEY AUTOINCREMENT,
    operation TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS logical_agents (
    id TEXT PRIMARY KEY,
    partition_name TEXT NOT NULL REFERENCES pool_partitions(name) ON DELETE RESTRICT,
    retention TEXT NOT NULL CHECK (retention IN ('resident','ephemeral')),
    state TEXT NOT NULL CHECK (state IN ('INITIALIZING','READY','ASSIGNED','REVIVING','DRAINING','SUSPENDED','RETIRED')),
    workstream_id TEXT REFERENCES workstreams(id) ON DELETE SET NULL,
    tags_json TEXT NOT NULL DEFAULT '[]',
    current_task_id TEXT REFERENCES tasks(id) ON DELETE SET NULL,
    pending_partition_name TEXT REFERENCES pool_partitions(name) ON DELETE SET NULL,
    retirement_requested INTEGER NOT NULL DEFAULT 0 CHECK (retirement_requested IN (0,1)),
    continuity_json TEXT NOT NULL DEFAULT '{}',
    continuity_version INTEGER NOT NULL DEFAULT 0,
    current_checkpoint_id TEXT,
    available_since REAL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS one_assigned_agent_per_task
ON logical_agents(current_task_id)
WHERE current_task_id IS NOT NULL AND state = 'ASSIGNED';

CREATE TABLE IF NOT EXISTS incarnations (
    id TEXT PRIMARY KEY,
    logical_agent_id TEXT NOT NULL REFERENCES logical_agents(id) ON DELETE RESTRICT,
    generation INTEGER NOT NULL CHECK (generation >= 1),
    execution_target TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('STARTING','WARM','COLD','LOST','TERMINATED')),
    runtime_handle_json TEXT NOT NULL DEFAULT '{}',
    started_at REAL NOT NULL,
    ended_at REAL,
    UNIQUE (logical_agent_id, generation)
);

CREATE TABLE IF NOT EXISTS attempts (
    id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE RESTRICT,
    logical_agent_id TEXT NOT NULL REFERENCES logical_agents(id) ON DELETE RESTRICT,
    incarnation_id TEXT REFERENCES incarnations(id) ON DELETE RESTRICT,
    attempt_number INTEGER NOT NULL CHECK (attempt_number >= 1),
    lease_epoch INTEGER NOT NULL CHECK (lease_epoch >= 1),
    state TEXT NOT NULL CHECK (state IN ('ACTIVE','SUCCEEDED','FAILED','EXPIRED','CANCELLED')),
    created_at REAL NOT NULL,
    ended_at REAL,
    UNIQUE (task_id, attempt_number)
);

CREATE UNIQUE INDEX IF NOT EXISTS one_active_attempt_per_task
ON attempts(task_id) WHERE state = 'ACTIVE';

CREATE TABLE IF NOT EXISTS leases (
    id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE RESTRICT,
    attempt_id TEXT NOT NULL UNIQUE REFERENCES attempts(id) ON DELETE RESTRICT,
    epoch INTEGER NOT NULL CHECK (epoch >= 1),
    state TEXT NOT NULL CHECK (state IN ('ACTIVE','RELEASED','EXPIRED','REVOKED')),
    expires_at REAL NOT NULL,
    heartbeat_at REAL NOT NULL,
    created_at REAL NOT NULL,
    ended_at REAL,
    UNIQUE (task_id, epoch)
);

CREATE UNIQUE INDEX IF NOT EXISTS one_active_lease_per_task
ON leases(task_id) WHERE state = 'ACTIVE';

CREATE TABLE IF NOT EXISTS executions (
    id TEXT PRIMARY KEY,
    request_id TEXT NOT NULL UNIQUE,
    task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE RESTRICT,
    attempt_id TEXT NOT NULL REFERENCES attempts(id) ON DELETE RESTRICT,
    incarnation_id TEXT NOT NULL REFERENCES incarnations(id) ON DELETE RESTRICT,
    execution_target TEXT NOT NULL,
    execution_profile TEXT NOT NULL,
    attempt_isolation INTEGER NOT NULL DEFAULT 0 CHECK (attempt_isolation IN (0,1)),
    state TEXT NOT NULL CHECK (state IN ('STARTING','RUNNING','SUCCEEDED','FAILED','LOST','UNKNOWN','TERMINATED')),
    runtime_handle_json TEXT NOT NULL DEFAULT '{}',
    outcome_json TEXT,
    failure_class TEXT,
    failure_code TEXT,
    failure_signature TEXT,
    terminal_confirmed INTEGER NOT NULL DEFAULT 0 CHECK (terminal_confirmed IN (0,1)),
    quiescent_confirmed INTEGER NOT NULL DEFAULT 0 CHECK (quiescent_confirmed IN (0,1)),
    started_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    ended_at REAL
);

CREATE TABLE IF NOT EXISTS results (
    id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL UNIQUE REFERENCES tasks(id) ON DELETE RESTRICT,
    batch_id TEXT NOT NULL REFERENCES batches(id) ON DELETE RESTRICT,
    attempt_id TEXT NOT NULL REFERENCES attempts(id) ON DELETE RESTRICT,
    logical_agent_id TEXT NOT NULL REFERENCES logical_agents(id) ON DELETE RESTRICT,
    execution_id TEXT REFERENCES executions(id) ON DELETE SET NULL,
    payload_json TEXT NOT NULL,
    summary TEXT,
    checkpoint_id TEXT,
    workspace_state_ref TEXT,
    state TEXT NOT NULL CHECK (state IN ('AVAILABLE','ACKED')),
    created_at REAL NOT NULL,
    consumed_at REAL,
    consumer_ref TEXT,
    disposition TEXT
);

CREATE TABLE IF NOT EXISTS failures (
    id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE RESTRICT,
    attempt_id TEXT REFERENCES attempts(id) ON DELETE SET NULL,
    execution_id TEXT REFERENCES executions(id) ON DELETE SET NULL,
    failure_class TEXT NOT NULL,
    failure_code TEXT,
    normalized_signature TEXT,
    detail TEXT,
    created_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS escalations (
    id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE RESTRICT,
    batch_id TEXT NOT NULL REFERENCES batches(id) ON DELETE RESTRICT,
    logical_agent_id TEXT REFERENCES logical_agents(id) ON DELETE SET NULL,
    workstream_id TEXT REFERENCES workstreams(id) ON DELETE SET NULL,
    failure_class TEXT NOT NULL,
    normalized_signature TEXT,
    snapshot_json TEXT NOT NULL,
    decision_required TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('OPEN','RESOLVED','CANCELLED')),
    created_at REAL NOT NULL,
    resolved_at REAL
);

CREATE UNIQUE INDEX IF NOT EXISTS one_open_escalation_per_task
ON escalations(task_id) WHERE state = 'OPEN';

CREATE TABLE IF NOT EXISTS checkpoints (
    id TEXT PRIMARY KEY,
    logical_agent_id TEXT NOT NULL REFERENCES logical_agents(id) ON DELETE RESTRICT,
    task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE RESTRICT,
    attempt_id TEXT NOT NULL REFERENCES attempts(id) ON DELETE RESTRICT,
    lease_epoch INTEGER NOT NULL,
    continuity_version INTEGER NOT NULL,
    capsule_json TEXT NOT NULL,
    project_state_ref TEXT,
    created_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS notification_outbox (
    id TEXT PRIMARY KEY,
    event_type TEXT NOT NULL,
    aggregate_type TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('PENDING','DELIVERED','ACKED')),
    delivery_attempts INTEGER NOT NULL DEFAULT 0,
    next_delivery_at REAL NOT NULL,
    created_at REAL NOT NULL,
    delivered_at REAL,
    acknowledged_at REAL,
    last_error TEXT
);

CREATE INDEX IF NOT EXISTS tasks_queue_idx
ON tasks(state, next_eligible_at, priority DESC, created_at);
CREATE INDEX IF NOT EXISTS leases_expiry_idx ON leases(state, expires_at);
CREATE INDEX IF NOT EXISTS outbox_delivery_idx
ON notification_outbox(state, next_delivery_at, created_at);
CREATE INDEX IF NOT EXISTS executions_attempt_idx ON executions(attempt_id, updated_at);
CREATE UNIQUE INDEX IF NOT EXISTS one_active_execution_per_incarnation
ON executions(incarnation_id)
WHERE state IN ('STARTING','RUNNING','UNKNOWN');
"""


def _legacy_incarnation_terminal(
    execution: sqlite3.Row, migration_time: float
) -> tuple[str, float | None]:
    """Derive conservative physical-presence state for a V0.1 Execution."""

    state = str(execution["state"])
    if state == "RUNNING":
        return "WARM", None
    if state in {"STARTING", "UNKNOWN"}:
        return "LOST", execution["ended_at"] or migration_time
    if (
        state in {"SUCCEEDED", "FAILED", "TERMINATED"}
        and bool(execution["terminal_confirmed"])
        and bool(execution["quiescent_confirmed"])
    ):
        return "TERMINATED", execution["ended_at"] or migration_time
    return "LOST", execution["ended_at"] or migration_time


class Database:
    def __init__(self, path: str | Path):
        self.path = str(path)

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=30.0, isolation_level=None)
        try:
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA foreign_keys = ON")
            conn.execute("PRAGMA busy_timeout = 30000")
            deadline = time.monotonic() + 30.0
            while True:
                try:
                    conn.execute("PRAGMA journal_mode = WAL")
                    break
                except sqlite3.OperationalError as exc:
                    if "locked" not in str(exc).lower() or time.monotonic() >= deadline:
                        raise
                    time.sleep(0.05)
            conn.execute("PRAGMA synchronous = FULL")
            return conn
        except BaseException:
            conn.close()
            raise

    def initialize(self) -> None:
        conn = self.connect()
        try:
            conn.executescript(SCHEMA)
            row = conn.execute(
                "SELECT MAX(version) AS version FROM schema_migrations"
            ).fetchone()
            discovered = row["version"] if row and row["version"] is not None else 0
            if discovered < 3:
                # v3 may rebuild Attempts.  Foreign keys must be disabled
                # before BEGIN so dependent table declarations retain the
                # authoritative `attempts` name during the swap.
                conn.execute("PRAGMA foreign_keys = OFF")
            conn.execute("BEGIN IMMEDIATE")
            try:
                row = conn.execute(
                    "SELECT MAX(version) AS version FROM schema_migrations"
                ).fetchone()
                current = row["version"] if row and row["version"] is not None else 0
                if current > SCHEMA_VERSION:
                    raise RuntimeError(
                        f"database schema {current} is newer than supported {SCHEMA_VERSION}"
                    )
                if current == 0:
                    conn.execute(
                        "INSERT INTO schema_migrations(version, applied_at) VALUES (?, ?)",
                        (1, utc_now()),
                    )
                    current = 1
                if current < 2:
                    self._migrate_v1_to_v2(conn)
                    conn.execute(
                        "INSERT INTO schema_migrations(version, applied_at) VALUES (?, ?)",
                        (2, utc_now()),
                    )
                    current = 2
                if current < 3:
                    self._migrate_v2_to_v3(conn)
                    conn.execute(
                        "INSERT INTO schema_migrations(version, applied_at) VALUES (?, ?)",
                        (3, utc_now()),
                    )
                    current = 3
                if current < 4:
                    self._migrate_v3_to_v4(conn)
                    conn.execute(
                        "INSERT INTO schema_migrations(version, applied_at) VALUES (?, ?)",
                        (4, utc_now()),
                    )
                    current = 4
                else:
                    # Reassert correctness indexes/triggers if an operator
                    # removed one without changing the schema marker.
                    self._migrate_v3_to_v4(conn)
                if current < 5:
                    self._migrate_v4_to_v5(conn)
                    conn.execute(
                        "INSERT INTO schema_migrations(version, applied_at) VALUES (?, ?)",
                        (5, utc_now()),
                    )
                    current = 5
                else:
                    self._migrate_v4_to_v5(conn)
                if current < 6:
                    self._migrate_v5_to_v6(conn)
                    conn.execute(
                        "INSERT INTO schema_migrations(version, applied_at) VALUES (?, ?)",
                        (6, utc_now()),
                    )
                    current = 6
                else:
                    self._migrate_v5_to_v6(conn)
                if current < 7:
                    self._migrate_v6_to_v7(conn)
                    conn.execute(
                        "INSERT INTO schema_migrations(version, applied_at) VALUES (?, ?)",
                        (7, utc_now()),
                    )
                else:
                    self._migrate_v6_to_v7(conn)
                conn.execute("COMMIT")
                conn.execute("PRAGMA foreign_keys = ON")
                violations = conn.execute("PRAGMA foreign_key_check").fetchall()
                if violations:
                    raise RuntimeError(
                        f"database migration produced foreign-key violations: {violations}"
                    )
            except BaseException:
                if conn.in_transaction:
                    conn.execute("ROLLBACK")
                raise
        finally:
            conn.close()

    @staticmethod
    def _migrate_v1_to_v2(conn: sqlite3.Connection) -> None:
        """Make one Incarnation represent exactly one physical Execution.

        V0.1 allowed an Incarnation to be reused after its Codex app-server
        session had closed.  Preserve the historical rows by assigning every
        additional Execution a new generation, then close orphaned legacy
        presence before installing the V0.1.1 uniqueness constraints.
        """

        reused = conn.execute(
            "SELECT incarnation_id FROM executions GROUP BY incarnation_id HAVING COUNT(*) > 1"
        ).fetchall()
        duplicate_attempt = conn.execute(
            "SELECT attempt_id FROM executions GROUP BY attempt_id HAVING COUNT(*) > 1 LIMIT 1"
        ).fetchone()
        if duplicate_attempt:
            raise RuntimeError(
                "schema v2 migration cannot normalize multiple Executions for one Attempt"
            )
        now = utc_now()
        for group in reused:
            incarnation_id = str(group["incarnation_id"])
            incarnation = conn.execute(
                "SELECT * FROM incarnations WHERE id=?", (incarnation_id,)
            ).fetchone()
            executions = conn.execute(
                "SELECT * FROM executions WHERE incarnation_id=? "
                "ORDER BY started_at,id",
                (incarnation_id,),
            ).fetchall()
            conn.execute(
                "UPDATE incarnations SET state=?,ended_at=? WHERE id=?",
                (*_legacy_incarnation_terminal(executions[0], now), incarnation_id),
            )
            generation = int(
                conn.execute(
                    "SELECT COALESCE(MAX(generation),0) FROM incarnations "
                    "WHERE logical_agent_id=?",
                    (incarnation["logical_agent_id"],),
                ).fetchone()[0]
            )
            for execution in executions[1:]:
                generation += 1
                replacement_id = new_id("inc")
                state, ended_at = _legacy_incarnation_terminal(execution, now)
                conn.execute(
                    "INSERT INTO incarnations(id,logical_agent_id,generation,execution_target,state,"
                    "runtime_handle_json,started_at,ended_at) VALUES(?,?,?,?,?,?,?,?)",
                    (
                        replacement_id,
                        incarnation["logical_agent_id"],
                        generation,
                        execution["execution_target"],
                        state,
                        execution["runtime_handle_json"],
                        execution["started_at"],
                        ended_at,
                    ),
                )
                conn.execute(
                    "UPDATE executions SET incarnation_id=? WHERE id=?",
                    (replacement_id, execution["id"]),
                )
                conn.execute(
                    "UPDATE attempts SET incarnation_id=? WHERE id=?",
                    (replacement_id, execution["attempt_id"]),
                )

        # A v1 STARTING/UNKNOWN Execution never proves attachable physical
        # presence after upgrade, even when its Attempt is still authoritative.
        # Startup reconciliation may later confirm that exact Execution and
        # move its LOST Incarnation back to WARM through the fenced path.
        conn.execute(
            "UPDATE incarnations SET state='LOST',ended_at=COALESCE(ended_at,?) "
            "WHERE state IN ('STARTING','WARM','COLD') AND EXISTS ("
            "SELECT 1 FROM executions e WHERE e.incarnation_id=incarnations.id "
            "AND e.state IN ('STARTING','UNKNOWN'))",
            (now,),
        )

        # A reservation that never started can close at the migration
        # boundary.  An unconfirmed historical Execution is conservatively
        # LOST instead: inactive authority does not prove physical quiescence.
        conn.execute(
            "UPDATE incarnations SET state='TERMINATED',ended_at=COALESCE(ended_at,?) "
            "WHERE state IN ('STARTING','WARM','COLD') AND NOT EXISTS ("
            "SELECT 1 FROM attempts a WHERE a.incarnation_id=incarnations.id "
            "AND a.state='ACTIVE') AND NOT EXISTS ("
            "SELECT 1 FROM executions e WHERE e.incarnation_id=incarnations.id)",
            (now,),
        )
        conn.execute(
            "UPDATE incarnations SET state='LOST',ended_at=COALESCE(ended_at,?) "
            "WHERE state IN ('STARTING','WARM','COLD') AND NOT EXISTS ("
            "SELECT 1 FROM attempts a WHERE a.incarnation_id=incarnations.id "
            "AND a.state='ACTIVE') AND EXISTS ("
            "SELECT 1 FROM executions e WHERE e.incarnation_id=incarnations.id)",
            (now,),
        )

        # Corrupt/legacy target switching could leave more than one active
        # embodiment.  Retain the newest generation and fence older presence
        # as LOST; do not claim physical quiescence that was never observed.
        active = conn.execute(
            "SELECT logical_agent_id FROM incarnations "
            "WHERE state IN ('STARTING','WARM','COLD') GROUP BY logical_agent_id "
            "HAVING COUNT(*) > 1"
        ).fetchall()
        for group in active:
            rows = conn.execute(
                "SELECT id FROM incarnations WHERE logical_agent_id=? "
                "AND state IN ('STARTING','WARM','COLD') ORDER BY generation DESC,id DESC",
                (group["logical_agent_id"],),
            ).fetchall()
            for stale in rows[1:]:
                conn.execute(
                    "UPDATE incarnations SET state='LOST',ended_at=COALESCE(ended_at,?) WHERE id=?",
                    (now, stale["id"]),
                )

        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS one_active_incarnation_per_agent "
            "ON incarnations(logical_agent_id) "
            "WHERE state IN ('STARTING','WARM','COLD')"
        )
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS one_execution_per_incarnation "
            "ON executions(incarnation_id)"
        )

    @staticmethod
    def _migrate_v2_to_v3(conn: sqlite3.Connection) -> None:
        """Detach claim authority from physical presence and permit sequential reuse."""

        conn.execute("DROP INDEX IF EXISTS one_execution_per_incarnation")
        duplicate_active = conn.execute(
            "SELECT incarnation_id FROM executions "
            "WHERE state IN ('STARTING','RUNNING','UNKNOWN') "
            "GROUP BY incarnation_id HAVING COUNT(*) > 1 LIMIT 1"
        ).fetchone()
        if duplicate_active:
            raise RuntimeError(
                "schema v3 migration found multiple active Executions for one Incarnation"
            )
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS one_active_execution_per_incarnation "
            "ON executions(incarnation_id) "
            "WHERE state IN ('STARTING','RUNNING','UNKNOWN')"
        )

        # SQLite cannot drop a NOT NULL constraint in place.  Keep child FKs
        # aimed at the stable table name while rebuilding Attempts.
        incarnation_column = next(
            row for row in conn.execute("PRAGMA table_info(attempts)").fetchall()
            if row["name"] == "incarnation_id"
        )
        if bool(incarnation_column["notnull"]):
            conn.execute("DROP INDEX IF EXISTS one_active_attempt_per_task")
            conn.execute("PRAGMA legacy_alter_table = ON")
            conn.execute("ALTER TABLE attempts RENAME TO attempts_v2")
            conn.execute(
                "CREATE TABLE attempts ("
                "id TEXT PRIMARY KEY,"
                "task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE RESTRICT,"
                "logical_agent_id TEXT NOT NULL REFERENCES logical_agents(id) ON DELETE RESTRICT,"
                "incarnation_id TEXT REFERENCES incarnations(id) ON DELETE RESTRICT,"
                "attempt_number INTEGER NOT NULL CHECK (attempt_number >= 1),"
                "lease_epoch INTEGER NOT NULL CHECK (lease_epoch >= 1),"
                "state TEXT NOT NULL CHECK (state IN "
                "('ACTIVE','SUCCEEDED','FAILED','EXPIRED','CANCELLED')),"
                "created_at REAL NOT NULL,ended_at REAL,"
                "UNIQUE (task_id, attempt_number))"
            )
            conn.execute(
                "INSERT INTO attempts SELECT id,task_id,logical_agent_id,incarnation_id,"
                "attempt_number,lease_epoch,state,created_at,ended_at FROM attempts_v2"
            )
            conn.execute("DROP TABLE attempts_v2")
            conn.execute("PRAGMA legacy_alter_table = OFF")
            conn.execute(
                "CREATE UNIQUE INDEX one_active_attempt_per_task "
                "ON attempts(task_id) WHERE state='ACTIVE'"
            )

        # Existing SQLite topology is authoritative after upgrade.  Fresh
        # databases remain unmarked until configuration bootstraps them.
        if conn.execute("SELECT 1 FROM pool_partitions LIMIT 1").fetchone():
            now = utc_now()
            conn.execute(
                "INSERT INTO scheduler_meta(key,value_json,updated_at) VALUES(?,?,?) "
                "ON CONFLICT(key) DO NOTHING",
                ("topology_bootstrapped", json_dumps({"source": "sqlite-v2"}), now),
            )

    @staticmethod
    def _migrate_v3_to_v4(conn: sqlite3.Connection) -> None:
        """Close optional-work and authority uniqueness at the storage layer.

        Ambiguous legacy authority is never normalized.  The operator must
        inspect and repair the v3 database before retrying the migration.
        """

        optional = conn.execute(
            "SELECT id FROM tasks WHERE required<>1 LIMIT 1"
        ).fetchone()
        if optional:
            raise RuntimeError(
                f"schema v4 migration found unsupported optional Task {optional['id']}"
            )
        duplicate_agent = conn.execute(
            "SELECT logical_agent_id FROM attempts WHERE state='ACTIVE' "
            "GROUP BY logical_agent_id HAVING COUNT(*)>1 LIMIT 1"
        ).fetchone()
        if duplicate_agent:
            raise RuntimeError(
                "schema v4 migration found multiple ACTIVE Attempts for LogicalAgent "
                f"{duplicate_agent['logical_agent_id']}"
            )
        duplicate_attempt = conn.execute(
            "SELECT attempt_id FROM executions GROUP BY attempt_id "
            "HAVING COUNT(*)>1 LIMIT 1"
        ).fetchone()
        if duplicate_attempt:
            raise RuntimeError(
                "schema v4 migration found multiple Executions for Attempt "
                f"{duplicate_attempt['attempt_id']}"
            )

        conn.execute("DROP INDEX IF EXISTS one_active_assignment_per_agent")
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS one_assigned_agent_per_task "
            "ON logical_agents(current_task_id) "
            "WHERE current_task_id IS NOT NULL AND state='ASSIGNED'"
        )
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS one_active_attempt_per_agent "
            "ON attempts(logical_agent_id) WHERE state='ACTIVE'"
        )
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS one_execution_per_attempt "
            "ON executions(attempt_id)"
        )
        conn.execute(
            "CREATE TRIGGER IF NOT EXISTS tasks_required_insert_only "
            "BEFORE INSERT ON tasks WHEN NEW.required<>1 BEGIN "
            "SELECT RAISE(ABORT,'optional Tasks are not supported'); END"
        )
        conn.execute(
            "CREATE TRIGGER IF NOT EXISTS tasks_required_update_only "
            "BEFORE UPDATE OF required ON tasks WHEN NEW.required<>1 BEGIN "
            "SELECT RAISE(ABORT,'optional Tasks are not supported'); END"
        )

    @staticmethod
    def _migrate_v4_to_v5(conn: sqlite3.Connection) -> None:
        """Freeze writer isolation per Execution and close legacy drain residue.

        Existing Executions are conservatively marked non-isolated.  Current
        target configuration is not evidence about the safety properties under
        which a historical physical Execution was started.
        """

        execution_columns = {
            str(row["name"]) for row in conn.execute("PRAGMA table_info(executions)")
        }
        if "attempt_isolation" not in execution_columns:
            conn.execute(
                "ALTER TABLE executions ADD COLUMN attempt_isolation INTEGER "
                "NOT NULL DEFAULT 0 CHECK (attempt_isolation IN (0,1))"
            )

        # V0.1.1 could leave excess unassigned agents permanently DRAINING.
        # retirement_requested=1 is already an explicit semantic-retirement
        # decision, so completing it does not invent a new lifecycle choice.
        conn.execute(
            "UPDATE logical_agents SET state='RETIRED',retirement_requested=0,"
            "available_since=NULL,updated_at=? "
            "WHERE state='DRAINING' AND current_task_id IS NULL "
            "AND pending_partition_name IS NULL AND retirement_requested=1",
            (utc_now(),),
        )

    @staticmethod
    def _migrate_v5_to_v6(conn: sqlite3.Connection) -> None:
        """Preserve unresolved physical work and close semantic retirement presence."""

        now = utc_now()
        conn.execute(
            "UPDATE executions SET state='UNKNOWN',ended_at=NULL,updated_at=? "
            "WHERE state='FAILED' AND terminal_confirmed=0",
            (now,),
        )
        conn.execute(
            "UPDATE incarnations SET state='LOST',ended_at=COALESCE(ended_at,?) "
            "WHERE state IN ('STARTING','WARM','COLD') "
            "AND logical_agent_id IN (SELECT id FROM logical_agents WHERE state='RETIRED')",
            (now,),
        )

    @staticmethod
    def _migrate_v6_to_v7(conn: sqlite3.Connection) -> None:
        """Reject ambiguous topology damage created before MERGE/RETIRE closure.

        Pre-closure MERGE revisions did not snapshot either partition's declared
        capacity, so lost desired topology cannot be reconstructed from runtime
        population.  Pre-closure MERGE and RETIRE could also strand nonterminal
        Tasks on inactive partitions.  Both conditions require an explicit
        operator repair rather than a guessed migration.
        """

        revisions = conn.execute(
            "SELECT revision,payload_json FROM pool_topology_revisions "
            "WHERE operation='MERGE' ORDER BY revision"
        ).fetchall()
        for revision in revisions:
            try:
                payload = json_loads(revision["payload_json"], {})
            except (TypeError, ValueError, json.JSONDecodeError):
                payload = None
            source_capacity = (
                payload.get("source_capacity") if isinstance(payload, dict) else None
            )
            target_capacity = (
                payload.get("target_capacity") if isinstance(payload, dict) else None
            )
            valid_snapshot = (
                isinstance(source_capacity, int)
                and not isinstance(source_capacity, bool)
                and source_capacity >= 0
                and isinstance(target_capacity, int)
                and not isinstance(target_capacity, bool)
                and target_capacity >= 0
            )
            if not valid_snapshot:
                raise RuntimeError(
                    "LEGACY_TOPOLOGY_REPAIR_REQUIRED: MERGE revision "
                    f"{revision['revision']} lacks valid declared-capacity snapshots"
                )

        stranded = conn.execute(
            "SELECT t.id,t.state,t.partition_name FROM tasks t "
            "JOIN pool_partitions p ON p.name=t.partition_name "
            "WHERE t.state NOT IN ('COMPLETED','CANCELLED') AND p.active=0 "
            "ORDER BY t.created_at,t.id LIMIT 1"
        ).fetchone()
        if stranded:
            raise RuntimeError(
                "LEGACY_TOPOLOGY_REPAIR_REQUIRED: nonterminal Task "
                f"{stranded['id']} in state {stranded['state']} remains on inactive "
                f"partition {stranded['partition_name']}"
            )

    @contextlib.contextmanager
    def transaction(self, *, immediate: bool = True) -> Iterator[sqlite3.Connection]:
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
            yield conn
            conn.execute("COMMIT")
        except BaseException:
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()

    def fetch_one(self, sql: str, params: tuple[Any, ...] = ()) -> sqlite3.Row | None:
        conn = self.connect()
        try:
            return conn.execute(sql, params).fetchone()
        finally:
            conn.close()

    def fetch_all(self, sql: str, params: tuple[Any, ...] = ()) -> list[sqlite3.Row]:
        conn = self.connect()
        try:
            return list(conn.execute(sql, params).fetchall())
        finally:
            conn.close()

    def integrity_check(self) -> str:
        conn = self.connect()
        try:
            return str(conn.execute("PRAGMA integrity_check").fetchone()[0])
        finally:
            conn.close()
