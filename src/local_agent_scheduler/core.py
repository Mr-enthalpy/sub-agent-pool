from __future__ import annotations

import sqlite3
from collections.abc import Mapping, Sequence
from typing import Any

from .enums import (
    AgentState,
    AttemptState,
    BatchState,
    ContinuityPreference,
    EscalationState,
    ExecutionState,
    FailureClass,
    IncarnationState,
    LeaseState,
    OutboxState,
    ResultState,
    Retention,
    TaskState,
    WorkspaceMode,
)
from .errors import InvalidTransition, NotFound, StaleAuthority
from .models import CONTINUITY_KEYS, Claim, PartitionSpec, TaskSpec, tags_match
from .storage import Database, json_dumps, json_loads, new_id, utc_now


class Scheduler:
    """Transactional Scheduler Core with no frontend-specific dependencies."""

    def __init__(
        self,
        database: Database,
        *,
        lease_seconds: float = 120.0,
        continuity_max_bytes: int = 16_384,
    ) -> None:
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        self.db = database
        self.lease_seconds = float(lease_seconds)
        self.continuity_max_bytes = int(continuity_max_bytes)

    def initialize(self) -> None:
        self.db.initialize()
        now = utc_now()
        with self.db.transaction() as conn:
            conn.execute(
                "INSERT INTO scheduler_meta(key,value_json,updated_at) VALUES('lifecycle',?,?) "
                "ON CONFLICT(key) DO NOTHING",
                (json_dumps({"state": "START"}), now),
            )

    def set_lifecycle(self, state: str) -> None:
        with self.db.transaction() as conn:
            conn.execute(
                "INSERT INTO scheduler_meta(key,value_json,updated_at) VALUES('lifecycle',?,?) "
                "ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json,updated_at=excluded.updated_at",
                (json_dumps({"state": state}), utc_now()),
            )

    # ------------------------------------------------------------------
    # Workstreams and topology

    def create_workstream(
        self, name: str, *, project_state_ref: str | None = None, workstream_id: str | None = None
    ) -> str:
        workstream_id = workstream_id or new_id("ws")
        now = utc_now()
        with self.db.transaction() as conn:
            conn.execute(
                "INSERT INTO workstreams(id,name,project_state_ref,created_at,updated_at) "
                "VALUES(?,?,?,?,?)",
                (workstream_id, name, project_state_ref, now, now),
            )
        return workstream_id

    def upsert_partition(self, spec: PartitionSpec) -> int:
        if spec.desired_capacity < 0:
            raise ValueError("desired_capacity must be non-negative")
        now = utc_now()
        payload = {
            "name": spec.name,
            "desired_capacity": spec.desired_capacity,
            "retention": spec.retention.value,
            "execution_target": spec.execution_target,
            "execution_profile": spec.execution_profile,
            "tags": list(spec.tags),
        }
        with self.db.transaction() as conn:
            existing = conn.execute(
                "SELECT * FROM pool_partitions WHERE name=?", (spec.name,)
            ).fetchone()
            if existing:
                if not existing["active"]:
                    raise InvalidTransition(
                        "pool upsert cannot reactivate an inactive partition"
                    )
                structural_mismatches = []
                if existing["retention"] != spec.retention.value:
                    structural_mismatches.append("retention")
                if existing["execution_target"] != spec.execution_target:
                    structural_mismatches.append("execution_target")
                if existing["execution_profile"] != spec.execution_profile:
                    structural_mismatches.append("execution_profile")
                if sorted(json_loads(existing["tags_json"], [])) != sorted(spec.tags):
                    structural_mismatches.append("tags")
                if structural_mismatches:
                    raise InvalidTransition(
                        "pool upsert cannot mutate an existing partition's structural "
                        f"definition: {', '.join(structural_mismatches)}"
                    )
                if int(existing["desired_capacity"]) != spec.desired_capacity:
                    raise InvalidTransition(
                        "pool upsert cannot resize an existing partition; use pool resize"
                    )
                return int(existing["topology_revision"])
            cursor = conn.execute(
                "INSERT INTO pool_topology_revisions(operation,payload_json,created_at) VALUES(?,?,?)",
                ("UPSERT", json_dumps(payload), now),
            )
            revision = int(cursor.lastrowid)
            conn.execute(
                "INSERT INTO pool_partitions(name,desired_capacity,retention,execution_target,"
                "execution_profile,tags_json,active,topology_revision,created_at,updated_at) "
                "VALUES(?,?,?,?,?,?,1,?,?,?)",
                (
                    spec.name,
                    spec.desired_capacity,
                    spec.retention.value,
                    spec.execution_target,
                    spec.execution_profile,
                    json_dumps(list(spec.tags)),
                    revision,
                    now,
                    now,
                ),
            )
        return revision

    @staticmethod
    def _canonical_partition(
        conn: sqlite3.Connection, partition_name: str
    ) -> sqlite3.Row:
        """Resolve a desired partition through durable MERGE revisions."""

        visited: set[str] = set()
        current = partition_name
        while current not in visited:
            visited.add(current)
            partition = conn.execute(
                "SELECT * FROM pool_partitions WHERE name=?", (current,)
            ).fetchone()
            if not partition:
                raise InvalidTransition(
                    f"desired partition {partition_name!r} does not exist"
                )
            if partition["active"]:
                return partition
            if not partition["merged_into"]:
                raise InvalidTransition(
                    f"desired partition {partition_name!r} is retired"
                )
            current = str(partition["merged_into"])
        raise InvalidTransition(
            f"partition merge cycle while resolving {partition_name!r}"
        )

    @staticmethod
    def _unsafe_cross_target_execution(
        conn: sqlite3.Connection,
        agent_id: str,
        target_execution_target: str,
        now: float,
    ) -> sqlite3.Row | None:
        cross_target_executions = conn.execute(
            "SELECT e.id,e.attempt_isolation,e.quiescent_confirmed,"
            "a.state AS attempt_state,l.state AS lease_state,l.expires_at,"
            "t.workspace_mode FROM executions e "
            "JOIN incarnations i ON i.id=e.incarnation_id "
            "JOIN attempts a ON a.id=e.attempt_id "
            "JOIN tasks t ON t.id=e.task_id "
            "LEFT JOIN leases l ON l.attempt_id=a.id "
            "WHERE i.logical_agent_id=? AND e.state IN ('STARTING','RUNNING','UNKNOWN') "
            "AND e.execution_target<>?",
            (agent_id, target_execution_target),
        ).fetchall()
        return next(
            (
                execution
                for execution in cross_target_executions
                if not (
                    execution["attempt_state"] != AttemptState.ACTIVE.value
                    and (
                        execution["lease_state"] is None
                        or execution["lease_state"] != LeaseState.ACTIVE.value
                        or float(execution["expires_at"]) <= now
                    )
                    and (
                        execution["workspace_mode"] == WorkspaceMode.READ_ONLY.value
                        or bool(execution["attempt_isolation"])
                        or bool(execution["quiescent_confirmed"])
                    )
                )
            ),
            None,
        )

    def _commit_partition_cutover(
        self,
        conn: sqlite3.Connection,
        agent: sqlite3.Row,
        target_partition: str,
        now: float,
    ) -> sqlite3.Row:
        """Commit desired membership and lifecycle policy at a safe boundary.

        Both idle topology operations and completed assignment boundaries use
        this path.  Cross-target reusable presence is fenced only after no
        Attempt or Execution remains active, then current membership and the
        target partition's effective retention are committed atomically.
        """

        target = self._canonical_partition(conn, target_partition)
        if conn.execute(
            "SELECT 1 FROM attempts WHERE logical_agent_id=? AND state='ACTIVE' LIMIT 1",
            (agent["id"],),
        ).fetchone():
            raise InvalidTransition("an assigned LogicalAgent must use a drain boundary")
        unsafe_execution = self._unsafe_cross_target_execution(
            conn,
            str(agent["id"]),
            str(target["execution_target"]),
            now,
        )
        if unsafe_execution:
            raise InvalidTransition(
                "a topology cutover cannot abandon an unsafe physical Execution"
            )
        conn.execute(
            "UPDATE incarnations SET state='LOST',ended_at=COALESCE(ended_at,?) "
            "WHERE logical_agent_id=? AND execution_target<>? "
            "AND state IN ('STARTING','WARM','COLD')",
            (now, agent["id"], target["execution_target"]),
        )
        conn.execute(
            "UPDATE logical_agents SET partition_name=?,retention=?,"
            "pending_partition_name=NULL,retirement_requested=0,updated_at=? WHERE id=?",
            (target["name"], target["retention"], now, agent["id"]),
        )
        return target

    def _request_partition_cutover(
        self,
        conn: sqlite3.Connection,
        agent: sqlite3.Row,
        target_partition: str,
        now: float,
    ) -> sqlite3.Row:
        """Commit a safe cutover or retain an unsafe suspended writer as desired state."""

        target = self._canonical_partition(conn, target_partition)
        unsafe_execution = self._unsafe_cross_target_execution(
            conn,
            str(agent["id"]),
            str(target["execution_target"]),
            now,
        )
        if (
            unsafe_execution
            and agent["state"] == AgentState.SUSPENDED.value
            and not agent["current_task_id"]
        ):
            conn.execute(
                "UPDATE logical_agents SET pending_partition_name=?,updated_at=? WHERE id=?",
                (target["name"], now, agent["id"]),
            )
            return target
        return self._commit_partition_cutover(conn, agent, str(target["name"]), now)

    @staticmethod
    def _retire_logical_agent(
        conn: sqlite3.Connection, agent_id: str, now: float
    ) -> None:
        """Commit semantic death and fence every reusable physical presence."""

        conn.execute(
            "UPDATE incarnations SET state='LOST',ended_at=COALESCE(ended_at,?) "
            "WHERE logical_agent_id=? AND state IN ('STARTING','WARM','COLD')",
            (now, agent_id),
        )
        conn.execute(
            "UPDATE logical_agents SET state='RETIRED',current_task_id=NULL,"
            "pending_partition_name=NULL,retirement_requested=0,available_since=NULL,"
            "updated_at=? WHERE id=?",
            (now, agent_id),
        )

    def bootstrap_partitions(self, specs: Sequence[PartitionSpec]) -> bool:
        """Apply human configuration once; SQLite owns topology thereafter."""

        now = utc_now()
        with self.db.transaction() as conn:
            if conn.execute(
                "SELECT 1 FROM scheduler_meta WHERE key='topology_bootstrapped'"
            ).fetchone():
                return False
            if conn.execute("SELECT 1 FROM pool_partitions LIMIT 1").fetchone():
                raise InvalidTransition(
                    "topology exists without a bootstrap marker; explicit repair is required"
                )
            for spec in specs:
                if spec.desired_capacity < 0:
                    raise ValueError("desired_capacity must be non-negative")
                payload = {
                    "name": spec.name,
                    "desired_capacity": spec.desired_capacity,
                    "retention": spec.retention.value,
                    "execution_target": spec.execution_target,
                    "execution_profile": spec.execution_profile,
                    "tags": list(spec.tags),
                }
                cursor = conn.execute(
                    "INSERT INTO pool_topology_revisions(operation,payload_json,created_at) "
                    "VALUES(?,?,?)",
                    ("BOOTSTRAP", json_dumps(payload), now),
                )
                conn.execute(
                    "INSERT INTO pool_partitions(name,desired_capacity,retention,execution_target,"
                    "execution_profile,tags_json,active,topology_revision,created_at,updated_at) "
                    "VALUES(?,?,?,?,?,?,1,?,?,?)",
                    (
                        spec.name,
                        spec.desired_capacity,
                        spec.retention.value,
                        spec.execution_target,
                        spec.execution_profile,
                        json_dumps(list(spec.tags)),
                        int(cursor.lastrowid),
                        now,
                        now,
                    ),
                )
            conn.execute(
                "INSERT INTO scheduler_meta(key,value_json,updated_at) VALUES(?,?,?)",
                ("topology_bootstrapped", json_dumps({"source": "configuration"}), now),
            )
        return True

    def birth_agent(
        self,
        partition_name: str,
        *,
        workstream_id: str | None = None,
        tags: Sequence[str] | None = None,
    ) -> str:
        with self.db.transaction() as conn:
            return self._birth_agent(conn, partition_name, workstream_id=workstream_id, tags=tags)

    def _birth_agent(
        self,
        conn: sqlite3.Connection,
        partition_name: str,
        *,
        workstream_id: str | None = None,
        tags: Sequence[str] | None = None,
    ) -> str:
        partition = conn.execute(
            "SELECT * FROM pool_partitions WHERE name=? AND active=1", (partition_name,)
        ).fetchone()
        if not partition:
            raise NotFound(f"active partition {partition_name!r} not found")
        agent_id = new_id("agent")
        now = utc_now()
        effective_tags = list(tags) if tags is not None else json_loads(partition["tags_json"], [])
        continuity: dict[str, Any] = {}
        if workstream_id:
            workstream = self._required(conn, "workstreams", workstream_id)
            if workstream["project_state_ref"]:
                continuity["CURRENT CHECKPOINT"] = {
                    "project_state_ref": workstream["project_state_ref"]
                }
        conn.execute(
            "INSERT INTO logical_agents(id,partition_name,retention,state,workstream_id,tags_json,"
            "continuity_json,available_since,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
            (
                agent_id,
                partition_name,
                partition["retention"],
                AgentState.READY.value,
                workstream_id,
                json_dumps(effective_tags),
                json_dumps(continuity),
                now,
                now,
                now,
            ),
        )
        return agent_id

    def reconcile_pool(self) -> dict[str, int]:
        born = retired = draining = 0
        now = utc_now()
        with self.db.transaction() as conn:
            idle_drains = conn.execute(
                "SELECT * FROM logical_agents WHERE state='DRAINING' "
                "AND current_task_id IS NULL AND pending_partition_name IS NOT NULL "
                "AND retirement_requested=0 ORDER BY id"
            ).fetchall()
            for agent in idle_drains:
                self._release_agent(conn, agent["id"], now)
            partitions = conn.execute(
                "SELECT * FROM pool_partitions WHERE active=1 ORDER BY name"
            ).fetchall()
            for partition in partitions:
                members = conn.execute(
                    "SELECT * FROM logical_agents "
                    "WHERE COALESCE(pending_partition_name,partition_name)=? "
                    "AND state IN ('INITIALIZING','READY','ASSIGNED','DRAINING','REVIVING') "
                    "AND NOT (state='DRAINING' AND retirement_requested=1) "
                    "ORDER BY created_at,id",
                    (partition["name"],),
                ).fetchall()
                deficit = int(partition["desired_capacity"]) - len(members)
                for _ in range(max(0, deficit)):
                    self._birth_agent(conn, partition["name"])
                    born += 1
                excess = max(0, -deficit)
                if excess:
                    candidates = sorted(
                        members,
                        key=lambda row: (
                            row["state"] == AgentState.ASSIGNED.value,
                            row["state"] != AgentState.READY.value,
                            -row["created_at"],
                            row["id"],
                        ),
                    )
                    for member in candidates[:excess]:
                        if member["state"] in (
                            AgentState.READY.value,
                            AgentState.INITIALIZING.value,
                            AgentState.REVIVING.value,
                        ) and not member["current_task_id"]:
                            self._retire_logical_agent(conn, member["id"], now)
                            retired += 1
                        else:
                            conn.execute(
                                "UPDATE logical_agents SET state='DRAINING',retirement_requested=1,updated_at=? "
                                "WHERE id=?",
                                (now, member["id"]),
                            )
                            draining += 1
        return {"born": born, "retired": retired, "draining": draining}

    def ensure_task_consumers(self) -> int:
        """Birth identities only when queued affinity has no compatible live identity."""
        born = 0
        with self.db.transaction() as conn:
            tasks = conn.execute(
                "SELECT t.* FROM tasks t JOIN batches b ON b.id=t.batch_id "
                "WHERE t.state='QUEUED' AND b.state='ACTIVE' "
                "ORDER BY t.priority DESC,t.created_at,t.id"
            ).fetchall()
            for task in tasks:
                required_tags = set(json_loads(task["affinity_tags_json"], []))
                agents = conn.execute(
                    "SELECT * FROM logical_agents WHERE partition_name=? AND state='READY'",
                    (task["partition_name"],),
                ).fetchall()
                compatible = False
                for agent in agents:
                    if not required_tags.issubset(json_loads(agent["tags_json"], [])):
                        continue
                    if (
                        task["continuity"] == ContinuityPreference.REQUIRED.value
                        and task["workstream_id"] != agent["workstream_id"]
                    ):
                        continue
                    compatible = True
                    break
                if compatible:
                    continue
                partition = conn.execute(
                    "SELECT * FROM pool_partitions WHERE name=? AND active=1",
                    (task["partition_name"],),
                ).fetchone()
                if not partition:
                    continue
                tags = sorted(set(json_loads(partition["tags_json"], [])) | required_tags)
                self._birth_agent(
                    conn,
                    task["partition_name"],
                    workstream_id=task["workstream_id"],
                    tags=tags,
                )
                born += 1
        return born

    def move_agent(self, agent_id: str, target_partition: str) -> None:
        now = utc_now()
        with self.db.transaction() as conn:
            agent = self._required(conn, "logical_agents", agent_id)
            target = self._required(
                conn, "pool_partitions", target_partition, key="name", active=True
            )
            if agent["state"] == AgentState.RETIRED.value:
                raise InvalidTransition("a retired LogicalAgent cannot move")
            if agent["state"] == AgentState.ASSIGNED.value or agent["current_task_id"]:
                conn.execute(
                    "UPDATE logical_agents SET pending_partition_name=?,updated_at=? WHERE id=?",
                    (target_partition, now, agent_id),
                )
            else:
                self._request_partition_cutover(conn, agent, target_partition, now)
                next_state = (
                    AgentState.READY.value
                    if agent["state"] == AgentState.DRAINING.value
                    else agent["state"]
                )
                conn.execute(
                    "UPDATE logical_agents SET state=?,"
                    "available_since=CASE WHEN ?='READY' THEN COALESCE(available_since,?) "
                    "ELSE available_since END,updated_at=? WHERE id=?",
                    (
                        next_state,
                        next_state,
                        now,
                        now,
                        agent_id,
                    ),
                )

    def resize_partition(self, name: str, desired_capacity: int) -> int:
        if desired_capacity < 0:
            raise ValueError("desired_capacity must be non-negative")
        now = utc_now()
        with self.db.transaction() as conn:
            self._required(conn, "pool_partitions", name, key="name", active=True)
            cursor = conn.execute(
                "INSERT INTO pool_topology_revisions(operation,payload_json,created_at) VALUES(?,?,?)",
                (
                    "RESIZE",
                    json_dumps({"name": name, "desired_capacity": desired_capacity}),
                    now,
                ),
            )
            revision = int(cursor.lastrowid)
            conn.execute(
                "UPDATE pool_partitions SET desired_capacity=?,topology_revision=?,updated_at=? "
                "WHERE name=?",
                (desired_capacity, revision, now, name),
            )
        return revision

    def move_capacity(self, source: str, target: str, count: int) -> int:
        """Move desired capacity and deterministically selected identities.

        READY identities move first.  ASSIGNED identities receive a pending
        transition that is applied only at their assignment boundary.  A
        source deficit is allowed: target reconciliation births any identity
        not available to move without inventing runtime population as desired
        topology.
        """

        if source == target:
            raise ValueError("source and target partitions must differ")
        if count <= 0:
            raise ValueError("count must be positive")
        now = utc_now()
        with self.db.transaction() as conn:
            source_partition = self._required(
                conn, "pool_partitions", source, key="name", active=True
            )
            target_partition = self._required(
                conn, "pool_partitions", target, key="name", active=True
            )
            source_capacity = int(source_partition["desired_capacity"])
            target_capacity = int(target_partition["desired_capacity"])
            if count > source_capacity:
                raise InvalidTransition(
                    "cannot move more capacity than the source desired capacity"
                )
            cursor = conn.execute(
                "INSERT INTO pool_topology_revisions(operation,payload_json,created_at) VALUES(?,?,?)",
                (
                    "MOVE_CAPACITY",
                    json_dumps({"source": source, "target": target, "count": count}),
                    now,
                ),
            )
            revision = int(cursor.lastrowid)
            conn.execute(
                "UPDATE pool_partitions SET desired_capacity=?,topology_revision=?,updated_at=? "
                "WHERE name=?",
                (source_capacity - count, revision, now, source),
            )
            conn.execute(
                "UPDATE pool_partitions SET desired_capacity=?,topology_revision=?,updated_at=? "
                "WHERE name=?",
                (target_capacity + count, revision, now, target),
            )

            members = conn.execute(
                "SELECT * FROM logical_agents "
                "WHERE COALESCE(pending_partition_name,partition_name)=? "
                "AND state IN ('INITIALIZING','READY','ASSIGNED','DRAINING','REVIVING','SUSPENDED') "
                "AND retirement_requested=0 "
                "ORDER BY CASE WHEN state='READY' AND current_task_id IS NULL "
                "THEN 0 ELSE 1 END,"
                "COALESCE(available_since,created_at),id",
                (source,),
            ).fetchall()
            for agent in members[:count]:
                if agent["current_task_id"] or agent["state"] == AgentState.ASSIGNED.value:
                    conn.execute(
                        "UPDATE logical_agents SET state='DRAINING',pending_partition_name=?,"
                        "available_since=NULL,updated_at=? WHERE id=?",
                        (target, now, agent["id"]),
                    )
                else:
                    self._request_partition_cutover(conn, agent, target, now)
                    if agent["state"] == AgentState.DRAINING.value:
                        conn.execute(
                            "UPDATE logical_agents SET state='READY',available_since=?,"
                            "updated_at=? WHERE id=?",
                            (now, now, agent["id"]),
                        )
        return revision

    def merge_partitions(self, source: str, target: str) -> int:
        if source == target:
            raise ValueError("source and target partitions must differ")
        now = utc_now()
        with self.db.transaction() as conn:
            source_partition = self._required(
                conn, "pool_partitions", source, key="name", active=True
            )
            target_partition = self._required(
                conn, "pool_partitions", target, key="name", active=True
            )
            cursor = conn.execute(
                "INSERT INTO pool_topology_revisions(operation,payload_json,created_at) VALUES(?,?,?)",
                (
                    "MERGE",
                    json_dumps(
                        {
                            "source": source,
                            "target": target,
                            "source_capacity": int(source_partition["desired_capacity"]),
                            "target_capacity": int(target_partition["desired_capacity"]),
                        }
                    ),
                    now,
                ),
            )
            revision = int(cursor.lastrowid)
            merged_capacity = int(source_partition["desired_capacity"]) + int(
                target_partition["desired_capacity"]
            )
            # Task.partition_name is future scheduling classification.  An
            # already-active Attempt keeps its frozen agent, target, profile,
            # and lease authority; only a later retry observes this migration.
            conn.execute(
                "UPDATE tasks SET partition_name=?,updated_at=? WHERE partition_name=? "
                "AND state NOT IN ('COMPLETED','CANCELLED')",
                (target, now, source),
            )
            # pending_partition_name is desired membership.  Rebase every
            # inbound transition before source becomes inactive so a later
            # assignment boundary cannot commit into the retired source.
            conn.execute(
                "UPDATE logical_agents SET pending_partition_name=?,updated_at=? "
                "WHERE pending_partition_name=? AND state<>'RETIRED'",
                (target, now, source),
            )
            immediate_members = conn.execute(
                "SELECT * FROM logical_agents WHERE partition_name=? "
                "AND (state IN ('READY','INITIALIZING','REVIVING') "
                "OR (state='DRAINING' AND current_task_id IS NULL))",
                (source,),
            ).fetchall()
            for agent in immediate_members:
                desired = agent["pending_partition_name"] or target
                if desired == source:
                    desired = target
                if agent["state"] == AgentState.DRAINING.value:
                    conn.execute(
                        "UPDATE logical_agents SET pending_partition_name=? WHERE id=?",
                        (desired, agent["id"]),
                    )
                    self._release_agent(conn, agent["id"], now)
                else:
                    self._request_partition_cutover(conn, agent, desired, now)
            conn.execute(
                "UPDATE logical_agents SET pending_partition_name=?,updated_at=? "
                "WHERE partition_name=? AND state IN ('ASSIGNED','DRAINING','SUSPENDED') "
                "AND (pending_partition_name IS NULL OR pending_partition_name=?)",
                (target, now, source, source),
            )
            conn.execute(
                "UPDATE pool_partitions SET active=0,desired_capacity=0,merged_into=?,"
                "topology_revision=?,updated_at=? WHERE name=?",
                (target, revision, now, source),
            )
            conn.execute(
                "UPDATE pool_partitions SET desired_capacity=?,topology_revision=?,updated_at=? "
                "WHERE name=?",
                (merged_capacity, revision, now, target),
            )
        return revision

    def retire_partition(self, name: str) -> int:
        now = utc_now()
        with self.db.transaction() as conn:
            self._required(conn, "pool_partitions", name, key="name", active=True)
            nonterminal = conn.execute(
                "SELECT id,state FROM tasks WHERE partition_name=? "
                "AND state NOT IN ('COMPLETED','CANCELLED') ORDER BY created_at,id LIMIT 1",
                (name,),
            ).fetchone()
            if nonterminal:
                raise InvalidTransition(
                    f"cannot retire partition with nonterminal Task {nonterminal['id']} "
                    f"in state {nonterminal['state']}"
                )
            inbound = conn.execute(
                "SELECT id FROM logical_agents WHERE pending_partition_name=? "
                "AND state<>'RETIRED' ORDER BY id LIMIT 1",
                (name,),
            ).fetchone()
            if inbound:
                raise InvalidTransition(
                    f"cannot retire partition with desired LogicalAgent {inbound['id']}"
                )
            departing = conn.execute(
                "SELECT * FROM logical_agents WHERE partition_name=? "
                "AND pending_partition_name IS NOT NULL AND current_task_id IS NULL "
                "AND state<>'RETIRED' ORDER BY id",
                (name,),
            ).fetchall()
            for agent in departing:
                if agent["state"] == AgentState.DRAINING.value:
                    self._release_agent(conn, agent["id"], now)
                else:
                    self._request_partition_cutover(
                        conn, agent, agent["pending_partition_name"], now
                    )
            cursor = conn.execute(
                "INSERT INTO pool_topology_revisions(operation,payload_json,created_at) VALUES(?,?,?)",
                ("RETIRE", json_dumps({"name": name}), now),
            )
            revision = int(cursor.lastrowid)
            idle_retirements = conn.execute(
                "SELECT id FROM logical_agents WHERE partition_name=? "
                "AND pending_partition_name IS NULL "
                "AND state IN ('READY','INITIALIZING','SUSPENDED','REVIVING')",
                (name,),
            ).fetchall()
            for agent in idle_retirements:
                self._retire_logical_agent(conn, agent["id"], now)
            conn.execute(
                "UPDATE logical_agents SET state='DRAINING',retirement_requested=1,updated_at=? "
                "WHERE partition_name=? AND pending_partition_name IS NULL AND state='ASSIGNED'",
                (now, name),
            )
            conn.execute(
                "UPDATE pool_partitions SET active=0,desired_capacity=0,topology_revision=?,updated_at=? "
                "WHERE name=?",
                (revision, now, name),
            )
        return revision

    # ------------------------------------------------------------------
    # Task graph and claims

    def submit_batch(
        self,
        tasks: Sequence[TaskSpec],
        *,
        metadata: Mapping[str, Any] | None = None,
        batch_id: str | None = None,
    ) -> tuple[str, dict[str, str]]:
        if not tasks:
            raise ValueError("a batch requires at least one task")
        names = [task.name for task in tasks]
        if len(names) != len(set(names)):
            raise ValueError("task names must be unique within a batch")
        ids = {task.name: task.task_id or new_id("task") for task in tasks}
        if len(set(ids.values())) != len(ids):
            raise ValueError("task IDs must be unique")
        dependencies: dict[str, tuple[str, ...]] = {}
        for task in tasks:
            resolved: list[str] = []
            for dependency in task.dependencies:
                if dependency in ids:
                    resolved.append(ids[dependency])
                elif dependency in ids.values():
                    resolved.append(dependency)
                else:
                    raise ValueError(f"unknown dependency {dependency!r} for {task.name!r}")
            dependencies[ids[task.name]] = tuple(resolved)
        self._assert_acyclic(dependencies)

        batch_id = batch_id or new_id("batch")
        now = utc_now()
        with self.db.transaction() as conn:
            conn.execute(
                "INSERT INTO batches(id,state,metadata_json,created_at,updated_at) VALUES(?,?,?,?,?)",
                (batch_id, BatchState.ACTIVE.value, json_dumps(metadata or {}), now, now),
            )
            for task in tasks:
                self._required(
                    conn, "pool_partitions", task.partition, key="name", active=True
                )
                task_id = ids[task.name]
                state = TaskState.BLOCKED if dependencies[task_id] else TaskState.QUEUED
                policy = task.retry_policy
                conn.execute(
                    "INSERT INTO tasks(id,batch_id,name,payload_json,acceptance_json,partition_name,"
                    "workstream_id,continuity,affinity_tags_json,workspace_mode,required,priority,state,"
                    "max_attempts,retry_classes_json,base_backoff_seconds,max_backoff_seconds,"
                    "supersedes_task_id,created_at,updated_at) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        task_id,
                        batch_id,
                        task.name,
                        json_dumps(task.payload),
                        json_dumps(task.acceptance),
                        task.partition,
                        task.workstream_id,
                        task.continuity.value,
                        json_dumps(list(task.affinity_tags)),
                        task.workspace_mode.value,
                        1,
                        task.priority,
                        state.value,
                        policy.max_attempts,
                        json_dumps([item.value for item in policy.retry_classes]),
                        policy.base_backoff_seconds,
                        policy.max_backoff_seconds,
                        task.supersedes_task_id,
                        now,
                        now,
                    ),
                )
            for task_id, dependency_ids in dependencies.items():
                for dependency_id in dependency_ids:
                    conn.execute(
                        "INSERT INTO task_dependencies(task_id,depends_on_task_id) VALUES(?,?)",
                        (task_id, dependency_id),
                    )
        return batch_id, ids

    def claim_next(self, logical_agent_id: str, *, now: float | None = None) -> Claim | None:
        now = utc_now() if now is None else now
        with self.db.transaction() as conn:
            agent = self._required(conn, "logical_agents", logical_agent_id)
            if agent["state"] != AgentState.READY.value or agent["current_task_id"]:
                return None
            partition = self._required(
                conn, "pool_partitions", agent["partition_name"], key="name", active=True
            )
            candidates = conn.execute(
                "SELECT t.* FROM tasks t JOIN batches b ON b.id=t.batch_id "
                "WHERE t.state='QUEUED' AND b.state='ACTIVE' AND t.partition_name=? "
                "AND (next_eligible_at IS NULL OR next_eligible_at<=?) "
                "ORDER BY priority DESC,t.created_at,t.id",
                (agent["partition_name"], now),
            ).fetchall()
            agent_tags = json_loads(agent["tags_json"], [])
            eligible: list[tuple[int, sqlite3.Row]] = []
            for task in candidates:
                task_tags = json_loads(task["affinity_tags_json"], [])
                if not tags_match(task_tags, agent_tags):
                    continue
                continuity = ContinuityPreference(task["continuity"])
                same_workstream = bool(
                    task["workstream_id"] and task["workstream_id"] == agent["workstream_id"]
                )
                if continuity is ContinuityPreference.REQUIRED and not same_workstream:
                    continue
                score = 0 if same_workstream else (1 if continuity is ContinuityPreference.NONE else 2)
                eligible.append((score, task))
            if not eligible:
                return None
            eligible.sort(key=lambda item: (item[0], -item[1]["priority"], item[1]["created_at"], item[1]["id"]))
            return self._claim_selected(conn, agent, partition, eligible[0][1], now)

    def claim_next_available(self, *, now: float | None = None) -> Claim | None:
        """Transactionally select the highest-priority Task and its best consumer."""

        now = utc_now() if now is None else now
        with self.db.transaction() as conn:
            tasks = conn.execute(
                "SELECT t.* FROM tasks t JOIN batches b ON b.id=t.batch_id "
                "JOIN pool_partitions p ON p.name=t.partition_name "
                "WHERE t.state='QUEUED' AND b.state='ACTIVE' AND p.active=1 "
                "AND (t.next_eligible_at IS NULL OR t.next_eligible_at<=?) "
                "ORDER BY t.priority DESC,t.created_at,t.id",
                (now,),
            ).fetchall()
            for task in tasks:
                agents = conn.execute(
                    "SELECT * FROM logical_agents WHERE partition_name=? AND state='READY' "
                    "AND current_task_id IS NULL ORDER BY available_since,id",
                    (task["partition_name"],),
                ).fetchall()
                task_tags = json_loads(task["affinity_tags_json"], [])
                eligible: list[tuple[int, float, str, sqlite3.Row]] = []
                for agent in agents:
                    if not tags_match(task_tags, json_loads(agent["tags_json"], [])):
                        continue
                    same_workstream = bool(
                        task["workstream_id"]
                        and task["workstream_id"] == agent["workstream_id"]
                    )
                    continuity = ContinuityPreference(task["continuity"])
                    if continuity is ContinuityPreference.REQUIRED and not same_workstream:
                        continue
                    continuity_rank = (
                        0
                        if continuity is not ContinuityPreference.NONE and same_workstream
                        else 1
                    )
                    eligible.append(
                        (
                            continuity_rank,
                            float(agent["available_since"] or agent["created_at"]),
                            str(agent["id"]),
                            agent,
                        )
                    )
                if eligible:
                    eligible.sort(key=lambda item: item[:3])
                    agent = eligible[0][3]
                    partition = self._required(
                        conn, "pool_partitions", task["partition_name"], key="name", active=True
                    )
                    return self._claim_selected(conn, agent, partition, task, now)
            return None

    def _claim_selected(
        self,
        conn: sqlite3.Connection,
        agent: sqlite3.Row,
        partition: sqlite3.Row,
        task: sqlite3.Row,
        now: float,
    ) -> Claim:
            epoch = int(task["fencing_epoch"]) + 1
            attempt_number = int(
                conn.execute(
                    "SELECT COUNT(*) FROM attempts WHERE task_id=?", (task["id"],)
                ).fetchone()[0]
            ) + 1
            attempt_id = new_id("attempt")
            lease_id = new_id("lease")
            expires_at = now + self.lease_seconds
            conn.execute(
                "INSERT INTO attempts(id,task_id,logical_agent_id,incarnation_id,attempt_number,"
                "lease_epoch,state,created_at) VALUES(?,?,?,?,?,?,?,?)",
                (
                    attempt_id,
                    task["id"],
                    agent["id"],
                    None,
                    attempt_number,
                    epoch,
                    AttemptState.ACTIVE.value,
                    now,
                ),
            )
            conn.execute(
                "INSERT INTO leases(id,task_id,attempt_id,epoch,state,expires_at,heartbeat_at,created_at) "
                "VALUES(?,?,?,?,?,?,?,?)",
                (
                    lease_id,
                    task["id"],
                    attempt_id,
                    epoch,
                    LeaseState.ACTIVE.value,
                    expires_at,
                    now,
                    now,
                ),
            )
            conn.execute(
                "UPDATE tasks SET state='LEASED',current_attempt_id=?,fencing_epoch=?,updated_at=? "
                "WHERE id=? AND state='QUEUED'",
                (attempt_id, epoch, now, task["id"]),
            )
            conn.execute(
                "UPDATE logical_agents SET state='ASSIGNED',current_task_id=?,workstream_id=COALESCE(?,workstream_id),"
                "available_since=NULL,updated_at=? WHERE id=? AND state='READY'",
                (task["id"], task["workstream_id"], now, agent["id"]),
            )
            return Claim(
                task_id=task["id"],
                batch_id=task["batch_id"],
                attempt_id=attempt_id,
                attempt_number=attempt_number,
                lease_id=lease_id,
                lease_epoch=epoch,
                lease_expires_at=expires_at,
                logical_agent_id=agent["id"],
                incarnation_id=None,
                execution_target=partition["execution_target"],
                execution_profile=partition["execution_profile"],
                workspace_mode=WorkspaceMode(task["workspace_mode"]),
                payload=json_loads(task["payload_json"], {}),
                acceptance=json_loads(task["acceptance_json"], {}),
                workstream_id=task["workstream_id"],
            )

    def _ensure_incarnation(
        self, conn: sqlite3.Connection, logical_agent_id: str, target: str, now: float
    ) -> str:
        current = conn.execute(
            "SELECT * FROM incarnations WHERE logical_agent_id=? "
            "AND state IN ('STARTING','WARM','COLD') ORDER BY generation DESC LIMIT 1",
            (logical_agent_id,),
        ).fetchone()
        if current:
            if current["execution_target"] != target:
                raise InvalidTransition(
                    f"logical agent {logical_agent_id} already has active incarnation "
                    f"{current['id']} on target {current['execution_target']}"
                )
            return str(current["id"])
        generation = int(
            conn.execute(
                "SELECT COALESCE(MAX(generation),0)+1 FROM incarnations WHERE logical_agent_id=?",
                (logical_agent_id,),
            ).fetchone()[0]
        )
        incarnation_id = new_id("inc")
        conn.execute(
            "INSERT INTO incarnations(id,logical_agent_id,generation,execution_target,state,started_at) "
            "VALUES(?,?,?,?,?,?)",
            (
                incarnation_id,
                logical_agent_id,
                generation,
                target,
                IncarnationState.STARTING.value,
                now,
            ),
        )
        return incarnation_id

    @staticmethod
    def _record_incarnation_presence(
        conn: sqlite3.Connection,
        incarnation_id: str | None,
        execution_state: ExecutionState,
        *,
        terminal_confirmed: bool,
        quiescent_confirmed: bool,
        incarnation_reusable: bool = False,
        now: float,
    ) -> None:
        """Update physical presence without granting Task/Lease authority."""

        if incarnation_id is None:
            return

        if execution_state is ExecutionState.RUNNING:
            conn.execute(
                "UPDATE incarnations SET state='WARM',ended_at=NULL WHERE id=? "
                "AND state IN ('STARTING','WARM','COLD')",
                (incarnation_id,),
            )
            return
        if execution_state not in {
            ExecutionState.SUCCEEDED,
            ExecutionState.FAILED,
            ExecutionState.LOST,
            ExecutionState.TERMINATED,
        }:
            return
        if incarnation_reusable and terminal_confirmed and quiescent_confirmed:
            conn.execute(
                "UPDATE incarnations SET state='WARM',ended_at=NULL WHERE id=? "
                "AND state IN ('STARTING','WARM','COLD')",
                (incarnation_id,),
            )
            return
        confirmed_end = (
            execution_state is not ExecutionState.LOST
            and terminal_confirmed
            and quiescent_confirmed
        )
        next_state = (
            IncarnationState.TERMINATED.value
            if confirmed_end
            else IncarnationState.LOST.value
        )
        allowed = (
            "('STARTING','WARM','COLD','LOST')"
            if confirmed_end
            else "('STARTING','WARM','COLD')"
        )
        conn.execute(
            f"UPDATE incarnations SET state=?,ended_at=COALESCE(ended_at,?) WHERE id=? "
            f"AND state IN {allowed}",
            (next_state, now, incarnation_id),
        )

    # ------------------------------------------------------------------
    # Execution, fencing, result flow

    def create_execution(
        self,
        claim: Claim,
        *,
        request_id: str | None = None,
        attempt_isolation: bool = False,
    ) -> tuple[str, str]:
        execution_id = new_id("exec")
        request_id = request_id or new_id("request")
        now = utc_now()
        with self.db.transaction() as conn:
            attempt, _lease, _task = self._validate_authority(
                conn, claim.attempt_id, claim.lease_epoch
            )
            incarnation_id = attempt["incarnation_id"] or self._ensure_incarnation(
                conn, claim.logical_agent_id, claim.execution_target, now
            )
            if conn.execute(
                "SELECT 1 FROM executions WHERE incarnation_id=? "
                "AND state IN ('STARTING','RUNNING','UNKNOWN')",
                (incarnation_id,),
            ).fetchone():
                raise InvalidTransition(
                    f"incarnation {incarnation_id} already owns an active Execution"
                )
            conn.execute(
                "UPDATE attempts SET incarnation_id=? WHERE id=? AND incarnation_id IS NULL",
                (incarnation_id, claim.attempt_id),
            )
            conn.execute(
                "INSERT INTO executions(id,request_id,task_id,attempt_id,incarnation_id,execution_target,"
                "execution_profile,attempt_isolation,state,started_at,updated_at) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                (
                    execution_id,
                    request_id,
                    claim.task_id,
                    claim.attempt_id,
                    incarnation_id,
                    claim.execution_target,
                    claim.execution_profile,
                    int(attempt_isolation),
                    ExecutionState.STARTING.value,
                    now,
                    now,
                ),
            )
        return execution_id, request_id

    def confirm_execution_running(
        self,
        attempt_id: str,
        lease_epoch: int,
        execution_id: str,
        *,
        runtime_handle: Mapping[str, Any],
    ) -> None:
        self.confirm_running_and_renew_authority(
            attempt_id,
            lease_epoch,
            execution_id,
            runtime_handle=runtime_handle,
        )

    def confirm_running_and_renew_authority(
        self,
        attempt_id: str,
        lease_epoch: int,
        execution_id: str,
        *,
        runtime_handle: Mapping[str, Any],
        now: float | None = None,
    ) -> float:
        """Confirm RUNNING and establish the first supervision lease atomically."""

        now = utc_now() if now is None else now
        expires_at = now + self.lease_seconds
        with self.db.transaction() as conn:
            attempt, lease, task = self._validate_authority(
                conn, attempt_id, lease_epoch, now=now
            )
            execution = self._required(conn, "executions", execution_id)
            if execution["attempt_id"] != attempt_id:
                raise StaleAuthority("execution does not belong to current attempt")
            if execution["state"] not in {
                ExecutionState.RUNNING.value,
                ExecutionState.STARTING.value,
                ExecutionState.UNKNOWN.value,
            }:
                raise InvalidTransition(
                    f"execution {execution_id} cannot enter RUNNING from {execution['state']}"
                )
            conn.execute(
                "UPDATE executions SET state='RUNNING',runtime_handle_json=?,updated_at=? WHERE id=?",
                (json_dumps(runtime_handle), now, execution_id),
            )
            conn.execute(
                "UPDATE tasks SET state='RUNNING',updated_at=? WHERE id=? AND current_attempt_id=?",
                (now, task["id"], attempt_id),
            )
            conn.execute(
                "UPDATE incarnations SET state='WARM',runtime_handle_json=? WHERE id=?",
                (json_dumps(runtime_handle), attempt["incarnation_id"]),
            )
            cursor = conn.execute(
                "UPDATE leases SET heartbeat_at=?,expires_at=? WHERE id=? AND state='ACTIVE' "
                "AND expires_at>?",
                (now, expires_at, lease["id"], now),
            )
            if cursor.rowcount != 1:
                raise StaleAuthority("lease expired before RUNNING supervision was established")
        return expires_at

    def record_start_ambiguity(
        self,
        attempt_id: str,
        lease_epoch: int,
        execution_id: str,
        *,
        runtime_handle: Mapping[str, Any] | None = None,
        detail: str | None = None,
    ) -> None:
        with self.db.transaction() as conn:
            self._validate_authority(conn, attempt_id, lease_epoch)
            execution = self._required(conn, "executions", execution_id)
            if execution["attempt_id"] != attempt_id:
                raise StaleAuthority("execution does not belong to current attempt")
            if execution["state"] not in {
                ExecutionState.STARTING.value,
                ExecutionState.UNKNOWN.value,
            }:
                raise InvalidTransition(
                    f"execution {execution_id} cannot enter UNKNOWN from {execution['state']}"
                )
            conn.execute(
                "UPDATE executions SET state='UNKNOWN',runtime_handle_json=?,outcome_json=?,updated_at=? WHERE id=?",
                (
                    json_dumps(runtime_handle or {}),
                    json_dumps({"detail": detail}),
                    utc_now(),
                    execution_id,
                ),
            )

    def record_physical_outcome(
        self,
        execution_id: str,
        *,
        state: ExecutionState,
        runtime_handle: Mapping[str, Any] | None = None,
        payload: Mapping[str, Any] | None = None,
        failure_class: FailureClass | None = None,
        failure_code: str | None = None,
        failure_signature: str | None = None,
        terminal_confirmed: bool = False,
        quiescent_confirmed: bool = False,
    ) -> None:
        """Record physical history without granting Task authority."""
        now = utc_now()
        with self.db.transaction() as conn:
            execution = self._required(conn, "executions", execution_id)
            allowed = {
                ExecutionState.STARTING.value: {
                    ExecutionState.STARTING,
                    ExecutionState.RUNNING,
                    ExecutionState.SUCCEEDED,
                    ExecutionState.FAILED,
                    ExecutionState.LOST,
                    ExecutionState.UNKNOWN,
                    ExecutionState.TERMINATED,
                },
                ExecutionState.UNKNOWN.value: {
                    ExecutionState.UNKNOWN,
                    ExecutionState.RUNNING,
                    ExecutionState.SUCCEEDED,
                    ExecutionState.FAILED,
                    ExecutionState.LOST,
                    ExecutionState.TERMINATED,
                },
                ExecutionState.RUNNING.value: {
                    ExecutionState.RUNNING,
                    ExecutionState.SUCCEEDED,
                    ExecutionState.FAILED,
                    ExecutionState.LOST,
                    ExecutionState.TERMINATED,
                },
                ExecutionState.LOST.value: {
                    ExecutionState.SUCCEEDED,
                    ExecutionState.FAILED,
                    ExecutionState.LOST,
                    ExecutionState.TERMINATED,
                },
                ExecutionState.SUCCEEDED.value: {
                    ExecutionState.SUCCEEDED,
                    ExecutionState.TERMINATED,
                },
                ExecutionState.FAILED.value: {
                    ExecutionState.FAILED,
                    ExecutionState.TERMINATED,
                },
                ExecutionState.TERMINATED.value: {ExecutionState.TERMINATED},
            }
            if state not in allowed.get(execution["state"], set()):
                raise InvalidTransition(
                    f"execution {execution_id} cannot transition from {execution['state']} to {state.value}"
                )
            encoded_handle = (
                json_dumps(runtime_handle) if runtime_handle is not None else None
            )
            conn.execute(
                "UPDATE executions SET state=?,runtime_handle_json=COALESCE(?,runtime_handle_json),"
                "outcome_json=COALESCE(?,outcome_json),"
                "failure_class=COALESCE(?,failure_class),failure_code=COALESCE(?,failure_code),"
                "failure_signature=COALESCE(?,failure_signature),"
                "terminal_confirmed=MAX(terminal_confirmed,?),"
                "quiescent_confirmed=MAX(quiescent_confirmed,?),updated_at=?,"
                "ended_at=CASE WHEN ? THEN ? ELSE ended_at END WHERE id=?",
                (
                    state.value,
                    encoded_handle,
                    json_dumps(payload) if payload is not None else None,
                    failure_class.value if failure_class else None,
                    failure_code,
                    failure_signature,
                    int(terminal_confirmed),
                    int(quiescent_confirmed),
                    now,
                    int(terminal_confirmed),
                    now,
                    execution_id,
                ),
            )
            if encoded_handle is not None and execution["incarnation_id"]:
                conn.execute(
                    "UPDATE incarnations SET runtime_handle_json=? WHERE id=?",
                    (encoded_handle, execution["incarnation_id"]),
                )
            self._record_incarnation_presence(
                conn,
                execution["incarnation_id"],
                state,
                terminal_confirmed=terminal_confirmed,
                quiescent_confirmed=quiescent_confirmed,
                now=now,
            )

    def heartbeat(
        self, attempt_id: str, lease_epoch: int, *, now: float | None = None
    ) -> float:
        now = utc_now() if now is None else now
        expires_at = now + self.lease_seconds
        with self.db.transaction() as conn:
            _attempt, lease, _task = self._validate_authority(
                conn, attempt_id, lease_epoch, now=now
            )
            execution = conn.execute(
                "SELECT id FROM executions WHERE attempt_id=? "
                "AND state='RUNNING' LIMIT 1",
                (attempt_id,),
            ).fetchone()
            if execution is None:
                raise StaleAuthority(
                    "attempt has no active Execution eligible for heartbeat"
                )
            conn.execute(
                "UPDATE leases SET heartbeat_at=?,expires_at=? WHERE id=?",
                (now, expires_at, lease["id"]),
            )
        return expires_at

    def renew_active_leases(
        self,
        supervised_attempt_ids: set[str] | None = None,
        *,
        now: float | None = None,
    ) -> int:
        """Renew only authority backed by Executions this process can supervise."""

        now = utc_now() if now is None else now
        supervised = sorted(supervised_attempt_ids or ())
        if not supervised:
            return 0
        expires_at = now + self.lease_seconds
        placeholders = ",".join("?" for _ in supervised)
        with self.db.transaction() as conn:
            cursor = conn.execute(
                "UPDATE leases SET heartbeat_at=?,expires_at=? WHERE state='ACTIVE' "
                "AND expires_at>? "
                "AND attempt_id IN (SELECT a.id FROM attempts a JOIN tasks t ON t.id=a.task_id "
                "JOIN executions e ON e.attempt_id=a.id "
                "WHERE a.state='ACTIVE' AND t.current_attempt_id=a.id "
                "AND e.state='RUNNING' "
                f"AND t.fencing_epoch=a.lease_epoch AND a.id IN ({placeholders}))",
                (now, expires_at, now, *supervised),
            )
            return cursor.rowcount

    def ack_success(
        self,
        attempt_id: str,
        lease_epoch: int,
        *,
        execution_id: str | None,
        payload: Mapping[str, Any],
        summary: str | None = None,
        continuity_capsule: Mapping[str, Any] | None = None,
        project_state_ref: str | None = None,
        workspace_state_ref: str | None = None,
        quiescent_confirmed: bool = True,
        incarnation_reusable: bool = False,
    ) -> str | None:
        now = utc_now()
        result_id = new_id("result")
        with self.db.transaction() as conn:
            attempt, lease, task = self._validate_authority(conn, attempt_id, lease_epoch)
            execution = None
            if execution_id is not None:
                execution = self._required(conn, "executions", execution_id)
                if execution["attempt_id"] != attempt_id:
                    raise StaleAuthority("execution does not belong to current attempt")
            if (
                task["workspace_mode"] == WorkspaceMode.WRITE.value
                and execution is not None
                and not (quiescent_confirmed or bool(execution["attempt_isolation"]))
            ):
                conn.execute(
                    "UPDATE executions SET state='SUCCEEDED',outcome_json=?,terminal_confirmed=1,"
                    "quiescent_confirmed=0,updated_at=?,ended_at=? WHERE id=?",
                    (json_dumps(payload), now, now, execution_id),
                )
                self._record_incarnation_presence(
                    conn,
                    attempt["incarnation_id"],
                    ExecutionState.SUCCEEDED,
                    terminal_confirmed=True,
                    quiescent_confirmed=False,
                    now=now,
                )
                self._record_failure(
                    conn,
                    task["id"],
                    attempt_id,
                    execution_id,
                    FailureClass.WRITER_QUIESCENCE_UNKNOWN,
                    "WRITER_SUCCESS_NOT_QUIESCENT",
                    "WRITER_SUCCESS_NOT_QUIESCENT",
                    "writer reported success but physical quiescence is unknown",
                    now,
                )
                self._suspend_current(
                    conn,
                    attempt,
                    lease,
                    task,
                    FailureClass.WRITER_QUIESCENCE_UNKNOWN,
                    "WRITER_SUCCESS_NOT_QUIESCENT",
                    "writer reported success but physical quiescence is unknown",
                    now,
                )
                return None
            checkpoint_id = None
            if continuity_capsule is not None:
                checkpoint_id = self._promote_checkpoint(
                    conn,
                    attempt,
                    task,
                    lease_epoch,
                    continuity_capsule,
                    project_state_ref,
                    now,
                )
            if execution is not None:
                if execution["state"] not in {
                    ExecutionState.STARTING.value,
                    ExecutionState.RUNNING.value,
                    ExecutionState.UNKNOWN.value,
                }:
                    raise InvalidTransition(
                        f"execution {execution_id} cannot succeed from {execution['state']}"
                    )
                conn.execute(
                    "UPDATE executions SET state='SUCCEEDED',outcome_json=?,terminal_confirmed=1,"
                    "quiescent_confirmed=?,updated_at=?,ended_at=? WHERE id=?",
                    (json_dumps(payload), int(quiescent_confirmed), now, now, execution_id),
                )
            self._record_incarnation_presence(
                conn,
                attempt["incarnation_id"],
                ExecutionState.SUCCEEDED,
                terminal_confirmed=True,
                quiescent_confirmed=quiescent_confirmed,
                incarnation_reusable=incarnation_reusable,
                now=now,
            )
            conn.execute(
                "UPDATE attempts SET state='SUCCEEDED',ended_at=? WHERE id=?",
                (now, attempt_id),
            )
            conn.execute(
                "UPDATE leases SET state='RELEASED',ended_at=? WHERE id=?",
                (now, lease["id"]),
            )
            conn.execute(
                "UPDATE tasks SET state='COMPLETED',current_attempt_id=NULL,next_eligible_at=NULL,"
                "updated_at=? WHERE id=?",
                (now, task["id"]),
            )
            conn.execute(
                "INSERT INTO results(id,task_id,batch_id,attempt_id,logical_agent_id,execution_id,"
                "payload_json,summary,checkpoint_id,workspace_state_ref,state,created_at) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    result_id,
                    task["id"],
                    task["batch_id"],
                    attempt_id,
                    attempt["logical_agent_id"],
                    execution_id,
                    json_dumps(payload),
                    summary,
                    checkpoint_id,
                    workspace_state_ref,
                    ResultState.AVAILABLE.value,
                    now,
                ),
            )
            self._release_agent(conn, attempt["logical_agent_id"], now)
            self._release_dependencies(conn, task["batch_id"], now)
            self._recompute_batch(conn, task["batch_id"], now)
        return result_id

    def nack(
        self,
        attempt_id: str,
        lease_epoch: int,
        *,
        failure_class: FailureClass,
        execution_id: str | None = None,
        failure_code: str | None = None,
        failure_signature: str | None = None,
        detail: str | None = None,
        terminal_confirmed: bool = True,
        quiescent_confirmed: bool = True,
        incarnation_reusable: bool = False,
        now: float | None = None,
    ) -> TaskState:
        now = utc_now() if now is None else now
        with self.db.transaction() as conn:
            attempt, lease, task = self._validate_authority(conn, attempt_id, lease_epoch)
            execution = None
            if execution_id is not None:
                execution = self._required(conn, "executions", execution_id)
                if execution["attempt_id"] != attempt_id:
                    raise StaleAuthority("execution does not belong to current attempt")
            self._record_failure(
                conn,
                task["id"],
                attempt_id,
                execution_id,
                failure_class,
                failure_code,
                failure_signature,
                detail,
                now,
            )
            if execution is not None:
                if execution["state"] not in {
                    ExecutionState.STARTING.value,
                    ExecutionState.RUNNING.value,
                    ExecutionState.UNKNOWN.value,
                }:
                    raise InvalidTransition(
                        f"execution {execution_id} cannot fail from {execution['state']}"
                    )
                conn.execute(
                    "UPDATE executions SET state=?,failure_class=?,failure_code=?,"
                    "failure_signature=?,terminal_confirmed=?,quiescent_confirmed=?,updated_at=?,"
                    "ended_at=CASE WHEN ? THEN ? ELSE ended_at END WHERE id=?",
                    (
                        (
                            ExecutionState.FAILED.value
                            if terminal_confirmed
                            else ExecutionState.UNKNOWN.value
                        ),
                        failure_class.value,
                        failure_code,
                        failure_signature,
                        int(terminal_confirmed),
                        int(quiescent_confirmed),
                        now,
                        int(terminal_confirmed),
                        now,
                        execution_id,
                    ),
                )
            self._record_incarnation_presence(
                conn,
                attempt["incarnation_id"],
                (
                    ExecutionState.LOST
                    if failure_class is FailureClass.EXECUTION_LOST
                    else ExecutionState.FAILED
                ),
                terminal_confirmed=terminal_confirmed,
                quiescent_confirmed=quiescent_confirmed,
                incarnation_reusable=incarnation_reusable,
                now=now,
            )
            retry_classes = set(json_loads(task["retry_classes_json"], []))
            attempts_remaining = int(attempt["attempt_number"]) < int(task["max_attempts"])
            retry_allowed = failure_class.value in retry_classes and attempts_remaining
            writer_safe = (
                task["workspace_mode"] != WorkspaceMode.WRITE.value
                or execution is None
                or quiescent_confirmed
                or bool(execution["attempt_isolation"])
            )
            if retry_allowed and writer_safe:
                delay = min(
                    float(task["max_backoff_seconds"]),
                    float(task["base_backoff_seconds"])
                    * (2 ** max(0, int(attempt["attempt_number"]) - 1)),
                )
                conn.execute(
                    "UPDATE attempts SET state='FAILED',ended_at=? WHERE id=?",
                    (now, attempt_id),
                )
                conn.execute(
                    "UPDATE leases SET state='RELEASED',ended_at=? WHERE id=?",
                    (now, lease["id"]),
                )
                conn.execute(
                    "UPDATE tasks SET state='RETRY_WAIT',current_attempt_id=NULL,next_eligible_at=?,"
                    "updated_at=? WHERE id=?",
                    (now + delay, now, task["id"]),
                )
                self._release_agent(conn, attempt["logical_agent_id"], now)
                return TaskState.RETRY_WAIT
            suspension_class = (
                FailureClass.WRITER_QUIESCENCE_UNKNOWN
                if not writer_safe
                else failure_class
            )
            self._suspend_current(
                conn,
                attempt,
                lease,
                task,
                suspension_class,
                failure_signature,
                detail,
                now,
            )
            return TaskState.SUSPENDED

    def promote_retry_wait(self, *, now: float | None = None) -> int:
        now = utc_now() if now is None else now
        with self.db.transaction() as conn:
            cursor = conn.execute(
                "UPDATE tasks SET state='QUEUED',next_eligible_at=NULL,updated_at=? "
                "WHERE state='RETRY_WAIT' AND next_eligible_at<=?",
                (now, now),
            )
            return cursor.rowcount

    def expire_leases(
        self,
        *,
        now: float | None = None,
        recover_unstarted: bool = False,
    ) -> dict[str, int]:
        now = utc_now() if now is None else now
        retried = suspended = 0
        with self.db.transaction() as conn:
            rows = conn.execute(
                "SELECT l.*,a.logical_agent_id,a.attempt_number,a.incarnation_id,"
                "t.batch_id,t.workspace_mode,t.max_attempts,t.retry_classes_json,"
                "t.base_backoff_seconds,t.max_backoff_seconds,t.workstream_id,"
                "e.id AS execution_id,e.attempt_isolation,e.terminal_confirmed,e.quiescent_confirmed "
                "FROM leases l JOIN attempts a ON a.id=l.attempt_id JOIN tasks t ON t.id=l.task_id "
                "LEFT JOIN executions e ON e.attempt_id=a.id "
                "WHERE l.state='ACTIVE' AND (l.expires_at<=? OR (?=1 AND e.id IS NULL)) "
                "ORDER BY l.expires_at,e.started_at DESC",
                (now, int(recover_unstarted)),
            ).fetchall()
            seen: set[str] = set()
            for row in rows:
                if row["attempt_id"] in seen:
                    continue
                seen.add(row["attempt_id"])
                conn.execute(
                    "UPDATE leases SET state='EXPIRED',ended_at=? WHERE id=? AND state='ACTIVE'",
                    (now, row["id"]),
                )
                conn.execute(
                    "UPDATE attempts SET state='EXPIRED',ended_at=? WHERE id=? AND state='ACTIVE'",
                    (now, row["attempt_id"]),
                )
                conn.execute(
                    "UPDATE incarnations SET state='LOST',ended_at=? WHERE id=? "
                    "AND state IN ('STARTING','WARM','COLD')",
                    (now, row["incarnation_id"]),
                )
                orphaned_claim = row["execution_id"] is None
                failure_code = "CLAIM_ORPHANED" if orphaned_claim else "LEASE_EXPIRED"
                self._record_failure(
                    conn,
                    row["task_id"],
                    row["attempt_id"],
                    row["execution_id"],
                    FailureClass.EXECUTION_LOST,
                    failure_code,
                    None,
                    (
                        "scheduler recovery found an active claim without an Execution"
                        if orphaned_claim
                        else "lease expired before authoritative completion"
                    ),
                    now,
                )
                writer_safe = (
                    row["workspace_mode"] != WorkspaceMode.WRITE.value
                    or row["execution_id"] is None
                    or bool(row["quiescent_confirmed"])
                    or bool(row["attempt_isolation"])
                )
                retry_classes = set(json_loads(row["retry_classes_json"], []))
                retry_allowed = (
                    FailureClass.EXECUTION_LOST.value in retry_classes
                    and int(row["attempt_number"]) < int(row["max_attempts"])
                )
                if retry_allowed and writer_safe:
                    self._release_agent(conn, row["logical_agent_id"], now)
                    delay = min(
                        float(row["max_backoff_seconds"]),
                        float(row["base_backoff_seconds"])
                        * (2 ** max(0, int(row["attempt_number"]) - 1)),
                    )
                    conn.execute(
                        "UPDATE tasks SET state='RETRY_WAIT',current_attempt_id=NULL,"
                        "next_eligible_at=?,updated_at=? WHERE id=?",
                        (now + delay, now, row["task_id"]),
                    )
                    retried += 1
                else:
                    failure_class = (
                        FailureClass.WRITER_QUIESCENCE_UNKNOWN
                        if not writer_safe
                        else FailureClass.EXECUTION_LOST
                    )
                    conn.execute(
                        "UPDATE tasks SET state='SUSPENDED',current_attempt_id=NULL,updated_at=? WHERE id=?",
                        (now, row["task_id"]),
                    )
                    if not writer_safe:
                        conn.execute(
                            "UPDATE logical_agents SET state='SUSPENDED',current_task_id=NULL,"
                            "available_since=NULL,updated_at=? WHERE id=? AND state<>'RETIRED'",
                            (now, row["logical_agent_id"]),
                        )
                    else:
                        self._release_agent(conn, row["logical_agent_id"], now)
                    self._create_escalation(
                        conn,
                        task_id=row["task_id"],
                        batch_id=row["batch_id"],
                        logical_agent_id=row["logical_agent_id"],
                        workstream_id=row["workstream_id"],
                        failure_class=failure_class,
                        signature=failure_code,
                        detail="writer quiescence unknown" if not writer_safe else "retry unavailable",
                        now=now,
                    )
                    self._recompute_batch(conn, row["batch_id"], now)
                    suspended += 1
        return {"retried": retried, "suspended": suspended}

    def promote_checkpoint(
        self,
        attempt_id: str,
        lease_epoch: int,
        capsule: Mapping[str, Any],
        *,
        project_state_ref: str | None = None,
    ) -> str:
        with self.db.transaction() as conn:
            attempt, _lease, task = self._validate_authority(conn, attempt_id, lease_epoch)
            return self._promote_checkpoint(
                conn, attempt, task, lease_epoch, capsule, project_state_ref, utc_now()
            )

    def _promote_checkpoint(
        self,
        conn: sqlite3.Connection,
        attempt: sqlite3.Row,
        task: sqlite3.Row,
        lease_epoch: int,
        capsule: Mapping[str, Any],
        project_state_ref: str | None,
        now: float,
    ) -> str:
        unknown = set(capsule) - CONTINUITY_KEYS
        if unknown:
            raise ValueError(f"unknown continuity keys: {sorted(unknown)}")
        encoded = json_dumps(capsule)
        if len(encoded.encode("utf-8")) > self.continuity_max_bytes:
            raise ValueError("continuity capsule exceeds configured byte limit")
        agent = self._required(conn, "logical_agents", attempt["logical_agent_id"])
        version = int(agent["continuity_version"]) + 1
        checkpoint_id = new_id("checkpoint")
        conn.execute(
            "INSERT INTO checkpoints(id,logical_agent_id,task_id,attempt_id,lease_epoch,"
            "continuity_version,capsule_json,project_state_ref,created_at) VALUES(?,?,?,?,?,?,?,?,?)",
            (
                checkpoint_id,
                attempt["logical_agent_id"],
                task["id"],
                attempt["id"],
                lease_epoch,
                version,
                encoded,
                project_state_ref,
                now,
            ),
        )
        conn.execute(
            "UPDATE logical_agents SET continuity_json=?,continuity_version=?,current_checkpoint_id=?,"
            "updated_at=? WHERE id=?",
            (encoded, version, checkpoint_id, now, attempt["logical_agent_id"]),
        )
        return checkpoint_id

    def ack_result(
        self,
        result_id: str,
        *,
        consumer_ref: str,
        disposition: str = "consumed",
    ) -> None:
        now = utc_now()
        with self.db.transaction() as conn:
            result = self._required(conn, "results", result_id)
            if result["state"] == ResultState.ACKED.value:
                return
            conn.execute(
                "UPDATE results SET state='ACKED',consumed_at=?,consumer_ref=?,disposition=? WHERE id=?",
                (now, consumer_ref, disposition, result_id),
            )

    # ------------------------------------------------------------------
    # Recovery primitives and lifecycle

    def revive_agent(self, logical_agent_id: str, execution_target: str) -> str:
        now = utc_now()
        with self.db.transaction() as conn:
            agent = self._required(conn, "logical_agents", logical_agent_id)
            if agent["state"] == AgentState.RETIRED.value:
                raise InvalidTransition("a semantically retired LogicalAgent cannot revive")
            if conn.execute(
                "SELECT 1 FROM escalations WHERE logical_agent_id=? AND state='OPEN' "
                "AND failure_class=? LIMIT 1",
                (logical_agent_id, FailureClass.WRITER_QUIESCENCE_UNKNOWN.value),
            ).fetchone():
                raise InvalidTransition(
                    "writer physical-safety obligation must be resolved before revival"
                )
            active_attempt = conn.execute(
                "SELECT a.id FROM attempts a WHERE a.logical_agent_id=? AND a.state='ACTIVE' LIMIT 1",
                (logical_agent_id,),
            ).fetchone()
            if agent["current_task_id"] or active_attempt:
                raise InvalidTransition(
                    "an assigned LogicalAgent must close or fence its active Attempt before revival"
                )
            partition = self._required(
                conn, "pool_partitions", agent["partition_name"], key="name", active=True
            )
            if partition["execution_target"] != execution_target:
                raise InvalidTransition("revival target must match the active partition")
            conn.execute(
                "UPDATE logical_agents SET state='READY',available_since=?,updated_at=? WHERE id=?",
                (now, now, logical_agent_id),
            )
            return logical_agent_id

    def mark_incarnation_lost(self, incarnation_id: str) -> None:
        now = utc_now()
        with self.db.transaction() as conn:
            incarnation = self._required(conn, "incarnations", incarnation_id)
            if incarnation["state"] in {
                IncarnationState.LOST.value,
                IncarnationState.TERMINATED.value,
            }:
                return
            conn.execute(
                "UPDATE incarnations SET state='LOST',ended_at=? WHERE id=?",
                (now, incarnation_id),
            )
            agent = self._required(conn, "logical_agents", incarnation["logical_agent_id"])
            if agent["state"] == AgentState.READY.value:
                conn.execute(
                    "UPDATE logical_agents SET state='REVIVING',available_since=NULL,updated_at=? WHERE id=?",
                    (now, agent["id"]),
                )
            elif (
                agent["state"] == AgentState.DRAINING.value
                and not agent["current_task_id"]
                and agent["pending_partition_name"]
            ):
                self._release_agent(conn, agent["id"], now)

    def revive_eligible_agents(self) -> int:
        """Restore logical availability without inventing physical presence."""
        candidates = self.db.fetch_all(
            "SELECT a.id,p.execution_target FROM logical_agents a "
            "JOIN pool_partitions p ON p.name=a.partition_name "
            "WHERE a.state='REVIVING' AND p.active=1 ORDER BY a.id"
        )
        revived = 0
        for candidate in candidates:
            self.revive_agent(candidate["id"], candidate["execution_target"])
            revived += 1
        return revived

    def cancel_task(
        self,
        task_id: str,
        *,
        quiescence_confirmed: bool = False,
    ) -> None:
        now = utc_now()
        with self.db.transaction() as conn:
            task = self._required(conn, "tasks", task_id)
            if task["state"] in (TaskState.COMPLETED.value, TaskState.CANCELLED.value):
                return
            if task["current_attempt_id"]:
                attempt = self._required(conn, "attempts", task["current_attempt_id"])
                execution = conn.execute(
                    "SELECT * FROM executions WHERE attempt_id=? ORDER BY started_at DESC LIMIT 1",
                    (attempt["id"],),
                ).fetchone()
                writer_unknown = (
                    task["workspace_mode"] == WorkspaceMode.WRITE.value
                    and execution is not None
                    and not (
                        quiescence_confirmed
                        or bool(execution["attempt_isolation"])
                        or bool(execution["quiescent_confirmed"])
                    )
                )
                physical_quiescent = execution is None or quiescence_confirmed or bool(
                    execution["quiescent_confirmed"]
                )
                self._record_incarnation_presence(
                    conn,
                    attempt["incarnation_id"],
                    (
                        ExecutionState.TERMINATED
                        if physical_quiescent
                        else ExecutionState.LOST
                    ),
                    terminal_confirmed=physical_quiescent,
                    quiescent_confirmed=physical_quiescent,
                    now=now,
                )
                conn.execute(
                    "UPDATE attempts SET state='CANCELLED',ended_at=? WHERE id=? AND state='ACTIVE'",
                    (now, attempt["id"]),
                )
                conn.execute(
                    "UPDATE leases SET state='REVOKED',ended_at=? WHERE attempt_id=? AND state='ACTIVE'",
                    (now, attempt["id"]),
                )
                if writer_unknown:
                    conn.execute(
                        "UPDATE logical_agents SET state='SUSPENDED',current_task_id=NULL,"
                        "available_since=NULL,updated_at=? WHERE id=? AND state<>'RETIRED'",
                        (now, attempt["logical_agent_id"]),
                    )
                    self._create_escalation(
                        conn,
                        task_id=task["id"],
                        batch_id=task["batch_id"],
                        logical_agent_id=attempt["logical_agent_id"],
                        workstream_id=task["workstream_id"],
                        failure_class=FailureClass.WRITER_QUIESCENCE_UNKNOWN,
                        signature="CANCELLED_WRITER_NOT_QUIESCENT",
                        detail="scheduler authority cancelled but physical writer quiescence is unknown",
                        now=now,
                    )
                else:
                    self._release_agent(conn, attempt["logical_agent_id"], now)
            conn.execute(
                "UPDATE tasks SET state='CANCELLED',current_attempt_id=NULL,updated_at=? WHERE id=?",
                (now, task_id),
            )
            self._recompute_batch(conn, task["batch_id"], now)

    def cancel_batch(self, batch_id: str) -> None:
        now = utc_now()
        with self.db.transaction() as conn:
            batch = self._required(conn, "batches", batch_id)
            if batch["state"] == BatchState.CANCELLED.value:
                return
            active = conn.execute(
                "SELECT a.id,a.logical_agent_id,a.incarnation_id,t.id AS task_id,"
                "t.workspace_mode,t.workstream_id "
                "FROM attempts a JOIN tasks t ON t.id=a.task_id "
                "WHERE t.batch_id=? AND a.state='ACTIVE'",
                (batch_id,),
            ).fetchall()
            conn.execute(
                "UPDATE escalations SET state='CANCELLED',resolved_at=? WHERE batch_id=? "
                "AND state='OPEN' AND failure_class<>?",
                (now, batch_id, FailureClass.WRITER_QUIESCENCE_UNKNOWN.value),
            )
            for attempt in active:
                execution = conn.execute(
                    "SELECT * FROM executions WHERE attempt_id=? ORDER BY started_at DESC LIMIT 1",
                    (attempt["id"],),
                ).fetchone()
                writer_unknown = (
                    attempt["workspace_mode"] == WorkspaceMode.WRITE.value
                    and execution is not None
                    and not (
                        bool(execution["quiescent_confirmed"])
                        or bool(execution["attempt_isolation"])
                    )
                )
                physical_quiescent = execution is None or bool(
                    execution["quiescent_confirmed"]
                )
                self._record_incarnation_presence(
                    conn,
                    attempt["incarnation_id"],
                    (
                        ExecutionState.TERMINATED
                        if physical_quiescent
                        else ExecutionState.LOST
                    ),
                    terminal_confirmed=physical_quiescent,
                    quiescent_confirmed=physical_quiescent,
                    now=now,
                )
                conn.execute(
                    "UPDATE attempts SET state='CANCELLED',ended_at=? WHERE id=?",
                    (now, attempt["id"]),
                )
                conn.execute(
                    "UPDATE leases SET state='REVOKED',ended_at=? WHERE attempt_id=? AND state='ACTIVE'",
                    (now, attempt["id"]),
                )
                if writer_unknown:
                    conn.execute(
                        "UPDATE logical_agents SET state='SUSPENDED',current_task_id=NULL,"
                        "available_since=NULL,updated_at=? WHERE id=? AND state<>'RETIRED'",
                        (now, attempt["logical_agent_id"]),
                    )
                    self._create_escalation(
                        conn,
                        task_id=attempt["task_id"],
                        batch_id=batch_id,
                        logical_agent_id=attempt["logical_agent_id"],
                        workstream_id=attempt["workstream_id"],
                        failure_class=FailureClass.WRITER_QUIESCENCE_UNKNOWN,
                        signature="CANCELLED_WRITER_NOT_QUIESCENT",
                        detail="batch cancelled but physical writer quiescence is unknown",
                        now=now,
                    )
                else:
                    self._release_agent(conn, attempt["logical_agent_id"], now)
            conn.execute(
                "UPDATE tasks SET state='CANCELLED',current_attempt_id=NULL,updated_at=? "
                "WHERE batch_id=? AND state NOT IN ('COMPLETED','CANCELLED')",
                (now, batch_id),
            )
            conn.execute(
                "UPDATE batches SET state='CANCELLED',updated_at=? WHERE id=?",
                (now, batch_id),
            )

    def resolve_escalation(
        self,
        escalation_id: str,
        *,
        operation: str,
        quiescence_confirmed: bool = False,
    ) -> None:
        if operation not in {"retry", "cancel_task", "release_cancelled_writer"}:
            raise ValueError(
                "supported operations are retry, cancel_task, and release_cancelled_writer"
            )
        now = utc_now()
        with self.db.transaction() as conn:
            escalation = self._required(conn, "escalations", escalation_id)
            if escalation["state"] != EscalationState.OPEN.value:
                raise InvalidTransition("escalation is not open")
            task = self._required(conn, "tasks", escalation["task_id"])
            latest = conn.execute(
                "SELECT a.incarnation_id,e.id AS execution_id,e.attempt_isolation "
                "FROM attempts a "
                "LEFT JOIN executions e ON e.attempt_id=a.id WHERE a.task_id=? "
                "ORDER BY a.attempt_number DESC LIMIT 1",
                (task["id"],),
            ).fetchone()
            frozen_isolation = bool(latest and latest["attempt_isolation"])
            if operation == "release_cancelled_writer":
                if (
                    task["state"] != TaskState.CANCELLED.value
                    or escalation["failure_class"]
                    != FailureClass.WRITER_QUIESCENCE_UNKNOWN.value
                    or not (quiescence_confirmed or frozen_isolation)
                ):
                    raise InvalidTransition(
                        "cancelled writer release requires confirmed quiescence or attempt isolation"
                    )
                self._finalize_escalated_writer_presence(
                    conn,
                    latest,
                    quiescence_confirmed=quiescence_confirmed,
                    frozen_isolation=frozen_isolation,
                    now=now,
                )
                if escalation["logical_agent_id"]:
                    self._prepare_agent_revival_after_safety(
                        conn, escalation["logical_agent_id"], now
                    )
            elif operation == "retry":
                if task["state"] != TaskState.SUSPENDED.value:
                    raise InvalidTransition("only a suspended task can be retried")
                if (
                    task["workspace_mode"] == WorkspaceMode.WRITE.value
                    and escalation["failure_class"]
                    == FailureClass.WRITER_QUIESCENCE_UNKNOWN.value
                    and not (quiescence_confirmed or frozen_isolation)
                ):
                    raise InvalidTransition(
                        "writer retry requires confirmed quiescence or attempt isolation"
                    )
                conn.execute(
                    "UPDATE tasks SET state='QUEUED',next_eligible_at=NULL,updated_at=? WHERE id=?",
                    (now, task["id"]),
                )
                conn.execute(
                    "UPDATE batches SET state='ACTIVE',updated_at=? WHERE id=? AND state='SUSPENDED'",
                    (now, task["batch_id"]),
                )
                if (
                    escalation["failure_class"]
                    == FailureClass.WRITER_QUIESCENCE_UNKNOWN.value
                    and escalation["logical_agent_id"]
                ):
                    self._finalize_escalated_writer_presence(
                        conn,
                        latest,
                        quiescence_confirmed=quiescence_confirmed,
                        frozen_isolation=frozen_isolation,
                        now=now,
                    )
                    self._prepare_agent_revival_after_safety(
                        conn, escalation["logical_agent_id"], now
                    )
            else:
                conn.execute(
                    "UPDATE tasks SET state='CANCELLED',updated_at=? WHERE id=?",
                    (now, task["id"]),
                )
                if (
                    escalation["failure_class"]
                    == FailureClass.WRITER_QUIESCENCE_UNKNOWN.value
                ):
                    self._recompute_batch(conn, task["batch_id"], now)
                    return
            conn.execute(
                "UPDATE escalations SET state='RESOLVED',resolved_at=? WHERE id=?",
                (now, escalation_id),
            )
            self._recompute_batch(conn, task["batch_id"], now)

    # ------------------------------------------------------------------
    # Diagnostics

    def get(self, table: str, entity_id: str) -> dict[str, Any]:
        allowed = {
            "tasks",
            "batches",
            "attempts",
            "leases",
            "results",
            "failures",
            "escalations",
            "logical_agents",
            "incarnations",
            "executions",
            "checkpoints",
            "notification_outbox",
        }
        if table not in allowed:
            raise ValueError(f"unsupported table {table!r}")
        row = self.db.fetch_one(f"SELECT * FROM {table} WHERE id=?", (entity_id,))
        if not row:
            raise NotFound(f"{table} {entity_id!r} not found")
        return dict(row)

    def list(self, table: str, *, state: str | None = None) -> list[dict[str, Any]]:
        allowed = {
            "tasks",
            "batches",
            "attempts",
            "leases",
            "results",
            "failures",
            "escalations",
            "logical_agents",
            "incarnations",
            "executions",
            "notification_outbox",
            "pool_partitions",
        }
        if table not in allowed:
            raise ValueError(f"unsupported table {table!r}")
        key = {
            "pool_partitions": "name",
            "incarnations": "started_at",
            "executions": "started_at",
        }.get(table, "created_at")
        if state is None:
            rows = self.db.fetch_all(f"SELECT * FROM {table} ORDER BY {key}")
        else:
            rows = self.db.fetch_all(f"SELECT * FROM {table} WHERE state=? ORDER BY {key}", (state,))
        return [dict(row) for row in rows]

    def status(self) -> dict[str, Any]:
        lifecycle = self.db.fetch_one("SELECT value_json FROM scheduler_meta WHERE key='lifecycle'")
        counts: dict[str, Any] = {}
        for table in ("tasks", "batches", "logical_agents", "executions", "results", "escalations"):
            rows = self.db.fetch_all(f"SELECT state,COUNT(*) AS count FROM {table} GROUP BY state")
            counts[table] = {row["state"]: row["count"] for row in rows}
        counts["active_leases"] = self.db.fetch_one(
            "SELECT COUNT(*) AS count FROM leases WHERE state='ACTIVE'"
        )["count"]
        counts["integrity"] = self.db.integrity_check()
        return {
            "lifecycle": json_loads(lifecycle["value_json"], {}) if lifecycle else {},
            "counts": counts,
        }

    # ------------------------------------------------------------------
    # Internal transactional helpers

    @staticmethod
    def _assert_acyclic(graph: Mapping[str, Sequence[str]]) -> None:
        visiting: set[str] = set()
        visited: set[str] = set()

        def visit(node: str) -> None:
            if node in visiting:
                raise ValueError("task dependency graph contains a cycle")
            if node in visited:
                return
            visiting.add(node)
            for dependency in graph.get(node, ()):
                visit(dependency)
            visiting.remove(node)
            visited.add(node)

        for node in graph:
            visit(node)

    @staticmethod
    def _required(
        conn: sqlite3.Connection,
        table: str,
        value: str,
        *,
        key: str = "id",
        active: bool = False,
    ) -> sqlite3.Row:
        sql = f"SELECT * FROM {table} WHERE {key}=?"
        params: tuple[Any, ...] = (value,)
        if active:
            sql += " AND active=1"
        row = conn.execute(sql, params).fetchone()
        if not row:
            raise NotFound(f"{table} {value!r} not found")
        return row

    def _validate_authority(
        self,
        conn: sqlite3.Connection,
        attempt_id: str,
        lease_epoch: int,
        *,
        now: float | None = None,
    ) -> tuple[sqlite3.Row, sqlite3.Row, sqlite3.Row]:
        authority_time = utc_now() if now is None else now
        attempt = self._required(conn, "attempts", attempt_id)
        task = self._required(conn, "tasks", attempt["task_id"])
        lease = conn.execute(
            "SELECT * FROM leases WHERE attempt_id=?", (attempt_id,)
        ).fetchone()
        if not lease:
            raise StaleAuthority("attempt has no lease")
        if (
            attempt["state"] != AttemptState.ACTIVE.value
            or lease["state"] != LeaseState.ACTIVE.value
            or int(attempt["lease_epoch"]) != int(lease_epoch)
            or int(lease["epoch"]) != int(lease_epoch)
            or task["current_attempt_id"] != attempt_id
            or int(task["fencing_epoch"]) != int(lease_epoch)
            or float(lease["expires_at"]) <= authority_time
        ):
            raise StaleAuthority("attempt no longer owns authoritative task state")
        return attempt, lease, task

    def _release_agent(
        self,
        conn: sqlite3.Connection,
        agent_id: str,
        now: float,
    ) -> None:
        agent = self._required(conn, "logical_agents", agent_id)
        pending = agent["pending_partition_name"]
        if agent["retirement_requested"]:
            self._retire_logical_agent(conn, agent_id, now)
            return

        target_name = pending or agent["partition_name"]
        target = self._commit_partition_cutover(conn, agent, target_name, now)
        retire = target["retention"] == Retention.EPHEMERAL.value
        if retire:
            self._retire_logical_agent(conn, agent_id, now)
            return
        state = AgentState.READY.value
        available_since = now
        conn.execute(
            "UPDATE logical_agents SET state=?,current_task_id=NULL,"
            "pending_partition_name=NULL,available_since=?,updated_at=? WHERE id=?",
            (state, available_since, now, agent_id),
        )

    def _prepare_agent_revival_after_safety(
        self, conn: sqlite3.Connection, agent_id: str, now: float
    ) -> None:
        """Finalize desired topology before a safety-suspended agent can revive."""

        agent = self._required(conn, "logical_agents", agent_id)
        if agent["state"] == AgentState.RETIRED.value:
            return
        self._release_agent(conn, agent_id, now)
        released = self._required(conn, "logical_agents", agent_id)
        if released["state"] == AgentState.READY.value:
            conn.execute(
                "UPDATE logical_agents SET state='REVIVING',available_since=NULL,updated_at=? "
                "WHERE id=? AND state='READY'",
                (now, agent_id),
            )

    @staticmethod
    def _finalize_escalated_writer_presence(
        conn: sqlite3.Connection,
        latest: sqlite3.Row | None,
        *,
        quiescence_confirmed: bool,
        frozen_isolation: bool,
        now: float,
    ) -> None:
        if not latest:
            return
        if latest["execution_id"]:
            if quiescence_confirmed:
                conn.execute(
                    "UPDATE executions SET state=CASE "
                    "WHEN state IN ('STARTING','RUNNING','UNKNOWN') THEN 'TERMINATED' "
                    "ELSE state END,terminal_confirmed=1,quiescent_confirmed=1,"
                    "updated_at=?,ended_at=COALESCE(ended_at,?) WHERE id=?",
                    (now, now, latest["execution_id"]),
                )
            elif frozen_isolation:
                conn.execute(
                    "UPDATE executions SET state=CASE "
                    "WHEN state IN ('STARTING','RUNNING','UNKNOWN') THEN 'LOST' "
                    "ELSE state END,updated_at=?,ended_at=COALESCE(ended_at,?) WHERE id=?",
                    (now, now, latest["execution_id"]),
                )
        if latest["incarnation_id"]:
            next_state = "TERMINATED" if quiescence_confirmed else "LOST"
            conn.execute(
                "UPDATE incarnations SET state=?,ended_at=COALESCE(ended_at,?) WHERE id=? "
                "AND state IN ('STARTING','WARM','COLD','LOST')",
                (next_state, now, latest["incarnation_id"]),
            )

    @staticmethod
    def _release_dependencies(conn: sqlite3.Connection, batch_id: str, now: float) -> None:
        conn.execute(
            "UPDATE tasks SET state='QUEUED',updated_at=? WHERE batch_id=? AND state='BLOCKED' "
            "AND NOT EXISTS (SELECT 1 FROM task_dependencies d JOIN tasks p ON p.id=d.depends_on_task_id "
            "WHERE d.task_id=tasks.id AND p.state<>'COMPLETED')",
            (now, batch_id),
        )

    def _recompute_batch(self, conn: sqlite3.Connection, batch_id: str, now: float) -> None:
        batch = self._required(conn, "batches", batch_id)
        if batch["state"] == BatchState.CANCELLED.value:
            return
        summary = conn.execute(
            "SELECT "
            "SUM(CASE WHEN state='SUSPENDED' THEN 1 ELSE 0 END) suspended,"
            "SUM(CASE WHEN state='CANCELLED' THEN 1 ELSE 0 END) cancelled,"
            "SUM(CASE WHEN state<>'COMPLETED' THEN 1 ELSE 0 END) incomplete "
            "FROM tasks WHERE batch_id=?",
            (batch_id,),
        ).fetchone()
        if (summary["suspended"] or 0) > 0 or (summary["cancelled"] or 0) > 0:
            next_state = BatchState.SUSPENDED
        elif (summary["incomplete"] or 0) == 0:
            next_state = BatchState.COMPLETED
        else:
            next_state = BatchState.ACTIVE
        if batch["state"] != next_state.value:
            conn.execute(
                "UPDATE batches SET state=?,updated_at=? WHERE id=?",
                (next_state.value, now, batch_id),
            )
            if next_state is BatchState.COMPLETED:
                self._enqueue_event(
                    conn,
                    "BATCH_RESULTS_READY",
                    "batch",
                    batch_id,
                    {"batch_id": batch_id},
                    now,
                )

    def _record_failure(
        self,
        conn: sqlite3.Connection,
        task_id: str,
        attempt_id: str | None,
        execution_id: str | None,
        failure_class: FailureClass,
        failure_code: str | None,
        failure_signature: str | None,
        detail: str | None,
        now: float,
    ) -> str:
        failure_id = new_id("failure")
        conn.execute(
            "INSERT INTO failures(id,task_id,attempt_id,execution_id,failure_class,failure_code,"
            "normalized_signature,detail,created_at) VALUES(?,?,?,?,?,?,?,?,?)",
            (
                failure_id,
                task_id,
                attempt_id,
                execution_id,
                failure_class.value,
                failure_code,
                failure_signature,
                detail,
                now,
            ),
        )
        return failure_id

    def _suspend_current(
        self,
        conn: sqlite3.Connection,
        attempt: sqlite3.Row,
        lease: sqlite3.Row,
        task: sqlite3.Row,
        failure_class: FailureClass,
        signature: str | None,
        detail: str | None,
        now: float,
    ) -> None:
        conn.execute(
            "UPDATE attempts SET state='FAILED',ended_at=? WHERE id=? AND state='ACTIVE'",
            (now, attempt["id"]),
        )
        conn.execute(
            "UPDATE leases SET state='REVOKED',ended_at=? WHERE id=? AND state='ACTIVE'",
            (now, lease["id"]),
        )
        conn.execute(
            "UPDATE tasks SET state='SUSPENDED',current_attempt_id=NULL,updated_at=? WHERE id=?",
            (now, task["id"]),
        )
        if failure_class is FailureClass.WRITER_QUIESCENCE_UNKNOWN:
            conn.execute(
                "UPDATE logical_agents SET state='SUSPENDED',current_task_id=NULL,"
                "available_since=NULL,updated_at=? WHERE id=? AND state<>'RETIRED'",
                (now, attempt["logical_agent_id"]),
            )
        else:
            self._release_agent(conn, attempt["logical_agent_id"], now)
        self._create_escalation(
            conn,
            task_id=task["id"],
            batch_id=task["batch_id"],
            logical_agent_id=attempt["logical_agent_id"],
            workstream_id=task["workstream_id"],
            failure_class=failure_class,
            signature=signature,
            detail=detail,
            now=now,
        )
        self._recompute_batch(conn, task["batch_id"], now)

    def _create_escalation(
        self,
        conn: sqlite3.Connection,
        *,
        task_id: str,
        batch_id: str,
        logical_agent_id: str | None,
        workstream_id: str | None,
        failure_class: FailureClass,
        signature: str | None,
        detail: str | None,
        now: float,
    ) -> str:
        existing = conn.execute(
            "SELECT id FROM escalations WHERE task_id=? AND state='OPEN'", (task_id,)
        ).fetchone()
        if existing:
            return str(existing["id"])
        escalation_id = new_id("escalation")
        attempts = [
            dict(row)
            for row in conn.execute(
                "SELECT id,attempt_number,lease_epoch,state,created_at,ended_at FROM attempts "
                "WHERE task_id=? ORDER BY attempt_number",
                (task_id,),
            ).fetchall()
        ]
        snapshot = {
            "task_id": task_id,
            "logical_agent_id": logical_agent_id,
            "workstream_id": workstream_id,
            "failure_class": failure_class.value,
            "normalized_failure_signature": signature,
            "attempt_history": attempts,
            "detail": detail,
        }
        conn.execute(
            "INSERT INTO escalations(id,task_id,batch_id,logical_agent_id,workstream_id,"
            "failure_class,normalized_signature,snapshot_json,decision_required,state,created_at) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
            (
                escalation_id,
                task_id,
                batch_id,
                logical_agent_id,
                workstream_id,
                failure_class.value,
                signature,
                json_dumps(snapshot),
                "Root must choose an explicit recovery primitive",
                EscalationState.OPEN.value,
                now,
            ),
        )
        self._enqueue_event(
            conn,
            "DECISION_REQUIRED",
            "escalation",
            escalation_id,
            {"escalation_id": escalation_id, "task_id": task_id, "batch_id": batch_id},
            now,
        )
        return escalation_id

    @staticmethod
    def _enqueue_event(
        conn: sqlite3.Connection,
        event_type: str,
        aggregate_type: str,
        aggregate_id: str,
        payload: Mapping[str, Any],
        now: float,
    ) -> str:
        event_id = new_id("event")
        conn.execute(
            "INSERT INTO notification_outbox(id,event_type,aggregate_type,aggregate_id,payload_json,"
            "state,next_delivery_at,created_at) VALUES(?,?,?,?,?,?,?,?)",
            (
                event_id,
                event_type,
                aggregate_type,
                aggregate_id,
                json_dumps(payload),
                OutboxState.PENDING.value,
                now,
                now,
            ),
        )
        return event_id
