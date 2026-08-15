from __future__ import annotations

import sys
import tempfile
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from local_agent_scheduler.core import Scheduler
from local_agent_scheduler.enums import AgentState, Retention
from local_agent_scheduler.models import PartitionSpec, TaskSpec
from local_agent_scheduler.storage import Database, SCHEMA_VERSION, json_dumps


class IncarnationMigrationCase(unittest.TestCase):
    @staticmethod
    def _downgrade_schema_markers(database: Database) -> None:
        with database.transaction() as conn:
            conn.execute("DROP INDEX IF EXISTS one_active_execution_per_incarnation")
            conn.execute("DROP INDEX one_active_incarnation_per_agent")
            conn.execute("DELETE FROM schema_migrations WHERE version>=2")

    def test_v1_reused_incarnation_is_split_without_losing_history(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            database = Database(Path(temporary) / "scheduler.db")
            scheduler = Scheduler(database)
            scheduler.initialize()
            scheduler.upsert_partition(
                PartitionSpec("general", 1, Retention.RESIDENT, "local", "default")
            )
            scheduler.reconcile_pool()
            agent_id = scheduler.list("logical_agents", state=AgentState.READY.value)[0]["id"]

            _batch, _ids = scheduler.submit_batch([TaskSpec("first", {})])
            first = scheduler.claim_next(agent_id)
            first_execution, _ = scheduler.create_execution(first)
            scheduler.confirm_execution_running(
                first.attempt_id,
                first.lease_epoch,
                first_execution,
                runtime_handle={"physical": "one"},
            )
            scheduler.ack_success(
                first.attempt_id,
                first.lease_epoch,
                execution_id=first_execution,
                payload={"ok": 1},
            )

            _batch, _ids = scheduler.submit_batch([TaskSpec("second", {})])
            second = scheduler.claim_next(agent_id)
            second_execution, _ = scheduler.create_execution(second)
            first_incarnation_id = scheduler.get("executions", first_execution)[
                "incarnation_id"
            ]
            second_incarnation_id = scheduler.get("executions", second_execution)[
                "incarnation_id"
            ]

            # Reconstruct the V0.1 defect: both Executions and Attempts point
            # at one WARM Incarnation, and the database reports schema v1.
            with database.transaction() as conn:
                conn.execute("DROP INDEX IF EXISTS one_active_execution_per_incarnation")
                conn.execute("DROP INDEX one_active_incarnation_per_agent")
                conn.execute(
                    "UPDATE executions SET incarnation_id=? WHERE id=?",
                    (first_incarnation_id, second_execution),
                )
                conn.execute(
                    "UPDATE attempts SET incarnation_id=? WHERE id=?",
                    (first_incarnation_id, second.attempt_id),
                )
                conn.execute("DELETE FROM incarnations WHERE id=?", (second_incarnation_id,))
                conn.execute(
                    "UPDATE incarnations SET state='WARM',ended_at=NULL WHERE id=?",
                    (first_incarnation_id,),
                )
                conn.execute("DELETE FROM schema_migrations WHERE version>=2")

            database.initialize()

            self.assertEqual(SCHEMA_VERSION, 7)
            self.assertEqual(
                database.fetch_one(
                    "SELECT MAX(version) AS version FROM schema_migrations"
                )["version"],
                7,
            )
            migrated_first = database.fetch_one(
                "SELECT incarnation_id FROM executions WHERE id=?", (first_execution,)
            )
            migrated_second = database.fetch_one(
                "SELECT incarnation_id FROM executions WHERE id=?", (second_execution,)
            )
            self.assertNotEqual(
                migrated_first["incarnation_id"], migrated_second["incarnation_id"]
            )
            self.assertEqual(
                database.fetch_one(
                    "SELECT state FROM incarnations WHERE id=?",
                    (migrated_first["incarnation_id"],),
                )["state"],
                "TERMINATED",
            )
            self.assertEqual(
                database.fetch_one(
                    "SELECT state FROM incarnations WHERE id=?",
                    (migrated_second["incarnation_id"],),
                )["state"],
                "LOST",
            )
            self.assertEqual(database.integrity_check(), "ok")

    def test_v1_single_unknown_execution_is_conservatively_lost(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            database = Database(Path(temporary) / "scheduler.db")
            scheduler = Scheduler(database)
            scheduler.initialize()
            scheduler.upsert_partition(
                PartitionSpec("general", 1, Retention.RESIDENT, "local", "default")
            )
            scheduler.reconcile_pool()
            agent_id = scheduler.list("logical_agents", state=AgentState.READY.value)[0]["id"]
            _batch, _ids = scheduler.submit_batch([TaskSpec("unknown", {})])
            claim = scheduler.claim_next(agent_id)
            execution_id, request_id = scheduler.create_execution(claim)
            incarnation_id = scheduler.get("executions", execution_id)["incarnation_id"]
            scheduler.record_start_ambiguity(
                claim.attempt_id,
                claim.lease_epoch,
                execution_id,
                runtime_handle={"request_id": request_id},
            )
            self._downgrade_schema_markers(database)

            database.initialize()

            self.assertEqual(
                database.fetch_one(
                    "SELECT state FROM incarnations WHERE id=?", (incarnation_id,)
                )["state"],
                "LOST",
            )
            self.assertEqual(
                database.fetch_one(
                    "SELECT state FROM attempts WHERE id=?", (claim.attempt_id,)
                )["state"],
                "ACTIVE",
            )

    def test_concurrent_first_initialize_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "concurrent.db"
            with ThreadPoolExecutor(max_workers=4) as pool:
                errors = list(pool.map(lambda _index: Database(path).initialize(), range(4)))
            self.assertEqual(errors, [None, None, None, None])
            database = Database(path)
            self.assertEqual(
                database.fetch_one(
                    "SELECT MAX(version) AS version FROM schema_migrations"
                )["version"],
                SCHEMA_VERSION,
            )
            self.assertEqual(database.integrity_check(), "ok")

    @staticmethod
    def _downgrade_v4_constraints(database: Database) -> None:
        with database.transaction() as conn:
            conn.execute("DROP INDEX IF EXISTS one_active_attempt_per_agent")
            conn.execute("DROP INDEX IF EXISTS one_execution_per_attempt")
            conn.execute("DROP TRIGGER IF EXISTS tasks_required_insert_only")
            conn.execute("DROP TRIGGER IF EXISTS tasks_required_update_only")
            conn.execute("DELETE FROM schema_migrations WHERE version>=4")

    def _legacy_scheduler(self, path: Path):
        database = Database(path)
        scheduler = Scheduler(database)
        scheduler.initialize()
        scheduler.upsert_partition(
            PartitionSpec("general", 1, Retention.RESIDENT, "local", "default")
        )
        scheduler.reconcile_pool()
        return database, scheduler

    def test_v4_rejects_multiple_active_attempts_for_one_agent_without_normalizing(self):
        with tempfile.TemporaryDirectory() as temporary:
            database, scheduler = self._legacy_scheduler(Path(temporary) / "agent.db")
            _batch, ids = scheduler.submit_batch([TaskSpec("one", {}), TaskSpec("two", {})])
            agent_id = scheduler.list("logical_agents", state="READY")[0]["id"]
            self._downgrade_v4_constraints(database)
            now = time.time()
            with database.transaction() as conn:
                for index, task_id in enumerate(ids.values(), start=1):
                    conn.execute(
                        "INSERT INTO attempts(id,task_id,logical_agent_id,incarnation_id,"
                        "attempt_number,lease_epoch,state,created_at) VALUES(?,?,?,?,?,?,?,?)",
                        (f"legacy-attempt-{index}", task_id, agent_id, None, 1, 1, "ACTIVE", now),
                    )
            with self.assertRaisesRegex(RuntimeError, "multiple ACTIVE Attempts"):
                database.initialize()
            self.assertEqual(
                database.fetch_one("SELECT MAX(version) version FROM schema_migrations")["version"],
                3,
            )
            self.assertEqual(
                len(database.fetch_all("SELECT id FROM attempts WHERE state='ACTIVE'")), 2
            )

    def test_v4_rejects_multiple_executions_for_one_attempt_without_normalizing(self):
        with tempfile.TemporaryDirectory() as temporary:
            database, scheduler = self._legacy_scheduler(Path(temporary) / "execution.db")
            _batch, _ids = scheduler.submit_batch([TaskSpec("one", {})])
            agent_id = scheduler.list("logical_agents", state="READY")[0]["id"]
            claim = scheduler.claim_next(agent_id)
            execution_id, _request = scheduler.create_execution(claim)
            incarnation_id = scheduler.get("executions", execution_id)["incarnation_id"]
            self._downgrade_v4_constraints(database)
            now = time.time()
            with database.transaction() as conn:
                conn.execute(
                    "INSERT INTO executions(id,request_id,task_id,attempt_id,incarnation_id,"
                    "execution_target,execution_profile,state,runtime_handle_json,outcome_json,"
                    "terminal_confirmed,quiescent_confirmed,started_at,updated_at,ended_at) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        "legacy-execution",
                        "legacy-request",
                        claim.task_id,
                        claim.attempt_id,
                        incarnation_id,
                        "local",
                        "default",
                        "SUCCEEDED",
                        "{}",
                        "{}",
                        1,
                        1,
                        now,
                        now,
                        now,
                    ),
                )
            with self.assertRaisesRegex(RuntimeError, "multiple Executions"):
                database.initialize()
            self.assertEqual(
                database.fetch_one("SELECT MAX(version) version FROM schema_migrations")["version"],
                3,
            )
            self.assertEqual(
                len(database.fetch_all("SELECT id FROM executions WHERE attempt_id=?", (claim.attempt_id,))),
                2,
            )

    def test_v4_rejects_legacy_optional_task_without_normalizing(self):
        with tempfile.TemporaryDirectory() as temporary:
            database, scheduler = self._legacy_scheduler(Path(temporary) / "optional.db")
            _batch, ids = scheduler.submit_batch([TaskSpec("one", {})])
            self._downgrade_v4_constraints(database)
            conn = database.connect()
            try:
                conn.execute("PRAGMA ignore_check_constraints=ON")
                conn.execute("BEGIN IMMEDIATE")
                conn.execute("UPDATE tasks SET required=0 WHERE id=?", (ids["one"],))
                conn.execute("COMMIT")
            finally:
                conn.close()
            with self.assertRaisesRegex(RuntimeError, "unsupported optional Task"):
                database.initialize()
            self.assertEqual(
                database.fetch_one("SELECT MAX(version) version FROM schema_migrations")["version"],
                3,
            )
            self.assertEqual(
                database.fetch_one("SELECT required FROM tasks WHERE id=?", (ids["one"],))[
                    "required"
                ],
                0,
            )

    def test_v5_defaults_legacy_execution_isolation_to_false(self):
        with tempfile.TemporaryDirectory() as temporary:
            database, scheduler = self._legacy_scheduler(Path(temporary) / "isolation.db")
            _batch, _ids = scheduler.submit_batch([TaskSpec("legacy", {})])
            claim = scheduler.claim_next_available()
            execution_id, _request = scheduler.create_execution(
                claim, attempt_isolation=True
            )
            with database.transaction() as conn:
                conn.execute("DELETE FROM schema_migrations WHERE version>=5")
                conn.execute("ALTER TABLE executions DROP COLUMN attempt_isolation")

            database.initialize()

            execution = database.fetch_one(
                "SELECT attempt_isolation FROM executions WHERE id=?", (execution_id,)
            )
            self.assertEqual(execution["attempt_isolation"], 0)
            self.assertEqual(
                database.fetch_one(
                    "SELECT MAX(version) version FROM schema_migrations"
                )["version"],
                7,
            )

    def test_v6_preserves_unresolved_execution_and_fences_retired_presence(self):
        with tempfile.TemporaryDirectory() as temporary:
            database, scheduler = self._legacy_scheduler(
                Path(temporary) / "physical-closure.db"
            )
            _batch, ids = scheduler.submit_batch([TaskSpec("legacy-unresolved", {})])
            claim = scheduler.claim_next_available()
            execution_id, _request = scheduler.create_execution(claim)
            scheduler.confirm_execution_running(
                claim.attempt_id,
                claim.lease_epoch,
                execution_id,
                runtime_handle={"thread_id": "legacy", "turn_id": "unresolved"},
            )
            execution = scheduler.get("executions", execution_id)
            with database.transaction() as conn:
                conn.execute(
                    "UPDATE executions SET state='FAILED',terminal_confirmed=0,"
                    "quiescent_confirmed=0,ended_at=NULL WHERE id=?",
                    (execution_id,),
                )
                conn.execute(
                    "UPDATE attempts SET state='FAILED' WHERE id=?", (claim.attempt_id,)
                )
                conn.execute(
                    "UPDATE leases SET state='REVOKED' WHERE id=?", (claim.lease_id,)
                )
                conn.execute(
                    "UPDATE tasks SET state='SUSPENDED',current_attempt_id=NULL WHERE id=?",
                    (ids["legacy-unresolved"],),
                )
                conn.execute(
                    "UPDATE logical_agents SET state='RETIRED',current_task_id=NULL WHERE id=?",
                    (claim.logical_agent_id,),
                )
                conn.execute(
                    "UPDATE incarnations SET state='WARM',ended_at=NULL WHERE id=?",
                    (execution["incarnation_id"],),
                )
                conn.execute("DELETE FROM schema_migrations WHERE version>=6")

            database.initialize()

            self.assertEqual(
                scheduler.get("executions", execution_id)["state"], "UNKNOWN"
            )
            self.assertEqual(
                scheduler.get("incarnations", execution["incarnation_id"])["state"],
                "LOST",
            )
            self.assertEqual(
                database.fetch_one(
                    "SELECT MAX(version) version FROM schema_migrations"
                )["version"],
                7,
            )

    def test_v7_rejects_legacy_merge_without_guessing_capacity_or_moving_task(self):
        with tempfile.TemporaryDirectory() as temporary:
            database, scheduler = self._legacy_scheduler(
                Path(temporary) / "legacy-merge.db"
            )
            scheduler.upsert_partition(
                PartitionSpec("source", 2, Retention.RESIDENT, "local", "default")
            )
            scheduler.upsert_partition(
                PartitionSpec("target", 3, Retention.RESIDENT, "local", "default")
            )
            scheduler.reconcile_pool()
            _batch, ids = scheduler.submit_batch(
                [TaskSpec("stranded", {}, partition="source")]
            )
            self._downgrade_v4_constraints(database)
            with database.transaction() as conn:
                revision = conn.execute(
                    "INSERT INTO pool_topology_revisions(operation,payload_json,created_at) "
                    "VALUES(?,?,?)",
                    (
                        "MERGE",
                        json_dumps({"source": "source", "target": "target"}),
                        time.time(),
                    ),
                ).lastrowid
                conn.execute(
                    "UPDATE logical_agents SET partition_name='target' "
                    "WHERE partition_name='source'"
                )
                conn.execute(
                    "UPDATE pool_partitions SET active=0,desired_capacity=0,"
                    "merged_into='target',topology_revision=? WHERE name='source'",
                    (revision,),
                )

            with self.assertRaisesRegex(
                RuntimeError,
                "LEGACY_TOPOLOGY_REPAIR_REQUIRED: MERGE revision .*declared-capacity",
            ):
                database.initialize()

            self.assertEqual(
                database.fetch_one(
                    "SELECT MAX(version) version FROM schema_migrations"
                )["version"],
                3,
            )
            self.assertEqual(
                dict(
                    database.fetch_one(
                        "SELECT active,desired_capacity,merged_into FROM pool_partitions "
                        "WHERE name='source'"
                    )
                ),
                {"active": 0, "desired_capacity": 0, "merged_into": "target"},
            )
            self.assertEqual(
                database.fetch_one(
                    "SELECT desired_capacity FROM pool_partitions WHERE name='target'"
                )["desired_capacity"],
                3,
            )
            self.assertEqual(
                database.fetch_one(
                    "SELECT partition_name FROM tasks WHERE id=?", (ids["stranded"],)
                )["partition_name"],
                "source",
            )

    def test_v7_rejects_legacy_retire_with_nonterminal_tasks_atomically(self):
        with tempfile.TemporaryDirectory() as temporary:
            database, scheduler = self._legacy_scheduler(
                Path(temporary) / "legacy-retire.db"
            )
            scheduler.upsert_partition(
                PartitionSpec("retired", 1, Retention.RESIDENT, "local", "default")
            )
            _batch, ids = scheduler.submit_batch(
                [
                    TaskSpec("queued", {}, partition="retired"),
                    TaskSpec(
                        "blocked",
                        {},
                        partition="retired",
                        dependencies=("queued",),
                    ),
                    TaskSpec("retry", {}, partition="retired"),
                ]
            )
            self._downgrade_v4_constraints(database)
            with database.transaction() as conn:
                conn.execute(
                    "UPDATE tasks SET state='RETRY_WAIT',next_eligible_at=? WHERE id=?",
                    (time.time() + 60, ids["retry"]),
                )
                revision = conn.execute(
                    "INSERT INTO pool_topology_revisions(operation,payload_json,created_at) "
                    "VALUES(?,?,?)",
                    ("RETIRE", json_dumps({"name": "retired"}), time.time()),
                ).lastrowid
                conn.execute(
                    "UPDATE pool_partitions SET active=0,desired_capacity=0,"
                    "topology_revision=? WHERE name='retired'",
                    (revision,),
                )

            before = {
                row["id"]: (row["state"], row["partition_name"])
                for row in database.fetch_all(
                    "SELECT id,state,partition_name FROM tasks WHERE id IN (?,?,?)",
                    (ids["queued"], ids["blocked"], ids["retry"]),
                )
            }
            with self.assertRaisesRegex(
                RuntimeError,
                "LEGACY_TOPOLOGY_REPAIR_REQUIRED: nonterminal Task .*inactive partition retired",
            ):
                database.initialize()

            self.assertEqual(
                database.fetch_one(
                    "SELECT MAX(version) version FROM schema_migrations"
                )["version"],
                3,
            )
            self.assertEqual(
                {
                    row["id"]: (row["state"], row["partition_name"])
                    for row in database.fetch_all(
                        "SELECT id,state,partition_name FROM tasks WHERE id IN (?,?,?)",
                        (ids["queued"], ids["blocked"], ids["retry"]),
                    )
                },
                before,
            )
            self.assertEqual(
                dict(
                    database.fetch_one(
                        "SELECT active,desired_capacity FROM pool_partitions "
                        "WHERE name='retired'"
                    )
                ),
                {"active": 0, "desired_capacity": 0},
            )

    def test_v5_retires_legacy_unassigned_retirement_drain(self):
        with tempfile.TemporaryDirectory() as temporary:
            database, scheduler = self._legacy_scheduler(Path(temporary) / "drain.db")
            agent_id = scheduler.list("logical_agents", state="READY")[0]["id"]
            with database.transaction() as conn:
                conn.execute("DELETE FROM schema_migrations WHERE version>=5")
                conn.execute(
                    "UPDATE logical_agents SET state='DRAINING',current_task_id=NULL,"
                    "pending_partition_name=NULL,retirement_requested=1 WHERE id=?",
                    (agent_id,),
                )

            database.initialize()

            agent = database.fetch_one(
                "SELECT state,retirement_requested FROM logical_agents WHERE id=?",
                (agent_id,),
            )
            self.assertEqual(agent["state"], "RETIRED")
            self.assertEqual(agent["retirement_requested"], 0)


if __name__ == "__main__":
    unittest.main()
