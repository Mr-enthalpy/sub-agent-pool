from __future__ import annotations

import sys
import subprocess
import tempfile
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from local_agent_scheduler.adapters.codex import AppServerSession, CodexAppServerAdapter
from local_agent_scheduler.cli import _partition_spec_for_upsert, _task_specs, build_parser
from local_agent_scheduler.config import ExecutionTargetConfig, load_config
from local_agent_scheduler.core import Scheduler
from local_agent_scheduler.enums import (
    AgentState,
    ContinuityPreference,
    ExecutionState,
    FailureClass,
    Retention,
    WorkspaceMode,
)
from local_agent_scheduler.errors import (
    AdapterError,
    ConfigurationError,
    InvalidTransition,
    NotFound,
    StaleAuthority,
)
from local_agent_scheduler.models import (
    ExecutionOutcome,
    ExecutionRequest,
    PartitionSpec,
    RetryPolicy,
    TaskSpec,
)
from local_agent_scheduler.runtime import Dispatcher, SchedulerDaemon
from local_agent_scheduler.root_bridge import OutboxDispatcher
from local_agent_scheduler.storage import Database


class CapturingSession:
    instances: list["CapturingSession"] = []

    def __init__(self, *_args, **_kwargs):
        self.session_id = f"session-{len(self.instances)}"
        self.calls: list[tuple[str, dict]] = []
        self.process = type("Process", (), {"poll": lambda self: None})()
        self.instances.append(self)

    def request(self, method, params, **_kwargs):
        self.calls.append((method, params))
        if method == "thread/start":
            return {"thread": {"id": "thread"}}
        return {"turn": {"id": "turn"}}

    def notifications(self):
        return []

    def close(self, **_kwargs):
        return True


class AdvancingDeadlineSession:
    clock = [0.0]
    constructor_timeouts: list[float] = []
    request_timeouts: list[tuple[str, float]] = []

    def __init__(self, _command, _process_cwd, timeout):
        self.session_id = "deadline-session"
        self.process = type("Process", (), {"poll": lambda self: None})()
        self.__class__.constructor_timeouts.append(timeout)
        self.__class__.clock[0] += 0.2

    def request(self, method, _params, *, timeout):
        self.__class__.request_timeouts.append((method, timeout))
        self.__class__.clock[0] += 0.2
        if method == "thread/start":
            return {"thread": {"id": "thread"}}
        return {"turn": {"id": "turn"}}

    def notifications(self):
        return []

    def close(self, **_kwargs):
        return True


class BoundaryDeadlineSession:
    clock = [0.0]
    expire_after = "session"
    closed: list[str] = []

    def __init__(self, _command, _process_cwd, _timeout):
        self.session_id = f"boundary-{self.expire_after}"
        self.process = type("Process", (), {"poll": lambda self: None})()
        self.__class__.clock[0] += 1.0 if self.expire_after == "session" else 0.1

    def request(self, method, _params, *, timeout):
        if method == "thread/start":
            result = {"thread": {"id": "boundary-thread"}}
            self.__class__.clock[0] += (
                0.9 if self.expire_after == "thread" else 0.1
            )
            return result
        result = {"turn": {"id": "boundary-turn"}}
        self.__class__.clock[0] += 0.8 if self.expire_after == "turn" else 0.1
        return result

    def notifications(self):
        return []

    def close(self, **_kwargs):
        self.__class__.closed.append(self.session_id)
        return True


class BlockingBridge:
    def __init__(self):
        self.started = threading.Event()
        self.release = threading.Event()

    def deliver(self, *_args):
        self.started.set()
        if not self.release.wait(2):
            raise TimeoutError("test bridge was not released")
        return type("Delivery", (), {"delivered": True, "detail": None})()


class AdvancingFailureBridge:
    def __init__(self, clock):
        self.clock = clock

    def deliver(self, *_args):
        self.clock[0] += 100
        return type("Delivery", (), {"delivered": False, "detail": "slow failure"})()


class EmptyReadableStream:
    def __iter__(self):
        return iter(())


class BlockingStdin:
    def __init__(self, released: threading.Event, block_at: str):
        self.released = released
        self.block_at = block_at

    def write(self, value):
        if self.block_at == "write":
            self.released.wait()
        return len(value)

    def flush(self):
        if self.block_at == "flush":
            self.released.wait()


class BlockingStdioProcess:
    def __init__(self, block_at: str):
        self.released = threading.Event()
        self.stdin = BlockingStdin(self.released, block_at)
        self.stdout = EmptyReadableStream()
        self.stderr = EmptyReadableStream()
        self.returncode = None
        self.kill_calls = 0

    def poll(self):
        return self.returncode

    def terminate(self):
        self.returncode = -15
        self.released.set()

    def kill(self):
        self.kill_calls += 1
        self.returncode = -9
        self.released.set()

    def wait(self, timeout=None):
        if self.returncode is None:
            raise subprocess.TimeoutExpired("codex app-server", timeout)
        return self.returncode


class SuccessfulTerminalAdapter:
    def reconcile_start(self, _request_id, runtime_handle):
        return type(
            "Start",
            (),
            {
                "state": ExecutionState.TERMINATED,
                "runtime_handle": runtime_handle,
                "ambiguous": False,
                "failure_class": None,
                "failure_code": None,
                "detail": None,
            },
        )()

    def observe_execution(self, _handle):
        return type(
            "Observation",
            (),
            {
                "state": ExecutionState.SUCCEEDED,
                "terminal_confirmed": True,
                "quiescent_confirmed": True,
                "detail": None,
            },
        )()

    def collect_outcome(self, _handle):
        return ExecutionOutcome(ExecutionState.SUCCEEDED, payload={"ok": True})


class ForbiddenStartAdapter:
    def __init__(self):
        self.start_calls = 0

    def start_execution(self, _request):
        self.start_calls += 1
        raise AssertionError("unavailable execution profile reached adapter start")


class ClosureCase(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.db = Database(Path(self.temp.name) / "scheduler.db")
        self.scheduler = Scheduler(self.db, lease_seconds=2)
        self.scheduler.initialize()
        self.scheduler.upsert_partition(
            PartitionSpec("general", 1, Retention.RESIDENT, "codex", "default")
        )
        self.scheduler.reconcile_pool()

    def tearDown(self):
        self.temp.cleanup()

    def agent(self):
        return self.scheduler.list("logical_agents", state="READY")[0]["id"]

    def test_batch_suspension_is_transactional_claim_barrier(self):
        policy = RetryPolicy(max_attempts=1, retry_classes=())
        batch, _ids = self.scheduler.submit_batch(
            [TaskSpec("bad", {}, retry_policy=policy), TaskSpec("sibling", {})]
        )
        claim = self.scheduler.claim_next(self.agent())
        self.scheduler.nack(
            claim.attempt_id, claim.lease_epoch, failure_class=FailureClass.UNKNOWN
        )
        self.assertEqual(self.scheduler.get("batches", batch)["state"], "SUSPENDED")
        self.assertIsNone(self.scheduler.claim_next(self.agent()))
        self.assertIsNone(self.scheduler.claim_next_available())

    def test_writer_success_without_quiescence_suspends_without_result(self):
        _batch, ids = self.scheduler.submit_batch(
            [TaskSpec("writer", {}, workspace_mode=WorkspaceMode.WRITE)]
        )
        claim = self.scheduler.claim_next(self.agent())
        execution, _ = self.scheduler.create_execution(claim)
        result = self.scheduler.ack_success(
            claim.attempt_id,
            claim.lease_epoch,
            execution_id=execution,
            payload={"reported": "success"},
            quiescent_confirmed=False,
        )
        self.assertIsNone(result)
        self.assertEqual(self.scheduler.get("tasks", ids["writer"])["state"], "SUSPENDED")
        self.assertEqual(self.scheduler.list("results"), [])
        self.assertEqual(
            self.scheduler.get("logical_agents", claim.logical_agent_id)["state"],
            "SUSPENDED",
        )

    def test_writer_success_uses_frozen_execution_isolation(self):
        _batch, ids = self.scheduler.submit_batch(
            [TaskSpec("isolated-writer", {}, workspace_mode=WorkspaceMode.WRITE)]
        )
        claim = self.scheduler.claim_next(self.agent())
        execution, _ = self.scheduler.create_execution(
            claim, attempt_isolation=True
        )

        result = self.scheduler.ack_success(
            claim.attempt_id,
            claim.lease_epoch,
            execution_id=execution,
            payload={"reported": "success"},
            quiescent_confirmed=False,
        )

        self.assertIsNotNone(result)
        self.assertEqual(
            self.scheduler.get("tasks", ids["isolated-writer"])["state"],
            "COMPLETED",
        )
        self.assertEqual(
            self.scheduler.get("executions", execution)["attempt_isolation"], 1
        )

    def test_writer_nack_uses_frozen_execution_isolation(self):
        policy = RetryPolicy(
            max_attempts=2,
            retry_classes=(FailureClass.EXECUTION_LOST,),
            base_backoff_seconds=0,
            max_backoff_seconds=0,
        )
        _batch, ids = self.scheduler.submit_batch(
            [
                TaskSpec(
                    "isolated-writer-failure",
                    {},
                    workspace_mode=WorkspaceMode.WRITE,
                    retry_policy=policy,
                )
            ]
        )
        claim = self.scheduler.claim_next(self.agent())
        execution, _ = self.scheduler.create_execution(
            claim, attempt_isolation=True
        )

        state = self.scheduler.nack(
            claim.attempt_id,
            claim.lease_epoch,
            execution_id=execution,
            failure_class=FailureClass.EXECUTION_LOST,
            quiescent_confirmed=False,
            terminal_confirmed=False,
        )

        self.assertEqual(state.value, "RETRY_WAIT")
        self.assertEqual(
            self.scheduler.get("tasks", ids["isolated-writer-failure"])["state"],
            "RETRY_WAIT",
        )

    def test_writer_claim_before_execution_is_safe_to_retry(self):
        policy = RetryPolicy(
            max_attempts=2,
            retry_classes=(FailureClass.EXECUTION_LOST,),
            base_backoff_seconds=0,
            max_backoff_seconds=0,
        )
        _batch, ids = self.scheduler.submit_batch(
            [TaskSpec("writer", {}, workspace_mode=WorkspaceMode.WRITE, retry_policy=policy)]
        )
        claim = self.scheduler.claim_next(self.agent(), now=10)
        self.assertIsNone(claim.incarnation_id)
        expired = self.scheduler.expire_leases(now=13)
        self.assertEqual(expired["retried"], 1)
        self.scheduler.promote_retry_wait(now=13)
        self.assertEqual(self.scheduler.get("tasks", ids["writer"])["state"], "QUEUED")

    def test_core_lease_renewal_requires_an_active_execution(self):
        _batch, _ids = self.scheduler.submit_batch([TaskSpec("supervised-only", {})])
        claim = self.scheduler.claim_next(self.agent())

        self.assertEqual(
            self.scheduler.renew_active_leases({claim.attempt_id}),
            0,
        )
        with self.assertRaises(StaleAuthority):
            self.scheduler.heartbeat(claim.attempt_id, claim.lease_epoch)

        execution_id, _request = self.scheduler.create_execution(claim)
        self.assertEqual(self.scheduler.renew_active_leases({claim.attempt_id}), 0)
        with self.assertRaises(StaleAuthority):
            self.scheduler.heartbeat(claim.attempt_id, claim.lease_epoch)
        with self.db.transaction() as conn:
            conn.execute(
                "UPDATE executions SET state='UNKNOWN' WHERE id=?", (execution_id,)
            )
        self.assertEqual(self.scheduler.renew_active_leases({claim.attempt_id}), 0)
        with self.assertRaises(StaleAuthority):
            self.scheduler.heartbeat(claim.attempt_id, claim.lease_epoch)
        self.scheduler.confirm_execution_running(
            claim.attempt_id,
            claim.lease_epoch,
            execution_id,
            runtime_handle={"live": True},
        )
        self.assertEqual(
            self.scheduler.renew_active_leases(
                {claim.attempt_id}, now=claim.lease_expires_at + 1
            ),
            0,
        )
        self.assertEqual(
            self.scheduler.renew_active_leases({claim.attempt_id}),
            1,
        )
        self.scheduler.heartbeat(claim.attempt_id, claim.lease_epoch)

        self.scheduler.record_physical_outcome(
            execution_id,
            state=ExecutionState.TERMINATED,
            terminal_confirmed=True,
            quiescent_confirmed=True,
        )
        self.assertEqual(
            self.scheduler.renew_active_leases({claim.attempt_id}),
            0,
        )
        with self.assertRaises(StaleAuthority):
            self.scheduler.heartbeat(claim.attempt_id, claim.lease_epoch)

    def test_cancelled_writer_keeps_safety_escalation_open(self):
        _batch, ids = self.scheduler.submit_batch(
            [TaskSpec("writer", {}, workspace_mode=WorkspaceMode.WRITE)]
        )
        claim = self.scheduler.claim_next(self.agent())
        self.scheduler.create_execution(claim)
        self.scheduler.cancel_task(ids["writer"])
        escalation = self.scheduler.list("escalations", state="OPEN")[0]
        self.scheduler.resolve_escalation(escalation["id"], operation="cancel_task")
        self.assertEqual(
            self.scheduler.get("escalations", escalation["id"])["state"], "OPEN"
        )
        with self.assertRaises(InvalidTransition):
            self.scheduler.revive_agent(claim.logical_agent_id, "codex")

    def test_suspended_member_does_not_block_capacity_birth(self):
        old = self.agent()
        with self.db.transaction() as conn:
            conn.execute("UPDATE logical_agents SET state='SUSPENDED' WHERE id=?", (old,))
        result = self.scheduler.reconcile_pool()
        self.assertEqual(result["born"], 1)
        self.assertNotEqual(self.agent(), old)

    def test_unassigned_initializing_and_reviving_excess_retire_without_drain(self):
        for state in (AgentState.INITIALIZING.value, AgentState.REVIVING.value):
            with self.subTest(state=state):
                agent_id = self.agent()
                with self.db.transaction() as conn:
                    conn.execute(
                        "UPDATE logical_agents SET state=?,current_task_id=NULL WHERE id=?",
                        (state, agent_id),
                    )
                self.scheduler.resize_partition("general", 0)
                result = self.scheduler.reconcile_pool()
                self.assertEqual(result["retired"], 1)
                self.assertEqual(result["draining"], 0)
                self.assertEqual(
                    self.scheduler.get("logical_agents", agent_id)["state"], "RETIRED"
                )
                self.scheduler.resize_partition("general", 1)
                self.scheduler.reconcile_pool()

    def test_semantic_retirement_fences_idle_reusable_incarnation(self):
        agent_id = self.agent()
        with self.db.transaction() as conn:
            incarnation_id = self.scheduler._ensure_incarnation(
                conn, agent_id, "codex", time.time()
            )
            conn.execute(
                "UPDATE incarnations SET state='WARM' WHERE id=?", (incarnation_id,)
            )

        self.scheduler.resize_partition("general", 0)
        self.assertEqual(self.scheduler.reconcile_pool()["retired"], 1)

        self.assertEqual(
            self.scheduler.get("logical_agents", agent_id)["state"], "RETIRED"
        )
        self.assertEqual(
            self.scheduler.get("incarnations", incarnation_id)["state"], "LOST"
        )

    def test_assignment_boundary_retirement_fences_reusable_incarnation(self):
        _batch, _ids = self.scheduler.submit_batch([TaskSpec("retiring-busy", {})])
        claim = self.scheduler.claim_next_available()
        execution_id, _request = self.scheduler.create_execution(claim)
        incarnation_id = self.scheduler.get("executions", execution_id)["incarnation_id"]
        self.scheduler.confirm_execution_running(
            claim.attempt_id,
            claim.lease_epoch,
            execution_id,
            runtime_handle={"live": True},
        )
        self.scheduler.resize_partition("general", 0)
        self.assertEqual(self.scheduler.reconcile_pool()["draining"], 1)

        self.scheduler.ack_success(
            claim.attempt_id,
            claim.lease_epoch,
            execution_id=execution_id,
            payload={},
            incarnation_reusable=True,
        )

        self.assertEqual(
            self.scheduler.get("logical_agents", claim.logical_agent_id)["state"],
            "RETIRED",
        )
        self.assertEqual(
            self.scheduler.get("incarnations", incarnation_id)["state"], "LOST"
        )

    def test_excess_prefers_unassigned_identity_over_busy_agent(self):
        self.scheduler.resize_partition("general", 2)
        self.scheduler.reconcile_pool()
        _batch, _ids = self.scheduler.submit_batch([TaskSpec("busy", {})])
        claim = self.scheduler.claim_next_available()
        unassigned = self.scheduler.list("logical_agents", state="READY")[0]["id"]
        with self.db.transaction() as conn:
            conn.execute(
                "UPDATE logical_agents SET state='REVIVING',available_since=NULL WHERE id=?",
                (unassigned,),
            )
        self.scheduler.resize_partition("general", 1)
        result = self.scheduler.reconcile_pool()
        self.assertEqual(result["retired"], 1)
        self.assertEqual(result["draining"], 0)
        self.assertEqual(self.scheduler.get("logical_agents", unassigned)["state"], "RETIRED")
        self.assertEqual(
            self.scheduler.get("logical_agents", claim.logical_agent_id)["state"], "ASSIGNED"
        )

    def test_move_capacity_prefers_ready_and_defers_busy_identity(self):
        self.scheduler.resize_partition("general", 2)
        self.scheduler.upsert_partition(
            PartitionSpec("target", 0, Retention.RESIDENT, "codex", "default")
        )
        self.scheduler.reconcile_pool()
        _batch, _ids = self.scheduler.submit_batch([TaskSpec("busy", {})])
        claim = self.scheduler.claim_next_available()
        ready = self.scheduler.list("logical_agents", state="READY")[0]
        revision = self.scheduler.move_capacity("general", "target", 2)
        self.assertGreater(revision, 0)
        self.assertEqual(
            self.scheduler.get("logical_agents", ready["id"])["partition_name"], "target"
        )
        busy = self.scheduler.get("logical_agents", claim.logical_agent_id)
        self.assertEqual(busy["partition_name"], "general")
        self.assertEqual(busy["pending_partition_name"], "target")
        partitions = {row["name"]: row for row in self.scheduler.list("pool_partitions")}
        self.assertEqual(partitions["general"]["desired_capacity"], 0)
        self.assertEqual(partitions["target"]["desired_capacity"], 2)

    def test_move_capacity_preserves_unassigned_initializing_identity(self):
        self.scheduler.upsert_partition(
            PartitionSpec("target", 0, Retention.RESIDENT, "codex", "default")
        )
        agent_id = self.agent()
        with self.db.transaction() as conn:
            conn.execute(
                "UPDATE logical_agents SET state='INITIALIZING',current_task_id=NULL,"
                "available_since=NULL WHERE id=?",
                (agent_id,),
            )

        self.scheduler.move_capacity("general", "target", 1)

        moved = self.scheduler.get("logical_agents", agent_id)
        self.assertEqual(moved["state"], "INITIALIZING")
        self.assertEqual(moved["partition_name"], "target")
        self.assertIsNone(moved["pending_partition_name"])
        self.assertEqual(self.scheduler.reconcile_pool()["born"], 0)
        self.assertNotEqual(
            self.scheduler.get("logical_agents", agent_id)["state"], "RETIRED"
        )

    def test_move_capacity_preserves_unassigned_reviving_identity(self):
        self.scheduler.upsert_partition(
            PartitionSpec("target", 0, Retention.RESIDENT, "codex", "default")
        )
        agent_id = self.agent()
        with self.db.transaction() as conn:
            conn.execute(
                "UPDATE logical_agents SET state='REVIVING',current_task_id=NULL,"
                "available_since=NULL WHERE id=?",
                (agent_id,),
            )

        self.scheduler.move_capacity("general", "target", 1)

        moved = self.scheduler.get("logical_agents", agent_id)
        self.assertEqual(moved["state"], "REVIVING")
        self.assertEqual(moved["partition_name"], "target")
        self.assertIsNone(moved["pending_partition_name"])
        self.assertEqual(self.scheduler.reconcile_pool()["born"], 0)
        self.assertNotEqual(
            self.scheduler.get("logical_agents", agent_id)["state"], "RETIRED"
        )

    def test_pending_membership_composes_across_merge_and_reconciliation(self):
        self.scheduler.upsert_partition(
            PartitionSpec("queued-target", 0, Retention.RESIDENT, "codex", "default")
        )
        self.scheduler.upsert_partition(
            PartitionSpec("final-target", 0, Retention.RESIDENT, "codex", "default")
        )
        _batch, _ids = self.scheduler.submit_batch([TaskSpec("busy-chain", {})])
        claim = self.scheduler.claim_next_available()
        execution_id, _request = self.scheduler.create_execution(claim)
        self.scheduler.confirm_execution_running(
            claim.attempt_id,
            claim.lease_epoch,
            execution_id,
            runtime_handle={"live": True},
        )

        self.scheduler.move_capacity("general", "queued-target", 1)
        self.assertEqual(
            self.scheduler.get("logical_agents", claim.logical_agent_id)[
                "pending_partition_name"
            ],
            "queued-target",
        )
        self.scheduler.merge_partitions("queued-target", "final-target")

        desired = self.scheduler.get("logical_agents", claim.logical_agent_id)
        self.assertEqual(desired["partition_name"], "general")
        self.assertEqual(desired["pending_partition_name"], "final-target")
        self.assertEqual(self.scheduler.reconcile_pool()["born"], 0)
        self.scheduler.ack_success(
            claim.attempt_id,
            claim.lease_epoch,
            execution_id=execution_id,
            payload={},
        )
        committed = self.scheduler.get("logical_agents", claim.logical_agent_id)
        self.assertEqual(committed["partition_name"], "final-target")
        self.assertIsNone(committed["pending_partition_name"])

    def test_pending_membership_composes_across_consecutive_capacity_moves(self):
        self.scheduler.upsert_partition(
            PartitionSpec("middle", 0, Retention.RESIDENT, "codex", "default")
        )
        self.scheduler.upsert_partition(
            PartitionSpec("final", 0, Retention.RESIDENT, "codex", "default")
        )
        _batch, _ids = self.scheduler.submit_batch([TaskSpec("busy-moves", {})])
        claim = self.scheduler.claim_next_available()

        self.scheduler.move_capacity("general", "middle", 1)
        self.scheduler.move_capacity("middle", "final", 1)

        desired = self.scheduler.get("logical_agents", claim.logical_agent_id)
        self.assertEqual(desired["partition_name"], "general")
        self.assertEqual(desired["pending_partition_name"], "final")
        self.assertEqual(self.scheduler.reconcile_pool()["born"], 0)
        self.scheduler.ack_success(
            claim.attempt_id, claim.lease_epoch, execution_id=None, payload={}
        )
        committed = self.scheduler.get("logical_agents", claim.logical_agent_id)
        self.assertEqual(committed["partition_name"], "final")
        self.assertIsNone(committed["pending_partition_name"])

    def test_merge_does_not_overwrite_an_existing_different_desired_partition(self):
        self.scheduler.upsert_partition(
            PartitionSpec("existing-desired", 0, Retention.RESIDENT, "codex", "default")
        )
        self.scheduler.upsert_partition(
            PartitionSpec("merge-target", 0, Retention.RESIDENT, "codex", "default")
        )
        _batch, _ids = self.scheduler.submit_batch([TaskSpec("busy-preserve", {})])
        claim = self.scheduler.claim_next_available()
        self.scheduler.move_agent(claim.logical_agent_id, "existing-desired")

        self.scheduler.merge_partitions("general", "merge-target")

        desired = self.scheduler.get("logical_agents", claim.logical_agent_id)
        self.assertEqual(desired["partition_name"], "general")
        self.assertEqual(desired["pending_partition_name"], "existing-desired")
        self.scheduler.ack_success(
            claim.attempt_id, claim.lease_epoch, execution_id=None, payload={}
        )
        self.assertEqual(
            self.scheduler.get("logical_agents", claim.logical_agent_id)[
                "partition_name"
            ],
            "existing-desired",
        )

    def test_retire_rejects_inbound_desired_membership_atomically(self):
        self.scheduler.upsert_partition(
            PartitionSpec("target", 0, Retention.RESIDENT, "codex", "default")
        )
        _batch, _ids = self.scheduler.submit_batch([TaskSpec("busy-retire-target", {})])
        claim = self.scheduler.claim_next_available()
        self.scheduler.move_capacity("general", "target", 1)
        revisions = int(
            self.db.fetch_one(
                "SELECT COUNT(*) AS count FROM pool_topology_revisions"
            )["count"]
        )

        with self.assertRaisesRegex(InvalidTransition, "desired LogicalAgent"):
            self.scheduler.retire_partition("target")

        self.assertEqual(
            int(
                self.db.fetch_one(
                    "SELECT COUNT(*) AS count FROM pool_topology_revisions"
                )["count"]
            ),
            revisions,
        )
        agent = self.scheduler.get("logical_agents", claim.logical_agent_id)
        self.assertEqual(agent["pending_partition_name"], "target")
        target = next(
            row
            for row in self.scheduler.list("pool_partitions")
            if row["name"] == "target"
        )
        self.assertEqual(target["active"], 1)

    def test_assignment_boundary_adopts_target_retention_policy(self):
        self.scheduler.upsert_partition(
            PartitionSpec("ephemeral", 0, Retention.EPHEMERAL, "codex", "default")
        )
        _batch, _ids = self.scheduler.submit_batch([TaskSpec("to-ephemeral", {})])
        claim = self.scheduler.claim_next_available()
        self.scheduler.move_capacity("general", "ephemeral", 1)
        self.scheduler.ack_success(
            claim.attempt_id, claim.lease_epoch, execution_id=None, payload={}
        )
        moved = self.scheduler.get("logical_agents", claim.logical_agent_id)
        self.assertEqual(moved["partition_name"], "ephemeral")
        self.assertEqual(moved["retention"], Retention.EPHEMERAL.value)
        self.assertEqual(moved["state"], "RETIRED")

        self.scheduler.upsert_partition(
            PartitionSpec("ephemeral-source", 1, Retention.EPHEMERAL, "codex", "default")
        )
        self.scheduler.upsert_partition(
            PartitionSpec("resident-target", 0, Retention.RESIDENT, "codex", "default")
        )
        self.scheduler.reconcile_pool()
        _batch, _ids = self.scheduler.submit_batch(
            [TaskSpec("to-resident", {}, partition="ephemeral-source")]
        )
        source_agent = next(
            row
            for row in self.scheduler.list("logical_agents", state="READY")
            if row["partition_name"] == "ephemeral-source"
        )
        resident_claim = self.scheduler.claim_next(source_agent["id"])
        self.scheduler.move_capacity("ephemeral-source", "resident-target", 1)
        self.scheduler.ack_success(
            resident_claim.attempt_id,
            resident_claim.lease_epoch,
            execution_id=None,
            payload={},
        )
        resident = self.scheduler.get(
            "logical_agents", resident_claim.logical_agent_id
        )
        self.assertEqual(resident["partition_name"], "resident-target")
        self.assertEqual(resident["retention"], Retention.RESIDENT.value)
        self.assertEqual(resident["state"], "READY")

    def _suspend_writer_with_desired_partition(
        self,
        target_name: str,
        *,
        retention: Retention = Retention.RESIDENT,
        merge: bool = False,
    ):
        self.scheduler.upsert_partition(
            PartitionSpec(target_name, 0, retention, "other", "default")
        )
        policy = RetryPolicy(
            max_attempts=2,
            retry_classes=(FailureClass.EXECUTION_LOST,),
            base_backoff_seconds=0,
            max_backoff_seconds=0,
        )
        _batch, ids = self.scheduler.submit_batch(
            [
                TaskSpec(
                    f"writer-{target_name}",
                    {},
                    workspace_mode=WorkspaceMode.WRITE,
                    retry_policy=policy,
                )
            ]
        )
        claim = self.scheduler.claim_next_available()
        execution_id, _request = self.scheduler.create_execution(claim)
        self.scheduler.confirm_execution_running(
            claim.attempt_id,
            claim.lease_epoch,
            execution_id,
            runtime_handle={"live": True},
        )
        if merge:
            self.scheduler.merge_partitions("general", target_name)
        else:
            self.scheduler.move_capacity("general", target_name, 1)
        self.scheduler.expire_leases(now=claim.lease_expires_at + 1)
        escalation = self.scheduler.list("escalations", state="OPEN")[-1]
        return ids[f"writer-{target_name}"], claim, execution_id, escalation

    def test_writer_retry_commits_pending_move_capacity_before_revival(self):
        _task_id, claim, execution_id, escalation = (
            self._suspend_writer_with_desired_partition("writer-target")
        )

        self.scheduler.resolve_escalation(
            escalation["id"], operation="retry", quiescence_confirmed=True
        )

        agent = self.scheduler.get("logical_agents", claim.logical_agent_id)
        self.assertEqual(agent["partition_name"], "writer-target")
        self.assertIsNone(agent["pending_partition_name"])
        self.assertEqual(agent["state"], "REVIVING")
        self.assertEqual(self.scheduler.get("executions", execution_id)["state"], "TERMINATED")
        self.assertEqual(self.scheduler.revive_eligible_agents(), 1)
        self.assertEqual(
            self.scheduler.get("logical_agents", claim.logical_agent_id)["state"],
            "READY",
        )

    def test_writer_retry_commits_canonical_merged_target_before_revival(self):
        _task_id, claim, _execution_id, escalation = (
            self._suspend_writer_with_desired_partition(
                "merged-writer-target", merge=True
            )
        )

        self.scheduler.resolve_escalation(
            escalation["id"], operation="retry", quiescence_confirmed=True
        )
        self.assertEqual(self.scheduler.revive_eligible_agents(), 1)

        agent = self.scheduler.get("logical_agents", claim.logical_agent_id)
        self.assertEqual(agent["partition_name"], "merged-writer-target")
        self.assertIsNone(agent["pending_partition_name"])
        self.assertEqual(agent["state"], "READY")
        self.assertEqual(self.scheduler.reconcile_pool()["born"], 0)

    def test_writer_retry_applies_ephemeral_destination_retirement(self):
        _task_id, claim, _execution_id, escalation = (
            self._suspend_writer_with_desired_partition(
                "ephemeral-writer-target", retention=Retention.EPHEMERAL
            )
        )

        self.scheduler.resolve_escalation(
            escalation["id"], operation="retry", quiescence_confirmed=True
        )

        agent = self.scheduler.get("logical_agents", claim.logical_agent_id)
        self.assertEqual(agent["partition_name"], "ephemeral-writer-target")
        self.assertEqual(agent["retention"], Retention.EPHEMERAL.value)
        self.assertIsNone(agent["pending_partition_name"])
        self.assertEqual(agent["state"], "RETIRED")

    def test_cancelled_writer_release_commits_pending_membership(self):
        self.scheduler.upsert_partition(
            PartitionSpec("cancel-target", 0, Retention.RESIDENT, "other", "default")
        )
        _batch, ids = self.scheduler.submit_batch(
            [TaskSpec("cancel-moving-writer", {}, workspace_mode=WorkspaceMode.WRITE)]
        )
        claim = self.scheduler.claim_next_available()
        execution_id, _request = self.scheduler.create_execution(claim)
        self.scheduler.confirm_execution_running(
            claim.attempt_id,
            claim.lease_epoch,
            execution_id,
            runtime_handle={"live": True},
        )
        self.scheduler.move_capacity("general", "cancel-target", 1)
        self.scheduler.cancel_task(ids["cancel-moving-writer"])
        escalation = self.scheduler.list("escalations", state="OPEN")[-1]

        self.scheduler.resolve_escalation(
            escalation["id"],
            operation="release_cancelled_writer",
            quiescence_confirmed=True,
        )
        self.assertEqual(self.scheduler.revive_eligible_agents(), 1)

        agent = self.scheduler.get("logical_agents", claim.logical_agent_id)
        self.assertEqual(agent["partition_name"], "cancel-target")
        self.assertIsNone(agent["pending_partition_name"])
        self.assertEqual(agent["state"], "READY")
        self.assertEqual(self.scheduler.get("executions", execution_id)["state"], "TERMINATED")

    def _expire_cross_target_execution(self, *, workspace_mode, isolated):
        self.scheduler.upsert_partition(
            PartitionSpec("expiry-target", 0, Retention.RESIDENT, "other", "default")
        )
        policy = RetryPolicy(
            max_attempts=2,
            retry_classes=(FailureClass.EXECUTION_LOST,),
            base_backoff_seconds=0,
            max_backoff_seconds=0,
        )
        _batch, ids = self.scheduler.submit_batch(
            [TaskSpec("expiry-cutover", {}, workspace_mode=workspace_mode, retry_policy=policy)]
        )
        claim = self.scheduler.claim_next_available()
        execution_id, _request = self.scheduler.create_execution(
            claim, attempt_isolation=isolated
        )
        self.scheduler.confirm_execution_running(
            claim.attempt_id,
            claim.lease_epoch,
            execution_id,
            runtime_handle={"live": True},
        )
        self.scheduler.move_capacity("general", "expiry-target", 1)
        expired = self.scheduler.expire_leases(now=claim.lease_expires_at + 10)
        return ids["expiry-cutover"], claim, execution_id, expired

    def test_read_only_expiry_detaches_topology_but_keeps_stale_execution_history(self):
        task_id, claim, execution_id, expired = self._expire_cross_target_execution(
            workspace_mode=WorkspaceMode.READ_ONLY, isolated=False
        )
        self.assertEqual(expired["retried"], 1)
        self.assertEqual(self.scheduler.get("tasks", task_id)["state"], "RETRY_WAIT")
        agent = self.scheduler.get("logical_agents", claim.logical_agent_id)
        self.assertEqual(agent["partition_name"], "expiry-target")
        self.assertIsNone(agent["pending_partition_name"])
        self.assertEqual(self.scheduler.get("executions", execution_id)["state"], "RUNNING")

        self.scheduler.record_physical_outcome(
            execution_id,
            state=ExecutionState.TERMINATED,
            terminal_confirmed=True,
            quiescent_confirmed=True,
        )
        self.assertEqual(self.scheduler.get("executions", execution_id)["state"], "TERMINATED")
        self.assertEqual(self.scheduler.get("tasks", task_id)["state"], "RETRY_WAIT")

    def test_isolated_writer_expiry_detaches_topology_safely(self):
        task_id, claim, execution_id, expired = self._expire_cross_target_execution(
            workspace_mode=WorkspaceMode.WRITE, isolated=True
        )
        self.assertEqual(expired["retried"], 1)
        self.assertEqual(self.scheduler.get("tasks", task_id)["state"], "RETRY_WAIT")
        self.assertEqual(
            self.scheduler.get("logical_agents", claim.logical_agent_id)["partition_name"],
            "expiry-target",
        )
        self.assertEqual(self.scheduler.get("executions", execution_id)["state"], "RUNNING")

    def _expire_before_topology_change(self, *, workspace_mode, isolated):
        policy = RetryPolicy(
            max_attempts=2,
            retry_classes=(FailureClass.EXECUTION_LOST,),
            base_backoff_seconds=0,
            max_backoff_seconds=0,
        )
        _batch, ids = self.scheduler.submit_batch(
            [
                TaskSpec(
                    "expiry-first",
                    {},
                    workspace_mode=workspace_mode,
                    retry_policy=policy,
                )
            ]
        )
        claim = self.scheduler.claim_next_available()
        execution_id, _request = self.scheduler.create_execution(
            claim, attempt_isolation=isolated
        )
        self.scheduler.confirm_execution_running(
            claim.attempt_id,
            claim.lease_epoch,
            execution_id,
            runtime_handle={"thread_id": "stale-thread", "turn_id": "stale-turn"},
        )
        self.scheduler.expire_leases(now=claim.lease_expires_at + 10)
        return ids["expiry-first"], claim, execution_id

    def test_read_only_expiry_then_cross_target_move_preserves_identity(self):
        self.scheduler.upsert_partition(
            PartitionSpec("move-after-expiry", 0, Retention.RESIDENT, "other", "default")
        )
        task_id, claim, execution_id = self._expire_before_topology_change(
            workspace_mode=WorkspaceMode.READ_ONLY, isolated=False
        )

        self.scheduler.move_capacity("general", "move-after-expiry", 1)

        agent = self.scheduler.get("logical_agents", claim.logical_agent_id)
        self.assertEqual(agent["partition_name"], "move-after-expiry")
        self.assertEqual(agent["state"], "READY")
        self.assertIsNone(agent["pending_partition_name"])
        self.assertEqual(self.scheduler.get("tasks", task_id)["state"], "RETRY_WAIT")
        self.assertEqual(self.scheduler.get("executions", execution_id)["state"], "RUNNING")

    def test_isolated_writer_expiry_then_cross_target_merge_preserves_identity(self):
        self.scheduler.upsert_partition(
            PartitionSpec("merge-after-expiry", 0, Retention.RESIDENT, "other", "default")
        )
        _task_id, claim, execution_id = self._expire_before_topology_change(
            workspace_mode=WorkspaceMode.WRITE, isolated=True
        )

        self.scheduler.merge_partitions("general", "merge-after-expiry")

        agent = self.scheduler.get("logical_agents", claim.logical_agent_id)
        self.assertEqual(agent["partition_name"], "merge-after-expiry")
        self.assertEqual(agent["state"], "READY")
        self.assertEqual(self.scheduler.get("executions", execution_id)["state"], "RUNNING")
        self.assertEqual(self.scheduler.reconcile_pool()["born"], 0)

    def test_unsafe_writer_expiry_then_move_stages_desired_membership(self):
        self.scheduler.upsert_partition(
            PartitionSpec("unsafe-move-after-expiry", 0, Retention.RESIDENT, "other", "default")
        )
        _task_id, claim, execution_id = self._expire_before_topology_change(
            workspace_mode=WorkspaceMode.WRITE, isolated=False
        )
        escalation = self.scheduler.list("escalations", state="OPEN")[-1]

        self.scheduler.move_capacity("general", "unsafe-move-after-expiry", 1)

        suspended = self.scheduler.get("logical_agents", claim.logical_agent_id)
        self.assertEqual(suspended["state"], "SUSPENDED")
        self.assertEqual(suspended["partition_name"], "general")
        self.assertEqual(
            suspended["pending_partition_name"], "unsafe-move-after-expiry"
        )
        self.assertEqual(self.scheduler.get("executions", execution_id)["state"], "RUNNING")

        replacement_result = self.scheduler.reconcile_pool()
        self.assertEqual(replacement_result["born"], 1)
        replacement = next(
            agent
            for agent in self.scheduler.list("logical_agents", state="READY")
            if agent["partition_name"] == "unsafe-move-after-expiry"
        )

        self.scheduler.resolve_escalation(
            escalation["id"], operation="retry", quiescence_confirmed=True
        )
        committed = self.scheduler.get("logical_agents", claim.logical_agent_id)
        self.assertEqual(committed["partition_name"], "unsafe-move-after-expiry")
        self.assertEqual(committed["state"], "REVIVING")
        self.assertIsNone(committed["pending_partition_name"])

        convergence = self.scheduler.reconcile_pool()
        self.assertEqual(convergence["retired"], 1)
        self.assertEqual(
            self.scheduler.get("logical_agents", replacement["id"])["state"],
            "RETIRED",
        )
        self.assertEqual(self.scheduler.revive_eligible_agents(), 1)
        self.assertEqual(
            self.scheduler.get("logical_agents", claim.logical_agent_id)["state"],
            "READY",
        )
        self.assertEqual(
            self.scheduler.reconcile_pool(), {"born": 0, "retired": 0, "draining": 0}
        )

    def test_suspended_nonisolated_writer_merge_defers_cutover_until_quiescent(self):
        self.scheduler.upsert_partition(
            PartitionSpec("merge-after-suspend", 0, Retention.RESIDENT, "other", "default")
        )
        policy = RetryPolicy(
            max_attempts=2,
            retry_classes=(FailureClass.EXECUTION_LOST,),
            base_backoff_seconds=0,
            max_backoff_seconds=0,
        )
        _batch, _ids = self.scheduler.submit_batch(
            [TaskSpec("unsafe-writer", {}, workspace_mode=WorkspaceMode.WRITE, retry_policy=policy)]
        )
        claim = self.scheduler.claim_next_available()
        execution_id, _request = self.scheduler.create_execution(claim)
        self.scheduler.confirm_execution_running(
            claim.attempt_id,
            claim.lease_epoch,
            execution_id,
            runtime_handle={"live": True},
        )
        self.scheduler.expire_leases(now=claim.lease_expires_at + 10)
        escalation = self.scheduler.list("escalations", state="OPEN")[-1]

        self.scheduler.merge_partitions("general", "merge-after-suspend")
        suspended = self.scheduler.get("logical_agents", claim.logical_agent_id)
        self.assertEqual(suspended["state"], "SUSPENDED")
        self.assertEqual(suspended["partition_name"], "general")
        self.assertEqual(suspended["pending_partition_name"], "merge-after-suspend")

        self.scheduler.resolve_escalation(
            escalation["id"], operation="retry", quiescence_confirmed=True
        )
        committed = self.scheduler.get("logical_agents", claim.logical_agent_id)
        self.assertEqual(committed["partition_name"], "merge-after-suspend")
        self.assertIsNone(committed["pending_partition_name"])
        self.assertEqual(committed["state"], "REVIVING")

    def test_busy_cross_target_cutover_fences_reusable_presence(self):
        self.scheduler.upsert_partition(
            PartitionSpec("other", 0, Retention.RESIDENT, "other", "default")
        )
        _batch, _ids = self.scheduler.submit_batch([TaskSpec("cross-target", {})])
        claim = self.scheduler.claim_next_available()
        execution_id, _request = self.scheduler.create_execution(claim)
        execution = self.scheduler.get("executions", execution_id)
        old_incarnation = execution["incarnation_id"]
        self.scheduler.confirm_execution_running(
            claim.attempt_id,
            claim.lease_epoch,
            execution_id,
            runtime_handle={"live": True},
        )
        self.scheduler.move_capacity("general", "other", 1)
        self.scheduler.ack_success(
            claim.attempt_id,
            claim.lease_epoch,
            execution_id=execution_id,
            payload={},
            incarnation_reusable=True,
        )

        moved = self.scheduler.get("logical_agents", claim.logical_agent_id)
        self.assertEqual(moved["partition_name"], "other")
        self.assertEqual(
            self.scheduler.get("incarnations", old_incarnation)["state"], "LOST"
        )
        _batch, _ids = self.scheduler.submit_batch(
            [TaskSpec("after-cutover", {}, partition="other")]
        )
        replacement = self.scheduler.claim_next(claim.logical_agent_id)
        replacement_execution, _request = self.scheduler.create_execution(replacement)
        new_incarnation = self.scheduler.get("executions", replacement_execution)[
            "incarnation_id"
        ]
        self.assertNotEqual(new_incarnation, old_incarnation)
        self.assertEqual(
            self.scheduler.get("incarnations", new_incarnation)["execution_target"],
            "other",
        )

    def test_cross_target_move_capacity_cuts_over_idle_presence_immediately(self):
        self.scheduler.resize_partition("general", 3)
        self.scheduler.upsert_partition(
            PartitionSpec("other", 0, Retention.RESIDENT, "other", "default")
        )
        self.scheduler.reconcile_pool()
        agents = self.scheduler.list("logical_agents", state="READY")
        incarnation_ids = []
        with self.db.transaction() as conn:
            for agent, state in zip(agents, ("STARTING", "WARM", "COLD"), strict=True):
                incarnation = self.scheduler._ensure_incarnation(
                    conn, agent["id"], "codex", time.time()
                )
                incarnation_ids.append(incarnation)
                conn.execute(
                    "UPDATE incarnations SET state=? WHERE id=?", (state, incarnation)
                )

        self.scheduler.move_capacity("general", "other", 3)

        for agent, incarnation in zip(agents, incarnation_ids, strict=True):
            moved = self.scheduler.get("logical_agents", agent["id"])
            self.assertEqual(moved["state"], "READY")
            self.assertEqual(moved["partition_name"], "other")
            self.assertIsNone(moved["pending_partition_name"])
            self.assertEqual(
                self.scheduler.get("incarnations", incarnation)["state"], "LOST"
            )
        self.assertEqual(self.scheduler.reconcile_pool()["born"], 0)

    def test_cli_rejects_optional_tasks_and_exposes_partition_surface(self):
        with self.assertRaisesRegex(ValueError, "optional Tasks are not supported"):
            _task_specs({"tasks": [{"name": "optional", "payload": {}, "required": False}]})
        parser = build_parser()
        for command in (
            ["pool", "upsert", "x", "1", "resident", "codex", "default"],
            ["pool", "resize", "x", "2"],
            ["pool", "move-capacity", "x", "y", "1"],
            ["pool", "move-agent", "agent", "y"],
            ["pool", "merge", "x", "y"],
            ["pool", "retire", "x"],
        ):
            with self.subTest(command=command):
                self.assertEqual(parser.parse_args(command).command, "pool")

    def test_pool_upsert_cli_rejects_unknown_target_and_profile(self):
        config = load_config(
            Path(__file__).resolve().parents[1] / "config" / "scheduler.example.toml"
        )
        parser = build_parser()
        valid = parser.parse_args(
            ["pool", "upsert", "new", "1", "resident", "local_codex", "default"]
        )
        self.assertEqual(_partition_spec_for_upsert(valid, config).name, "new")
        with self.assertRaisesRegex(ConfigurationError, "unknown execution target"):
            _partition_spec_for_upsert(
                parser.parse_args(
                    ["pool", "upsert", "new", "1", "resident", "missing", "default"]
                ),
                config,
            )
        with self.assertRaisesRegex(ConfigurationError, "unknown execution profile"):
            _partition_spec_for_upsert(
                parser.parse_args(
                    ["pool", "upsert", "new", "1", "resident", "local_codex", "missing"]
                ),
                config,
            )
        with self.assertRaisesRegex(ConfigurationError, "--config is required"):
            _partition_spec_for_upsert(valid, None)

    def test_preferred_workstream_agent_wins_global_match(self):
        ws = self.scheduler.create_workstream("same")
        generic = self.agent()
        preferred = self.scheduler.birth_agent("general", workstream_id=ws)
        _batch, _ids = self.scheduler.submit_batch(
            [
                TaskSpec(
                    "affinity",
                    {},
                    workstream_id=ws,
                    continuity=ContinuityPreference.PREFERRED,
                )
            ]
        )
        claim = self.scheduler.claim_next_available()
        self.assertEqual(claim.logical_agent_id, preferred)
        self.assertNotEqual(claim.logical_agent_id, generic)

    def test_idle_ready_agent_has_no_fake_incarnation_and_sequential_reuse_allowed(self):
        agent = self.agent()
        self.assertEqual(self.scheduler.list("incarnations"), [])
        _batch, _ids = self.scheduler.submit_batch([TaskSpec("one", {})])
        first = self.scheduler.claim_next(agent)
        first_execution, _ = self.scheduler.create_execution(first)
        incarnation = self.scheduler.get("executions", first_execution)["incarnation_id"]
        self.scheduler.ack_success(
            first.attempt_id,
            first.lease_epoch,
            execution_id=first_execution,
            payload={},
            incarnation_reusable=True,
        )
        _batch, _ids = self.scheduler.submit_batch([TaskSpec("two", {})])
        second = self.scheduler.claim_next(agent)
        second_execution, _ = self.scheduler.create_execution(second)
        self.assertEqual(
            self.scheduler.get("executions", second_execution)["incarnation_id"],
            incarnation,
        )

    def test_read_only_sandbox_and_live_ambiguous_identity(self):
        CapturingSession.instances.clear()
        adapter = CodexAppServerAdapter(session_factory=CapturingSession)
        request = ExecutionRequest(
            "request", "execution", "task", "attempt", 1, "agent", "inc", "codex",
            "default", self.temp.name, "inspect", WorkspaceMode.READ_ONLY, {}, {}
        )
        started = adapter.start_execution(request)
        self.assertEqual(started.state, ExecutionState.RUNNING)
        self.assertEqual(CapturingSession.instances[0].calls[0][1]["sandbox"], "read-only")
        incomplete = {
            "adapter_session_id": CapturingSession.instances[0].session_id,
            "thread_id": "thread",
        }
        self.assertEqual(adapter.observe_execution(incomplete).state, ExecutionState.UNKNOWN)

    def test_start_execution_uses_one_method_deadline_across_all_stages(self):
        AdvancingDeadlineSession.clock = [0.0]
        AdvancingDeadlineSession.constructor_timeouts = []
        AdvancingDeadlineSession.request_timeouts = []
        adapter = CodexAppServerAdapter(
            request_timeout=1.0, session_factory=AdvancingDeadlineSession
        )
        request = ExecutionRequest(
            "request", "execution", "task", "attempt", 1, "agent", "inc", "codex",
            "default", self.temp.name, "inspect", WorkspaceMode.READ_ONLY, {}, {}
        )
        with patch(
            "local_agent_scheduler.adapters.codex.time.monotonic",
            side_effect=lambda: AdvancingDeadlineSession.clock[0],
        ):
            started = adapter.start_execution(request)

        self.assertEqual(started.state, ExecutionState.RUNNING)
        self.assertAlmostEqual(AdvancingDeadlineSession.constructor_timeouts[0], 1.0)
        self.assertEqual(
            [name for name, _timeout in AdvancingDeadlineSession.request_timeouts],
            ["thread/start", "turn/start"],
        )
        thread_timeout = AdvancingDeadlineSession.request_timeouts[0][1]
        turn_timeout = AdvancingDeadlineSession.request_timeouts[1][1]
        self.assertGreater(thread_timeout, turn_timeout)
        self.assertAlmostEqual(thread_timeout, 0.8)
        self.assertAlmostEqual(turn_timeout, 0.6)

    def test_start_deadline_captures_every_successful_stage_before_timeout(self):
        expected = {
            "session": {"adapter_session_id"},
            "thread": {"adapter_session_id", "thread_id"},
            "turn": {"adapter_session_id", "thread_id", "turn_id"},
        }
        for boundary, locators in expected.items():
            with self.subTest(boundary=boundary):
                BoundaryDeadlineSession.clock = [0.0]
                BoundaryDeadlineSession.expire_after = boundary
                BoundaryDeadlineSession.closed = []
                adapter = CodexAppServerAdapter(
                    request_timeout=1.0, session_factory=BoundaryDeadlineSession
                )
                request = ExecutionRequest(
                    f"request-{boundary}",
                    f"execution-{boundary}",
                    "task",
                    "attempt",
                    1,
                    "agent",
                    "inc",
                    "codex",
                    "default",
                    self.temp.name,
                    "inspect",
                    WorkspaceMode.READ_ONLY,
                    {},
                    {},
                )
                with patch(
                    "local_agent_scheduler.adapters.codex.time.monotonic",
                    side_effect=lambda: BoundaryDeadlineSession.clock[0],
                ):
                    started = adapter.start_execution(request)

                self.assertEqual(started.state, ExecutionState.UNKNOWN)
                self.assertTrue(started.ambiguous)
                self.assertTrue(locators.issubset(started.runtime_handle))
                session_id = started.runtime_handle["adapter_session_id"]
                self.assertIn(session_id, adapter._sessions)
                self.assertEqual(BoundaryDeadlineSession.closed, [])

    def test_app_server_constructor_failure_cleans_created_process(self):
        process = BlockingStdioProcess("none")
        process.stdin = None
        with patch(
            "local_agent_scheduler.adapters.codex.subprocess.Popen",
            return_value=process,
        ), self.assertRaisesRegex(AdapterError, "failed to open"):
            AppServerSession(("codex", "app-server"), None, 0.05)
        self.assertIsNotNone(process.poll())

    def test_running_confirmation_atomically_renews_near_deadline_lease(self):
        _batch, _ids = self.scheduler.submit_batch([TaskSpec("near-deadline", {})])
        claim = self.scheduler.claim_next_available()
        execution_id, _request = self.scheduler.create_execution(claim)
        with self.db.transaction() as conn:
            conn.execute(
                "UPDATE leases SET heartbeat_at=90,expires_at=100 WHERE id=?",
                (claim.lease_id,),
            )

        renewed_until = self.scheduler.confirm_running_and_renew_authority(
            claim.attempt_id,
            claim.lease_epoch,
            execution_id,
            runtime_handle={"live": True},
            now=99.99,
        )

        lease = self.scheduler.get("leases", claim.lease_id)
        self.assertAlmostEqual(renewed_until, 101.99)
        self.assertAlmostEqual(lease["heartbeat_at"], 99.99)
        self.assertAlmostEqual(lease["expires_at"], 101.99)
        self.assertEqual(self.scheduler.get("executions", execution_id)["state"], "RUNNING")

    def test_app_server_stdio_write_and_flush_share_request_deadline(self):
        for block_at in ("write", "flush"):
            with self.subTest(block_at=block_at):
                process = BlockingStdioProcess(block_at)
                started = time.monotonic()
                with patch(
                    "local_agent_scheduler.adapters.codex.subprocess.Popen",
                    return_value=process,
                ), self.assertRaisesRegex(TimeoutError, "timed out while writing"):
                    AppServerSession(("codex", "app-server"), None, 0.05)
                elapsed = time.monotonic() - started
                self.assertLess(elapsed, 0.5)
                self.assertEqual(process.kill_calls, 1)
                self.assertIsNotNone(process.poll())

    def test_topology_bootstrap_does_not_resurrect_retired_partition(self):
        with tempfile.TemporaryDirectory() as temporary:
            database = Database(Path(temporary) / "topology.db")
            first = Scheduler(database)
            first.initialize()
            spec = PartitionSpec(
                "obsolete", 1, Retention.RESIDENT, "codex", "default"
            )
            self.assertTrue(first.bootstrap_partitions([spec]))
            first.retire_partition("obsolete")
            restarted = Scheduler(database)
            restarted.initialize()
            self.assertFalse(restarted.bootstrap_partitions([spec]))
            partition = restarted.list("pool_partitions")[0]
            self.assertEqual(partition["active"], 0)
            self.assertEqual(partition["desired_capacity"], 0)
            with self.assertRaisesRegex(InvalidTransition, "cannot reactivate"):
                restarted.upsert_partition(spec)

    def test_merge_migrates_future_task_classification_not_active_authority(self):
        self.scheduler.upsert_partition(
            PartitionSpec("target", 1, Retention.RESIDENT, "other", "other-profile")
        )
        policy = RetryPolicy(
            max_attempts=2,
            retry_classes=(FailureClass.TRANSIENT_EXTERNAL,),
            base_backoff_seconds=0,
            max_backoff_seconds=0,
        )
        _batch, ids = self.scheduler.submit_batch(
            [
                TaskSpec("running", {}, priority=10, retry_policy=policy),
                TaskSpec("queued", {}),
                TaskSpec("blocked", {}, dependencies=("running",)),
            ]
        )
        claim = self.scheduler.claim_next(self.agent())
        self.assertEqual(claim.task_id, ids["running"])
        execution_id, _request = self.scheduler.create_execution(claim)
        self.scheduler.confirm_execution_running(
            claim.attempt_id, claim.lease_epoch, execution_id, runtime_handle={"live": True}
        )
        attempt_before = self.scheduler.get("attempts", claim.attempt_id)
        execution_before = self.scheduler.get("executions", execution_id)

        self.scheduler.merge_partitions("general", "target")

        for task_id in ids.values():
            self.assertEqual(self.scheduler.get("tasks", task_id)["partition_name"], "target")
        attempt_after = self.scheduler.get("attempts", claim.attempt_id)
        execution_after = self.scheduler.get("executions", execution_id)
        for key in ("logical_agent_id", "lease_epoch", "incarnation_id"):
            self.assertEqual(attempt_after[key], attempt_before[key])
        for key in ("execution_target", "execution_profile", "incarnation_id"):
            self.assertEqual(execution_after[key], execution_before[key])
        self.assertEqual(execution_after["execution_target"], "codex")

        self.scheduler.nack(
            claim.attempt_id,
            claim.lease_epoch,
            execution_id=execution_id,
            failure_class=FailureClass.TRANSIENT_EXTERNAL,
        )
        self.scheduler.promote_retry_wait()
        replacement = self.scheduler.claim_next_available()
        self.assertIsNotNone(replacement)
        self.assertEqual(replacement.task_id, ids["running"])
        self.assertEqual(replacement.execution_target, "other")
        self.assertEqual(replacement.execution_profile, "other-profile")
        with self.assertRaises(NotFound):
            self.scheduler.submit_batch([TaskSpec("stranded", {}, partition="general")])

    def test_merge_adds_declared_capacities_and_preserves_population(self):
        self.scheduler.resize_partition("general", 2)
        self.scheduler.upsert_partition(
            PartitionSpec("target", 3, Retention.RESIDENT, "codex", "default")
        )
        self.scheduler.reconcile_pool()
        self.scheduler.merge_partitions("general", "target")
        partitions = {row["name"]: row for row in self.scheduler.list("pool_partitions")}
        self.assertEqual(partitions["general"]["desired_capacity"], 0)
        self.assertEqual(partitions["general"]["active"], 0)
        self.assertEqual(partitions["general"]["merged_into"], "target")
        self.assertEqual(partitions["target"]["desired_capacity"], 5)
        result = self.scheduler.reconcile_pool()
        self.assertEqual(result["retired"], 0)
        self.assertEqual(result["draining"], 0)
        live = [
            row
            for row in self.scheduler.list("logical_agents")
            if row["partition_name"] == "target" and row["state"] != "RETIRED"
        ]
        self.assertEqual(len(live), 5)

    def test_merge_moves_unassigned_reviving_identity_immediately(self):
        self.scheduler.upsert_partition(
            PartitionSpec("target", 0, Retention.RESIDENT, "codex", "default")
        )
        agent_id = self.agent()
        with self.db.transaction() as conn:
            conn.execute(
                "UPDATE logical_agents SET state='REVIVING',available_since=NULL WHERE id=?",
                (agent_id,),
            )
        self.scheduler.merge_partitions("general", "target")
        agent = self.scheduler.get("logical_agents", agent_id)
        self.assertEqual(agent["partition_name"], "target")
        self.assertIsNone(agent["pending_partition_name"])
        self.assertEqual(self.scheduler.revive_eligible_agents(), 1)
        self.assertEqual(self.scheduler.get("logical_agents", agent_id)["state"], "READY")

    def test_every_task_participates_in_single_batch_result_boundary(self):
        _batch, _ids = self.scheduler.submit_batch([TaskSpec("a", {}), TaskSpec("b", {})])
        first = self.scheduler.claim_next_available()
        self.scheduler.ack_success(first.attempt_id, first.lease_epoch, execution_id=None, payload={})
        self.assertEqual(self.scheduler.list("notification_outbox"), [])
        second = self.scheduler.claim_next_available()
        self.scheduler.ack_success(second.attempt_id, second.lease_epoch, execution_id=None, payload={})
        events = self.scheduler.list("notification_outbox")
        self.assertEqual([event["event_type"] for event in events], ["BATCH_RESULTS_READY"])

    def test_cross_target_move_agent_cuts_over_idle_presence_immediately(self):
        self.scheduler.upsert_partition(
            PartitionSpec("other", 0, Retention.RESIDENT, "other", "default")
        )
        agent = self.agent()
        with self.db.transaction() as conn:
            incarnation = self.scheduler._ensure_incarnation(conn, agent, "codex", time.time())
            conn.execute("UPDATE incarnations SET state='WARM' WHERE id=?", (incarnation,))
        self.scheduler.move_agent(agent, "other")
        moved = self.scheduler.get("logical_agents", agent)
        self.assertEqual(moved["state"], "READY")
        self.assertEqual(moved["partition_name"], "other")
        self.assertEqual(self.scheduler.get("incarnations", incarnation)["state"], "LOST")

    def test_cross_target_merge_preserves_idle_identity_without_replacement_birth(self):
        self.scheduler.upsert_partition(
            PartitionSpec("other", 0, Retention.RESIDENT, "other", "default")
        )
        agent = self.agent()
        with self.db.transaction() as conn:
            incarnation = self.scheduler._ensure_incarnation(
                conn, agent, "codex", time.time()
            )
            conn.execute(
                "UPDATE incarnations SET state='WARM' WHERE id=?", (incarnation,)
            )

        self.scheduler.merge_partitions("general", "other")

        moved = self.scheduler.get("logical_agents", agent)
        self.assertEqual(moved["state"], "READY")
        self.assertEqual(moved["partition_name"], "other")
        self.assertIsNone(moved["pending_partition_name"])
        self.assertEqual(self.scheduler.get("incarnations", incarnation)["state"], "LOST")
        reconciled = self.scheduler.reconcile_pool()
        self.assertEqual(reconciled["born"], 0)
        self.assertEqual(reconciled["retired"], 0)

    def test_upsert_is_create_or_idempotent_structural_redeclaration(self):
        original = next(
            row
            for row in self.scheduler.list("pool_partitions")
            if row["name"] == "general"
        )
        revision_count = int(
            self.db.fetch_one("SELECT COUNT(*) AS count FROM pool_topology_revisions")[
                "count"
            ]
        )
        same = PartitionSpec("general", 1, Retention.RESIDENT, "codex", "default")
        self.assertEqual(
            self.scheduler.upsert_partition(same), original["topology_revision"]
        )
        self.assertEqual(
            int(
                self.db.fetch_one(
                    "SELECT COUNT(*) AS count FROM pool_topology_revisions"
                )["count"]
            ),
            revision_count,
        )
        mutations = (
            PartitionSpec("general", 2, Retention.RESIDENT, "codex", "default"),
            PartitionSpec("general", 1, Retention.EPHEMERAL, "codex", "default"),
            PartitionSpec("general", 1, Retention.RESIDENT, "other", "default"),
            PartitionSpec("general", 1, Retention.RESIDENT, "codex", "other"),
            PartitionSpec(
                "general", 1, Retention.RESIDENT, "codex", "default", ("new",)
            ),
        )
        for mutation in mutations:
            with self.subTest(mutation=mutation), self.assertRaises(InvalidTransition):
                self.scheduler.upsert_partition(mutation)

    def test_unavailable_execution_target_is_normalized_not_raised(self):
        policy = RetryPolicy(max_attempts=1, retry_classes=())
        _batch, ids = self.scheduler.submit_batch(
            [TaskSpec("unavailable", {}, retry_policy=policy)]
        )
        dispatcher = Dispatcher(
            self.scheduler,
            adapters={},
            targets={},
            execution_profiles={"default"},
            workspace_root=self.temp.name,
        )
        self.assertEqual(dispatcher.dispatch_ready(), 0)
        self.assertEqual(self.scheduler.get("tasks", ids["unavailable"])["state"], "SUSPENDED")
        failure = self.scheduler.list("failures")[-1]
        self.assertEqual(failure["failure_class"], "RESOURCE_UNAVAILABLE")

    def test_empty_profile_registry_rejects_removed_persisted_profile(self):
        with self.db.transaction() as conn:
            conn.execute(
                "UPDATE pool_partitions SET execution_profile='removed-profile' "
                "WHERE name='general'"
            )
        policy = RetryPolicy(max_attempts=1, retry_classes=())
        _batch, ids = self.scheduler.submit_batch(
            [TaskSpec("removed-profile", {}, retry_policy=policy)]
        )
        adapter = ForbiddenStartAdapter()
        dispatcher = Dispatcher(
            self.scheduler,
            adapters={"codex": adapter},
            targets={
                "codex": ExecutionTargetConfig(
                    "codex", "codex_app_server", False, True
                )
            },
            execution_profiles=set(),
            workspace_root=self.temp.name,
        )

        self.assertEqual(dispatcher.dispatch_ready(), 0)
        self.assertEqual(adapter.start_calls, 0)
        self.assertEqual(
            self.scheduler.get("tasks", ids["removed-profile"])["state"], "SUSPENDED"
        )
        failure = self.scheduler.list("failures")[-1]
        self.assertEqual(failure["failure_class"], "RESOURCE_UNAVAILABLE")
        self.assertEqual(
            failure["failure_code"], "EXECUTION_CONFIGURATION_UNAVAILABLE"
        )

    def test_poll_terminal_outcome_tolerates_missing_target_configuration(self):
        _batch, ids = self.scheduler.submit_batch([TaskSpec("already-running", {})])
        claim = self.scheduler.claim_next(self.agent())
        execution_id, _request = self.scheduler.create_execution(claim)
        self.scheduler.confirm_execution_running(
            claim.attempt_id,
            claim.lease_epoch,
            execution_id,
            runtime_handle={"live": True},
        )
        dispatcher = Dispatcher(
            self.scheduler,
            adapters={"codex": SuccessfulTerminalAdapter()},
            targets={},
            execution_profiles={"default"},
            workspace_root=self.temp.name,
        )
        self.assertEqual(dispatcher.poll_executions(), 1)
        self.assertEqual(self.scheduler.get("tasks", ids["already-running"])["state"], "COMPLETED")

    def test_missing_adapter_closes_read_only_authority_instead_of_renewing(self):
        policy = RetryPolicy(
            max_attempts=2,
            retry_classes=(FailureClass.RESOURCE_UNAVAILABLE,),
            base_backoff_seconds=0,
            max_backoff_seconds=0,
        )
        _batch, ids = self.scheduler.submit_batch(
            [TaskSpec("adapter-gone", {}, retry_policy=policy)]
        )
        claim = self.scheduler.claim_next(self.agent())
        execution_id, _request = self.scheduler.create_execution(claim)
        self.scheduler.confirm_execution_running(
            claim.attempt_id,
            claim.lease_epoch,
            execution_id,
            runtime_handle={"live": True},
        )
        lease_before = self.scheduler.get("leases", claim.lease_id)["expires_at"]
        dispatcher = Dispatcher(
            self.scheduler,
            adapters={},
            targets={
                "codex": ExecutionTargetConfig(
                    "codex", "codex_app_server", False, True
                )
            },
            execution_profiles={"default"},
            workspace_root=self.temp.name,
        )

        self.assertEqual(dispatcher.renew_supervised_leases(), 0)
        self.assertEqual(
            self.scheduler.get("leases", claim.lease_id)["expires_at"], lease_before
        )
        self.assertEqual(dispatcher.poll_executions(recovery=True), 1)
        self.assertEqual(
            self.scheduler.get("tasks", ids["adapter-gone"])["state"], "RETRY_WAIT"
        )
        self.assertEqual(self.scheduler.get("leases", claim.lease_id)["state"], "RELEASED")
        self.assertEqual(self.scheduler.get("executions", execution_id)["state"], "UNKNOWN")

        recovered = Dispatcher(
            self.scheduler,
            adapters={"codex": SuccessfulTerminalAdapter()},
            targets={},
            execution_profiles={"default"},
            workspace_root=self.temp.name,
        )
        self.assertEqual(recovered.poll_executions(recovery=True), 1)
        execution = self.scheduler.get("executions", execution_id)
        self.assertEqual(execution["state"], "SUCCEEDED")
        self.assertEqual(execution["terminal_confirmed"], 1)
        self.assertEqual(execution["quiescent_confirmed"], 1)

    def test_missing_adapter_suspends_unknown_physical_writer(self):
        policy = RetryPolicy(max_attempts=1, retry_classes=())
        _batch, ids = self.scheduler.submit_batch(
            [
                TaskSpec(
                    "writer-adapter-gone",
                    {},
                    workspace_mode=WorkspaceMode.WRITE,
                    retry_policy=policy,
                )
            ]
        )
        claim = self.scheduler.claim_next(self.agent())
        execution_id, _request = self.scheduler.create_execution(claim)
        self.scheduler.confirm_execution_running(
            claim.attempt_id,
            claim.lease_epoch,
            execution_id,
            runtime_handle={"live": True},
        )
        dispatcher = Dispatcher(
            self.scheduler,
            adapters={},
            targets={
                "codex": ExecutionTargetConfig(
                    "codex", "codex_app_server", False, True
                )
            },
            execution_profiles={"default"},
            workspace_root=self.temp.name,
        )

        self.assertEqual(dispatcher.poll_executions(recovery=True), 1)
        self.assertEqual(
            self.scheduler.get("tasks", ids["writer-adapter-gone"])["state"],
            "SUSPENDED",
        )
        self.assertEqual(
            self.scheduler.get("logical_agents", claim.logical_agent_id)["state"],
            "SUSPENDED",
        )
        escalation = self.scheduler.list("escalations", state="OPEN")[0]
        self.assertEqual(
            escalation["failure_class"], FailureClass.WRITER_QUIESCENCE_UNKNOWN.value
        )

    def test_missing_adapter_writer_keeps_pending_cutover_until_quiescent(self):
        self.scheduler.upsert_partition(
            PartitionSpec("adapter-recovery-target", 0, Retention.RESIDENT, "other", "default")
        )
        _batch, _ids = self.scheduler.submit_batch(
            [TaskSpec("moving-writer-adapter-gone", {}, workspace_mode=WorkspaceMode.WRITE)]
        )
        claim = self.scheduler.claim_next_available()
        execution_id, _request = self.scheduler.create_execution(claim)
        self.scheduler.confirm_execution_running(
            claim.attempt_id,
            claim.lease_epoch,
            execution_id,
            runtime_handle={"thread_id": "physical", "turn_id": "writer"},
        )
        self.scheduler.move_capacity("general", "adapter-recovery-target", 1)
        dispatcher = Dispatcher(
            self.scheduler,
            adapters={},
            targets={
                "codex": ExecutionTargetConfig(
                    "codex", "codex_app_server", False, True
                )
            },
            execution_profiles={"default"},
            workspace_root=self.temp.name,
        )

        self.assertEqual(dispatcher.poll_executions(recovery=True), 1)
        execution = self.scheduler.get("executions", execution_id)
        agent = self.scheduler.get("logical_agents", claim.logical_agent_id)
        self.assertEqual(execution["state"], "UNKNOWN")
        self.assertEqual(execution["terminal_confirmed"], 0)
        self.assertEqual(execution["quiescent_confirmed"], 0)
        self.assertEqual(agent["state"], "SUSPENDED")
        self.assertEqual(agent["partition_name"], "general")
        self.assertEqual(agent["pending_partition_name"], "adapter-recovery-target")

        self.scheduler.record_physical_outcome(
            execution_id,
            state=ExecutionState.TERMINATED,
            terminal_confirmed=True,
            quiescent_confirmed=True,
        )
        escalation = self.scheduler.list("escalations", state="OPEN")[-1]
        self.scheduler.resolve_escalation(
            escalation["id"], operation="retry", quiescence_confirmed=True
        )
        recovered = self.scheduler.get("logical_agents", claim.logical_agent_id)
        self.assertEqual(recovered["partition_name"], "adapter-recovery-target")
        self.assertEqual(recovered["state"], "REVIVING")

    def test_notifier_backoff_is_measured_from_delivery_completion(self):
        _batch, _ids = self.scheduler.submit_batch([TaskSpec("notify-later", {})])
        claim = self.scheduler.claim_next(self.agent())
        self.scheduler.ack_success(
            claim.attempt_id, claim.lease_epoch, execution_id=None, payload={}
        )
        clock = [time.time() + 1]
        dispatcher = OutboxDispatcher(self.scheduler.db, AdvancingFailureBridge(clock))
        with patch("local_agent_scheduler.root_bridge.utc_now", side_effect=lambda: clock[0]):
            self.assertEqual(dispatcher.deliver_pending(now=clock[0]), 0)
        event = self.scheduler.list("notification_outbox")[0]
        self.assertEqual(event["delivery_attempts"], 1)
        self.assertEqual(event["next_delivery_at"], clock[0] + 2)

    def test_slow_notifier_does_not_block_dispatcher_or_lease_supervision(self):
        _batch, _ids = self.scheduler.submit_batch([TaskSpec("notify", {})])
        completed = self.scheduler.claim_next(self.agent())
        self.scheduler.ack_success(
            completed.attempt_id, completed.lease_epoch, execution_id=None, payload={}
        )
        _batch, _ids = self.scheduler.submit_batch([TaskSpec("leased", {})])
        claim = self.scheduler.claim_next(self.agent())
        execution_id, _request = self.scheduler.create_execution(claim)
        self.scheduler.confirm_execution_running(
            claim.attempt_id, claim.lease_epoch, execution_id, runtime_handle={"live": True}
        )
        bridge = BlockingBridge()
        dispatcher = Dispatcher(
            self.scheduler,
            adapters={"codex": type(
                "RunningAdapter",
                (),
                {
                    "observe_execution": lambda _self, _handle: type(
                        "Observation",
                        (),
                        {
                            "state": ExecutionState.RUNNING,
                            "terminal_confirmed": False,
                            "quiescent_confirmed": False,
                            "detail": None,
                        },
                    )(),
                },
            )()},
            targets={
                "codex": ExecutionTargetConfig(
                    "codex", "codex_app_server", False, True
                )
            },
            workspace_root=self.temp.name,
            outbox=OutboxDispatcher(self.scheduler.db, bridge),
        )
        self.assertEqual(dispatcher.poll_executions(), 1)
        self.assertEqual(dispatcher.supervised_attempt_ids(), {claim.attempt_id})
        with self.db.transaction() as conn:
            conn.execute(
                "UPDATE leases SET expires_at=? WHERE id=?",
                (time.time() + 0.1, claim.lease_id),
            )
        daemon = SchedulerDaemon(
            dispatcher, poll_seconds=0.01, heartbeat_seconds=0.03
        )
        daemon._start_supervision()
        daemon._start_notifier()
        try:
            self.assertTrue(bridge.started.wait(1))
            with ThreadPoolExecutor(max_workers=1) as pool:
                snapshot = pool.submit(dispatcher.tick).result(timeout=1)
            self.assertGreaterEqual(snapshot["observed"], 1)
            self.assertFalse(bridge.release.is_set())
        finally:
            bridge.release.set()
            daemon._stop_notifier()
            daemon._stop_supervision()
        self.assertEqual(self.scheduler.expire_leases(now=time.time())["retried"], 0)
        self.assertEqual(
            self.scheduler.get("leases", claim.lease_id)["state"], "ACTIVE"
        )
        self.assertEqual(
            self.scheduler.list("notification_outbox")[0]["state"], "DELIVERED"
        )

    def test_scheduler_daemon_is_single_run_when_notifier_is_still_stopping(self):
        _batch, _ids = self.scheduler.submit_batch([TaskSpec("notify-once", {})])
        claim = self.scheduler.claim_next_available()
        self.scheduler.ack_success(
            claim.attempt_id,
            claim.lease_epoch,
            execution_id=None,
            payload={"done": True},
        )
        bridge = BlockingBridge()
        dispatcher = Dispatcher(
            self.scheduler,
            adapters={},
            targets={},
            workspace_root=self.temp.name,
            outbox=OutboxDispatcher(self.scheduler.db, bridge),
        )
        daemon = SchedulerDaemon(
            dispatcher, poll_seconds=0.01, heartbeat_seconds=0.03
        )

        daemon.run_until_idle(max_wait_seconds=0.05)
        self.assertTrue(bridge.started.is_set())
        self.assertIsNotNone(daemon._notifier_thread)
        self.assertTrue(daemon._notifier_thread.is_alive())
        with self.assertRaisesRegex(RuntimeError, "single-run"):
            daemon.run_until_idle(max_wait_seconds=0.05)

        bridge.release.set()
        daemon._notifier_thread.join(timeout=1)


if __name__ == "__main__":
    unittest.main()
