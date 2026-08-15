from __future__ import annotations

import io
import json
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from local_agent_scheduler.cli import main as cli_main
from local_agent_scheduler.config import ExecutionTargetConfig, load_config
from local_agent_scheduler.adapters.codex import CodexAppServerAdapter
from local_agent_scheduler.core import Scheduler
from local_agent_scheduler.enums import (
    ExecutionState,
    FailureClass,
    Retention,
    TaskState,
    WorkspaceMode,
)
from local_agent_scheduler.errors import ConfigurationError, StaleAuthority
from local_agent_scheduler.models import (
    ExecutionObservation,
    ExecutionOutcome,
    PartitionSpec,
    RetryPolicy,
    StartObservation,
    TaskSpec,
)
from local_agent_scheduler.root_bridge import (
    CodexAppServerRootBridge,
    FilesystemRootBridge,
    OutboxDispatcher,
)
from local_agent_scheduler.runtime import Dispatcher, SchedulerDaemon
from local_agent_scheduler.storage import Database


class FakeAdapter:
    def __init__(self) -> None:
        self.handles: dict[str, str] = {}

    def start_execution(self, request):
        handle = {"request_id": request.request_id, "execution_id": request.execution_id}
        self.handles[request.execution_id] = "running"
        return StartObservation(ExecutionState.RUNNING, handle)

    def observe_execution(self, runtime_handle):
        return ExecutionObservation(
            ExecutionState.SUCCEEDED, terminal_confirmed=True, quiescent_confirmed=True
        )

    def collect_outcome(self, runtime_handle):
        return ExecutionOutcome(
            ExecutionState.SUCCEEDED,
            payload={"adapter": "ok"},
            summary="ok",
            terminal_confirmed=True,
            quiescent_confirmed=True,
        )

    def reconcile_start(self, request_id, runtime_handle):
        return StartObservation(ExecutionState.UNKNOWN, runtime_handle, ambiguous=True)

    def interrupt_execution(self, runtime_handle):
        return ExecutionObservation(ExecutionState.TERMINATED, True, True)

    def terminate_execution(self, runtime_handle):
        return ExecutionObservation(ExecutionState.TERMINATED, True, True)


class BrokenBridge:
    def deliver(self, event_id, event_type, payload):
        raise OSError("root unavailable")


class RecoveredAdapter(FakeAdapter):
    def reconcile_start(self, request_id, runtime_handle):
        return StartObservation(ExecutionState.SUCCEEDED, runtime_handle)


class ObservationConfirmedAdapter(FakeAdapter):
    def observe_execution(self, runtime_handle):
        return ExecutionObservation(
            ExecutionState.TERMINATED,
            terminal_confirmed=True,
            quiescent_confirmed=True,
        )

    def collect_outcome(self, runtime_handle):
        return ExecutionOutcome(
            ExecutionState.FAILED,
            failure_class=FailureClass.UNKNOWN,
            terminal_confirmed=False,
            quiescent_confirmed=False,
        )


class HangingAdapter(FakeAdapter):
    def observe_execution(self, runtime_handle):
        return ExecutionObservation(ExecutionState.RUNNING)

    def collect_outcome(self, runtime_handle):
        return ExecutionOutcome(ExecutionState.RUNNING)


class AlwaysAmbiguousAdapter(FakeAdapter):
    def start_execution(self, request):
        handle = {"request_id": request.request_id, "execution_id": request.execution_id}
        return StartObservation(ExecutionState.UNKNOWN, handle, ambiguous=True)

    def reconcile_start(self, request_id, runtime_handle):
        return StartObservation(ExecutionState.UNKNOWN, runtime_handle, ambiguous=True)


class FailedStartAdapter(FakeAdapter):
    def start_execution(self, request):
        return StartObservation(
            ExecutionState.FAILED,
            {"request_id": request.request_id, "thread_id": "late-failed-thread"},
            failure_class=FailureClass.START_FAILURE,
            failure_code="LATE_START_FAILED",
            detail="bounded start returned after authority ended",
        )


class ReconcileRunningWithNewHandleAdapter(FakeAdapter):
    def reconcile_start(self, request_id, runtime_handle):
        return StartObservation(
            ExecutionState.RUNNING,
            {
                "request_id": request_id,
                "thread_id": "recovered-thread",
                "turn_id": "recovered-turn",
            },
        )


class RecoveryFailsAfterAdmissionAdapter(FakeAdapter):
    def observe_execution(self, runtime_handle):
        if runtime_handle.get("recovery_order") == 2:
            raise RuntimeError("injected recovery observation failure")
        return ExecutionObservation(ExecutionState.RUNNING)


class FakeProcess:
    def poll(self):
        return None


class FakeExitedProcess:
    def poll(self):
        return 1


class FakeRootSession:
    instances = []

    def __init__(self, command, process_cwd, timeout):
        self.process = FakeProcess()
        self.requests = []
        self.closed = False
        self.__class__.instances.append(self)

    def request(self, method, params, **_kwargs):
        self.requests.append((method, params))
        if method == "thread/resume":
            return {"thread": {"id": params["threadId"]}}
        return {"turn": {"id": "root-turn"}}

    def notifications(self):
        return [
            {
                "method": "turn/completed",
                "params": {"turn": {"id": "root-turn", "status": "completed"}},
            }
        ]

    def close(self, **_kwargs):
        self.closed = True
        return True


class FakeStoredSession(FakeRootSession):
    def request(self, method, params, **_kwargs):
        self.requests.append((method, params))
        if method == "thread/read":
            return {
                "thread": {
                    "id": params["threadId"],
                    "status": "completed",
                    "turns": [
                        {
                            "id": "stored-turn",
                            "status": "completed",
                            "items": [{"type": "agentMessage", "text": '{"restored": true}'}],
                        }
                    ],
                }
            }
        return super().request(method, params)


class FakeInterruptedSession(FakeRootSession):
    def request(self, method, params, **_kwargs):
        self.requests.append((method, params))
        if method == "thread/read":
            return {
                "thread": {
                    "id": params["threadId"],
                    "turns": [
                        {"id": "interrupted-turn", "status": "interrupted", "error": None}
                    ],
                }
            }
        return super().request(method, params)


class FakePersistedRootSession(FakeRootSession):
    persisted_status = "completed"

    def request(self, method, params, **_kwargs):
        self.requests.append((method, params))
        if method == "thread/resume":
            return {"thread": {"id": params["threadId"]}}
        if method == "turn/start":
            return {"turn": {"id": "root-turn"}}
        if method == "thread/read":
            return {
                "thread": {
                    "id": params["threadId"],
                    "turns": [
                        {"id": "unrelated-turn", "status": "completed"},
                        {"id": "root-turn", "status": self.persisted_status},
                    ],
                }
            }
        raise AssertionError(f"unexpected method: {method}")

    def notifications(self):
        return []


class FakePersistedInterruptedRootSession(FakePersistedRootSession):
    persisted_status = "interrupted"


class RuntimeCase(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.db_path = self.root / "scheduler.db"
        self.scheduler = Scheduler(Database(self.db_path), lease_seconds=5)
        self.scheduler.initialize()
        self.scheduler.upsert_partition(
            PartitionSpec("general", 1, Retention.RESIDENT, "codex", "default")
        )
        self.scheduler.reconcile_pool()
        self.target = ExecutionTargetConfig("codex", "codex_app_server", False, True)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_dispatcher_completes_real_sqlite_flow(self) -> None:
        batch_id, ids = self.scheduler.submit_batch([TaskSpec("run", {"work": True})])
        adapter = FakeAdapter()
        bridge = FilesystemRootBridge(self.root / "events")
        dispatcher = Dispatcher(
            self.scheduler,
            adapters={"codex": adapter},
            targets={"codex": self.target},
            workspace_root=self.root,
            outbox=OutboxDispatcher(self.scheduler.db, bridge),
        )
        dispatcher.recover()
        first = dispatcher.tick()
        self.assertEqual(first["dispatched"], 1)
        second = dispatcher.tick()
        self.assertGreaterEqual(second["observed"], 1)
        self.assertEqual(self.scheduler.get("tasks", ids["run"])["state"], TaskState.COMPLETED)
        self.assertEqual(self.scheduler.get("batches", batch_id)["state"], "COMPLETED")
        daemon = SchedulerDaemon(dispatcher, poll_seconds=0.001)
        daemon._start_notifier()
        deadline = time.monotonic() + 1
        while not list((self.root / "events").glob("*.json")) and time.monotonic() < deadline:
            time.sleep(0.001)
        daemon._stop_notifier()
        envelopes = list((self.root / "events").glob("*.json"))
        self.assertEqual(len(envelopes), 1)

    def test_daemon_once_runs_until_live_execution_is_idle(self) -> None:
        _batch, ids = self.scheduler.submit_batch([TaskSpec("one-shot", {})])
        dispatcher = Dispatcher(
            self.scheduler,
            adapters={"codex": FakeAdapter()},
            targets={"codex": self.target},
            workspace_root=self.root,
        )
        totals = SchedulerDaemon(dispatcher, poll_seconds=0.001).run_until_idle(
            max_wait_seconds=1
        )
        self.assertEqual(totals["dispatched"], 1)
        self.assertGreaterEqual(totals["observed"], 1)
        self.assertEqual(self.scheduler.get("tasks", ids["one-shot"])["state"], "COMPLETED")

    def test_cli_daemon_starts_with_mixed_available_and_unavailable_topology(self) -> None:
        _seed_batch, _seed_ids = self.scheduler.submit_batch(
            [TaskSpec("pending-notification", {})]
        )
        seed_claim = self.scheduler.claim_next_available()
        self.scheduler.ack_success(
            seed_claim.attempt_id,
            seed_claim.lease_epoch,
            execution_id=None,
            payload={"seed": True},
        )
        self.assertTrue(self.scheduler.list("notification_outbox", state="PENDING"))

        self.scheduler.upsert_partition(
            PartitionSpec(
                "unavailable",
                1,
                Retention.RESIDENT,
                "missing-target",
                "missing-profile",
            )
        )
        self.scheduler.reconcile_pool()
        _healthy_batch, healthy_ids = self.scheduler.submit_batch(
            [TaskSpec("healthy", {}, partition="general")]
        )
        _bad_batch, bad_ids = self.scheduler.submit_batch(
            [
                TaskSpec(
                    "unavailable",
                    {},
                    partition="unavailable",
                    retry_policy=RetryPolicy(max_attempts=1, retry_classes=()),
                )
            ]
        )
        with self.scheduler.db.transaction() as conn:
            conn.execute(
                "INSERT INTO scheduler_meta(key,value_json,updated_at) VALUES(?,?,?)",
                ("topology_bootstrapped", "{}", time.time()),
            )
        config_path = self.root / "scheduler.toml"
        config_path.write_text(
            """schema_version = 1
database_path = "scheduler.db"
lease_seconds = 5
heartbeat_seconds = 1
continuity_max_bytes = 65536
dispatcher_poll_seconds = 0.001

[retry_defaults]
max_attempts = 1
retry_classes = []
base_backoff_seconds = 0
max_backoff_seconds = 0

[execution_profiles.default]

[[execution_targets]]
name = "codex"
adapter = "codex_app_server"
attempt_isolation = false
termination_confirmation = true

[adapters.codex_app_server]
command = ["codex", "app-server"]
cwd = "."
approval_policy = "never"
sandbox = "workspace-write"

[root_bridge]
kind = "filesystem"
inbox = "events"
request_timeout = 1
completion_timeout = 1
""",
            encoding="utf-8",
        )

        with patch(
            "local_agent_scheduler.cli.CodexAppServerAdapter",
            side_effect=lambda **_kwargs: FakeAdapter(),
        ), patch("sys.stdout", new=io.StringIO()):
            self.assertEqual(
                cli_main(["--config", str(config_path), "daemon", "--once"]),
                0,
            )

        self.assertEqual(
            self.scheduler.get("tasks", healthy_ids["healthy"])["state"],
            "COMPLETED",
        )
        self.assertEqual(
            self.scheduler.get("tasks", bad_ids["unavailable"])["state"],
            "SUSPENDED",
        )
        bad_failure = next(
            failure
            for failure in self.scheduler.list("failures")
            if failure["task_id"] == bad_ids["unavailable"]
        )
        self.assertEqual(
            bad_failure["failure_code"], "EXECUTION_CONFIGURATION_UNAVAILABLE"
        )
        self.assertEqual(
            self.scheduler.list("notification_outbox", state="PENDING"), []
        )
        self.assertTrue(list((self.root / "events").glob("*.json")))

    def test_daemon_once_timeout_terminates_instead_of_waiting_forever(self) -> None:
        _batch, ids = self.scheduler.submit_batch([TaskSpec("bounded-one-shot", {})])
        dispatcher = Dispatcher(
            self.scheduler,
            adapters={"codex": HangingAdapter()},
            targets={"codex": self.target},
            workspace_root=self.root,
        )
        totals = SchedulerDaemon(dispatcher, poll_seconds=0.001).run_until_idle(
            max_wait_seconds=0.01
        )
        self.assertEqual(totals["timed_out"], 1)
        self.assertEqual(
            self.scheduler.get("tasks", ids["bounded-one-shot"])["state"], "CANCELLED"
        )
        self.assertEqual(self.scheduler.list("executions", state="RUNNING"), [])

    def test_observation_quiescence_is_preserved_when_collect_omits_it(self) -> None:
        _batch, ids = self.scheduler.submit_batch([TaskSpec("terminal", {})])
        dispatcher = Dispatcher(
            self.scheduler,
            adapters={"codex": ObservationConfirmedAdapter()},
            targets={"codex": self.target},
            workspace_root=self.root,
        )
        dispatcher.tick()
        execution = self.scheduler.list("executions")[0]
        dispatcher.tick()
        self.assertEqual(self.scheduler.get("tasks", ids["terminal"])["state"], "SUSPENDED")
        self.assertEqual(
            self.scheduler.get("incarnations", execution["incarnation_id"])["state"],
            "TERMINATED",
        )

    def test_restart_after_claim_reconciles_without_losing_task(self) -> None:
        policy = RetryPolicy(
            max_attempts=2,
            retry_classes=(FailureClass.EXECUTION_LOST,),
            base_backoff_seconds=0,
            max_backoff_seconds=0,
        )
        _batch, ids = self.scheduler.submit_batch(
            [TaskSpec("claimed", {}, retry_policy=policy)]
        )
        agent = self.scheduler.list("logical_agents", state="READY")[0]
        claim = self.scheduler.claim_next(agent["id"], now=100)
        restarted = Scheduler(Database(self.db_path), lease_seconds=5)
        restarted.initialize()
        result = restarted.expire_leases(now=106)
        self.assertEqual(result["retried"], 1)
        restarted.promote_retry_wait(now=106)
        self.assertEqual(restarted.get("tasks", ids["claimed"])["state"], TaskState.QUEUED)

    def _expired_writer_after_isolation_config_change(
        self, *, frozen_isolation: bool, current_isolation: bool
    ) -> tuple[Scheduler, str]:
        policy = RetryPolicy(
            max_attempts=2,
            retry_classes=(FailureClass.EXECUTION_LOST,),
            base_backoff_seconds=0,
            max_backoff_seconds=0,
        )
        _batch, ids = self.scheduler.submit_batch(
            [
                TaskSpec(
                    "writer-isolation-snapshot",
                    {},
                    workspace_mode=WorkspaceMode.WRITE,
                    retry_policy=policy,
                )
            ]
        )
        claim = self.scheduler.claim_next_available()
        execution_id, _request_id = self.scheduler.create_execution(
            claim, attempt_isolation=frozen_isolation
        )
        self.scheduler.confirm_execution_running(
            claim.attempt_id,
            claim.lease_epoch,
            execution_id,
            runtime_handle={"writer": True},
        )
        with self.scheduler.db.transaction() as conn:
            conn.execute(
                "UPDATE leases SET expires_at=? WHERE id=?",
                (time.time() - 1, claim.lease_id),
            )

        restarted = Scheduler(Database(self.db_path), lease_seconds=5)
        restarted.initialize()
        dispatcher = Dispatcher(
            restarted,
            adapters={"codex": FakeAdapter()},
            targets={
                "codex": ExecutionTargetConfig(
                    "codex", "codex_app_server", current_isolation, True
                )
            },
            workspace_root=self.root,
        )
        dispatcher.tick()
        return restarted, ids["writer-isolation-snapshot"]

    def test_restart_cannot_retroactively_grant_writer_attempt_isolation(self) -> None:
        restarted, task_id = self._expired_writer_after_isolation_config_change(
            frozen_isolation=False,
            current_isolation=True,
        )

        self.assertEqual(restarted.get("tasks", task_id)["state"], "SUSPENDED")
        escalation = restarted.list("escalations", state="OPEN")[-1]
        self.assertEqual(
            escalation["failure_class"],
            FailureClass.WRITER_QUIESCENCE_UNKNOWN.value,
        )

    def test_restart_preserves_writer_isolation_when_current_config_removes_it(self) -> None:
        restarted, task_id = self._expired_writer_after_isolation_config_change(
            frozen_isolation=True,
            current_isolation=False,
        )

        self.assertEqual(restarted.get("tasks", task_id)["state"], "RETRY_WAIT")
        self.assertEqual(restarted.list("escalations", state="OPEN"), [])

    def test_restart_fences_unexpired_claim_without_execution_before_heartbeat(self) -> None:
        policy = RetryPolicy(
            max_attempts=2,
            retry_classes=(FailureClass.EXECUTION_LOST,),
            base_backoff_seconds=0,
            max_backoff_seconds=0,
        )
        _batch, ids = self.scheduler.submit_batch(
            [TaskSpec("unstarted", {}, retry_policy=policy)]
        )
        agent = self.scheduler.list("logical_agents", state="READY")[0]
        claim = self.scheduler.claim_next(agent["id"], now=time.time())
        original_expiry = self.scheduler.get("leases", claim.lease_id)["expires_at"]

        restarted = Scheduler(Database(self.db_path), lease_seconds=5)
        restarted.initialize()
        dispatcher = Dispatcher(
            restarted,
            adapters={"codex": FakeAdapter()},
            targets={"codex": self.target},
            workspace_root=self.root,
        )
        daemon = SchedulerDaemon(
            dispatcher, poll_seconds=0.01, heartbeat_seconds=0.02
        )
        try:
            recovery = dispatcher.recover(after_expiry=daemon._start_supervision)
        finally:
            daemon._stop_supervision()

        self.assertEqual(recovery["retried"], 1)
        self.assertEqual(restarted.get("tasks", ids["unstarted"])["state"], "QUEUED")
        lease = restarted.get("leases", claim.lease_id)
        self.assertEqual(lease["state"], "EXPIRED")
        self.assertEqual(lease["expires_at"], original_expiry)
        failure = restarted.list("failures")[-1]
        self.assertEqual(failure["failure_code"], "CLAIM_ORPHANED")

    def test_recovery_failure_stops_heartbeat_and_clears_admissions(self) -> None:
        self.scheduler.resize_partition("general", 2)
        self.scheduler.reconcile_pool()
        _batch, _ids = self.scheduler.submit_batch(
            [TaskSpec("recover-one", {}, priority=2), TaskSpec("recover-two", {})]
        )
        claims = []
        for order in (1, 2):
            claim = self.scheduler.claim_next_available()
            execution_id, _request = self.scheduler.create_execution(claim)
            self.scheduler.confirm_execution_running(
                claim.attempt_id,
                claim.lease_epoch,
                execution_id,
                runtime_handle={"recovery_order": order},
            )
            with self.scheduler.db.transaction() as conn:
                conn.execute(
                    "UPDATE executions SET started_at=? WHERE id=?",
                    (float(order), execution_id),
                )
            claims.append(claim)

        restarted = Scheduler(Database(self.db_path), lease_seconds=5)
        restarted.initialize()
        dispatcher = Dispatcher(
            restarted,
            adapters={"codex": RecoveryFailsAfterAdmissionAdapter()},
            targets={"codex": self.target},
            workspace_root=self.root,
        )
        daemon = SchedulerDaemon(
            dispatcher, poll_seconds=0.001, heartbeat_seconds=0.005
        )

        with self.assertRaisesRegex(RuntimeError, "injected recovery"):
            daemon.run_until_idle(max_wait_seconds=1)

        self.assertIsNone(daemon._heartbeat_thread)
        self.assertEqual(dispatcher.supervised_attempt_ids(), set())
        lease_after_cleanup = restarted.get("leases", claims[0].lease_id)["expires_at"]
        time.sleep(0.03)
        self.assertEqual(
            restarted.get("leases", claims[0].lease_id)["expires_at"],
            lease_after_cleanup,
        )

    def test_restart_ambiguous_execution_is_not_admitted_for_renewal(self) -> None:
        policy = RetryPolicy(
            max_attempts=2,
            retry_classes=(FailureClass.EXECUTION_LOST,),
            base_backoff_seconds=0,
            max_backoff_seconds=0,
        )
        _batch, ids = self.scheduler.submit_batch(
            [TaskSpec("ambiguous-restart", {}, retry_policy=policy)]
        )
        claim = self.scheduler.claim_next_available(now=time.time())
        execution_id, request_id = self.scheduler.create_execution(claim)
        self.scheduler.record_start_ambiguity(
            claim.attempt_id,
            claim.lease_epoch,
            execution_id,
            runtime_handle={"request_id": request_id},
            detail="start identity remains ambiguous",
        )
        original_expiry = self.scheduler.get("leases", claim.lease_id)["expires_at"]

        restarted = Scheduler(Database(self.db_path), lease_seconds=5)
        restarted.initialize()
        dispatcher = Dispatcher(
            restarted,
            adapters={"codex": AlwaysAmbiguousAdapter()},
            targets={"codex": self.target},
            workspace_root=self.root,
        )
        daemon = SchedulerDaemon(
            dispatcher, poll_seconds=0.001, heartbeat_seconds=0.005
        )
        try:
            dispatcher.recover(after_expiry=daemon._start_supervision)
            time.sleep(0.02)
            dispatcher.poll_executions(recovery=True)
            self.assertEqual(dispatcher.supervised_attempt_ids(), set())
            self.assertEqual(dispatcher.renew_supervised_leases(), 0)
        finally:
            daemon._stop_supervision()

        self.assertEqual(
            restarted.get("leases", claim.lease_id)["expires_at"], original_expiry
        )
        expired = restarted.expire_leases(now=original_expiry + 1)
        self.assertEqual(expired["retried"], 1)
        self.assertEqual(restarted.get("leases", claim.lease_id)["state"], "EXPIRED")
        self.assertEqual(
            restarted.get("tasks", ids["ambiguous-restart"])["state"], "RETRY_WAIT"
        )

    def test_same_daemon_ambiguous_start_is_not_admitted_for_renewal(self) -> None:
        policy = RetryPolicy(
            max_attempts=2,
            retry_classes=(FailureClass.EXECUTION_LOST,),
            base_backoff_seconds=0,
            max_backoff_seconds=0,
        )
        _batch, ids = self.scheduler.submit_batch(
            [TaskSpec("ambiguous-local", {}, retry_policy=policy)]
        )
        dispatcher = Dispatcher(
            self.scheduler,
            adapters={"codex": AlwaysAmbiguousAdapter()},
            targets={"codex": self.target},
            workspace_root=self.root,
        )
        daemon = SchedulerDaemon(
            dispatcher, poll_seconds=0.001, heartbeat_seconds=0.005
        )
        daemon._start_supervision()
        try:
            self.assertEqual(dispatcher.dispatch_ready(), 0)
            execution = self.scheduler.list("executions", state="UNKNOWN")[0]
            lease = self.scheduler.list("leases", state="ACTIVE")[0]
            original_expiry = lease["expires_at"]
            time.sleep(0.02)
            dispatcher.poll_executions()
            self.assertEqual(dispatcher.supervised_attempt_ids(), set())
            self.assertEqual(dispatcher.renew_supervised_leases(), 0)
        finally:
            daemon._stop_supervision()

        self.assertEqual(
            self.scheduler.get("leases", lease["id"])["expires_at"], original_expiry
        )
        expired = self.scheduler.expire_leases(now=original_expiry + 1)
        self.assertEqual(expired["retried"], 1)
        self.assertEqual(self.scheduler.get("leases", lease["id"])["state"], "EXPIRED")
        self.assertEqual(
            self.scheduler.get("tasks", ids["ambiguous-local"])["state"], "RETRY_WAIT"
        )
        self.assertEqual(
            self.scheduler.get("executions", execution["id"])["state"], "UNKNOWN"
        )

    def test_stale_running_start_does_not_escape_or_gain_admission(self) -> None:
        _batch, _ids = self.scheduler.submit_batch([TaskSpec("stale-start", {})])
        dispatcher = Dispatcher(
            self.scheduler,
            adapters={"codex": FakeAdapter()},
            targets={"codex": self.target},
            workspace_root=self.root,
        )

        with patch.object(
            self.scheduler,
            "confirm_running_and_renew_authority",
            side_effect=StaleAuthority("lease expired during bounded start"),
        ):
            self.assertEqual(dispatcher.dispatch_ready(), 0)

        execution = self.scheduler.list("executions")[0]
        self.assertEqual(execution["state"], "RUNNING")
        handle = json.loads(execution["runtime_handle_json"])
        self.assertEqual(handle["execution_id"], execution["id"])
        incarnation = self.scheduler.get("incarnations", execution["incarnation_id"])
        self.assertEqual(json.loads(incarnation["runtime_handle_json"]), handle)
        self.assertEqual(dispatcher.supervised_attempt_ids(), set())
        self.assertEqual(dispatcher.renew_supervised_leases(), 0)

    def test_late_ambiguous_start_is_physical_history_not_dispatcher_failure(self) -> None:
        _batch, _ids = self.scheduler.submit_batch([TaskSpec("late-ambiguous", {})])
        dispatcher = Dispatcher(
            self.scheduler,
            adapters={"codex": AlwaysAmbiguousAdapter()},
            targets={"codex": self.target},
            workspace_root=self.root,
        )
        with patch.object(
            self.scheduler,
            "record_start_ambiguity",
            side_effect=StaleAuthority("lease expired during bounded start"),
        ):
            self.assertEqual(dispatcher.dispatch_ready(), 0)

        execution = self.scheduler.list("executions")[0]
        self.assertEqual(execution["state"], "UNKNOWN")
        handle = json.loads(execution["runtime_handle_json"])
        self.assertEqual(handle["execution_id"], execution["id"])
        self.assertEqual(dispatcher.supervised_attempt_ids(), set())

    def test_late_failed_start_is_physical_history_not_dispatcher_failure(self) -> None:
        _batch, _ids = self.scheduler.submit_batch([TaskSpec("late-failed", {})])
        dispatcher = Dispatcher(
            self.scheduler,
            adapters={"codex": FailedStartAdapter()},
            targets={"codex": self.target},
            workspace_root=self.root,
        )
        with patch.object(
            self.scheduler,
            "nack",
            side_effect=StaleAuthority("lease expired during bounded start"),
        ):
            self.assertEqual(dispatcher.dispatch_ready(), 0)

        execution = self.scheduler.list("executions")[0]
        self.assertEqual(execution["state"], "FAILED")
        self.assertEqual(execution["failure_code"], "LATE_START_FAILED")
        self.assertEqual(
            json.loads(execution["runtime_handle_json"])["thread_id"],
            "late-failed-thread",
        )
        self.assertEqual(dispatcher.supervised_attempt_ids(), set())

    def test_stale_reconcile_running_persists_the_new_physical_locator(self) -> None:
        _batch, _ids = self.scheduler.submit_batch([TaskSpec("stale-reconcile", {})])
        agent = self.scheduler.list("logical_agents", state="READY")[0]
        claim = self.scheduler.claim_next(agent["id"])
        execution_id, request_id = self.scheduler.create_execution(claim)
        self.scheduler.record_start_ambiguity(
            claim.attempt_id,
            claim.lease_epoch,
            execution_id,
            runtime_handle={"request_id": request_id},
        )
        dispatcher = Dispatcher(
            self.scheduler,
            adapters={"codex": ReconcileRunningWithNewHandleAdapter()},
            targets={"codex": self.target},
            workspace_root=self.root,
        )
        with patch.object(
            self.scheduler,
            "confirm_running_and_renew_authority",
            side_effect=StaleAuthority("recovery authority ended"),
        ):
            self.assertEqual(dispatcher.poll_executions(recovery=True), 1)

        execution = self.scheduler.get("executions", execution_id)
        handle = json.loads(execution["runtime_handle_json"])
        self.assertEqual(execution["state"], "RUNNING")
        self.assertEqual(handle["thread_id"], "recovered-thread")
        self.assertEqual(handle["turn_id"], "recovered-turn")
        self.assertEqual(dispatcher.supervised_attempt_ids(), set())

    def test_restart_reconciles_completed_execution_before_notification(self) -> None:
        batch_id, ids = self.scheduler.submit_batch([TaskSpec("recovered", {})])
        agent = self.scheduler.list("logical_agents", state="READY")[0]
        claim = self.scheduler.claim_next(agent["id"])
        execution_id, request_id = self.scheduler.create_execution(claim)
        self.scheduler.record_start_ambiguity(
            claim.attempt_id,
            claim.lease_epoch,
            execution_id,
            runtime_handle={"request_id": request_id, "thread_id": "stored"},
            detail="response lost after start",
        )
        restarted = Scheduler(Database(self.db_path), lease_seconds=5)
        restarted.initialize()
        adapter = RecoveredAdapter()
        dispatcher = Dispatcher(
            restarted,
            adapters={"codex": adapter},
            targets={"codex": self.target},
            workspace_root=self.root,
        )
        recovery = dispatcher.recover()
        self.assertGreaterEqual(recovery["observed"], 1)
        self.assertEqual(restarted.get("tasks", ids["recovered"])["state"], "COMPLETED")
        self.assertEqual(restarted.get("batches", batch_id)["state"], "COMPLETED")

    def test_root_failure_does_not_change_result_or_batch(self) -> None:
        batch, ids = self.scheduler.submit_batch([TaskSpec("done", {})])
        agent = self.scheduler.list("logical_agents", state="READY")[0]
        claim = self.scheduler.claim_next(agent["id"])
        result = self.scheduler.ack_success(
            claim.attempt_id, claim.lease_epoch, execution_id=None, payload={"durable": True}
        )
        dispatcher = OutboxDispatcher(self.scheduler.db, BrokenBridge())
        self.assertEqual(dispatcher.deliver_pending(), 0)
        self.assertEqual(self.scheduler.get("results", result)["state"], "AVAILABLE")
        self.assertEqual(self.scheduler.get("batches", batch)["state"], "COMPLETED")
        pending = self.scheduler.list("notification_outbox", state="PENDING")
        self.assertTrue(all(row["delivery_attempts"] == 1 for row in pending))

    def test_filesystem_delivery_is_idempotent_and_acknowledgeable(self) -> None:
        _batch, _ids = self.scheduler.submit_batch([TaskSpec("done", {})])
        agent = self.scheduler.list("logical_agents", state="READY")[0]
        claim = self.scheduler.claim_next(agent["id"])
        self.scheduler.ack_success(
            claim.attempt_id, claim.lease_epoch, execution_id=None, payload={}
        )
        bridge = FilesystemRootBridge(self.root / "events")
        dispatcher = OutboxDispatcher(self.scheduler.db, bridge)
        delivered = dispatcher.deliver_pending()
        self.assertEqual(delivered, 1)
        files_before = sorted(path.name for path in (self.root / "events").glob("*.json"))
        self.assertEqual(dispatcher.deliver_pending(), 0)
        self.assertEqual(files_before, sorted(path.name for path in (self.root / "events").glob("*.json")))
        event = self.scheduler.list("notification_outbox", state="DELIVERED")[0]
        dispatcher.acknowledge(event["id"])
        self.assertEqual(self.scheduler.get("notification_outbox", event["id"])["state"], "ACKED")

    def test_configuration_is_strict(self) -> None:
        source = Path(__file__).resolve().parents[1] / "config" / "scheduler.example.toml"
        config = load_config(source)
        self.assertEqual(config.schema_version, 1)
        self.assertEqual(config.partitions[0].name, "general")
        invalid = self.root / "invalid.toml"
        invalid.write_text("schema_version=1\nunknown=true\n", encoding="utf-8")
        with self.assertRaises(ConfigurationError):
            load_config(invalid)
        wrong_bool = self.root / "wrong-bool.toml"
        wrong_bool.write_text(
            """
schema_version = 1
[execution_profiles.default]
[[execution_targets]]
name = "codex"
adapter = "codex_app_server"
attempt_isolation = "false"
termination_confirmation = true
""",
            encoding="utf-8",
        )
        with self.assertRaises(ConfigurationError):
            load_config(wrong_bool)
        invalid_sandbox = self.root / "invalid-sandbox.toml"
        invalid_sandbox.write_text(
            source.read_text(encoding="utf-8").replace(
                'sandbox = "workspace-write"', 'sandbox = "workspaceWrite"'
            ),
            encoding="utf-8",
        )
        with self.assertRaises(ConfigurationError):
            load_config(invalid_sandbox)
        unsafe_cadence = self.root / "unsafe-cadence.toml"
        unsafe_cadence.write_text(
            source.read_text(encoding="utf-8").replace(
                "heartbeat_seconds = 30", "heartbeat_seconds = 120"
            ),
            encoding="utf-8",
        )
        with self.assertRaises(ConfigurationError):
            load_config(unsafe_cadence)

    def test_codex_failure_classification_is_adapter_local(self) -> None:
        self.assertEqual(
            CodexAppServerAdapter._classify_failure("HTTP 429 rate limit"),
            FailureClass.TRANSIENT_EXTERNAL,
        )
        self.assertEqual(
            CodexAppServerAdapter._classify_failure("permission denied"),
            FailureClass.PERMISSION_FAILURE,
        )
        self.assertEqual(
            CodexAppServerAdapter._classify_failure("HTTP 502 bad gateway"),
            FailureClass.RESOURCE_UNAVAILABLE,
        )
        self.assertEqual(
            CodexAppServerAdapter._classify_failure(
                '{"codexErrorInfo":"usageLimitExceeded"}'
            ),
            FailureClass.RESOURCE_UNAVAILABLE,
        )
        self.assertEqual(
            CodexAppServerAdapter._classify_failure("connection reset by upstream"),
            FailureClass.TRANSIENT_EXTERNAL,
        )

    def test_codex_root_bridge_wakes_existing_thread_without_result_transport(self) -> None:
        FakeRootSession.instances.clear()
        bridge = CodexAppServerRootBridge(
            root_thread_id="root-thread",
            session_factory=FakeRootSession,
        )
        outcome = bridge.deliver(
            "event-1",
            "BATCH_RESULTS_READY",
            {"result_id": "result-1", "result_body": "must-not-be-transported"},
        )
        self.assertTrue(outcome.delivered)
        session = FakeRootSession.instances[-1]
        self.assertEqual(session.requests[0], ("thread/resume", {"threadId": "root-thread"}))
        notice = session.requests[1][1]["input"][0]["text"]
        self.assertIn("event-1", notice)
        self.assertIn("result-1", notice)
        self.assertNotIn("must-not-be-transported", notice)
        self.assertTrue(session.closed)

    def test_codex_root_bridge_reconciles_completed_persisted_exact_turn(self) -> None:
        FakePersistedRootSession.instances.clear()
        bridge = CodexAppServerRootBridge(
            root_thread_id="root-thread",
            completion_timeout=0.1,
            reconcile_interval=0.001,
            session_factory=FakePersistedRootSession,
        )
        outcome = bridge.deliver("event-1", "BATCH_RESULTS_READY", {"batch_id": "batch-1"})
        self.assertTrue(outcome.delivered)
        self.assertIn("persisted reconciliation", outcome.detail)
        session = FakePersistedRootSession.instances[-1]
        self.assertIn(
            ("thread/read", {"threadId": "root-thread", "includeTurns": True}),
            session.requests,
        )
        self.assertTrue(session.closed)

    def test_codex_root_bridge_reconciles_interrupted_persisted_exact_turn(self) -> None:
        FakePersistedInterruptedRootSession.instances.clear()
        bridge = CodexAppServerRootBridge(
            root_thread_id="root-thread",
            completion_timeout=0.1,
            reconcile_interval=0.001,
            session_factory=FakePersistedInterruptedRootSession,
        )
        outcome = bridge.deliver("event-1", "BATCH_RESULTS_READY", {"batch_id": "batch-1"})
        self.assertFalse(outcome.delivered)
        self.assertEqual(
            outcome.detail,
            "Codex Root turn ended as interrupted (persisted reconciliation)",
        )
        self.assertTrue(FakePersistedInterruptedRootSession.instances[-1].closed)

    def test_initialize_does_not_regress_ready_lifecycle(self) -> None:
        self.scheduler.set_lifecycle("READY")
        restarted = Scheduler(Database(self.db_path), lease_seconds=5)
        restarted.initialize()
        self.assertEqual(restarted.status()["lifecycle"]["state"], "READY")

    def test_codex_root_bridge_configuration_is_strict(self) -> None:
        source = Path(__file__).resolve().parents[1] / "config" / "scheduler.example.toml"
        configured = self.root / "codex-root.toml"
        text = source.read_text(encoding="utf-8").replace(
            'kind = "filesystem"',
            'kind = "codex_app_server"\nroot_thread_id = "root-thread"',
        )
        configured.write_text(text, encoding="utf-8")
        config = load_config(configured)
        self.assertEqual(config.root_bridge.kind, "codex_app_server")
        self.assertEqual(config.root_bridge.root_thread_id, "root-thread")

    def test_codex_adapter_recovers_outcome_from_persisted_thread(self) -> None:
        adapter = CodexAppServerAdapter(session_factory=FakeStoredSession)
        outcome = adapter.collect_outcome(
            {"thread_id": "persisted-thread", "turn_id": "stored-turn"}
        )
        self.assertEqual(outcome.state, ExecutionState.SUCCEEDED)
        self.assertEqual(outcome.payload, {"restored": True})
        self.assertTrue(FakeStoredSession.instances[-1].closed)

    def test_codex_adapter_classifies_persisted_interruption_as_execution_lost(self) -> None:
        adapter = CodexAppServerAdapter(session_factory=FakeInterruptedSession)
        outcome = adapter.collect_outcome(
            {"thread_id": "persisted-thread", "turn_id": "interrupted-turn"}
        )
        self.assertEqual(outcome.state, ExecutionState.FAILED)
        self.assertEqual(outcome.failure_class, FailureClass.EXECUTION_LOST)
        self.assertEqual(outcome.failure_code, "CODEX_STORED_TURN_INTERRUPTED")

    def test_codex_interrupt_closes_and_detaches_physical_session(self) -> None:
        adapter = CodexAppServerAdapter(session_factory=FakeRootSession)
        session = FakeRootSession((), None, 1)
        adapter._sessions["physical-session"] = session
        observation = adapter.interrupt_execution(
            {
                "adapter_session_id": "physical-session",
                "thread_id": "thread",
                "turn_id": "root-turn",
            }
        )
        self.assertEqual(observation.state, ExecutionState.TERMINATED)
        self.assertTrue(observation.quiescent_confirmed)
        self.assertTrue(session.closed)
        self.assertNotIn("physical-session", adapter._sessions)

    def test_codex_exited_session_is_detached_before_outcome_recovery(self) -> None:
        adapter = CodexAppServerAdapter(session_factory=FakeRootSession)
        session = FakeRootSession((), None, 1)
        session.process = FakeExitedProcess()
        adapter._sessions["exited-session"] = session
        observation = adapter.observe_execution(
            {
                "adapter_session_id": "exited-session",
                "thread_id": "thread",
                "turn_id": "missing-turn",
            }
        )
        self.assertEqual(observation.state, ExecutionState.LOST)
        self.assertTrue(session.closed)
        self.assertNotIn("exited-session", adapter._sessions)


if __name__ == "__main__":
    unittest.main()
