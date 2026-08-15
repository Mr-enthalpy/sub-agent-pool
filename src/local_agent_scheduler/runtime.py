from __future__ import annotations

import json
import signal
import threading
import time
from pathlib import Path
from typing import Callable, Mapping

from .adapters.base import ExecutionAdapter
from .config import ExecutionTargetConfig
from .core import Scheduler
from .enums import AgentState, ExecutionState, FailureClass, WorkspaceMode
from .errors import ConfigurationError, StaleAuthority
from .models import ExecutionObservation, ExecutionRequest
from .root_bridge import OutboxDispatcher
from .storage import json_loads


class Dispatcher:
    def __init__(
        self,
        scheduler: Scheduler,
        *,
        adapters: Mapping[str, ExecutionAdapter],
        targets: Mapping[str, ExecutionTargetConfig],
        execution_profiles: set[str] | None = None,
        workspace_root: str | Path,
        outbox: OutboxDispatcher | None = None,
    ) -> None:
        self.scheduler = scheduler
        self.adapters = dict(adapters)
        self.targets = dict(targets)
        self.execution_profiles = (
            None if execution_profiles is None else set(execution_profiles)
        )
        self.workspace_root = str(Path(workspace_root).resolve())
        self.outbox = outbox
        self._supervision_lock = threading.Lock()
        self._supervision_admissions: dict[str, str] = {}

    def recover(self, *, after_expiry: Callable[[], None] | None = None) -> dict[str, int]:
        self.clear_supervision_admissions()
        self.scheduler.set_lifecycle("RECOVERY")
        lease_result = self.scheduler.expire_leases(
            recover_unstarted=True,
        )
        if after_expiry is not None:
            after_expiry()
        observed = self.poll_executions(recovery=True)
        self.scheduler.promote_retry_wait()
        pool_result = self.scheduler.reconcile_pool()
        revived = self.scheduler.revive_eligible_agents()
        self.scheduler.set_lifecycle("READY")
        return {
            "observed": observed,
            "retried": lease_result["retried"],
            "suspended": lease_result["suspended"],
            "born": pool_result["born"],
            "retired": pool_result["retired"],
            "revived": revived,
        }

    def tick(self) -> dict[str, int]:
        self.scheduler.promote_retry_wait()
        expired = self.scheduler.expire_leases()
        pool = self.scheduler.reconcile_pool()
        revived = self.scheduler.revive_eligible_agents()
        affinity_births = self.scheduler.ensure_task_consumers()
        observed = self.poll_executions()
        dispatched = self.dispatch_ready()
        return {
            "dispatched": dispatched,
            "observed": observed,
            "retried": expired["retried"],
            "suspended": expired["suspended"],
            "born": pool["born"],
            "retired": pool["retired"],
            "revived": revived,
            "affinity_births": affinity_births,
            # Root notification delivery is deliberately isolated in the
            # daemon notifier thread.  A slow bridge must never stall this
            # execution-supervision control loop.
            "notifications": 0,
        }

    def _admit_supervision(self, execution_id: str, attempt_id: str) -> None:
        """Admit only work this daemon has positively observed as RUNNING."""

        with self._supervision_lock:
            self._supervision_admissions[execution_id] = attempt_id

    def _revoke_supervision(self, execution_id: str) -> None:
        with self._supervision_lock:
            self._supervision_admissions.pop(execution_id, None)

    def clear_supervision_admissions(self) -> None:
        with self._supervision_lock:
            self._supervision_admissions.clear()

    def supervised_attempt_ids(self) -> set[str]:
        """Return authority positively admitted by this daemon instance."""

        with self._supervision_lock:
            return set(self._supervision_admissions.values())

    def renew_supervised_leases(self) -> int:
        return self.scheduler.renew_active_leases(self.supervised_attempt_ids())

    def _record_start_physical_history(
        self,
        execution_id: str,
        start,
        *,
        state: ExecutionState,
        runtime_handle: Mapping[str, object] | None = None,
        terminal_confirmed: bool = False,
        quiescent_confirmed: bool = False,
    ) -> None:
        """Persist a start observation without restoring Task/Lease authority."""

        self.scheduler.record_physical_outcome(
            execution_id,
            state=state,
            runtime_handle=(
                start.runtime_handle if runtime_handle is None else runtime_handle
            ),
            payload={"detail": start.detail} if start.detail is not None else None,
            failure_class=(
                start.failure_class
                or (FailureClass.START_FAILURE if state is ExecutionState.FAILED else None)
            ),
            failure_code=start.failure_code,
            terminal_confirmed=terminal_confirmed,
            quiescent_confirmed=quiescent_confirmed,
        )

    def dispatch_ready(self) -> int:
        dispatched = 0
        while True:
            claim = self.scheduler.claim_next_available()
            if claim is None:
                break
            adapter = self.adapters.get(claim.execution_target)
            target = self.targets.get(claim.execution_target)
            profile_unavailable = bool(
                self.execution_profiles is not None
                and claim.execution_profile not in self.execution_profiles
            )
            if adapter is None or target is None or profile_unavailable:
                detail = (
                    f"execution target {claim.execution_target!r} is unavailable"
                    if adapter is None or target is None
                    else f"execution profile {claim.execution_profile!r} is unavailable"
                )
                self.scheduler.nack(
                    claim.attempt_id,
                    claim.lease_epoch,
                    failure_class=FailureClass.RESOURCE_UNAVAILABLE,
                    failure_code="EXECUTION_CONFIGURATION_UNAVAILABLE",
                    detail=detail,
                    terminal_confirmed=True,
                    quiescent_confirmed=True,
                )
                continue
            agent = self.scheduler.get("logical_agents", claim.logical_agent_id)
            execution_id, request_id = self.scheduler.create_execution(
                claim,
                attempt_isolation=target.attempt_isolation,
            )
            execution = self.scheduler.get("executions", execution_id)
            incarnation_id = execution["incarnation_id"]
            incarnation = self.scheduler.get("incarnations", incarnation_id)
            request = ExecutionRequest(
                request_id=request_id,
                execution_id=execution_id,
                task_id=claim.task_id,
                attempt_id=claim.attempt_id,
                lease_epoch=claim.lease_epoch,
                logical_agent_id=claim.logical_agent_id,
                incarnation_id=incarnation_id,
                execution_target=claim.execution_target,
                execution_profile=claim.execution_profile,
                cwd=self.workspace_root,
                prompt=self._render_prompt(claim, json_loads(agent["continuity_json"], {})),
                workspace_mode=claim.workspace_mode,
                continuity=json_loads(agent["continuity_json"], {}),
                incarnation_runtime_handle=json_loads(incarnation["runtime_handle_json"], {}),
            )
            start = adapter.start_execution(request)
            if start.state == ExecutionState.RUNNING:
                try:
                    self.scheduler.confirm_running_and_renew_authority(
                        claim.attempt_id,
                        claim.lease_epoch,
                        execution_id,
                        runtime_handle=start.runtime_handle,
                    )
                except StaleAuthority:
                    self._revoke_supervision(execution_id)
                    self._record_start_physical_history(
                        execution_id, start, state=ExecutionState.RUNNING
                    )
                else:
                    self._admit_supervision(execution_id, claim.attempt_id)
                    dispatched += 1
            elif start.ambiguous or start.state == ExecutionState.UNKNOWN:
                try:
                    self.scheduler.record_start_ambiguity(
                        claim.attempt_id,
                        claim.lease_epoch,
                        execution_id,
                        runtime_handle=start.runtime_handle,
                        detail=start.detail,
                    )
                except StaleAuthority:
                    self._revoke_supervision(execution_id)
                    self._record_start_physical_history(
                        execution_id, start, state=ExecutionState.UNKNOWN
                    )
            else:
                try:
                    self.scheduler.nack(
                        claim.attempt_id,
                        claim.lease_epoch,
                        failure_class=start.failure_class or FailureClass.START_FAILURE,
                        execution_id=execution_id,
                        failure_code=start.failure_code,
                        detail=start.detail,
                        terminal_confirmed=True,
                        quiescent_confirmed=True,
                    )
                except StaleAuthority:
                    self._revoke_supervision(execution_id)
                    physical_state = (
                        start.state
                        if start.state
                        in {
                            ExecutionState.FAILED,
                            ExecutionState.LOST,
                            ExecutionState.TERMINATED,
                        }
                        else ExecutionState.FAILED
                    )
                    self._record_start_physical_history(
                        execution_id,
                        start,
                        state=physical_state,
                        terminal_confirmed=True,
                        quiescent_confirmed=True,
                    )
        return dispatched

    def poll_executions(self, *, recovery: bool = False) -> int:
        count = 0
        executions = []
        for state in (ExecutionState.RUNNING, ExecutionState.STARTING, ExecutionState.UNKNOWN):
            executions.extend(self.scheduler.list("executions", state=state.value))
        for execution in executions:
            adapter = self.adapters.get(execution["execution_target"])
            if adapter is None:
                self._revoke_supervision(execution["id"])
                try:
                    attempt = self.scheduler.get("attempts", execution["attempt_id"])
                    self.scheduler.nack(
                        execution["attempt_id"],
                        int(attempt["lease_epoch"]),
                        failure_class=FailureClass.RESOURCE_UNAVAILABLE,
                        execution_id=execution["id"],
                        failure_code="EXECUTION_ADAPTER_UNAVAILABLE",
                        detail=(
                            f"execution adapter for target "
                            f"{execution['execution_target']!r} is unavailable"
                        ),
                        terminal_confirmed=False,
                        quiescent_confirmed=False,
                    )
                except StaleAuthority:
                    self.scheduler.record_physical_outcome(
                        execution["id"],
                        state=ExecutionState.UNKNOWN,
                        runtime_handle=json_loads(
                            execution["runtime_handle_json"], {}
                        ),
                        failure_class=FailureClass.RESOURCE_UNAVAILABLE,
                        failure_code="EXECUTION_ADAPTER_UNAVAILABLE",
                    )
                count += 1
                continue
            handle = json_loads(execution["runtime_handle_json"], {})
            if execution["state"] in (ExecutionState.STARTING.value, ExecutionState.UNKNOWN.value):
                start = adapter.reconcile_start(execution["request_id"], handle)
                if start.runtime_handle:
                    handle = dict(start.runtime_handle)
                if start.state == ExecutionState.RUNNING:
                    try:
                        attempt = self.scheduler.get("attempts", execution["attempt_id"])
                        self.scheduler.confirm_running_and_renew_authority(
                            execution["attempt_id"],
                            int(attempt["lease_epoch"]),
                            execution["id"],
                            runtime_handle=start.runtime_handle,
                        )
                        self._admit_supervision(
                            execution["id"], execution["attempt_id"]
                        )
                    except StaleAuthority:
                        self._revoke_supervision(execution["id"])
                        self._record_start_physical_history(
                            execution["id"],
                            start,
                            state=ExecutionState.RUNNING,
                            runtime_handle=handle,
                        )
                    count += 1
                    continue
                if start.ambiguous:
                    self._revoke_supervision(execution["id"])
                    self._record_start_physical_history(
                        execution["id"],
                        start,
                        state=ExecutionState.UNKNOWN,
                        runtime_handle=handle,
                    )
                    continue
                if start.state in {
                    ExecutionState.SUCCEEDED,
                    ExecutionState.FAILED,
                    ExecutionState.LOST,
                    ExecutionState.TERMINATED,
                }:
                    observation = ExecutionObservation(
                        start.state,
                        terminal_confirmed=True,
                        quiescent_confirmed=True,
                        detail=start.detail,
                    )
                else:
                    observation = adapter.observe_execution(handle)
            else:
                observation = adapter.observe_execution(handle)
            if observation.state == ExecutionState.RUNNING:
                try:
                    attempt = self.scheduler.get("attempts", execution["attempt_id"])
                    self.scheduler.heartbeat(
                        execution["attempt_id"], int(attempt["lease_epoch"])
                    )
                    self._admit_supervision(
                        execution["id"], execution["attempt_id"]
                    )
                except StaleAuthority:
                    self._revoke_supervision(execution["id"])
                count += 1
                continue
            if observation.state in (ExecutionState.UNKNOWN, ExecutionState.STARTING):
                self._revoke_supervision(execution["id"])
                continue
            self._revoke_supervision(execution["id"])
            outcome = adapter.collect_outcome(handle)
            terminal_confirmed = (
                observation.terminal_confirmed or outcome.terminal_confirmed
            )
            quiescent_confirmed = (
                observation.quiescent_confirmed or outcome.quiescent_confirmed
            )
            try:
                attempt = self.scheduler.get("attempts", execution["attempt_id"])
                epoch = int(attempt["lease_epoch"])
                if outcome.state == ExecutionState.SUCCEEDED:
                    self.scheduler.ack_success(
                        execution["attempt_id"],
                        epoch,
                        execution_id=execution["id"],
                        payload=outcome.payload or {},
                        summary=outcome.summary,
                        continuity_capsule=outcome.checkpoint,
                        quiescent_confirmed=quiescent_confirmed,
                        incarnation_reusable=outcome.incarnation_reusable,
                    )
                else:
                    self.scheduler.nack(
                        execution["attempt_id"],
                        epoch,
                        failure_class=outcome.failure_class or FailureClass.UNKNOWN,
                        execution_id=execution["id"],
                        failure_code=outcome.failure_code,
                        failure_signature=outcome.failure_signature,
                        terminal_confirmed=terminal_confirmed,
                        quiescent_confirmed=quiescent_confirmed,
                        incarnation_reusable=outcome.incarnation_reusable,
                    )
            except StaleAuthority:
                self.scheduler.record_physical_outcome(
                    execution["id"],
                    state=outcome.state,
                    runtime_handle=handle,
                    payload=outcome.payload,
                    failure_class=outcome.failure_class,
                    failure_code=outcome.failure_code,
                    failure_signature=outcome.failure_signature,
                    terminal_confirmed=terminal_confirmed,
                    quiescent_confirmed=quiescent_confirmed,
                )
            count += 1
        return count

    def interrupt_execution(self, execution_id: str, *, terminate: bool = False) -> dict[str, object]:
        execution = self.scheduler.get("executions", execution_id)
        self._revoke_supervision(execution_id)
        adapter = self.adapters.get(execution["execution_target"])
        if adapter is None:
            raise ConfigurationError(
                f"execution target {execution['execution_target']!r} is unavailable"
            )
        handle = json_loads(execution["runtime_handle_json"], {})
        observation = (
            adapter.terminate_execution(handle)
            if terminate
            else adapter.interrupt_execution(handle)
        )
        self.scheduler.record_physical_outcome(
            execution_id,
            state=(
                ExecutionState.LOST
                if observation.state == ExecutionState.UNKNOWN
                else observation.state
            ),
            failure_class=FailureClass.EXECUTION_LOST,
            failure_code="ROOT_TERMINATION",
            terminal_confirmed=observation.terminal_confirmed,
            quiescent_confirmed=observation.quiescent_confirmed,
        )
        self.scheduler.cancel_task(
            execution["task_id"],
            quiescence_confirmed=observation.quiescent_confirmed,
        )
        return {
            "execution_id": execution_id,
            "state": observation.state.value,
            "terminal_confirmed": observation.terminal_confirmed,
            "quiescent_confirmed": observation.quiescent_confirmed,
            "detail": observation.detail,
        }

    @staticmethod
    def _render_prompt(claim, continuity: Mapping[str, object]) -> str:
        sections = [
            "LOCAL AGENT SCHEDULER TASK",
            f"TASK_ID\n{claim.task_id}",
            f"ATTEMPT_ID\n{claim.attempt_id}",
            f"LEASE_EPOCH\n{claim.lease_epoch}",
            f"WORKSTREAM\n{claim.workstream_id or 'none'}",
            "OBJECTIVE\n" + json.dumps(claim.payload, ensure_ascii=False, sort_keys=True),
            "ACCEPTANCE\n" + json.dumps(claim.acceptance, ensure_ascii=False, sort_keys=True),
            "COMMITTED CONTINUITY\n" + json.dumps(continuity, ensure_ascii=False, sort_keys=True),
        ]
        if claim.workspace_mode is WorkspaceMode.WRITE:
            sections.append(
                "WRITER RECOVERY RULES\n"
                "The current workspace is authoritative. Inspect assignment-scoped state and diff "
                "before writing; continue idempotently; do not revert unrelated work."
            )
        sections.append(
            "RETURN\nReturn the authoritative result only when acceptance is satisfied. "
            "Do not claim Scheduler ACK; the Scheduler validates the current lease separately."
        )
        return "\n\n".join(sections)


class SchedulerDaemon:
    def __init__(
        self,
        dispatcher: Dispatcher,
        *,
        poll_seconds: float = 1.0,
        heartbeat_seconds: float = 30.0,
    ):
        self.dispatcher = dispatcher
        self.poll_seconds = poll_seconds
        self.heartbeat_seconds = heartbeat_seconds
        self._stopping = False
        self._heartbeat_stop = threading.Event()
        self._heartbeat_thread: threading.Thread | None = None
        self._notifier_stop = threading.Event()
        self._notifier_thread: threading.Thread | None = None
        self._run_lock = threading.Lock()
        self._run_started = False

    def _begin_single_run(self) -> None:
        """Claim this daemon object for its one supported run lifecycle."""

        with self._run_lock:
            if self._run_started:
                raise RuntimeError(
                    "SchedulerDaemon objects are single-run; construct a new daemon"
                )
            self._run_started = True

    def stop(self, *_args) -> None:
        self._stopping = True
        self._heartbeat_stop.set()
        self._notifier_stop.set()

    def _heartbeat_loop(self) -> None:
        while not self._heartbeat_stop.wait(self.heartbeat_seconds):
            try:
                self.dispatcher.renew_supervised_leases()
            except Exception:
                # A transient SQLite contention must not permanently disable
                # supervision; the next bounded heartbeat retries.
                continue

    def _start_supervision(self) -> None:
        if self._heartbeat_thread and self._heartbeat_thread.is_alive():
            return
        self._heartbeat_stop.clear()
        # Close the startup window before any immediately-following adapter or
        # RootBridge call can block the dispatcher longer than the old expiry.
        self.dispatcher.renew_supervised_leases()
        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop,
            name="scheduler-lease-heartbeat",
            daemon=True,
        )
        self._heartbeat_thread.start()

    def _stop_supervision(self) -> None:
        self._heartbeat_stop.set()
        if self._heartbeat_thread:
            self._heartbeat_thread.join(timeout=max(1.0, self.heartbeat_seconds * 2))
            self._heartbeat_thread = None
        self.dispatcher.clear_supervision_admissions()

    def _notifier_loop(self) -> None:
        """Deliver only durable wakeup events and persist delivery outcome."""

        outbox = self.dispatcher.outbox
        if outbox is None:
            return
        while not self._notifier_stop.is_set():
            try:
                outbox.deliver_pending()
            except Exception:
                # OutboxDispatcher normally records bridge failures itself.
                # Unexpected notifier failures remain isolated from scheduler
                # supervision and are retried on the next bounded iteration.
                pass
            self._notifier_stop.wait(self.poll_seconds)

    def _start_notifier(self) -> None:
        if self.dispatcher.outbox is None:
            return
        if self._notifier_thread and self._notifier_thread.is_alive():
            return
        self._notifier_stop.clear()
        self._notifier_thread = threading.Thread(
            target=self._notifier_loop,
            name="scheduler-root-notifier",
            daemon=True,
        )
        self._notifier_thread.start()

    def _stop_notifier(self) -> None:
        self._notifier_stop.set()
        if self._notifier_thread:
            # Concrete RootBridge implementations must bound each deliver()
            # call.  This bounded join keeps daemon shutdown responsive while
            # leaving the durable row PENDING if the process exits first.
            self._notifier_thread.join(timeout=max(1.0, self.poll_seconds * 2))
            if not self._notifier_thread.is_alive():
                self._notifier_thread = None

    def _wait_for_due_notifications(self, deadline: float) -> None:
        """Give the independent notifier a chance to finish an idle cycle."""

        if self.dispatcher.outbox is None:
            return
        while time.monotonic() < deadline:
            pending = self.dispatcher.scheduler.db.fetch_one(
                "SELECT 1 FROM notification_outbox WHERE state='PENDING' "
                "AND next_delivery_at<=? LIMIT 1",
                (time.time(),),
            )
            if not pending:
                return
            time.sleep(min(self.poll_seconds, 0.05))

    def run(self) -> None:
        self._begin_single_run()
        signal.signal(signal.SIGINT, self.stop)
        if hasattr(signal, "SIGTERM"):
            signal.signal(signal.SIGTERM, self.stop)
        try:
            self.dispatcher.recover(after_expiry=self._start_supervision)
            self._start_notifier()
            while not self._stopping:
                self.dispatcher.tick()
                time.sleep(self.poll_seconds)
        finally:
            self._stop_notifier()
            self._stop_supervision()

    def run_until_idle(self, *, max_wait_seconds: float) -> dict[str, int]:
        """Run one bounded work cycle without orphaning live adapter sessions."""

        if max_wait_seconds <= 0:
            raise ValueError("max_wait_seconds must be positive")
        self._begin_single_run()

        try:
            self.dispatcher.recover(after_expiry=self._start_supervision)
            self._start_notifier()
            totals: dict[str, int] = {}
            deadline = time.monotonic() + max_wait_seconds
            while True:
                snapshot = self.dispatcher.tick()
                for key, value in snapshot.items():
                    totals[key] = totals.get(key, 0) + value
                active = []
                for state in (
                    ExecutionState.STARTING.value,
                    ExecutionState.RUNNING.value,
                    ExecutionState.UNKNOWN.value,
                ):
                    active.extend(self.dispatcher.scheduler.list("executions", state=state))
                if not active:
                    self._wait_for_due_notifications(deadline)
                    return totals
                if time.monotonic() >= deadline:
                    totals["timed_out"] = len(active)
                    for execution in active:
                        self.dispatcher.interrupt_execution(execution["id"], terminate=True)
                    final = self.dispatcher.tick()
                    for key, value in final.items():
                        totals[key] = totals.get(key, 0) + value
                    return totals
                time.sleep(self.poll_seconds)
        finally:
            self._stop_notifier()
            self._stop_supervision()
