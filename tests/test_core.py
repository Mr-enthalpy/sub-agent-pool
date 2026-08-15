from __future__ import annotations

import sys
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from local_agent_scheduler.core import Scheduler
from local_agent_scheduler.enums import (
    AgentState,
    BatchState,
    ContinuityPreference,
    FailureClass,
    IncarnationState,
    ResultState,
    Retention,
    TaskState,
    WorkspaceMode,
    ExecutionState,
)
from local_agent_scheduler.errors import InvalidTransition, StaleAuthority
from local_agent_scheduler.models import PartitionSpec, RetryPolicy, TaskSpec
from local_agent_scheduler.storage import Database


class SchedulerCase(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temporary.name) / "scheduler.db"
        self.scheduler = Scheduler(Database(self.db_path), lease_seconds=10)
        self.scheduler.initialize()
        self.scheduler.upsert_partition(
            PartitionSpec("general", 1, Retention.RESIDENT, "local", "default")
        )
        self.scheduler.reconcile_pool()

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def ready_agent(self, partition: str = "general") -> str:
        agents = [
            agent
            for agent in self.scheduler.list("logical_agents", state=AgentState.READY.value)
            if agent["partition_name"] == partition
        ]
        self.assertTrue(agents)
        return agents[0]["id"]

    def running_claim(self, task: TaskSpec):
        batch_id, ids = self.scheduler.submit_batch([task])
        claim = self.scheduler.claim_next(self.ready_agent())
        self.assertIsNotNone(claim)
        execution_id, _ = self.scheduler.create_execution(claim)
        execution = self.scheduler.get("executions", execution_id)
        claim = replace(claim, incarnation_id=execution["incarnation_id"])
        self.scheduler.confirm_execution_running(
            claim.attempt_id,
            claim.lease_epoch,
            execution_id,
            runtime_handle={"thread_id": "thread", "turn_id": "turn"},
        )
        return batch_id, ids[task.name], claim, execution_id

    def test_normal_result_flow_is_atomic_and_batch_does_not_wait_for_root(self) -> None:
        batch_id, task_id, claim, execution_id = self.running_claim(
            TaskSpec("inspect", {"objective": "inspect"})
        )
        result_id = self.scheduler.ack_success(
            claim.attempt_id,
            claim.lease_epoch,
            execution_id=execution_id,
            payload={"answer": 42},
            summary="done",
        )
        self.assertEqual(self.scheduler.get("tasks", task_id)["state"], TaskState.COMPLETED)
        self.assertEqual(self.scheduler.get("results", result_id)["state"], ResultState.AVAILABLE)
        self.assertEqual(self.scheduler.get("batches", batch_id)["state"], BatchState.COMPLETED)
        self.scheduler.ack_result(result_id, consumer_ref="root")
        self.assertEqual(self.scheduler.get("results", result_id)["state"], ResultState.ACKED)
        self.assertEqual(self.scheduler.get("batches", batch_id)["state"], BatchState.COMPLETED)
        event_types = {row["event_type"] for row in self.scheduler.list("notification_outbox")}
        self.assertEqual(event_types, {"BATCH_RESULTS_READY"})

    def test_each_sequential_execution_gets_a_fresh_incarnation(self) -> None:
        _batch, _task, first, first_execution = self.running_claim(TaskSpec("first", {}))
        self.scheduler.ack_success(
            first.attempt_id,
            first.lease_epoch,
            execution_id=first_execution,
            payload={"first": True},
        )
        first_incarnation = self.scheduler.get("incarnations", first.incarnation_id)
        self.assertEqual(first_incarnation["state"], IncarnationState.TERMINATED)

        _batch, _ids = self.scheduler.submit_batch([TaskSpec("second", {})])
        second = self.scheduler.claim_next(first.logical_agent_id)
        second_execution, _ = self.scheduler.create_execution(second)
        second = replace(
            second,
            incarnation_id=self.scheduler.get("executions", second_execution)[
                "incarnation_id"
            ],
        )
        self.assertNotEqual(second.incarnation_id, first.incarnation_id)
        self.assertGreater(
            self.scheduler.get("incarnations", second.incarnation_id)["generation"],
            first_incarnation["generation"],
        )
        self.assertEqual(
            self.scheduler.get("executions", second_execution)["incarnation_id"],
            second.incarnation_id,
        )
        with self.assertRaises(InvalidTransition):
            self.scheduler.create_execution(second)

    def test_stale_terminal_outcome_closes_only_the_old_incarnation(self) -> None:
        policy = RetryPolicy(
            max_attempts=2,
            retry_classes=(FailureClass.EXECUTION_LOST,),
            base_backoff_seconds=0,
            max_backoff_seconds=0,
        )
        _batch, _task, old, old_execution = self.running_claim(
            TaskSpec("replace", {}, retry_policy=policy)
        )
        retry_time = old.lease_expires_at + 1
        self.scheduler.expire_leases(now=retry_time)
        self.scheduler.promote_retry_wait(now=retry_time)
        replacement = self.scheduler.claim_next(old.logical_agent_id, now=retry_time)
        replacement_execution, _ = self.scheduler.create_execution(replacement)
        replacement = replace(
            replacement,
            incarnation_id=self.scheduler.get("executions", replacement_execution)[
                "incarnation_id"
            ],
        )
        self.assertNotEqual(old.incarnation_id, replacement.incarnation_id)

        self.scheduler.record_physical_outcome(
            old_execution,
            state=ExecutionState.SUCCEEDED,
            terminal_confirmed=True,
            quiescent_confirmed=True,
        )
        self.assertEqual(
            self.scheduler.get("incarnations", old.incarnation_id)["state"],
            IncarnationState.TERMINATED,
        )
        self.assertEqual(
            self.scheduler.get("incarnations", replacement.incarnation_id)["state"],
            IncarnationState.STARTING,
        )
        self.assertEqual(
            self.scheduler.get("tasks", replacement.task_id)["current_attempt_id"],
            replacement.attempt_id,
        )

    def test_late_terminal_confirmation_refines_lost_physical_history(self) -> None:
        _batch, _task, claim, execution_id = self.running_claim(TaskSpec("late", {}))
        self.scheduler.record_physical_outcome(
            execution_id,
            state=ExecutionState.LOST,
            terminal_confirmed=False,
            quiescent_confirmed=False,
        )
        self.assertEqual(
            self.scheduler.get("incarnations", claim.incarnation_id)["state"],
            IncarnationState.LOST,
        )
        self.scheduler.record_physical_outcome(
            execution_id,
            state=ExecutionState.TERMINATED,
            terminal_confirmed=True,
            quiescent_confirmed=True,
        )
        execution = self.scheduler.get("executions", execution_id)
        self.assertEqual(execution["state"], ExecutionState.TERMINATED)
        self.assertEqual(execution["terminal_confirmed"], 1)
        self.assertEqual(execution["quiescent_confirmed"], 1)
        self.assertEqual(
            self.scheduler.get("incarnations", claim.incarnation_id)["state"],
            IncarnationState.TERMINATED,
        )

    def test_cold_presence_blocks_duplicate_incarnation_and_target_switch(self) -> None:
        agent_id = self.ready_agent()
        with self.scheduler.db.transaction() as conn:
            incarnation_id = self.scheduler._ensure_incarnation(conn, agent_id, "local", 100)
            conn.execute(
                "UPDATE incarnations SET state='COLD' WHERE id=?", (incarnation_id,)
            )
        self.assertEqual(self.scheduler.revive_eligible_agents(), 0)
        with self.scheduler.db.transaction() as conn:
            self.assertEqual(
                self.scheduler._ensure_incarnation(conn, agent_id, "local", 101),
                incarnation_id,
            )
        with self.assertRaises(InvalidTransition):
            with self.scheduler.db.transaction() as conn:
                self.scheduler._ensure_incarnation(conn, agent_id, "other", 102)

    def test_dependency_is_not_claimable_until_parent_completes(self) -> None:
        batch_id, ids = self.scheduler.submit_batch(
            [
                TaskSpec("first", {"n": 1}),
                TaskSpec("second", {"n": 2}, dependencies=("first",)),
            ]
        )
        self.assertEqual(self.scheduler.get("tasks", ids["second"])["state"], TaskState.BLOCKED)
        first = self.scheduler.claim_next(self.ready_agent())
        self.assertEqual(first.task_id, ids["first"])
        self.scheduler.ack_success(
            first.attempt_id, first.lease_epoch, execution_id=None, payload={"ok": True}
        )
        self.assertEqual(self.scheduler.get("tasks", ids["second"])["state"], TaskState.QUEUED)
        second = self.scheduler.claim_next(self.ready_agent())
        self.assertEqual(second.task_id, ids["second"])
        self.assertEqual(self.scheduler.get("batches", batch_id)["state"], BatchState.ACTIVE)

    def test_stale_attempt_cannot_ack_or_promote_checkpoint(self) -> None:
        policy = RetryPolicy(
            max_attempts=2,
            retry_classes=(FailureClass.EXECUTION_LOST,),
            base_backoff_seconds=0,
            max_backoff_seconds=0,
        )
        _batch, task_id, old, old_execution = self.running_claim(
            TaskSpec("retry", {}, retry_policy=policy)
        )
        self.scheduler.expire_leases(now=old.lease_expires_at + 1)
        self.scheduler.promote_retry_wait(now=old.lease_expires_at + 1)
        replacement = self.scheduler.claim_next(self.ready_agent(), now=old.lease_expires_at + 1)
        self.assertGreater(replacement.lease_epoch, old.lease_epoch)
        with self.assertRaises(StaleAuthority):
            self.scheduler.ack_success(
                old.attempt_id,
                old.lease_epoch,
                execution_id=old_execution,
                payload={"stale": True},
            )
        with self.assertRaises(StaleAuthority):
            self.scheduler.promote_checkpoint(
                old.attempt_id, old.lease_epoch, {"DECISIONS": ["stale"]}
            )
        checkpoint_id = self.scheduler.promote_checkpoint(
            replacement.attempt_id,
            replacement.lease_epoch,
            {"DECISIONS": ["current"]},
        )
        self.assertTrue(checkpoint_id.startswith("checkpoint_"))
        self.scheduler.ack_success(
            replacement.attempt_id,
            replacement.lease_epoch,
            execution_id=None,
            payload={"current": True},
        )
        self.assertEqual(self.scheduler.get("tasks", task_id)["state"], TaskState.COMPLETED)

    def test_expired_lease_cannot_be_renewed_or_acknowledged_before_sweep(self) -> None:
        _batch, _ids = self.scheduler.submit_batch([TaskSpec("deadline", {})])
        claim = self.scheduler.claim_next(self.ready_agent(), now=100)
        with self.assertRaises(StaleAuthority):
            self.scheduler.heartbeat(claim.attempt_id, claim.lease_epoch, now=111)
        with self.assertRaises(StaleAuthority):
            self.scheduler.ack_success(
                claim.attempt_id,
                claim.lease_epoch,
                execution_id=None,
                payload={"late": True},
            )

    def test_retry_exhaustion_suspends_task_and_batch_with_escalation(self) -> None:
        policy = RetryPolicy(max_attempts=1, retry_classes=(FailureClass.TRANSIENT_EXTERNAL,))
        batch_id, task_id, claim, execution_id = self.running_claim(
            TaskSpec("fails", {}, retry_policy=policy)
        )
        state = self.scheduler.nack(
            claim.attempt_id,
            claim.lease_epoch,
            execution_id=execution_id,
            failure_class=FailureClass.TRANSIENT_EXTERNAL,
        )
        self.assertEqual(state, TaskState.SUSPENDED)
        self.assertEqual(self.scheduler.get("tasks", task_id)["state"], TaskState.SUSPENDED)
        self.assertEqual(self.scheduler.get("batches", batch_id)["state"], BatchState.SUSPENDED)
        escalations = self.scheduler.list("escalations", state="OPEN")
        self.assertEqual(len(escalations), 1)

    def test_writer_unknown_quiescence_never_blindly_retries(self) -> None:
        policy = RetryPolicy(max_attempts=3, retry_classes=(FailureClass.TRANSIENT_EXTERNAL,))
        _batch, task_id, claim, execution_id = self.running_claim(
            TaskSpec(
                "writer",
                {},
                workspace_mode=WorkspaceMode.WRITE,
                retry_policy=policy,
            )
        )
        self.scheduler.nack(
            claim.attempt_id,
            claim.lease_epoch,
            execution_id=execution_id,
            failure_class=FailureClass.TRANSIENT_EXTERNAL,
            terminal_confirmed=False,
            quiescent_confirmed=False,
        )
        self.assertEqual(self.scheduler.get("tasks", task_id)["state"], TaskState.SUSPENDED)
        escalation = self.scheduler.list("escalations", state="OPEN")[0]
        self.assertEqual(escalation["failure_class"], FailureClass.WRITER_QUIESCENCE_UNKNOWN)
        self.assertEqual(
            self.scheduler.get("logical_agents", claim.logical_agent_id)["state"],
            AgentState.SUSPENDED,
        )
        with self.assertRaises(InvalidTransition):
            self.scheduler.resolve_escalation(escalation["id"], operation="retry")
        self.scheduler.resolve_escalation(
            escalation["id"], operation="retry", quiescence_confirmed=True
        )
        self.assertEqual(self.scheduler.get("tasks", task_id)["state"], TaskState.QUEUED)
        self.assertEqual(
            self.scheduler.get("logical_agents", claim.logical_agent_id)["state"],
            AgentState.REVIVING,
        )
        self.assertEqual(self.scheduler.revive_eligible_agents(), 1)
        self.assertEqual(
            self.scheduler.get("logical_agents", claim.logical_agent_id)["state"],
            AgentState.READY,
        )

    def test_execution_ownership_and_state_transitions_are_closed(self) -> None:
        self.scheduler.resize_partition("general", 2)
        self.scheduler.reconcile_pool()
        _batch, _ids = self.scheduler.submit_batch([TaskSpec("a", {}), TaskSpec("b", {})])
        agents = self.scheduler.list("logical_agents", state="READY")
        first = self.scheduler.claim_next(agents[0]["id"])
        second = self.scheduler.claim_next(agents[1]["id"])
        first_execution, _ = self.scheduler.create_execution(first)
        second_execution, _ = self.scheduler.create_execution(second)
        with self.assertRaises(StaleAuthority):
            self.scheduler.record_start_ambiguity(
                first.attempt_id,
                first.lease_epoch,
                second_execution,
                detail="cross-attempt",
            )
        with self.assertRaises(StaleAuthority):
            self.scheduler.nack(
                first.attempt_id,
                first.lease_epoch,
                execution_id=second_execution,
                failure_class=FailureClass.UNKNOWN,
            )
        self.scheduler.confirm_execution_running(
            first.attempt_id,
            first.lease_epoch,
            first_execution,
            runtime_handle={},
        )
        self.scheduler.ack_success(
            first.attempt_id,
            first.lease_epoch,
            execution_id=first_execution,
            payload={},
        )
        with self.assertRaises(InvalidTransition):
            self.scheduler.record_physical_outcome(
                first_execution, state=ExecutionState.RUNNING
            )

    def test_assigned_agent_cannot_revive_before_attempt_is_fenced(self) -> None:
        _batch, _task, claim, _execution = self.running_claim(TaskSpec("active", {}))
        with self.assertRaises(InvalidTransition):
            self.scheduler.revive_agent(claim.logical_agent_id, "local")

    def test_partial_batch_preserves_success_result(self) -> None:
        policy = RetryPolicy(max_attempts=1, retry_classes=())
        batch_id, ids = self.scheduler.submit_batch(
            [TaskSpec("ok", {}), TaskSpec("bad", {}, retry_policy=policy)]
        )
        first = self.scheduler.claim_next(self.ready_agent())
        result_id = self.scheduler.ack_success(
            first.attempt_id, first.lease_epoch, execution_id=None, payload={"ok": True}
        )
        second = self.scheduler.claim_next(self.ready_agent())
        self.scheduler.nack(
            second.attempt_id,
            second.lease_epoch,
            failure_class=FailureClass.UNKNOWN,
        )
        self.assertEqual(self.scheduler.get("batches", batch_id)["state"], BatchState.SUSPENDED)
        self.assertEqual(self.scheduler.get("results", result_id)["state"], ResultState.AVAILABLE)
        completed = [task for task in ids.values() if self.scheduler.get("tasks", task)["state"] == "COMPLETED"]
        self.assertEqual(len(completed), 1)

    def test_ephemeral_agent_retires_after_assignment(self) -> None:
        self.scheduler.upsert_partition(
            PartitionSpec("ephemeral", 1, Retention.EPHEMERAL, "local", "default")
        )
        self.scheduler.reconcile_pool()
        _batch, _ids = self.scheduler.submit_batch(
            [TaskSpec("once", {}, partition="ephemeral")]
        )
        agent_id = self.ready_agent("ephemeral")
        claim = self.scheduler.claim_next(agent_id)
        self.scheduler.ack_success(
            claim.attempt_id, claim.lease_epoch, execution_id=None, payload={}
        )
        self.assertEqual(self.scheduler.get("logical_agents", agent_id)["state"], AgentState.RETIRED)

    def test_batch_cancellation_revokes_authority_and_preserves_completed_result(self) -> None:
        self.scheduler.resize_partition("general", 2)
        self.scheduler.reconcile_pool()
        batch_id, ids = self.scheduler.submit_batch(
            [TaskSpec("done", {}), TaskSpec("active", {})]
        )
        agents = self.scheduler.list("logical_agents", state="READY")
        done = self.scheduler.claim_next(agents[0]["id"])
        result_id = self.scheduler.ack_success(
            done.attempt_id, done.lease_epoch, execution_id=None, payload={"kept": True}
        )
        active = self.scheduler.claim_next(agents[1]["id"])
        self.scheduler.cancel_batch(batch_id)
        self.assertEqual(self.scheduler.get("batches", batch_id)["state"], BatchState.CANCELLED)
        completed_task_id = done.task_id
        cancelled_task_id = next(task_id for task_id in ids.values() if task_id != completed_task_id)
        self.assertEqual(self.scheduler.get("tasks", completed_task_id)["state"], TaskState.COMPLETED)
        self.assertEqual(self.scheduler.get("results", result_id)["state"], ResultState.AVAILABLE)
        self.assertEqual(self.scheduler.get("tasks", cancelled_task_id)["state"], TaskState.CANCELLED)
        with self.assertRaises(StaleAuthority):
            self.scheduler.ack_success(
                active.attempt_id, active.lease_epoch, execution_id=None, payload={"stale": True}
            )

    def test_writer_cancellation_does_not_release_unknown_physical_writer(self) -> None:
        _batch, task_id, claim, _execution = self.running_claim(
            TaskSpec("cancel-writer", {}, workspace_mode=WorkspaceMode.WRITE)
        )
        self.scheduler.cancel_task(task_id)
        self.assertEqual(self.scheduler.get("tasks", task_id)["state"], TaskState.CANCELLED)
        self.assertEqual(
            self.scheduler.get("logical_agents", claim.logical_agent_id)["state"],
            AgentState.SUSPENDED,
        )
        escalation = self.scheduler.list("escalations", state="OPEN")[0]
        with self.assertRaises(InvalidTransition):
            self.scheduler.resolve_escalation(
                escalation["id"], operation="release_cancelled_writer"
            )
        self.scheduler.resolve_escalation(
            escalation["id"],
            operation="release_cancelled_writer",
            quiescence_confirmed=True,
        )
        self.assertEqual(
            self.scheduler.get("logical_agents", claim.logical_agent_id)["state"],
            AgentState.REVIVING,
        )


class TopologyCase(SchedulerCase):
    def test_deficit_move_merge_and_semantic_death(self) -> None:
        self.scheduler.upsert_partition(
            PartitionSpec("a", 2, Retention.RESIDENT, "local", "default")
        )
        self.scheduler.upsert_partition(
            PartitionSpec("b", 1, Retention.RESIDENT, "local", "default")
        )
        result = self.scheduler.reconcile_pool()
        self.assertGreaterEqual(result["born"], 3)
        agent = next(
            row for row in self.scheduler.list("logical_agents") if row["partition_name"] == "a"
        )
        self.scheduler.move_agent(agent["id"], "b")
        self.assertEqual(self.scheduler.get("logical_agents", agent["id"])["partition_name"], "b")
        other = next(
            row for row in self.scheduler.list("logical_agents") if row["partition_name"] == "a"
        )
        self.scheduler.merge_partitions("a", "b")
        self.assertEqual(self.scheduler.get("logical_agents", other["id"])["partition_name"], "b")
        self.scheduler.resize_partition("b", 0)
        self.scheduler.reconcile_pool()
        retired = [
            row for row in self.scheduler.list("logical_agents") if row["state"] == AgentState.RETIRED
        ]
        self.assertTrue(retired)
        with self.assertRaises(InvalidTransition):
            self.scheduler.revive_agent(retired[0]["id"], "local")

    def test_revival_preserves_identity_and_continuity(self) -> None:
        agent_id = self.ready_agent()
        _batch, _task_id, claim, _execution = self.running_claim(TaskSpec("checkpoint", {}))
        self.scheduler.promote_checkpoint(
            claim.attempt_id, claim.lease_epoch, {"INVARIANTS": ["identity"]}
        )
        self.scheduler.ack_success(
            claim.attempt_id, claim.lease_epoch, execution_id=None, payload={}
        )
        revived = self.scheduler.revive_agent(agent_id, "local")
        agent = self.scheduler.get("logical_agents", agent_id)
        self.assertEqual(agent["id"], agent_id)
        self.assertIn("identity", agent["continuity_json"])
        self.assertEqual(revived, agent_id)
        self.assertEqual(
            self.scheduler.list("incarnations", state=IncarnationState.STARTING.value), []
        )

    def test_busy_move_and_merge_apply_only_at_assignment_boundary(self) -> None:
        self.scheduler.upsert_partition(
            PartitionSpec("target", 0, Retention.RESIDENT, "local", "default")
        )
        _batch, _task, claim, _execution = self.running_claim(TaskSpec("busy", {}))
        self.scheduler.move_agent(claim.logical_agent_id, "target")
        during = self.scheduler.get("logical_agents", claim.logical_agent_id)
        self.assertEqual(during["partition_name"], "general")
        self.assertEqual(during["pending_partition_name"], "target")
        self.scheduler.ack_success(
            claim.attempt_id, claim.lease_epoch, execution_id=None, payload={}
        )
        after = self.scheduler.get("logical_agents", claim.logical_agent_id)
        self.assertEqual(after["partition_name"], "target")
        self.assertIsNone(after["pending_partition_name"])

    def test_partition_retirement_rejects_nonterminal_tasks_atomically(self) -> None:
        _batch, _task, claim, _execution = self.running_claim(TaskSpec("busy-retire", {}))
        revisions_before = self.scheduler.db.fetch_one(
            "SELECT COUNT(*) count FROM pool_topology_revisions"
        )["count"]
        with self.assertRaises(InvalidTransition):
            self.scheduler.retire_partition("general")
        self.assertEqual(
            self.scheduler.db.fetch_one(
                "SELECT COUNT(*) count FROM pool_topology_revisions"
            )["count"],
            revisions_before,
        )
        during = self.scheduler.get("logical_agents", claim.logical_agent_id)
        self.assertEqual(during["state"], AgentState.ASSIGNED)
        partition = next(
            row for row in self.scheduler.list("pool_partitions") if row["name"] == "general"
        )
        self.assertEqual(partition["active"], 1)

    def test_required_affinity_births_new_identity_from_project_state(self) -> None:
        workstream = self.scheduler.create_workstream(
            "design", project_state_ref="git:abcdef"
        )
        _batch, _ids = self.scheduler.submit_batch(
            [
                TaskSpec(
                    "continuity",
                    {},
                    workstream_id=workstream,
                    continuity=ContinuityPreference.REQUIRED,
                    affinity_tags=("specialist",),
                )
            ]
        )
        self.assertEqual(self.scheduler.ensure_task_consumers(), 1)
        newborn = next(
            agent
            for agent in self.scheduler.list("logical_agents")
            if agent["workstream_id"] == workstream
        )
        self.assertIn("git:abcdef", newborn["continuity_json"])
        self.assertIn("specialist", newborn["tags_json"])


if __name__ == "__main__":
    unittest.main()
