from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence

from .enums import (
    ContinuityPreference,
    ExecutionState,
    FailureClass,
    Retention,
    WorkspaceMode,
)


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 1
    retry_classes: tuple[FailureClass, ...] = (
        FailureClass.TRANSIENT_EXTERNAL,
        FailureClass.TIMEOUT,
        FailureClass.EXECUTION_LOST,
        FailureClass.RESOURCE_UNAVAILABLE,
    )
    base_backoff_seconds: float = 1.0
    max_backoff_seconds: float = 60.0

    def delay_for_attempt(self, attempt_number: int) -> float:
        exponent = max(0, attempt_number - 1)
        return min(self.max_backoff_seconds, self.base_backoff_seconds * (2**exponent))


@dataclass(frozen=True)
class TaskSpec:
    name: str
    payload: Mapping[str, Any]
    acceptance: Mapping[str, Any] = field(default_factory=dict)
    partition: str = "general"
    workstream_id: str | None = None
    continuity: ContinuityPreference = ContinuityPreference.NONE
    affinity_tags: tuple[str, ...] = ()
    workspace_mode: WorkspaceMode = WorkspaceMode.READ_ONLY
    dependencies: tuple[str, ...] = ()
    priority: int = 0
    retry_policy: RetryPolicy = field(default_factory=RetryPolicy)
    supersedes_task_id: str | None = None
    task_id: str | None = None


@dataclass(frozen=True)
class Claim:
    task_id: str
    batch_id: str
    attempt_id: str
    attempt_number: int
    lease_id: str
    lease_epoch: int
    lease_expires_at: float
    logical_agent_id: str
    incarnation_id: str | None
    execution_target: str
    execution_profile: str
    workspace_mode: WorkspaceMode
    payload: Mapping[str, Any]
    acceptance: Mapping[str, Any]
    workstream_id: str | None


@dataclass(frozen=True)
class ExecutionRequest:
    request_id: str
    execution_id: str
    task_id: str
    attempt_id: str
    lease_epoch: int
    logical_agent_id: str
    incarnation_id: str
    execution_target: str
    execution_profile: str
    cwd: str
    prompt: str
    workspace_mode: WorkspaceMode
    continuity: Mapping[str, Any]
    incarnation_runtime_handle: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class StartObservation:
    state: ExecutionState
    runtime_handle: Mapping[str, Any] = field(default_factory=dict)
    ambiguous: bool = False
    failure_class: FailureClass | None = None
    failure_code: str | None = None
    detail: str | None = None


@dataclass(frozen=True)
class ExecutionObservation:
    state: ExecutionState
    terminal_confirmed: bool = False
    quiescent_confirmed: bool = False
    detail: str | None = None


@dataclass(frozen=True)
class ExecutionOutcome:
    state: ExecutionState
    payload: Mapping[str, Any] | None = None
    summary: str | None = None
    checkpoint: Mapping[str, Any] | None = None
    failure_class: FailureClass | None = None
    failure_code: str | None = None
    failure_signature: str | None = None
    retry_hint: bool | None = None
    terminal_confirmed: bool = True
    quiescent_confirmed: bool = True
    incarnation_reusable: bool = False


@dataclass(frozen=True)
class DeliveryObservation:
    delivered: bool
    detail: str | None = None


@dataclass(frozen=True)
class PartitionSpec:
    name: str
    desired_capacity: int
    retention: Retention
    execution_target: str
    execution_profile: str
    tags: tuple[str, ...] = ()


CONTINUITY_KEYS: frozenset[str] = frozenset(
    {
        "INVARIANTS",
        "DECISIONS",
        "CURRENT DESIGN",
        "REJECTED ALTERNATIVES",
        "OPEN QUESTIONS",
        "KNOWN FAILURES",
        "CURRENT CHECKPOINT",
        "NEXT LIKELY STEPS",
    }
)


def tags_match(required: Sequence[str], actual: Sequence[str]) -> bool:
    return set(required).issubset(actual)
