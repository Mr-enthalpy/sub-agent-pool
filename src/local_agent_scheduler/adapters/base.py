from __future__ import annotations

from typing import Mapping, Protocol

from ..models import (
    ExecutionObservation,
    ExecutionOutcome,
    ExecutionRequest,
    StartObservation,
)


class ExecutionAdapter(Protocol):
    """Synchronous, bounded execution-terminal boundary.

    Every method is called directly by the Scheduler control loop and therefore
    MUST return within the adapter's configured operation deadline.  Adapters
    own deadlines for all underlying process, transport, and stream I/O and
    translate expiry into the existing timeout/ambiguous observations or
    outcomes.  The Scheduler does not provide a generic adapter watchdog.
    """

    def start_execution(self, request: ExecutionRequest) -> StartObservation: ...

    def observe_execution(self, runtime_handle: Mapping[str, object]) -> ExecutionObservation: ...

    def interrupt_execution(self, runtime_handle: Mapping[str, object]) -> ExecutionObservation: ...

    def terminate_execution(self, runtime_handle: Mapping[str, object]) -> ExecutionObservation: ...

    def collect_outcome(self, runtime_handle: Mapping[str, object]) -> ExecutionOutcome: ...

    def reconcile_start(
        self, request_id: str, runtime_handle: Mapping[str, object]
    ) -> StartObservation: ...
