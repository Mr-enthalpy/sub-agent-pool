from __future__ import annotations

import json
import queue
import subprocess
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Callable, Mapping

from ..enums import ExecutionState, FailureClass, WorkspaceMode
from ..errors import AdapterError
from ..models import (
    ExecutionObservation,
    ExecutionOutcome,
    ExecutionRequest,
    StartObservation,
)


class AppServerSession:
    def __init__(self, command: tuple[str, ...], process_cwd: str | None, timeout: float):
        if timeout <= 0:
            raise ValueError("timeout must be positive")
        initialization_deadline = time.monotonic() + timeout
        self.session_id = uuid.uuid4().hex
        self.timeout = timeout
        self.process = subprocess.Popen(
            command,
            cwd=process_cwd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
        )
        try:
            self._initialize(initialization_deadline)
        except BaseException:
            self._cleanup_failed_initialization()
            raise

    def _initialize(self, initialization_deadline: float) -> None:
        if not self.process.stdin or not self.process.stdout or not self.process.stderr:
            raise AdapterError("failed to open Codex app-server stdio")
        self._responses: dict[int, Mapping[str, Any]] = {}
        self._notifications: list[Mapping[str, Any]] = []
        self._stderr: list[str] = []
        self._condition = threading.Condition()
        self._write_lock = threading.Lock()
        self._next_id = 1
        self._reader = threading.Thread(target=self._read_stdout, daemon=True)
        self._stderr_reader = threading.Thread(target=self._read_stderr, daemon=True)
        self._reader.start()
        self._stderr_reader.start()
        self.request(
            "initialize",
            {
                "clientInfo": {
                    "name": "local_agent_scheduler",
                    "title": "Local Agent Scheduler",
                    "version": "0.1.2",
                }
            },
            timeout=self._remaining(initialization_deadline, "initialize"),
        )
        self.notify(
            "initialized",
            {},
            timeout=self._remaining(initialization_deadline, "initialized"),
        )

    def _cleanup_failed_initialization(self) -> None:
        if self.process.poll() is not None:
            return
        try:
            self.process.terminate()
            self.process.wait(timeout=min(self.timeout, 1.0))
        except (OSError, subprocess.TimeoutExpired):
            try:
                self.process.kill()
            except OSError:
                return
            try:
                self.process.wait(timeout=min(self.timeout, 1.0))
            except (OSError, subprocess.TimeoutExpired):
                pass

    @staticmethod
    def _remaining(deadline: float, operation: str) -> float:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError(f"Codex app-server operation timed out: {operation}")
        return remaining

    def _read_stdout(self) -> None:
        assert self.process.stdout is not None
        for line in self.process.stdout:
            try:
                message = json.loads(line)
            except json.JSONDecodeError:
                continue
            with self._condition:
                if "id" in message:
                    self._responses[int(message["id"])] = message
                else:
                    self._notifications.append(message)
                self._condition.notify_all()

    def _read_stderr(self) -> None:
        assert self.process.stderr is not None
        for line in self.process.stderr:
            self._stderr.append(line.rstrip())

    def _write(self, message: Mapping[str, Any]) -> None:
        if self.process.poll() is not None:
            raise AdapterError(
                f"Codex app-server exited with {self.process.returncode}: {' | '.join(self._stderr[-5:])}"
            )
        assert self.process.stdin is not None
        self.process.stdin.write(json.dumps(message, ensure_ascii=False, separators=(",", ":")) + "\n")
        self.process.stdin.flush()

    def _abort(self) -> None:
        """Break blocked stdio without waiting beyond the operation deadline."""

        if self.process.poll() is None:
            try:
                self.process.kill()
            except OSError:
                pass
        with self._condition:
            self._condition.notify_all()

    def _write_until(
        self, message: Mapping[str, Any], *, deadline: float, operation: str
    ) -> None:
        completed = threading.Event()
        errors: list[BaseException] = []

        def write() -> None:
            try:
                with self._write_lock:
                    self._write(message)
            except BaseException as exc:
                errors.append(exc)
            finally:
                completed.set()

        threading.Thread(
            target=write,
            name=f"codex-app-server-write-{operation}",
            daemon=True,
        ).start()
        remaining = deadline - time.monotonic()
        if not completed.is_set() and (
            remaining <= 0 or not completed.wait(remaining)
        ):
            self._abort()
            raise TimeoutError(
                f"Codex app-server request timed out while writing: {operation}"
            )
        if errors:
            self._abort()
            raise errors[0]

    def request(
        self, method: str, params: Mapping[str, Any], *, timeout: float | None = None
    ) -> Mapping[str, Any]:
        operation_timeout = self.timeout if timeout is None else timeout
        deadline = time.monotonic() + operation_timeout
        with self._condition:
            request_id = self._next_id
            self._next_id += 1
        self._write_until(
            {"method": method, "id": request_id, "params": params},
            deadline=deadline,
            operation=method,
        )
        try:
            with self._condition:
                while request_id not in self._responses:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise TimeoutError(
                            f"Codex app-server request timed out: {method}"
                        )
                    self._condition.wait(min(remaining, 0.25))
                    if self.process.poll() is not None and request_id not in self._responses:
                        raise AdapterError(
                            f"Codex app-server exited during {method}: "
                            f"{' | '.join(self._stderr[-5:])}"
                        )
                response = self._responses.pop(request_id)
        except TimeoutError:
            self._abort()
            raise
        if response.get("error"):
            raise AdapterError(f"{method}: {response['error']}")
        return response.get("result", {})

    def notify(
        self, method: str, params: Mapping[str, Any], *, timeout: float | None = None
    ) -> None:
        operation_timeout = self.timeout if timeout is None else timeout
        self._write_until(
            {"method": method, "params": params},
            deadline=time.monotonic() + operation_timeout,
            operation=method,
        )

    def notifications(self) -> list[Mapping[str, Any]]:
        with self._condition:
            return list(self._notifications)

    def close(self, *, terminate: bool = True, timeout: float | None = None) -> bool:
        close_timeout = min(self.timeout, 1.0) if timeout is None else timeout
        deadline = time.monotonic() + max(0.0, close_timeout)
        if self.process.poll() is None and terminate:
            self.process.terminate()
            try:
                self.process.wait(timeout=max(0.0, deadline - time.monotonic()))
            except subprocess.TimeoutExpired:
                self.process.kill()
                remaining = deadline - time.monotonic()
                if remaining > 0:
                    try:
                        self.process.wait(timeout=remaining)
                    except subprocess.TimeoutExpired:
                        pass
        return self.process.poll() is not None


class CodexAppServerAdapter:
    """Thin Codex frontend adapter using the official app-server stdio protocol."""

    def __init__(
        self,
        *,
        command: tuple[str, ...] = ("codex", "app-server"),
        process_cwd: str | None = None,
        approval_policy: str = "never",
        sandbox: str = "workspace-write",
        request_timeout: float = 30.0,
        profile_options: Mapping[str, Mapping[str, Any]] | None = None,
        session_factory: Callable[..., AppServerSession] = AppServerSession,
    ) -> None:
        self.command = command
        self.process_cwd = process_cwd
        self.approval_policy = approval_policy
        self.sandbox = sandbox
        if request_timeout <= 0:
            raise ValueError("request_timeout must be positive")
        self.request_timeout = request_timeout
        self.profile_options = dict(profile_options or {})
        self.session_factory = session_factory
        self._sessions: dict[str, AppServerSession] = {}

    @staticmethod
    def _remaining(deadline: float, operation: str) -> float:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError(f"Codex adapter method timed out: {operation}")
        return remaining

    def start_execution(self, request: ExecutionRequest) -> StartObservation:
        method_deadline = time.monotonic() + self.request_timeout
        session: AppServerSession | None = None
        runtime_handle: dict[str, Any] = {
            "request_id": request.request_id,
            "execution_id": request.execution_id,
        }
        try:
            if request.workspace_mode is WorkspaceMode.READ_ONLY:
                task_sandbox = "read-only"
            elif self.sandbox in {"workspace-write", "danger-full-access"}:
                task_sandbox = "workspace-write"
            else:
                raise RuntimeError(
                    "write Task exceeds the configured Codex adapter sandbox ceiling"
                )
            session = self.session_factory(
                self.command,
                self.process_cwd,
                self._remaining(method_deadline, "start_execution/session"),
            )
            runtime_handle["adapter_session_id"] = session.session_id
            self._remaining(method_deadline, "start_execution/session")
            thread_params: dict[str, Any] = {
                "cwd": request.cwd,
                "approvalPolicy": self.approval_policy,
                "sandbox": task_sandbox,
                "serviceName": "local_agent_scheduler",
            }
            options = self.profile_options.get(request.execution_profile, {})
            for key in ("model", "personality"):
                if key in options:
                    thread_params[key] = options[key]
            thread_result = session.request(
                "thread/start",
                thread_params,
                timeout=self._remaining(method_deadline, "start_execution/thread"),
            )
            thread_id = thread_result["thread"]["id"]
            runtime_handle["thread_id"] = thread_id
            self._remaining(method_deadline, "start_execution/thread")
            turn_params: dict[str, Any] = {
                "threadId": thread_id,
                "input": [{"type": "text", "text": request.prompt}],
                "cwd": request.cwd,
                "approvalPolicy": self.approval_policy,
            }
            if "effort" in options:
                turn_params["effort"] = options["effort"]
            turn_result = session.request(
                "turn/start",
                turn_params,
                timeout=self._remaining(method_deadline, "start_execution/turn"),
            )
            turn_id = turn_result["turn"]["id"]
            runtime_handle["turn_id"] = turn_id
            self._remaining(method_deadline, "start_execution/turn")
            self._sessions[session.session_id] = session
            return StartObservation(
                ExecutionState.RUNNING,
                runtime_handle,
            )
        except TimeoutError as exc:
            if session is not None:
                self._sessions[session.session_id] = session
            return StartObservation(
                ExecutionState.UNKNOWN,
                runtime_handle,
                ambiguous=True,
                failure_class=FailureClass.TIMEOUT,
                failure_code="APP_SERVER_START_TIMEOUT",
                detail=str(exc),
            )
        except Exception as exc:
            if session is not None:
                session.close()
            failure_class = self._classify_failure(str(exc))
            return StartObservation(
                ExecutionState.FAILED,
                {"request_id": request.request_id},
                failure_class=failure_class,
                failure_code="APP_SERVER_START_FAILED",
                detail=str(exc),
            )

    def observe_execution(self, runtime_handle: Mapping[str, object]) -> ExecutionObservation:
        return self._observe_until(
            runtime_handle, time.monotonic() + self.request_timeout
        )

    def _observe_until(
        self, runtime_handle: Mapping[str, object], deadline: float
    ) -> ExecutionObservation:
        session = self._live_session(runtime_handle)
        if session:
            if not runtime_handle.get("thread_id") or not runtime_handle.get("turn_id"):
                if session.process.poll() is not None:
                    self._close_live_session(
                        runtime_handle,
                        timeout=self._remaining(deadline, "observe_execution/close"),
                    )
                    return ExecutionObservation(
                        ExecutionState.LOST,
                        terminal_confirmed=True,
                        quiescent_confirmed=True,
                        detail="app-server exited before execution identity was established",
                    )
                return ExecutionObservation(
                    ExecutionState.UNKNOWN,
                    detail="live ambiguous start lacks exact thread/turn identity",
                )
            terminal = self._terminal_notification(session, runtime_handle)
            if terminal:
                status = self._turn_status(terminal)
                state = ExecutionState.SUCCEEDED if status == "completed" else ExecutionState.FAILED
                return ExecutionObservation(state, terminal_confirmed=True, quiescent_confirmed=True)
            if session.process.poll() is not None:
                self._close_live_session(
                    runtime_handle,
                    timeout=self._remaining(deadline, "observe_execution/close"),
                )
                return ExecutionObservation(
                    ExecutionState.LOST,
                    terminal_confirmed=True,
                    quiescent_confirmed=True,
                    detail="app-server process exited without a turn completion event",
                )
            return ExecutionObservation(ExecutionState.RUNNING)
        thread_id = runtime_handle.get("thread_id")
        if not thread_id:
            return ExecutionObservation(ExecutionState.UNKNOWN, detail="no thread handle")
        turn_id = runtime_handle.get("turn_id")
        return self._read_stored_thread(
            str(thread_id),
            str(turn_id) if turn_id is not None else None,
            deadline=deadline,
        )

    def collect_outcome(self, runtime_handle: Mapping[str, object]) -> ExecutionOutcome:
        deadline = time.monotonic() + self.request_timeout
        session = self._live_session(runtime_handle)
        if not session:
            thread_id = runtime_handle.get("thread_id")
            turn_id = runtime_handle.get("turn_id")
            recovered = (
                self._read_stored_outcome(
                    str(thread_id),
                    str(turn_id) if turn_id is not None else None,
                    deadline=deadline,
                )
                if thread_id
                else None
            )
            if recovered is not None:
                return recovered
            observation = self._observe_until(runtime_handle, deadline)
            return ExecutionOutcome(
                observation.state,
                failure_class=FailureClass.EXECUTION_LOST,
                failure_code="NO_LIVE_SESSION",
                failure_signature="CODEX_SESSION_NOT_ATTACHED",
                terminal_confirmed=observation.terminal_confirmed,
                quiescent_confirmed=observation.quiescent_confirmed,
            )
        terminal = self._terminal_notification(session, runtime_handle)
        if not terminal:
            return ExecutionOutcome(
                ExecutionState.RUNNING,
                terminal_confirmed=False,
                quiescent_confirmed=False,
            )
        status = self._turn_status(terminal)
        text = self._collect_agent_text(session.notifications())
        self._close_live_session(
            runtime_handle,
            timeout=self._remaining(deadline, "collect_outcome/close"),
        )
        if status == "completed":
            payload: Mapping[str, Any]
            try:
                parsed = json.loads(text)
                payload = parsed if isinstance(parsed, dict) else {"value": parsed}
            except (json.JSONDecodeError, TypeError):
                payload = {"final_response": text}
            return ExecutionOutcome(
                ExecutionState.SUCCEEDED,
                payload=payload,
                summary=text,
                terminal_confirmed=True,
                quiescent_confirmed=True,
            )
        detail = json.dumps(terminal, ensure_ascii=False, sort_keys=True)
        return ExecutionOutcome(
            ExecutionState.FAILED,
            failure_class=self._classify_failure(detail),
            failure_code="CODEX_TURN_FAILED",
            failure_signature=self._normalized_signature(detail),
            terminal_confirmed=True,
            quiescent_confirmed=True,
        )

    def reconcile_start(
        self, request_id: str, runtime_handle: Mapping[str, object]
    ) -> StartObservation:
        deadline = time.monotonic() + self.request_timeout
        if runtime_handle.get("request_id") not in (None, request_id):
            return StartObservation(
                ExecutionState.UNKNOWN,
                runtime_handle,
                ambiguous=True,
                failure_class=FailureClass.ADAPTER_PROTOCOL_FAILURE,
                failure_code="REQUEST_ID_MISMATCH",
            )
        observation = self._observe_until(runtime_handle, deadline)
        if observation.state in (ExecutionState.RUNNING, ExecutionState.SUCCEEDED):
            return StartObservation(observation.state, runtime_handle)
        return StartObservation(
            observation.state,
            runtime_handle,
            ambiguous=not observation.terminal_confirmed,
            failure_class=(
                FailureClass.EXECUTION_LOST
                if observation.state in (ExecutionState.LOST, ExecutionState.UNKNOWN)
                else None
            ),
            detail=observation.detail,
        )

    def interrupt_execution(self, runtime_handle: Mapping[str, object]) -> ExecutionObservation:
        return self._interrupt_until(
            runtime_handle, time.monotonic() + self.request_timeout
        )

    def _interrupt_until(
        self, runtime_handle: Mapping[str, object], deadline: float
    ) -> ExecutionObservation:
        session = self._live_session(runtime_handle)
        if not session:
            return ExecutionObservation(
                ExecutionState.UNKNOWN,
                terminal_confirmed=False,
                quiescent_confirmed=False,
                detail="cannot interrupt an unattached Codex session",
            )
        thread_id = runtime_handle.get("thread_id")
        turn_id = runtime_handle.get("turn_id")
        if not thread_id or not turn_id:
            return ExecutionObservation(ExecutionState.UNKNOWN, detail="missing thread/turn handle")
        try:
            session.request(
                "turn/interrupt",
                {"threadId": str(thread_id), "turnId": str(turn_id)},
                timeout=self._remaining(deadline, "interrupt_execution/request"),
            )
            while time.monotonic() < deadline:
                terminal = self._terminal_notification(session, runtime_handle)
                if terminal:
                    process_stopped = self._close_live_session(
                        runtime_handle,
                        timeout=self._remaining(deadline, "interrupt_execution/close"),
                    )
                    return ExecutionObservation(
                        ExecutionState.TERMINATED,
                        terminal_confirmed=True,
                        quiescent_confirmed=process_stopped,
                    )
                time.sleep(0.05)
            return ExecutionObservation(
                ExecutionState.UNKNOWN,
                terminal_confirmed=False,
                quiescent_confirmed=False,
                detail="interrupt accepted but terminal event not observed",
            )
        except Exception as exc:
            return ExecutionObservation(ExecutionState.UNKNOWN, detail=str(exc))

    def terminate_execution(self, runtime_handle: Mapping[str, object]) -> ExecutionObservation:
        deadline = time.monotonic() + self.request_timeout
        interrupted = self._interrupt_until(runtime_handle, deadline)
        if interrupted.terminal_confirmed and interrupted.quiescent_confirmed:
            return interrupted
        session = self._live_session(runtime_handle)
        try:
            close_timeout = self._remaining(deadline, "terminate_execution/close")
        except TimeoutError:
            close_timeout = 0.0
        process_stopped = (
            self._close_live_session(runtime_handle, timeout=close_timeout)
            if session
            else False
        )
        if process_stopped:
            return ExecutionObservation(
                ExecutionState.TERMINATED,
                terminal_confirmed=True,
                quiescent_confirmed=True,
            )
        return ExecutionObservation(
            ExecutionState.UNKNOWN,
            terminal_confirmed=interrupted.terminal_confirmed,
            quiescent_confirmed=False,
            detail="physical quiescence could not be confirmed",
        )

    def _live_session(self, runtime_handle: Mapping[str, object]) -> AppServerSession | None:
        session_id = runtime_handle.get("adapter_session_id")
        return self._sessions.get(str(session_id)) if session_id else None

    def _close_live_session(
        self,
        runtime_handle: Mapping[str, object],
        *,
        timeout: float | None = None,
    ) -> bool:
        session_id = runtime_handle.get("adapter_session_id")
        if not session_id:
            return False
        session = self._sessions.pop(str(session_id), None)
        if session is None:
            return False
        return session.close() if timeout is None else session.close(timeout=timeout)

    @staticmethod
    def _turn_status(notification: Mapping[str, Any]) -> str:
        params = notification.get("params", {})
        turn = params.get("turn", {}) if isinstance(params, Mapping) else {}
        return str(turn.get("status", "failed"))

    @staticmethod
    def _terminal_notification(
        session: AppServerSession, runtime_handle: Mapping[str, object]
    ) -> Mapping[str, Any] | None:
        expected_turn = runtime_handle.get("turn_id")
        if not expected_turn:
            return None
        expected_turn = str(expected_turn)
        for message in reversed(session.notifications()):
            if message.get("method") != "turn/completed":
                continue
            params = message.get("params", {})
            turn = params.get("turn", {}) if isinstance(params, Mapping) else {}
            if str(turn.get("id", "")) == expected_turn:
                return message
        return None

    @staticmethod
    def _collect_agent_text(messages: list[Mapping[str, Any]]) -> str:
        deltas: list[str] = []
        completed_text: str | None = None
        for message in messages:
            method = message.get("method")
            params = message.get("params", {})
            if not isinstance(params, Mapping):
                continue
            if method == "item/agentMessage/delta" and isinstance(params.get("delta"), str):
                deltas.append(str(params["delta"]))
            if method == "item/completed":
                item = params.get("item", {})
                if isinstance(item, Mapping) and item.get("type") == "agentMessage":
                    for key in ("text", "content"):
                        if isinstance(item.get(key), str):
                            completed_text = str(item[key])
        return completed_text if completed_text is not None else "".join(deltas)

    def _read_stored_thread(
        self, thread_id: str, turn_id: str | None, *, deadline: float
    ) -> ExecutionObservation:
        document = self._read_stored_document(thread_id, deadline=deadline)
        if document is None:
            return ExecutionObservation(
                ExecutionState.UNKNOWN,
                terminal_confirmed=False,
                quiescent_confirmed=False,
                detail="stored Codex thread could not be read",
            )
        turn = self._stored_turn(document, turn_id)
        if turn is None:
            return ExecutionObservation(
                ExecutionState.UNKNOWN,
                terminal_confirmed=False,
                quiescent_confirmed=False,
                detail="expected Codex turn is absent or ambiguous in stored thread",
            )
        status = str(turn.get("status", ""))
        if status == "inProgress":
            return ExecutionObservation(ExecutionState.RUNNING)
        if status == "completed":
            return ExecutionObservation(
                ExecutionState.SUCCEEDED, terminal_confirmed=True, quiescent_confirmed=True
            )
        if status in {"failed", "interrupted"}:
            return ExecutionObservation(
                ExecutionState.FAILED, terminal_confirmed=True, quiescent_confirmed=True
            )
        return ExecutionObservation(
            ExecutionState.UNKNOWN,
            terminal_confirmed=False,
            quiescent_confirmed=False,
            detail=f"stored turn has unknown status: {status}",
        )

    def _read_stored_document(
        self, thread_id: str, *, deadline: float
    ) -> Mapping[str, Any] | None:
        session: AppServerSession | None = None
        try:
            session = self.session_factory(
                self.command,
                self.process_cwd,
                self._remaining(deadline, "stored_thread/session"),
            )
            return session.request(
                "thread/read",
                {"threadId": thread_id, "includeTurns": True},
                timeout=self._remaining(deadline, "stored_thread/read"),
            )
        except Exception:
            return None
        finally:
            if session is not None:
                session.close(timeout=max(0.0, deadline - time.monotonic()))

    def _read_stored_outcome(
        self, thread_id: str, turn_id: str | None, *, deadline: float
    ) -> ExecutionOutcome | None:
        document = self._read_stored_document(thread_id, deadline=deadline)
        if document is None:
            return None
        turn = self._stored_turn(document, turn_id)
        if turn is None:
            return None
        status = str(turn.get("status", ""))
        if status in {"failed", "interrupted"}:
            detail = json.dumps(
                {"status": status, "error": turn.get("error")},
                ensure_ascii=False,
                sort_keys=True,
            )
            return ExecutionOutcome(
                ExecutionState.FAILED,
                failure_class=(
                    FailureClass.EXECUTION_LOST
                    if status == "interrupted"
                    else self._classify_failure(detail)
                ),
                failure_code=(
                    "CODEX_STORED_TURN_INTERRUPTED"
                    if status == "interrupted"
                    else "CODEX_STORED_TURN_FAILED"
                ),
                failure_signature=self._normalized_signature(detail),
                terminal_confirmed=True,
                quiescent_confirmed=True,
            )
        if status != "completed":
            return None
        text = self._find_last_agent_text(turn) or ""
        try:
            parsed = json.loads(text)
            payload = parsed if isinstance(parsed, dict) else {"value": parsed}
        except (json.JSONDecodeError, TypeError):
            payload = {"final_response": text, "recovered_thread_id": thread_id}
        return ExecutionOutcome(
            ExecutionState.SUCCEEDED,
            payload=payload,
            summary=text,
            terminal_confirmed=True,
            quiescent_confirmed=True,
        )

    @staticmethod
    def _stored_turn(
        document: Mapping[str, Any], turn_id: str | None
    ) -> Mapping[str, Any] | None:
        thread = document.get("thread", {})
        turns = thread.get("turns", []) if isinstance(thread, Mapping) else []
        candidates = [turn for turn in turns if isinstance(turn, Mapping)]
        if turn_id is not None:
            return next(
                (turn for turn in candidates if str(turn.get("id", "")) == turn_id),
                None,
            )
        # A newly-created execution owns a dedicated Codex thread in V0.1. If
        # turn/start replied ambiguously, exactly one stored turn is still safe
        # to reconcile; multiple turns are not guessed.
        return candidates[0] if len(candidates) == 1 else None

    @classmethod
    def _find_last_agent_text(cls, value: Any) -> str | None:
        found: list[str] = []

        def walk(item: Any) -> None:
            if isinstance(item, Mapping):
                if item.get("type") == "agentMessage":
                    for key in ("text", "content"):
                        if isinstance(item.get(key), str):
                            found.append(str(item[key]))
                for child in item.values():
                    walk(child)
            elif isinstance(item, list):
                for child in item:
                    walk(child)

        walk(value)
        return found[-1] if found else None

    @staticmethod
    def _classify_failure(detail: str) -> FailureClass:
        lowered = detail.lower()
        if (
            "429" in lowered
            or "rate limit" in lowered
            or "temporar" in lowered
            or "connection reset" in lowered
            or "connection closed" in lowered
            or "stream failure" in lowered
        ):
            return FailureClass.TRANSIENT_EXTERNAL
        if "timeout" in lowered or "timed out" in lowered:
            return FailureClass.TIMEOUT
        if (
            "unavailable" in lowered
            or "overloaded" in lowered
            or "usagelimit" in lowered
            or "usage limit" in lowered
            or "quota exceeded" in lowered
            or "insufficient quota" in lowered
            or any(code in lowered for code in ("502", "503", "504"))
        ):
            return FailureClass.RESOURCE_UNAVAILABLE
        if "permission" in lowered or "approval" in lowered or "denied" in lowered:
            return FailureClass.PERMISSION_FAILURE
        return FailureClass.ADAPTER_PROTOCOL_FAILURE

    @staticmethod
    def _normalized_signature(detail: str) -> str:
        compact = " ".join(detail.split())
        return compact[:240]
