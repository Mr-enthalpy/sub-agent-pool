from __future__ import annotations

import json
import os
import tempfile
import time
from pathlib import Path
from typing import Any, Callable, Mapping, Protocol

from .adapters.codex import AppServerSession
from .enums import OutboxState
from .models import DeliveryObservation
from .storage import Database, json_loads, utc_now


class RootBridge(Protocol):
    """Bounded wakeup side channel.

    Implementations must return within their configured delivery timeout.  The
    notifier may retry at least once; Result transport remains in SQLite.
    """

    def deliver(
        self, event_id: str, event_type: str, payload: Mapping[str, Any]
    ) -> DeliveryObservation: ...


class FilesystemRootBridge:
    """Atomic, idempotent wakeup envelopes for a local Codex Root consumer."""

    def __init__(self, inbox: str | Path):
        self.inbox = Path(inbox)

    def deliver(
        self, event_id: str, event_type: str, payload: Mapping[str, Any]
    ) -> DeliveryObservation:
        self.inbox.mkdir(parents=True, exist_ok=True)
        destination = self.inbox / f"{event_id}.json"
        if destination.exists():
            return DeliveryObservation(True, "already published")
        envelope = {
            "event_id": event_id,
            "event_type": event_type,
            "payload": payload,
        }
        fd, temporary = tempfile.mkstemp(prefix=f".{event_id}.", suffix=".tmp", dir=self.inbox)
        try:
            with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
                json.dump(envelope, handle, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, destination)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)
        return DeliveryObservation(True)


class CodexAppServerRootBridge:
    """Wake an existing Codex Root thread without transporting Result content.

    Delivery is acknowledged only after the notification turn reaches a terminal
    state. The durable outbox event id is included so an at-least-once receiver
    can deduplicate repeated wakeups.
    """

    def __init__(
        self,
        *,
        root_thread_id: str,
        command: tuple[str, ...] = ("codex", "app-server"),
        process_cwd: str | None = None,
        request_timeout: float = 30.0,
        completion_timeout: float = 120.0,
        reconcile_interval: float = 1.0,
        session_factory: Callable[..., AppServerSession] = AppServerSession,
    ) -> None:
        if not root_thread_id.strip():
            raise ValueError("root_thread_id cannot be empty")
        if request_timeout <= 0 or completion_timeout <= 0:
            raise ValueError("RootBridge timeouts must be positive")
        if reconcile_interval <= 0:
            raise ValueError("RootBridge reconcile_interval must be positive")
        self.root_thread_id = root_thread_id
        self.command = command
        self.process_cwd = process_cwd
        self.request_timeout = request_timeout
        self.completion_timeout = completion_timeout
        self.reconcile_interval = reconcile_interval
        self.session_factory = session_factory

    def deliver(
        self, event_id: str, event_type: str, payload: Mapping[str, Any]
    ) -> DeliveryObservation:
        session = self.session_factory(self.command, self.process_cwd, self.request_timeout)
        try:
            session.request("thread/resume", {"threadId": self.root_thread_id})
            notice = self._notification_text(event_id, event_type, payload)
            started = session.request(
                "turn/start",
                {
                    "threadId": self.root_thread_id,
                    "input": [{"type": "text", "text": notice}],
                },
            )
            turn_id = str(started["turn"]["id"])
            deadline = time.monotonic() + self.completion_timeout
            next_reconcile = time.monotonic()
            while time.monotonic() < deadline:
                status = self._notification_status(session.notifications(), turn_id)
                terminal = self._terminal_observation(status)
                if terminal is not None:
                    return terminal
                if session.process.poll() is not None:
                    return DeliveryObservation(False, "Codex Root app-server exited before completion")
                now = time.monotonic()
                if now >= next_reconcile:
                    remaining = max(0.001, deadline - now)
                    try:
                        document = session.request(
                            "thread/read",
                            {"threadId": self.root_thread_id, "includeTurns": True},
                            timeout=min(self.request_timeout, remaining),
                        )
                    except Exception:
                        # The notification stream remains authoritative while a
                        # bounded persisted-state read is temporarily unavailable.
                        pass
                    else:
                        status = self._stored_turn_status(document, turn_id)
                        terminal = self._terminal_observation(
                            status, source="persisted reconciliation"
                        )
                        if terminal is not None:
                            return terminal
                    next_reconcile = time.monotonic() + self.reconcile_interval
                time.sleep(0.05)
            return DeliveryObservation(False, "Codex Root notification turn timed out")
        finally:
            session.close()

    @staticmethod
    def _notification_status(
        notifications: list[Mapping[str, Any]], turn_id: str
    ) -> str | None:
        for message in reversed(notifications):
            if message.get("method") != "turn/completed":
                continue
            params = message.get("params", {})
            turn = params.get("turn", {}) if isinstance(params, Mapping) else {}
            if str(turn.get("id", "")) == turn_id:
                return str(turn.get("status", "failed"))
        return None

    @staticmethod
    def _stored_turn_status(document: Mapping[str, Any], turn_id: str) -> str | None:
        thread = document.get("thread", {})
        turns = thread.get("turns", []) if isinstance(thread, Mapping) else []
        for turn in turns:
            if isinstance(turn, Mapping) and str(turn.get("id", "")) == turn_id:
                return str(turn.get("status", ""))
        return None

    @staticmethod
    def _terminal_observation(
        status: str | None, *, source: str = "live notification"
    ) -> DeliveryObservation | None:
        if status == "completed":
            return DeliveryObservation(True, f"confirmed by {source}")
        if status in {"failed", "interrupted"}:
            return DeliveryObservation(False, f"Codex Root turn ended as {status} ({source})")
        return None

    @staticmethod
    def _notification_text(
        event_id: str, event_type: str, payload: Mapping[str, Any]
    ) -> str:
        indexes = {
            key: value
            for key, value in payload.items()
            if key.endswith("_id") or key.endswith("_ids")
        }
        return (
            "LOCAL AGENT SCHEDULER NOTIFICATION\n\n"
            f"EVENT_ID\n{event_id}\n\n"
            f"EVENT_TYPE\n{event_type}\n\n"
            "INDEXES\n"
            f"{json.dumps(indexes, ensure_ascii=False, sort_keys=True)}\n\n"
            "This is a wakeup/control notification, not Result transport. "
            "Read authoritative Scheduler state and Result Queue by event id; "
            "deduplicate repeated delivery using EVENT_ID."
        )


class OutboxDispatcher:
    def __init__(self, database: Database, bridge: RootBridge):
        self.db = database
        self.bridge = bridge

    def deliver_pending(self, *, now: float | None = None, limit: int = 100) -> int:
        now = utc_now() if now is None else now
        rows = self.db.fetch_all(
            "SELECT * FROM notification_outbox WHERE state='PENDING' AND next_delivery_at<=? "
            "ORDER BY created_at LIMIT ?",
            (now, limit),
        )
        delivered = 0
        for row in rows:
            try:
                observation = self.bridge.deliver(
                    row["id"], row["event_type"], json_loads(row["payload_json"], {})
                )
            except Exception as exc:  # RootBridge failure is data, not Task failure.
                observation = DeliveryObservation(False, str(exc))
            delivery_finished_at = utc_now()
            with self.db.transaction() as conn:
                current = conn.execute(
                    "SELECT state,delivery_attempts FROM notification_outbox WHERE id=?",
                    (row["id"],),
                ).fetchone()
                if not current or current["state"] != OutboxState.PENDING.value:
                    continue
                attempts = int(current["delivery_attempts"]) + 1
                if observation.delivered:
                    conn.execute(
                        "UPDATE notification_outbox SET state='DELIVERED',delivery_attempts=?,"
                        "delivered_at=?,last_error=NULL WHERE id=?",
                        (attempts, delivery_finished_at, row["id"]),
                    )
                    delivered += 1
                else:
                    delay = min(300.0, float(2 ** min(attempts, 8)))
                    conn.execute(
                        "UPDATE notification_outbox SET delivery_attempts=?,next_delivery_at=?,"
                        "last_error=? WHERE id=?",
                        (
                            attempts,
                            delivery_finished_at + delay,
                            observation.detail,
                            row["id"],
                        ),
                    )
        return delivered

    def acknowledge(self, event_id: str) -> None:
        now = utc_now()
        with self.db.transaction() as conn:
            conn.execute(
                "UPDATE notification_outbox SET state='ACKED',acknowledged_at=? "
                "WHERE id=? AND state IN ('PENDING','DELIVERED')",
                (now, event_id),
            )
