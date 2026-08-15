from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from .adapters.codex import CodexAppServerAdapter
from .config import SchedulerConfig, load_config
from .core import Scheduler
from .enums import ContinuityPreference, FailureClass, Retention, WorkspaceMode
from .errors import ConfigurationError
from .models import PartitionSpec, RetryPolicy, TaskSpec
from .root_bridge import CodexAppServerRootBridge, FilesystemRootBridge, OutboxDispatcher
from .runtime import Dispatcher, SchedulerDaemon
from .storage import Database


def _print(value: Any) -> None:
    print(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True, default=str))


def _load_context(args: argparse.Namespace) -> tuple[Scheduler, SchedulerConfig | None]:
    config = load_config(args.config) if args.config else None
    db_path = config.resolve(config.database_path) if config else args.db
    scheduler = Scheduler(
        Database(db_path),
        lease_seconds=config.lease_seconds if config else 120.0,
        continuity_max_bytes=config.continuity_max_bytes if config else 16_384,
    )
    scheduler.initialize()
    return scheduler, config


def _apply_config(scheduler: Scheduler, config: SchedulerConfig) -> None:
    scheduler.bootstrap_partitions(config.partitions)
    scheduler.reconcile_pool()


def _partition_spec_for_upsert(
    args: argparse.Namespace, config: SchedulerConfig | None
) -> PartitionSpec:
    if not config:
        raise ConfigurationError("--config is required for pool upsert")
    target_names = {target.name for target in config.execution_targets}
    if args.execution_target not in target_names:
        raise ConfigurationError(
            f"unknown execution target {args.execution_target!r}"
        )
    if args.execution_profile not in config.execution_profiles:
        raise ConfigurationError(
            f"unknown execution profile {args.execution_profile!r}"
        )
    return PartitionSpec(
        args.name,
        args.desired_capacity,
        Retention(args.retention),
        args.execution_target,
        args.execution_profile,
        tuple(args.tag),
    )


def _build_dispatcher(
    scheduler: Scheduler, config: SchedulerConfig
) -> tuple[Dispatcher, SchedulerDaemon]:
    profile_names = set(config.execution_profiles)
    process_cwd = config.resolve(config.codex_adapter.cwd)
    adapter = CodexAppServerAdapter(
        command=config.codex_adapter.command,
        process_cwd=process_cwd,
        approval_policy=config.codex_adapter.approval_policy,
        sandbox=config.codex_adapter.sandbox,
        profile_options=config.execution_profiles,
    )
    adapters = {target.name: adapter for target in config.execution_targets}
    targets = {target.name: target for target in config.execution_targets}
    if config.root_bridge.kind == "codex_app_server":
        bridge = CodexAppServerRootBridge(
            root_thread_id=config.root_bridge.root_thread_id or "",
            command=config.codex_adapter.command,
            process_cwd=process_cwd,
            request_timeout=config.root_bridge.request_timeout,
            completion_timeout=config.root_bridge.completion_timeout,
        )
    else:
        bridge = FilesystemRootBridge(config.resolve(config.root_bridge.inbox))
    outbox = OutboxDispatcher(scheduler.db, bridge)
    dispatcher = Dispatcher(
        scheduler,
        adapters=adapters,
        targets=targets,
        execution_profiles=profile_names,
        workspace_root=process_cwd,
        outbox=outbox,
    )
    return dispatcher, SchedulerDaemon(
        dispatcher,
        poll_seconds=config.dispatcher_poll_seconds,
        heartbeat_seconds=config.heartbeat_seconds,
    )


def _task_specs(
    document: dict[str, Any], default_retry: RetryPolicy | None = None
) -> list[TaskSpec]:
    specs: list[TaskSpec] = []
    for item in document.get("tasks", []):
        if "required" in item:
            raise ValueError(
                "optional Tasks are not supported: remove the 'required' field; "
                "every Batch Task participates in the completion barrier"
            )
        retry = item.get("retry_policy", {})
        defaults = default_retry or RetryPolicy()
        policy = RetryPolicy(
            max_attempts=int(retry.get("max_attempts", defaults.max_attempts)),
            retry_classes=tuple(
                FailureClass(value)
                for value in retry.get(
                    "retry_classes",
                    [item.value for item in defaults.retry_classes],
                )
            ),
            base_backoff_seconds=float(
                retry.get("base_backoff_seconds", defaults.base_backoff_seconds)
            ),
            max_backoff_seconds=float(
                retry.get("max_backoff_seconds", defaults.max_backoff_seconds)
            ),
        )
        specs.append(
            TaskSpec(
                name=item["name"],
                payload=item["payload"],
                acceptance=item.get("acceptance", {}),
                partition=item.get("partition", "general"),
                workstream_id=item.get("workstream_id"),
                continuity=ContinuityPreference(item.get("continuity", "none")),
                affinity_tags=tuple(item.get("affinity_tags", [])),
                workspace_mode=WorkspaceMode(item.get("workspace_mode", "read_only")),
                dependencies=tuple(item.get("dependencies", [])),
                priority=int(item.get("priority", 0)),
                retry_policy=policy,
                supersedes_task_id=item.get("supersedes_task_id"),
                task_id=item.get("task_id"),
            )
        )
    return specs


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="local-agent-scheduler")
    parser.add_argument("--db", default=".local-agent-scheduler.db")
    parser.add_argument("--config")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("init")
    commands.add_parser("status")

    for entity in ("task", "batch", "agent", "execution", "result", "escalation", "outbox"):
        sub = commands.add_parser(entity).add_subparsers(dest="action", required=True)
        listing = sub.add_parser("list")
        listing.add_argument("--state")
        show = sub.add_parser("show")
        show.add_argument("id")
        if entity == "batch":
            submit = sub.add_parser("submit")
            submit.add_argument("--file", required=True)
            cancel = sub.add_parser("cancel")
            cancel.add_argument("id")
        if entity == "task":
            cancel = sub.add_parser("cancel")
            cancel.add_argument("id")
        if entity == "result":
            ack = sub.add_parser("ack")
            ack.add_argument("id")
            ack.add_argument("--consumer", required=True)
        if entity == "escalation":
            resolve = sub.add_parser("resolve")
            resolve.add_argument("id")
            resolve.add_argument(
                "--operation",
                choices=("retry", "cancel_task", "release_cancelled_writer"),
                required=True,
            )
            resolve.add_argument("--confirm-quiescent", action="store_true")
        if entity == "execution":
            interrupt = sub.add_parser("interrupt")
            interrupt.add_argument("id")
            terminate = sub.add_parser("terminate")
            terminate.add_argument("id")

    pool = commands.add_parser("pool").add_subparsers(dest="action", required=True)
    pool.add_parser("show")
    pool.add_parser("reconcile")
    upsert = pool.add_parser("upsert")
    upsert.add_argument("name")
    upsert.add_argument("desired_capacity", type=int)
    upsert.add_argument("retention", choices=("resident", "ephemeral"))
    upsert.add_argument("execution_target")
    upsert.add_argument("execution_profile")
    upsert.add_argument("--tag", action="append", default=[])
    resize = pool.add_parser("resize")
    resize.add_argument("name")
    resize.add_argument("desired_capacity", type=int)
    move_capacity = pool.add_parser("move-capacity")
    move_capacity.add_argument("source")
    move_capacity.add_argument("target")
    move_capacity.add_argument("count", type=int)
    move = pool.add_parser(
        "move-agent",
        help="diagnostic/admin primitive; normal orchestration should move capacity",
    )
    move.add_argument("agent_id")
    move.add_argument("target_partition")
    merge = pool.add_parser("merge")
    merge.add_argument("source")
    merge.add_argument("target")
    retire = pool.add_parser("retire")
    retire.add_argument("partition")

    commands.add_parser("recover")
    daemon = commands.add_parser("daemon")
    daemon.add_argument("--once", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    scheduler, config = _load_context(args)
    if args.command == "init":
        if config:
            _apply_config(scheduler, config)
        _print(scheduler.status())
        return 0
    if args.command == "status":
        _print(scheduler.status())
        return 0

    table_map = {
        "task": "tasks",
        "batch": "batches",
        "agent": "logical_agents",
        "execution": "executions",
        "result": "results",
        "escalation": "escalations",
        "outbox": "notification_outbox",
    }
    if args.command in table_map:
        table = table_map[args.command]
        if args.action == "list":
            _print(scheduler.list(table, state=args.state))
            return 0
        if args.action == "show":
            _print(scheduler.get(table, args.id))
            return 0
        if args.command == "batch" and args.action == "submit":
            document = json.loads(Path(args.file).read_text(encoding="utf-8"))
            batch_id, task_ids = scheduler.submit_batch(
                _task_specs(document, config.retry_defaults if config else None),
                metadata=document.get("metadata", {}),
            )
            _print({"batch_id": batch_id, "task_ids": task_ids})
            return 0
        if args.command == "batch" and args.action == "cancel":
            scheduler.cancel_batch(args.id)
            _print(scheduler.get("batches", args.id))
            return 0
        if args.command == "task" and args.action == "cancel":
            scheduler.cancel_task(args.id)
            _print(scheduler.get("tasks", args.id))
            return 0
        if args.command == "result" and args.action == "ack":
            scheduler.ack_result(args.id, consumer_ref=args.consumer)
            _print(scheduler.get("results", args.id))
            return 0
        if args.command == "escalation" and args.action == "resolve":
            scheduler.resolve_escalation(
                args.id,
                operation=args.operation,
                quiescence_confirmed=args.confirm_quiescent,
            )
            _print(scheduler.get("escalations", args.id))
            return 0
        if args.command == "execution" and args.action in {"interrupt", "terminate"}:
            if not config:
                raise SystemExit("--config is required for execution interruption")
            _apply_config(scheduler, config)
            dispatcher, _daemon = _build_dispatcher(scheduler, config)
            _print(dispatcher.interrupt_execution(args.id, terminate=args.action == "terminate"))
            return 0

    if args.command == "pool":
        if args.action == "show":
            _print(
                {
                    "partitions": scheduler.list("pool_partitions"),
                    "agents": scheduler.list("logical_agents"),
                }
            )
        elif args.action == "reconcile":
            _print(scheduler.reconcile_pool())
        elif args.action == "upsert":
            revision = scheduler.upsert_partition(
                _partition_spec_for_upsert(args, config)
            )
            _print({"revision": revision})
        elif args.action == "resize":
            _print({"revision": scheduler.resize_partition(args.name, args.desired_capacity)})
        elif args.action == "move-capacity":
            _print(
                {
                    "revision": scheduler.move_capacity(
                        args.source, args.target, args.count
                    )
                }
            )
        elif args.action == "move-agent":
            scheduler.move_agent(args.agent_id, args.target_partition)
            _print(scheduler.get("logical_agents", args.agent_id))
        elif args.action == "retire":
            _print({"revision": scheduler.retire_partition(args.partition)})
        else:
            _print({"revision": scheduler.merge_partitions(args.source, args.target)})
        return 0

    if args.command in {"recover", "daemon"}:
        if not config:
            raise SystemExit("--config is required for recover/daemon")
        _apply_config(scheduler, config)
        dispatcher, daemon = _build_dispatcher(scheduler, config)
        if args.command == "recover":
            _print(dispatcher.recover())
            return 0
        if args.once:
            _print(daemon.run_until_idle(max_wait_seconds=config.lease_seconds))
            return 0
        daemon.run()
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main())
