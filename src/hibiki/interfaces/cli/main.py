from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from hibiki.application.bootstrap import bootstrap_core
from hibiki.domain.enums import ActorType
from hibiki.domain.types import AuthContext


def _auth_from_args(args: argparse.Namespace) -> AuthContext:
    return AuthContext(
        principal_id=args.principal,
        actor_id=args.actor,
        actor_type=ActorType(args.actor_type),
        auth_context_id=args.auth_context,
        scopes=frozenset(args.scope or []),
    )


def _print(result: object, as_json: bool) -> None:
    if as_json:
        if hasattr(result, "ok"):
            print(
                json.dumps(
                    {
                        "ok": result.ok,
                        "data": result.data,
                        "error_code": result.error_code,
                        "error_message": result.error_message,
                        "replayed": result.replayed,
                    },
                    indent=2,
                    default=str,
                )
            )
        else:
            print(json.dumps(result, indent=2, default=str))
    else:
        print(result)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="hibiki", description="HIBIKI M0 CLI")
    p.add_argument("--data-dir", type=Path, default=Path("./data"))
    p.add_argument("--json", action="store_true", help="JSON output")
    p.add_argument("--principal", default="human_1")
    p.add_argument("--actor", default="human_1")
    p.add_argument(
        "--actor-type",
        default="HUMAN",
        choices=["HUMAN", "USER_AGENT", "INTERNAL"],
    )
    p.add_argument("--auth-context", default="cli")
    p.add_argument("--scope", action="append", default=[])
    p.add_argument("--message-id", default=None)
    p.add_argument("--idempotency-key", default=None)

    sub = p.add_subparsers(dest="cmd", required=True)

    c = sub.add_parser("create-task")
    c.add_argument("--title", required=True)
    c.add_argument("--intent", default="")
    c.add_argument("--task-id", default=None)

    c = sub.add_parser("submit-contract")
    c.add_argument("--task-id", required=True)
    c.add_argument("--objective", default=None)

    c = sub.add_parser("approve-contract")
    c.add_argument("--decision-id", required=True)
    c.add_argument("--expected-hash", default=None)
    c.add_argument("--expected-version", type=int, default=None)

    c = sub.add_parser("activate-minimal-plan")
    c.add_argument("--task-id", required=True)

    c = sub.add_parser("dispatch")
    c.add_argument("--task-id", required=True)

    c = sub.add_parser("submit-result")
    c.add_argument("--run-id", required=True)
    c.add_argument("--outcome", default="COMPLETED")
    c.add_argument("--verdict", default=None)

    c = sub.add_parser("get-task")
    c.add_argument("--task-id", required=True)

    c = sub.add_parser("list-runs")
    c.add_argument("--task-id", required=True)

    c = sub.add_parser("list-events")
    c.add_argument("--task-id", required=True)

    c = sub.add_parser("pause")
    c.add_argument("--task-id", required=True)

    c = sub.add_parser("resume")
    c.add_argument("--task-id", required=True)

    c = sub.add_parser("cancel")
    c.add_argument("--task-id", required=True)

    c = sub.add_parser("reconcile")

    return p


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    svc, ctx = bootstrap_core(args.data_dir, fake_time=False)
    auth = _auth_from_args(args)
    as_json = args.json

    try:
        if args.cmd == "create-task":
            r = svc.execute(
                "create_task",
                auth,
                {"title": args.title, "intent": args.intent, "task_id": args.task_id},
                message_id=args.message_id,
                idempotency_key=args.idempotency_key,
            )
        elif args.cmd == "submit-contract":
            payload = {"task_id": args.task_id}
            if args.objective:
                payload["objective"] = args.objective
            r = svc.execute("submit_contract", auth, payload, message_id=args.message_id)
        elif args.cmd == "approve-contract":
            r = svc.execute(
                "approve_contract",
                auth,
                {
                    "decision_id": args.decision_id,
                    "expected_target_hash": args.expected_hash,
                    "expected_target_version": args.expected_version,
                },
                message_id=args.message_id,
            )
        elif args.cmd == "activate-minimal-plan":
            r = svc.execute(
                "activate_minimal_plan", auth, {"task_id": args.task_id}, message_id=args.message_id
            )
        elif args.cmd == "dispatch":
            r = svc.execute(
                "dispatch_ready_runs", auth, {"task_id": args.task_id}, message_id=args.message_id
            )
        elif args.cmd == "submit-result":
            result = {"outcome": args.outcome}
            if args.verdict:
                result["verdict"] = args.verdict
            r = svc.execute(
                "submit_result",
                auth,
                {"run_id": args.run_id, "result": result},
                message_id=args.message_id,
            )
        elif args.cmd == "get-task":
            r = svc.get_task(args.task_id)
            _print(r, as_json)
            return 0
        elif args.cmd == "list-runs":
            r = svc.list_runs(args.task_id)
            _print(r, as_json)
            return 0
        elif args.cmd == "list-events":
            r = svc.list_events(args.task_id)
            _print(r, as_json)
            return 0
        elif args.cmd == "pause":
            r = svc.execute("pause_task", auth, {"task_id": args.task_id})
        elif args.cmd == "resume":
            r = svc.execute("resume_task", auth, {"task_id": args.task_id})
        elif args.cmd == "cancel":
            r = svc.execute("cancel_task", auth, {"task_id": args.task_id})
        elif args.cmd == "reconcile":
            r = svc.reconcile()
            _print(r, as_json)
            return 0
        else:
            parser.error(f"unknown command {args.cmd}")
            return 2
        _print(r, as_json)
        return 0 if getattr(r, "ok", True) else 1
    finally:
        ctx["lock"].release()


if __name__ == "__main__":
    sys.exit(main())
