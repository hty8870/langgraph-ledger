# -*- coding: utf-8 -*-
"""CLI: verify / analyze / dag / repair / replay a recorded trace log.

    python -m langgraph_ledger verify  <thread.jsonl>
    python -m langgraph_ledger analyze <thread.jsonl>
    python -m langgraph_ledger dag     <thread.jsonl> [--mermaid]
    python -m langgraph_ledger repair  <thread.jsonl | trace-root/>
    python -m langgraph_ledger replay  <thread.jsonl>
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .analysis import analyze_log
from .dag import build_dag_from_file
from .repair import close_orphaned_run, repair_all
from .replay import replay_messages
from .verify import verify_log


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="langgraph_ledger",
                                     description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("verify", "analyze", "dag", "replay"):
        p = sub.add_parser(name)
        p.add_argument("log", help="path to a thread .jsonl trace log")
        if name == "dag":
            p.add_argument("--mermaid", action="store_true",
                           help="print a Mermaid flowchart instead of JSON")
    p = sub.add_parser("repair", help="close crash-orphaned runs "
                       "(a single log or a whole trace-root directory)")
    p.add_argument("target", help="a thread .jsonl log or a trace-root directory")
    args = parser.parse_args(argv)

    if args.command == "repair":
        target = Path(args.target)
        if target.is_dir():
            summary = repair_all(target)
        elif target.is_file():
            summary = close_orphaned_run(target)
        else:
            print(f"target not found: {target}", file=sys.stderr)
            return 2
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0

    if not Path(args.log).is_file():
        print(f"log not found: {args.log}", file=sys.stderr)
        return 2

    if args.command == "verify":
        report = verify_log(args.log)
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 0 if report else 1
    if args.command == "analyze":
        print(json.dumps(analyze_log(args.log), ensure_ascii=False, indent=2))
        return 0
    if args.command == "replay":
        print(json.dumps(replay_messages(args.log), ensure_ascii=False, indent=2))
        return 0
    dag = build_dag_from_file(args.log)
    print(dag.to_mermaid() if args.mermaid else json.dumps(dag.to_dict(), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
