# -*- coding: utf-8 -*-
"""CLI: verify / analyze / dag a recorded trace log.

    python -m langgraph_dsh_trace verify  <thread.jsonl>
    python -m langgraph_dsh_trace analyze <thread.jsonl>
    python -m langgraph_dsh_trace dag     <thread.jsonl> [--mermaid]
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .analysis import analyze_log
from .dag import build_dag_from_file
from .verify import verify_log


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="langgraph_dsh_trace",
                                     description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("verify", "analyze", "dag"):
        p = sub.add_parser(name)
        p.add_argument("log", help="path to a thread .jsonl trace log")
        if name == "dag":
            p.add_argument("--mermaid", action="store_true",
                           help="print a Mermaid flowchart instead of JSON")
    args = parser.parse_args(argv)

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
    dag = build_dag_from_file(args.log)
    print(dag.to_mermaid() if args.mermaid else json.dumps(dag.to_dict(), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
