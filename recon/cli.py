from __future__ import annotations

import argparse
import json
from typing import Any, Dict, List, Optional

from salam_ingest.common import PrintLogger
from salam_ingest.orchestrator_helpers import filter_tables
from salam_ingest.tools.spark import SparkTool

from .runner import run_reconciliation


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="recon")
    parser.add_argument("--config", required=True, help="Path to ingestion configuration file")
    parser.add_argument("--only-tables", help="Comma separated schema.table filters", default=None)
    parser.add_argument(
        "--checks",
        help="Comma separated reconciliation check types or names to execute (default: all configured)",
        default=None,
    )
    parser.add_argument(
        "--output-json",
        help="Optional path to write reconciliation results as JSON",
        default=None,
    )
    parser.add_argument(
        "--fail-on-warn",
        action="store_true",
        help="Exit with non-zero code if any check returns warn or fail status",
        default=False,
    )
    parser.add_argument(
        "--engine",
        choices=["spark", "sqlalchemy"],
        default="spark",
        help="Execution engine to use (default: spark)",
    )
    parser.add_argument(
        "--max-parallel",
        type=int,
        default=None,
        help="Maximum number of reconciliation tasks to run in parallel",
    )
    return parser.parse_args(argv)


def run_cli(argv: Optional[List[str]] = None) -> None:
    args = parse_args(argv)
    with open(args.config, "r", encoding="utf-8") as handle:
        cfg: Dict[str, Any] = json.load(handle)
    tables = filter_tables(cfg["tables"], args.only_tables)
    if not tables:
        print(json.dumps({"status": "no_tables"}))
        return
    logger = PrintLogger(job_name=cfg.get("runtime", {}).get("job_name", "recon"))
    if args.engine == "sqlalchemy":
        from salam_ingest.tools.sqlalchemy import SQLAlchemyTool

        tool = SQLAlchemyTool.from_config(cfg)
    else:
        tool = SparkTool.from_config(cfg)
        logger.spark = tool.spark
    try:
        selected_checks = None
        if args.checks:
            selected_checks = {entry.strip().lower() for entry in args.checks.split(",") if entry.strip()}
        results = run_reconciliation(
            tool=tool,
            cfg=cfg,
            tables=tables,
            logger=logger,
            selected_checks=selected_checks,
            max_parallel=args.max_parallel,
        )
    finally:
        tool.stop()
    if args.output_json:
        with open(args.output_json, "w", encoding="utf-8") as handle:
            json.dump(results, handle, indent=2, sort_keys=True)
    else:
        print(json.dumps(results, indent=2, sort_keys=True))
    if args.fail_on_warn:
        statuses = [item.get("status", "").lower() for item in results.get("checks", [])]
        if any(status in {"warn", "fail", "error"} for status in statuses):
            raise SystemExit(2)


__all__ = ["parse_args", "run_cli"]
