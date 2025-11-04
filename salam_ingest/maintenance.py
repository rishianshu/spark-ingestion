from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional

from .common import PrintLogger
from .endpoints.factory import EndpointFactory


def _log(logger: PrintLogger, level: str, msg: str, **fields: Any) -> None:
    getattr(logger, level.lower(), logger.info)(msg, **fields)


def run(
    tool,
    cfg: Dict[str, Any],
    tables: Iterable[Dict[str, Any]],
    *,
    load_date: str,
    mode: str,
    logger: PrintLogger,
) -> List[Dict[str, Any]]:
    """
    Execute maintenance operations for the provided tables.

    mode:
        - "dry-run": report planned actions only.
        - "apply": perform pending slice merges and ensure table metadata.
        - "rebuild-from-raw": rebuild the Iceberg table from RAW data (with backup/restore).
    """
    results: List[Dict[str, Any]] = []
    if mode == "rebuild-from-raw":
        for table_cfg in tables:
            schema = table_cfg.get("schema")
            table = table_cfg.get("table")
            keep_backup = bool((table_cfg.get("maintenance") or {}).get("keep_backup", False))
            try:
                sink = EndpointFactory.build_sink(tool, cfg, table_cfg)
            except Exception as exc:  # pragma: no cover - defensive
                _log(
                    logger,
                    "error",
                    "rebuild_sink_build_failed",
                    schema=schema,
                    table=table,
                    err=str(exc),
                )
                results.append({"schema": schema, "table": table, "status": "error", "error": str(exc)})
                continue
            if not hasattr(sink, "rebuild_from_raw"):
                results.append({"schema": schema, "table": table, "status": "unsupported"})
                _log(logger, "info", "rebuild_not_supported", schema=schema, table=table)
                continue
            outcome = sink.rebuild_from_raw(logger=logger, keep_backup=keep_backup)
            outcome.setdefault("schema", schema)
            outcome.setdefault("table", table)
            _log(
                logger,
                "info" if outcome.get("status") == "rebuild" else "warn",
                "rebuild_from_raw_result",
                schema=schema,
                table=table,
                status=outcome.get("status"),
                backup=outcome.get("backup_table"),
                rows=outcome.get("merged_rows"),
                error=outcome.get("error"),
            )
            results.append(outcome)
        return results
    apply_changes = mode == "apply"
    for table_cfg in tables:
        schema = table_cfg.get("schema")
        table = table_cfg.get("table")
        try:
            sink = EndpointFactory.build_sink(tool, cfg, table_cfg)
        except Exception as exc:  # pragma: no cover - defensive
            _log(
                logger,
                "error",
                "maintenance_sink_build_failed",
                schema=schema,
                table=table,
                err=str(exc),
            )
            results.append(
                {
                    "schema": schema,
                    "table": table,
                    "status": "error",
                    "error": str(exc),
                }
            )
            continue
        if not hasattr(sink, "maintenance_plan"):
            _log(
                logger,
                "info",
                "maintenance_not_supported",
                schema=schema,
                table=table,
            )
            results.append(
                {
                    "schema": schema,
                    "table": table,
                    "status": "unsupported",
                }
            )
            continue
        plan = sink.maintenance_plan()
        if not apply_changes:
            _log(
                logger,
                "info",
                "maintenance_plan",
                schema=schema,
                table=table,
                pending_slices=plan.get("pending_slices", 0),
                partition_spec=plan.get("partition_spec"),
                merge_filter=plan.get("merge_filter"),
            )
            plan["status"] = "dry_run"
            results.append(plan)
            continue
        try:
            outcome = sink.run_maintenance(load_date=load_date, logger=logger, apply_changes=True)
        except Exception as exc:  # pragma: no cover - defensive
            _log(
                logger,
                "error",
                "maintenance_apply_failed",
                schema=schema,
                table=table,
                err=str(exc),
            )
            results.append(
                {
                    "schema": schema,
                    "table": table,
                    "status": "error",
                    "error": str(exc),
                }
            )
            continue
        _log(
            logger,
            "info",
            "maintenance_applied",
            schema=schema,
            table=table,
            status=outcome.get("status"),
            merged_rows=outcome.get("merged_rows", 0),
            pending_slices=outcome.get("pending_slices", 0),
            partition_updated=outcome.get("partition_updated", False),
        )
        outcome.setdefault("schema", schema)
        outcome.setdefault("table", table)
        results.append(outcome)
    return results


__all__ = ["run"]
