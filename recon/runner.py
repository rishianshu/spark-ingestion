from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Sequence

from salam_ingest.common import PrintLogger
from salam_ingest.endpoints.base import SupportsQueryExecution
from salam_ingest.endpoints.factory import EndpointFactory
from salam_ingest.query.plan import QueryPlan, SelectItem
from salam_ingest.metadata import build_metadata_access, collect_metadata
from salam_ingest.events import emit_log

from .checks import registry
from .context import ReconContext
from .results import ReconRunSummary, ReconCheckResult


@dataclass
class _TablePlan:
    table_cfg: Dict[str, Any]
    checks: List[Dict[str, Any]] = field(default_factory=list)


def _resolve_table_plans(
    cfg: Dict[str, Any],
    tables: Iterable[Dict[str, Any]],
    *,
    selected_checks: Optional[Sequence[str]],
) -> List[_TablePlan]:
    selected_normalized = {entry.lower() for entry in selected_checks} if selected_checks else None
    plans: List[_TablePlan] = []
    defaults = cfg.get("runtime", {}).get("reconciliation_defaults") or {}
    default_checks = defaults.get("checks") or []
    for table_cfg in tables:
        recon_cfg = table_cfg.get("reconciliation")
        checks_cfg = list(default_checks)
        if isinstance(recon_cfg, dict):
            checks_cfg.extend(recon_cfg.get("checks") or [])
        elif isinstance(recon_cfg, list):
            checks_cfg.extend(recon_cfg)
        if not checks_cfg:
            continue
        enabled_checks: List[Dict[str, Any]] = []
        for entry in checks_cfg:
            if not isinstance(entry, dict):
                continue
            if str(entry.get("enabled", True)).lower() == "false":
                continue
            check_type = str(entry.get("type", "")).strip().lower()
            check_name = str(entry.get("name", check_type)).strip().lower()
            if selected_normalized and check_type not in selected_normalized and check_name not in selected_normalized:
                continue
            enabled_checks.append(entry)
        if enabled_checks:
            plans.append(_TablePlan(table_cfg=table_cfg, checks=enabled_checks))
    return plans


def run_reconciliation(
    *,
    tool,
    cfg: Dict[str, Any],
    tables: Iterable[Dict[str, Any]],
    logger: PrintLogger,
    selected_checks: Optional[Iterable[str]] = None,
    max_parallel: Optional[int] = None,
) -> Dict[str, Any]:
    plans = _resolve_table_plans(cfg, tables, selected_checks=selected_checks)
    if not plans:
        return {"status": "no_checks", "checks": []}
    metadata_access = build_metadata_access(cfg, logger)
    collect_metadata(cfg, [plan.table_cfg for plan in plans], tool, logger)
    spark = getattr(tool, "spark", None)
    results: List[ReconCheckResult] = []

    def run_plan(plan: _TablePlan) -> List[ReconCheckResult]:
        table_cfg = dict(plan.table_cfg)
        schema = table_cfg.get("schema")
        table = table_cfg.get("table")
        try:
            source_endpoint = EndpointFactory.build_source(cfg, table_cfg, tool, metadata=metadata_access)
        except Exception as exc:  # pragma: no cover - defensive
            emit_log(
                None,
                level="ERROR",
                msg="recon_source_build_failed",
                schema=schema,
                table=table,
                err=str(exc),
                logger=logger,
            )
            return [
                ReconCheckResult(
                    schema=schema,
                    table=table,
                    check_name="*",
                    check_type="build_source",
                    status="error",
                    detail=f"source_build_failed: {exc}",
                )
            ]
        if not isinstance(source_endpoint, SupportsQueryExecution):
            emit_log(
                None,
                level="ERROR",
                msg="recon_source_missing_query_capability",
                schema=schema,
                table=table,
                logger=logger,
            )
            return [
                ReconCheckResult(
                    schema=schema,
                    table=table,
                    check_name="*",
                    check_type="capability",
                    status="error",
                    detail="source_missing_query_capability",
                )
            ]
        target_endpoint = EndpointFactory.build_query_endpoint(
            tool,
            cfg,
            table_cfg,
            metadata=metadata_access,
            emitter=None,
            prefer_sink=True,
        )
        if target_endpoint is None:
            emit_log(
                None,
                level="WARN",
                msg="recon_target_missing_query_endpoint",
                schema=schema,
                table=table,
                logger=logger,
            )
        ctx = ReconContext(
            spark=spark,
            tool=tool,
            cfg=cfg,
            table_cfg=table_cfg,
            logger=logger,
            source=source_endpoint,
            target=target_endpoint,
        )
        plan_results: List[ReconCheckResult] = []
        for check_cfg in plan.checks:
            check_type = str(check_cfg.get("type", "")).strip().lower()
            check_cls = registry.get(check_type)
            if check_cls is None:
                plan_results.append(
                    ReconCheckResult(
                        schema=schema,
                        table=table,
                        check_name=str(check_cfg.get("name") or check_type or "unknown"),
                        check_type=check_type or "unknown",
                        status="error",
                        detail="unknown_check_type",
                    )
                )
                continue
            check = check_cls(ctx, check_cfg)
            plan_results.append(check.run())
        return plan_results

    parallel_config = max_parallel
    if parallel_config is None:
        parallel_config = int(cfg.get("runtime", {}).get("reconciliation_defaults", {}).get("max_parallel", 1))
    if parallel_config < 1:
        parallel_config = 1
    if getattr(tool, "spark", None) is not None and parallel_config > 1:
        emit_log(
            None,
            level="WARN",
            msg="recon_parallelism_limited_for_spark",
            requested=parallel_config,
            applied=1,
            logger=logger,
        )
        parallel_config = 1

    if parallel_config == 1:
        for plan in plans:
            results.extend(run_plan(plan))
    else:
        with ThreadPoolExecutor(max_workers=parallel_config) as executor:
            future_map = {executor.submit(run_plan, plan): plan for plan in plans}
            for future in as_completed(future_map):
                results.extend(future.result())
    summary = ReconRunSummary.from_results(results)
    payload = summary.to_dict()
    payload["checks"] = [result.to_dict() for result in results]
    return payload


__all__ = ["run_reconciliation"]
