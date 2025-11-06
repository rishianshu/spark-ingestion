from __future__ import annotations

from typing import Any, Dict, List

from salam_ingest.endpoints.base import SupportsQueryExecution
from salam_ingest.query.plan import QueryPlan, QueryResult, SelectItem

from ..results import ReconCheckResult
from .base import ReconCheck


class PlainCountCheck(ReconCheck):
    _TYPE = "plain_count"

    def _execute(self) -> ReconCheckResult:
        schema = self.context.table_cfg.get("schema")
        table = self.context.table_cfg.get("table")
        filter_expr = self.cfg.get("filter")
        query = QueryPlan(
            selects=[SelectItem(expression="COUNT(1)", alias="metric_count")],
        )
        if filter_expr:
            query = query.with_filter(filter_expr)
        source_count = self._count_endpoint(self.context.source, query, role="source")
        target_endpoint = self.context.target
        if target_endpoint is None:
            target_endpoint = self.context.source
        target_count = self._count_endpoint(target_endpoint, query, role="target")
        diff = abs(source_count - target_count)
        ratio = diff / source_count if source_count else None
        status = "pass"
        reason = None
        if diff > self.threshold.absolute:
            status = "fail"
            reason = f"abs_diff {diff} > {self.threshold.absolute}"
        if status == "pass" and self.threshold.percent is not None and source_count:
            if ratio * 100.0 > self.threshold.percent:
                status = "fail"
                reason = f"pct_diff {ratio * 100.0:.4f}% > {self.threshold.percent}%"
        detail = {
            "source_count": source_count,
            "target_count": target_count,
            "difference": diff,
            "difference_percent": ratio * 100.0 if ratio is not None else None,
        }
        if reason:
            detail["reason"] = reason
        return ReconCheckResult(
            schema=schema,
            table=table,
            status=status,
            detail=detail,
        )

    def _count_endpoint(self, endpoint: SupportsQueryExecution | None, query: QueryPlan, *, role: str) -> int:
        alias = query.selects[0].alias or "metric_count"
        if not isinstance(endpoint, SupportsQueryExecution):
            raise RuntimeError(f"{role}_endpoint_missing_sql_support")
        result = endpoint.execute_query_plan(query)
        value = result.scalar(alias)
        if value is None:
            return 0
        try:
            return int(value)
        except (TypeError, ValueError):
            return int(float(value))


class GroupCountCheck(ReconCheck):
    _TYPE = "group_count"

    def _execute(self) -> ReconCheckResult:
        schema = self.context.table_cfg.get("schema")
        table = self.context.table_cfg.get("table")
        group_by_cfg = self.cfg.get("group_by") or []
        if not group_by_cfg:
            return ReconCheckResult(
                schema=schema,
                table=table,
                status="error",
                detail="group_by_required",
            )
        group_exprs: List[str] = []
        selects: List[SelectItem] = []
        for idx, entry in enumerate(group_by_cfg):
            if isinstance(entry, dict):
                expr = entry.get("expr") or entry.get("expression")
                alias = entry.get("alias")
            else:
                expr = str(entry)
                alias = None
            if not expr or not isinstance(expr, str):
                continue
            alias = alias or f"group_{idx}"
            selects.append(SelectItem(expression=expr, alias=alias))
            group_exprs.append(expr)
        if not selects:
            return ReconCheckResult(
                schema=schema,
                table=table,
                status="error",
                detail="invalid_group_by",
            )
        metric_alias = "metric_count"
        selects.append(SelectItem(expression="COUNT(1)", alias=metric_alias))
        plan = QueryPlan(selects=selects, group_by=tuple(group_exprs))
        filter_expr = self.cfg.get("filter")
        if filter_expr:
            plan = plan.with_filter(filter_expr)
        source_map = _collect_counts(self.context.source, plan)
        target_endpoint = self.context.target or self.context.source
        target_map = _collect_counts(target_endpoint, plan)

        differences: List[Dict[str, Any]] = []

        all_keys = set(source_map) | set(target_map)
        for key in sorted(all_keys):
            s_count = source_map.get(key, 0)
            t_count = target_map.get(key, 0)
            if s_count == t_count:
                continue
            differences.append(
                {
                    "group": list(key),
                    "source_count": s_count,
                    "target_count": t_count,
                    "difference": t_count - s_count,
                }
            )

        status = "pass" if not differences else "fail"
        detail: Dict[str, Any] = {}
        if differences:
            detail["differences"] = differences[:20]
            detail["differences_total"] = len(differences)

        return ReconCheckResult(
            schema=schema,
            table=table,
            status=status,
            detail=detail or {"differences": []},
        )

        # end class


class PrimaryKeyUniquenessCheck(ReconCheck):
    _TYPE = "pk_uniqueness"

    def _execute(self) -> ReconCheckResult:
        schema = self.context.table_cfg.get("schema")
        table = self.context.table_cfg.get("table")
        pk_cols = self.context.table_cfg.get("primary_keys") or []
        if not pk_cols:
            return ReconCheckResult(
                schema=schema,
                table=table,
                status="skipped",
                detail="primary_keys_not_configured",
            )
        agg_expr = " && NULLIF"  # placeholder to satisfy linter (unused)
        filter_expr = self.cfg.get("filter")

        def _build_plan() -> QueryPlan:
            selects = [SelectItem(expression=col, alias=f"pk_{idx}") for idx, col in enumerate(pk_cols)]
            selects.append(SelectItem(expression="COUNT(1)", alias="metric_count"))
            plan = QueryPlan(selects=selects, group_by=tuple(pk_cols))
            if filter_expr:
                plan = plan.with_filter(filter_expr)
            return plan

        plan = _build_plan()

        source_map = _collect_counts(self.context.source, plan)
        target_endpoint = self.context.target or self.context.source
        target_map = _collect_counts(target_endpoint, plan)

        dup_source = [group for group, count in source_map.items() if count > 1]
        dup_target = [group for group, count in target_map.items() if count > 1]

        if not dup_source and not dup_target:
            return ReconCheckResult(
                schema=schema,
                table=table,
                status="pass",
                detail={"duplicates": []},
            )

        detail: Dict[str, Any] = {}
        if dup_source:
            detail["source_duplicates"] = [list(group) for group in dup_source[:20]]
            detail["source_duplicate_count"] = len(dup_source)
        if dup_target:
            detail["target_duplicates"] = [list(group) for group in dup_target[:20]]
            detail["target_duplicate_count"] = len(dup_target)
        return ReconCheckResult(
            schema=schema,
            table=table,
            status="fail",
            detail=detail,
        )


def _collect_counts(endpoint: SupportsQueryExecution | None, plan: QueryPlan) -> Dict[tuple, int]:
    if not isinstance(endpoint, SupportsQueryExecution):
        raise RuntimeError("endpoint_missing_query_support")
    result: QueryResult = endpoint.execute_query_plan(plan)
    records = result.to_dicts()
    counts: Dict[tuple, int] = {}
    group_aliases = [select.alias for select in plan.selects[:-1]]
    metric_alias = (plan.selects[-1].alias or "metric_count").lower()
    for row in records:
        lowered = {k.lower(): v for k, v in row.items()}
        key = []
        for alias in group_aliases:
            if not alias:
                raise RuntimeError("group_alias_missing")
            key.append(lowered.get(alias.lower()))
        metric_value = lowered.get(metric_alias)
        if metric_value is None:
            metric_value = 0
        counts[tuple(key)] = int(metric_value)
    return counts
