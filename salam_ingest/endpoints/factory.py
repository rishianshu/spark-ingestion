from __future__ import annotations

from typing import Any, Dict, Tuple

from salam_ingest.query.plan import QueryPlan, QueryResult, SelectItem

from .base import REGISTRY, SinkEndpoint, SourceEndpoint, SupportsQueryExecution
from .hdfs import HdfsParquetEndpoint
from .jdbc import JdbcEndpoint
from .jdbc_mssql import MSSQLEndpoint
from .jdbc_oracle import OracleEndpoint


class HiveQueryEndpoint(SupportsQueryExecution):
    def __init__(self, spark, table_name: str) -> None:
        self.spark = spark
        self.table_name = table_name

    def execute_query_plan(self, plan: QueryPlan) -> QueryResult:
        selects = plan.selects or (SelectItem(expression="*"),)
        select_clause = ", ".join(sel.render() for sel in selects)
        where_clause = ""
        if plan.filters:
            where_clause = " WHERE " + " AND ".join(f"({expr})" for expr in plan.filters)
        group_clause = ""
        if plan.group_by:
            group_clause = " GROUP BY " + ", ".join(plan.group_by)
        having_clause = ""
        if plan.having:
            having_clause = " HAVING " + " AND ".join(f"({expr})" for expr in plan.having)
        order_clause = ""
        if plan.order_by:
            order_clause = " ORDER BY " + ", ".join(order.render() for order in plan.order_by)
        limit_clause = ""
        if plan.limit is not None:
            limit_clause = f" LIMIT {int(plan.limit)}"
        sql = f"SELECT {select_clause} FROM {self.table_name}{where_clause}{group_clause}{having_clause}{order_clause}{limit_clause}"
        rows = [row.asDict(recursive=True) for row in self.spark.sql(sql).collect()]
        return QueryResult.from_records(rows)


class EndpointFactory:
    """Construct source/sink endpoints based on table configuration."""

    @staticmethod
    def build_source(
        cfg: Dict[str, Any],
        table_cfg: Dict[str, Any],
        tool,
        metadata=None,
        emitter=None,
    ) -> SourceEndpoint:
        if tool is None:
            raise ValueError("Execution tool required for source endpoint")
        jdbc_cfg = cfg.get("jdbc", {})
        dialect = (jdbc_cfg.get("dialect") or table_cfg.get("dialect") or "generic").lower()
        if dialect == "oracle":
            endpoint = OracleEndpoint(tool, jdbc_cfg, table_cfg, metadata_access=metadata, emitter=emitter)
        elif dialect in {"mssql", "sqlserver"}:
            endpoint = MSSQLEndpoint(tool, jdbc_cfg, table_cfg, metadata_access=metadata, emitter=emitter)
        else:
            endpoint = JdbcEndpoint(tool, jdbc_cfg, table_cfg, metadata_access=metadata, emitter=emitter)
        return endpoint

    @staticmethod
    def build_sink(
        tool,
        cfg: Dict[str, Any],
        table_cfg: Dict[str, Any],
    ) -> SinkEndpoint:
        spark = getattr(tool, "spark", None)
        if spark is None:
            raise ValueError("Execution tool must expose a Spark session for HDFS sinks")
        endpoint = HdfsParquetEndpoint(spark, cfg, table_cfg)
        return endpoint

    @staticmethod
    def build_query_endpoint(
        tool,
        cfg: Dict[str, Any],
        table_cfg: Dict[str, Any],
        metadata=None,
        emitter=None,
        prefer_sink: bool = True,
    ) -> SupportsQueryExecution | None:
        mode = table_cfg.get("mode", "scd1").lower()
        spark = getattr(tool, "spark", None)
        if spark is not None:
            if mode == "full":
                hive_cfg = cfg.get("runtime", {}).get("hive", {})
                if hive_cfg.get("enabled", False):
                    db = hive_cfg.get("db")
                    tbl = f"{table_cfg['schema']}__{table_cfg['table']}"
                    table_name = f"{db}.{tbl}" if db else tbl
                    return HiveQueryEndpoint(spark, table_name)
            if prefer_sink:
                endpoint = HdfsParquetEndpoint(spark, cfg, table_cfg)
                if isinstance(endpoint, SupportsQueryExecution):
                    return endpoint
        try:
            endpoint = EndpointFactory.build_source(cfg, table_cfg, tool, metadata=metadata, emitter=emitter)
        except Exception:
            return None
        return endpoint if isinstance(endpoint, SupportsQueryExecution) else None

    @staticmethod
    def build_endpoints(
        tool,
        cfg: Dict[str, Any],
        table_cfg: Dict[str, Any],
        metadata=None,
        emitter=None,
    ) -> Tuple[SourceEndpoint, SinkEndpoint]:
        return (
            EndpointFactory.build_source(cfg, table_cfg, tool, metadata=metadata, emitter=emitter),
            EndpointFactory.build_sink(tool, cfg, table_cfg),
        )


# Register defaults for simple lookup when needed
REGISTRY.register_source("jdbc", JdbcEndpoint)
REGISTRY.register_source("oracle", OracleEndpoint)
REGISTRY.register_source("mssql", MSSQLEndpoint)
REGISTRY.register_sink("hdfs", HdfsParquetEndpoint)
