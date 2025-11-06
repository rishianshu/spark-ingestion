from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, to_timestamp
from pyspark.sql.utils import AnalysisException

from ...common import RUN_ID
from ...io.filesystem import HDFSUtil
from ...io.paths import Paths
from ...staging import Staging
from ..base import (
    EndpointCapabilities,
    SupportsQueryExecution,
    IncrementalCommitResult,
    IncrementalContext,
    IngestionSlice,
    SinkEndpoint,
    SinkFinalizeResult,
    SinkWriteResult,
    SliceStageResult,
)
from salam_ingest.query.plan import QueryPlan, QueryResult, SelectItem, OrderItem
from .warehouse import HiveHelper, IcebergHelper


def _load_raw_increment_df(
    spark: SparkSession,
    runtime_cfg: Dict[str, Any],
    schema: str,
    table: str,
    last_ld: str,
    last_wm: str,
    incr_col: str,
    incr_type: str = "",
) -> DataFrame:
    _, _, base_raw = Paths.build(runtime_cfg, schema, table, "")
    all_raw = spark.read.format(runtime_cfg.get("write_format", "parquet")).load(base_raw)
    if incr_col.lower() not in [c.lower() for c in all_raw.columns]:
        return all_raw.limit(0)
    df = all_raw.where(col("load_date") >= lit(last_ld))
    incr_type = (incr_type or "").lower()
    if incr_type.startswith("epoch") or incr_type in ("bigint", "int", "numeric", "decimal"):
        df = df.where(col(incr_col).cast("long") > F.lit(int(str(last_wm))))
    else:
        df = df.where(to_timestamp(col(incr_col)) > to_timestamp(lit(last_wm)))
    return df


class HdfsParquetEndpoint(SinkEndpoint, SupportsQueryExecution):
    """Handles landing to RAW directories and optional Hive registration."""

    def __init__(self, spark: SparkSession, cfg: Dict[str, Any], table_cfg: Dict[str, Any]) -> None:
        self.spark = spark
        self.cfg = cfg
        self.runtime_cfg = cfg["runtime"]
        self.table_cfg = dict(table_cfg)
        self.schema = table_cfg["schema"]
        self.table = table_cfg["table"]
        self.load_date: Optional[str] = None
        self.base_raw = Paths.build(self.runtime_cfg, self.schema, self.table, "")[2]
        settings = self._resolve_partition_settings()
        self._partition_spec = settings["spec"]
        self._derived_columns = settings["derived_columns"]
        self._merge_filter_expr = settings["merge_filter"]
        self._caps = EndpointCapabilities(
            supports_write=True,
            supports_finalize=True,
            supports_publish=True,
            supports_watermark=True,
            supports_staging=True,
            supports_merge=True,
            event_metadata_keys=("location", "hive_table"),
        )

    # ------------------------------------------------------------------ SinkEndpoint protocol
    def configure(self, table_cfg: Dict[str, Any]) -> None:
        self.table_cfg.update(table_cfg)
        self.schema = self.table_cfg["schema"]
        self.table = self.table_cfg["table"]
        self.base_raw = Paths.build(self.runtime_cfg, self.schema, self.table, "")[2]
        settings = self._resolve_partition_settings()
        self._partition_spec = settings["spec"]
        self._derived_columns = settings["derived_columns"]
        self._merge_filter_expr = settings["merge_filter"]

    def capabilities(self) -> EndpointCapabilities:
        return self._caps

    def write_raw(
        self,
        df: DataFrame,
        *,
        mode: str,
        load_date: str,
        schema: str,
        table: str,
        rows: Optional[int] = None,
    ) -> SinkWriteResult:
        self.load_date = load_date
        raw_dir, _, _ = self._paths_for(load_date)
        df_augmented = self._apply_derived_columns(df)
        rows_written = df_augmented.count()
        if rows_written == 0:
            metrics = {
                "schema": schema,
                "table": table,
                "run_id": RUN_ID,
                "rows_written": 0,
                "status": "skipped",
                "phase": "raw",
                "load_date": load_date,
            }
            HDFSUtil.write_text(
                spark=self.spark,
                path=f"{raw_dir}/_METRICS.json",
                content=json.dumps(metrics),
            )
            event_payload = {"location": raw_dir, "rows_written": 0, "skipped": True}
            return SinkWriteResult(rows=0, path=raw_dir, event_payload=event_payload)
        rows = rows_written
        (
            df_augmented.write.format(self.runtime_cfg.get("write_format", "parquet"))
            .mode(mode)
            .option("compression", self.runtime_cfg.get("compression", "snappy"))
            .save(raw_dir)
        )
        metrics = {
            "schema": schema,
            "table": table,
            "run_id": RUN_ID,
            "rows_written": rows,
            "status": "success",
            "phase": "raw",
            "load_date": load_date,
        }
        HDFSUtil.write_text(
            spark=self.spark,
            path=f"{raw_dir}/_METRICS.json",
            content=json.dumps(metrics),
        )
        event_payload = {"location": raw_dir, "rows_written": rows}
        return SinkWriteResult(rows=rows, path=raw_dir, event_payload=event_payload)

    def finalize_full(
        self,
        *,
        load_date: str,
        schema: str,
        table: str,
    ) -> SinkFinalizeResult:
        raw_dir, final_dir, _ = self._paths_for(load_date)
        rt = self.runtime_cfg
        hive_table = None
        if rt.get("finalize_strategy") == "hive_set_location" and rt.get("hive", {}).get("enabled", False):
            hive_table = HiveHelper.set_location(
                self.spark,
                rt["hive"]["db"],
                schema,
                table,
                raw_dir,
            )
        else:
            jsc = self.spark._jsc
            jvm = self.spark.sparkContext._jvm
            conf = jsc.hadoopConfiguration()
            fs = jvm.org.apache.hadoop.fs.FileSystem.get(conf)
            Path = jvm.org.apache.hadoop.fs.Path
            src = Path(raw_dir)
            dst = Path(final_dir)
            tmp = Path(final_dir + ".__tmp_swap__")
            if fs.exists(tmp):
                fs.delete(tmp, True)
            ok = jvm.org.apache.hadoop.fs.FileUtil.copy(fs, src, fs, tmp, False, conf)
            if not ok:
                raise RuntimeError(f"Copy {raw_dir} -> {final_dir}/.__tmp_swap__ failed")
            if fs.exists(dst):
                fs.delete(dst, True)
            if not fs.rename(tmp, dst):
                raise RuntimeError(f"Atomic rename failed for {final_dir}")
        if rt.get("hive", {}).get("enabled", False):
            hive_db = rt["hive"]["db"]
            HiveHelper.create_unpartitioned_parquet_if_absent_with_schema(
                self.spark,
                hive_db,
                schema,
                table,
                final_dir,
            )
        event_payload = {"location": final_dir}
        if hive_table:
            event_payload["hive_table"] = hive_table
        return SinkFinalizeResult(final_path=final_dir, event_payload=event_payload)

    def stage_incremental_slice(
        self,
        df: DataFrame,
        *,
        context: IncrementalContext,
        slice_info: IngestionSlice,
    ) -> SliceStageResult:
        incr_col = context.incremental_column
        hi_value = slice_info.upper or context.planner_metadata.get("now_literal") or context.effective_watermark
        slice_path = Staging.slice_dir(
            self.cfg,
            context.schema,
            context.table,
            incr_col,
            slice_info.lower,
            hi_value,
        )
        if Staging.is_success(self.spark, slice_path) and Staging.is_landed(self.spark, slice_path):
            return SliceStageResult(
                slice=slice_info,
                path=slice_path,
                rows=0,
                skipped=True,
                event_payload={"slice_lower": slice_info.lower, "slice_upper": slice_info.upper, "skipped": True},
            )

        df_enriched = self._apply_derived_columns(df)
        rows = df_enriched.count()
        (
            df_enriched.write.format(self.runtime_cfg.get("write_format", "parquet"))
            .mode("overwrite")
            .option("compression", self.runtime_cfg.get("compression", "snappy"))
            .save(slice_path)
        )
        Staging.write_text(
            self.spark,
            f"{slice_path}/_RANGE.json",
            json.dumps({"lo": slice_info.lower, "hi": slice_info.upper, "planned_at": datetime.now().isoformat()}),
        )
        Staging.mark_success(self.spark, slice_path)
        event_payload = {
            "slice_lower": slice_info.lower,
            "slice_upper": slice_info.upper,
            "rows_written": rows,
        }
        return SliceStageResult(slice=slice_info, path=slice_path, rows=rows, event_payload=event_payload)

    def commit_incremental(
        self,
        *,
        load_date: str,
        schema: str,
        table: str,
        context: IncrementalContext,
        staged_slices: List[SliceStageResult],
    ) -> IncrementalCommitResult:
        raw_dir, _, _ = self._paths_for(load_date)
        active_slices = [s for s in staged_slices if not s.skipped]
        if not active_slices:
            return IncrementalCommitResult(
                rows=0,
                raw_path=raw_dir,
                new_watermark=context.last_watermark,
                new_loaded_date=context.last_loaded_date,
                additional_metadata={"skipped": True},
            )

        slice_paths = [s.path for s in active_slices]
        read_format = self.runtime_cfg.get("write_format", "parquet")
        window_df = self.spark.read.format(read_format).load(slice_paths)
        augmented_df = self._apply_derived_columns(window_df)
        total_rows = augmented_df.count()
        if total_rows == 0:
            for slice_result in active_slices:
                Staging.mark_landed(self.spark, slice_result.path)
            return IncrementalCommitResult(
                rows=0,
                raw_path=raw_dir,
                new_watermark=context.last_watermark,
                new_loaded_date=context.last_loaded_date,
                additional_metadata={"skipped": True, "staged_slices": len(active_slices)},
            )

        intermediate_payload: Dict[str, Any] = {}
        additional_metadata: Dict[str, Any] = {}

        inter_cfg = self.runtime_cfg.get("intermediate", {"enabled": False})
        if inter_cfg.get("enabled", False) and context.primary_keys:
            iceberg_table = IcebergHelper.merge_upsert(
                self.spark,
                {"runtime": self.runtime_cfg},
                schema,
                table,
                augmented_df,
                context.primary_keys,
                load_date,
                partition_col=inter_cfg.get("partition_col", "load_date"),
                incr_col=context.incremental_column,
                partition_cfg={
                    "spec": self._partition_spec,
                },
                merge_filter_expr=self._merge_filter_expr,
            )
            intermediate_payload["iceberg_table"] = iceberg_table
            if self.runtime_cfg.get("final_parquet_mirror", {}).get("enabled", False):
                mirror_info = IcebergHelper.mirror_to_parquet_for_date(
                    self.spark,
                    {"runtime": self.runtime_cfg},
                    schema,
                    table,
                    load_date,
                )
                additional_metadata.update(mirror_info)

        (
            augmented_df.write.format(read_format)
            .mode("append")
            .option("compression", self.runtime_cfg.get("compression", "snappy"))
            .save(raw_dir)
        )

        metrics = {
            "schema": schema,
            "table": table,
            "run_id": RUN_ID,
            "rows_written": total_rows,
            "status": "success",
            "phase": "raw",
            "load_date": load_date,
            "ts": datetime.now().astimezone().isoformat(),
        }
        HDFSUtil.write_text(self.spark, f"{raw_dir}/_METRICS.json", json.dumps(metrics))

        for slice_result in active_slices:
            Staging.mark_landed(self.spark, slice_result.path)

        raw_to_merge = _load_raw_increment_df(
            self.spark,
            self.runtime_cfg,
            schema,
            table,
            context.last_loaded_date,
            context.effective_watermark,
            context.incremental_column,
            context.incremental_type,
        )
        new_wm, new_ld = IcebergHelper._compute_wm_ld(
            raw_to_merge, context.incremental_column, context.is_epoch
        )

        raw_event_payload = {"rows_written": total_rows}
        additional_metadata.setdefault("staged_slices", len(active_slices))

        return IncrementalCommitResult(
            rows=total_rows,
            raw_path=raw_dir,
            new_watermark=new_wm,
            new_loaded_date=new_ld,
            raw_event_payload=raw_event_payload,
            intermediate_event_payload=intermediate_payload,
            additional_metadata=additional_metadata,
        )

    def publish_dataset(
        self,
        *,
        load_date: str,
        schema: str,
        table: str,
    ) -> Dict[str, Any]:
        hive_cfg = self.runtime_cfg.get("hive", {})
        if not hive_cfg.get("enabled", False):
            return {"status": "disabled"}
        final_dir = self._paths_for(load_date)[1]
        hive_db = hive_cfg["db"]
        HiveHelper.create_unpartitioned_parquet_if_absent_with_schema(
            self.spark,
            hive_db,
            schema,
            table,
            final_dir,
        )
        return {"status": "published", "hive_db": hive_db, "location": final_dir}

    def latest_watermark(
        self,
        *,
        schema: str,
        table: str,
    ) -> Optional[str]:
        incr_col = self.table_cfg.get("incremental_column")
        if not incr_col:
            return None
        try:
            df = self.spark.read.format(self.runtime_cfg.get("write_format", "parquet")).load(self.base_raw)
            if incr_col not in map(str.lower, df.columns):
                return None
            max_val = df.select(F.max(F.col(incr_col))).collect()[0][0]
            return str(max_val) if max_val is not None else None
        except Exception:
            return None

    # ------------------------------------------------------------------ helpers
    def _resolve_partition_settings(self) -> Dict[str, Any]:
        def _normalize_spec(entries: Iterable[Dict[str, Any]]) -> List[Dict[str, str]]:
            normalized: List[Dict[str, str]] = []
            for entry in entries:
                if not isinstance(entry, dict):
                    continue
                field = entry.get("field")
                if not field or not isinstance(field, str):
                    continue
                transform = entry.get("transform", "identity")
                transform_str = str(transform) if transform is not None else "identity"
                normalized.append({"field": field, "transform": transform_str})
            return normalized

        def _normalize_derived(entries: Iterable[Dict[str, Any]]) -> List[Dict[str, str]]:
            normalized: List[Dict[str, str]] = []
            for entry in entries:
                if not isinstance(entry, dict):
                    continue
                name = entry.get("name")
                expr = entry.get("expr")
                if not name or not isinstance(name, str):
                    continue
                if not expr or not isinstance(expr, str):
                    continue
                normalized.append({"name": name, "expr": expr})
            return normalized

        def _merge_expr(value: Any) -> Optional[str]:
            if value is None:
                return None
            if isinstance(value, str):
                return value
            if isinstance(value, dict):
                expr_val = value.get("expr")
                return expr_val if isinstance(expr_val, str) else None
            return None

        inter_cfg = self.runtime_cfg.get("intermediate", {})
        defaults = inter_cfg.get("partition_defaults") or {}
        default_spec = _normalize_spec(defaults.get("spec") or [])
        default_derived = _normalize_derived(defaults.get("derived_columns") or [])
        merge_filter_expr = _merge_expr(defaults.get("merge_filter"))

        table_intermediate = self.table_cfg.get("intermediate") or {}
        if table_intermediate and not isinstance(table_intermediate, dict):
            table_intermediate = {}
        partition_cfg = table_intermediate.get("partition") or {}
        if partition_cfg and not isinstance(partition_cfg, dict):
            partition_cfg = {}

        spec_mode = str(partition_cfg.get("spec_mode", "append")).lower()
        derived_mode = str(partition_cfg.get("derived_mode", spec_mode)).lower()
        table_spec = _normalize_spec(partition_cfg.get("spec") or [])
        table_derived = _normalize_derived(partition_cfg.get("derived_columns") or [])

        if spec_mode == "replace":
            resolved_spec = table_spec
        else:
            resolved_spec = default_spec + table_spec

        if derived_mode == "replace":
            resolved_derived = table_derived
        else:
            resolved_derived = default_derived + table_derived

        table_merge = _merge_expr(table_intermediate.get("merge_filter"))
        if table_merge is not None:
            merge_filter_expr = table_merge

        return {
            "spec": resolved_spec,
            "derived_columns": resolved_derived,
            "merge_filter": merge_filter_expr,
        }

    def _pending_slice_paths(self) -> List[str]:
        staging_cfg = self.cfg.get("runtime", {}).get("staging", {})
        incr_col = self.table_cfg.get("incremental_column")
        if not staging_cfg or not incr_col:
            return []
        base = f"{Staging.root(self.cfg)}/{self.schema}/{self.table}/inc={incr_col}/slices"
        try:
            jsc = self.spark._jsc
            jvm = self.spark.sparkContext._jvm
            conf = jsc.hadoopConfiguration()
            fs = jvm.org.apache.hadoop.fs.FileSystem.get(conf)
            Path = jvm.org.apache.hadoop.fs.Path
            base_path = Path(base)
            if not fs.exists(base_path):
                return []
            statuses = fs.listStatus(base_path)
        except Exception:
            return []
        pending: List[str] = []
        for st in statuses:
            if not st.isDirectory():
                continue
            path_str = st.getPath().toString()
            if Staging.exists(self.spark, f"{path_str}/_SUCCESS") and not Staging.exists(self.spark, f"{path_str}/_LANDED"):
                pending.append(path_str)
        return sorted(pending)

    def _apply_derived_columns(self, df: DataFrame) -> DataFrame:
        if not self._derived_columns:
            return df
        result = df
        for entry in self._derived_columns:
            try:
                result = result.withColumn(entry["name"], F.expr(entry["expr"]))
            except Exception as exc:  # pragma: no cover - defensive logging
                raise RuntimeError(
                    f"Failed to apply derived column '{entry['name']}' with expression '{entry['expr']}': {exc}"
                ) from exc
        return result

    def maintenance_plan(self) -> Dict[str, Any]:
        pending_paths = self._pending_slice_paths()
        return {
            "schema": self.schema,
            "table": self.table,
            "partition_spec": list(self._partition_spec),
            "derived_columns": list(self._derived_columns),
            "merge_filter": self._merge_filter_expr,
            "pending_slices": len(pending_paths),
            "pending_slice_paths": pending_paths,
        }

    def run_maintenance(
        self,
        *,
        load_date: str,
        logger,
        apply_changes: bool,
    ) -> Dict[str, Any]:
        plan = self.maintenance_plan()
        result = dict(plan)
        result["merged_rows"] = 0
        result.setdefault("partition_updated", False)
        if not apply_changes:
            result["status"] = "dry_run"
            return result

        schema = self.schema
        table = self.table
        slice_paths = plan.get("pending_slice_paths", [])
        pk_cols = self.table_cfg.get("primary_keys") or []

        if not slice_paths:
            rewrite_result = self._rewrite_full_table(load_date=load_date, pk_cols=pk_cols)
            result.update(rewrite_result)
            result["pending_slices"] = 0
            result["pending_slice_paths"] = []
            return result

        read_format = self.runtime_cfg.get("write_format", "parquet")
        try:
            df = self.spark.read.format(read_format).load(slice_paths)
        except Exception as exc:  # pragma: no cover - defensive logging
            result["status"] = "error"
            result["error"] = str(exc)
            return result

        df_augmented = self._apply_derived_columns(df)
        rows = df_augmented.count()
        result["merged_rows"] = rows
        if rows == 0:
            for path in slice_paths:
                Staging.mark_landed(self.spark, path)
            result["status"] = "no_data"
            result["pending_slices"] = 0
            result["pending_slice_paths"] = []
            return result

        inter_cfg = self.runtime_cfg.get("intermediate", {"enabled": False})
        if inter_cfg.get("enabled", False) and pk_cols:
            try:
                IcebergHelper.merge_upsert(
                    self.spark,
                    {"runtime": self.runtime_cfg},
                    schema,
                    table,
                    df_augmented,
                    pk_cols,
                    load_date,
                    partition_col=inter_cfg.get("partition_col", "load_date"),
                    incr_col=self.table_cfg.get("incremental_column"),
                    partition_cfg={"spec": self._partition_spec},
                )
            except Exception as exc:  # pragma: no cover - defensive logging
                result["status"] = "error"
                result["error"] = str(exc)
                return result
            try:
                updated = IcebergHelper.set_partition_spec(
                    self.spark,
                    {"runtime": self.runtime_cfg},
                    schema,
                    table,
                    {"spec": self._partition_spec} if self._partition_spec else None,
                )
                result["partition_updated"] = bool(updated)
                if updated:
                    IcebergHelper.rewrite_data_files(
                        self.spark,
                        {"runtime": self.runtime_cfg},
                        schema,
                        table,
                    )
            except Exception as exc:  # pragma: no cover - defensive logging
                result["status"] = "error"
                result["error"] = f"partition_update_failed: {exc}"
                return result

        for path in slice_paths:
            Staging.mark_landed(self.spark, path)
        result["status"] = "applied"
        result["pending_slices"] = 0
        result["pending_slice_paths"] = []
        return result

    def _rewrite_full_table(self, *, load_date: str, pk_cols: List[str]) -> Dict[str, Any]:
        inter_cfg = self.runtime_cfg.get("intermediate", {"enabled": False})
        if not inter_cfg.get("enabled", False):
            return {
                "status": "noop",
                "merged_rows": 0,
                "partition_updated": False,
            }
        catalog = inter_cfg.get("catalog")
        database = inter_cfg.get("db")
        if not catalog or not database:
            return {
                "status": "error",
                "error": "intermediate_catalog_missing",
                "merged_rows": 0,
                "partition_updated": False,
            }
        IcebergHelper.setup_catalog(self.spark, {"runtime": self.runtime_cfg})
        try:
            existing_df = self.spark.table(
                IcebergHelper.qualified_name({"runtime": self.runtime_cfg}, self.schema, self.table, quoted=True)
            )
        except AnalysisException as exc:  # pragma: no cover - defensive logging
            return {
                "status": "error",
                "error": f"iceberg_table_missing: {exc.desc}",
                "merged_rows": 0,
                "partition_updated": False,
            }

        missing_cols = [
            entry["name"]
            for entry in self._derived_columns
            if entry["name"] not in existing_df.columns
        ]
        if missing_cols and not pk_cols:
            return {
                "status": "error",
                "error": f"derived_columns_require_primary_keys:{','.join(missing_cols)}",
                "merged_rows": 0,
                "partition_updated": False,
            }

        merged_rows = 0
        updated = False
        if pk_cols:
            augmented = self._apply_derived_columns(existing_df)
            try:
                merged_rows = augmented.count()
                IcebergHelper.merge_upsert(
                    self.spark,
                    {"runtime": self.runtime_cfg},
                    self.schema,
                    self.table,
                    augmented,
                    pk_cols,
                    load_date,
                    partition_col=inter_cfg.get("partition_col", "load_date"),
                    incr_col=self.table_cfg.get("incremental_column"),
                    partition_cfg={"spec": self._partition_spec},
                )
            except Exception as exc:  # pragma: no cover - defensive logging
                return {
                    "status": "error",
                    "error": str(exc),
                    "merged_rows": merged_rows,
                    "partition_updated": False,
                }

        try:
            updated = IcebergHelper.set_partition_spec(
                self.spark,
                {"runtime": self.runtime_cfg},
                self.schema,
                self.table,
                {"spec": self._partition_spec} if self._partition_spec else None,
            )
            if updated:
                IcebergHelper.rewrite_data_files(
                    self.spark,
                    {"runtime": self.runtime_cfg},
                    self.schema,
                    self.table,
                )
        except Exception as exc:  # pragma: no cover - defensive logging
            return {
                "status": "error",
                "error": f"partition_update_failed: {exc}",
                "merged_rows": merged_rows,
                "partition_updated": False,
            }

        status = "noop"
        if merged_rows:
            status = "rewritten"
        if updated:
            status = "repartitioned"
        return {
            "status": status,
            "merged_rows": merged_rows,
            "partition_updated": bool(updated),
        }

    def _paths_for(self, load_date: str) -> Tuple[str, str, str]:
        return Paths.build(self.runtime_cfg, self.schema, self.table, load_date)

    def execute_query_plan(self, plan: QueryPlan) -> QueryResult:
        selects = plan.selects or (SelectItem(expression="*"),)
        runtime_wrapper = {"runtime": self.runtime_cfg}
        IcebergHelper.setup_catalog(self.spark, runtime_wrapper)
        IcebergHelper.ensure_namespace(self.spark, runtime_wrapper)
        table_name = IcebergHelper.qualified_name(
            runtime_wrapper,
            self.schema,
            self.table,
            quoted=True,
        )
        select_clause = ", ".join(select.render() for select in selects)
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
        sql = f"SELECT {select_clause} FROM {table_name}{where_clause}{group_clause}{having_clause}{order_clause}{limit_clause}"
        df = self.spark.sql(sql)
        rows = [row.asDict(recursive=True) for row in df.collect()]
        return QueryResult.from_records(rows)

    def rebuild_from_raw(self, *, logger, keep_backup: bool = False) -> Dict[str, Any]:
        inter_cfg = self.runtime_cfg.get("intermediate", {"enabled": False})
        if not inter_cfg.get("enabled", False):
            return {"status": "error", "error": "intermediate_disabled"}
        catalog = inter_cfg.get("catalog")
        database = inter_cfg.get("db")
        if not catalog or not database:
            return {"status": "error", "error": "intermediate_catalog_missing"}

        IcebergHelper.setup_catalog(
            self.spark,
            {"runtime": self.runtime_cfg},
        )
        IcebergHelper.ensure_namespace(
            self.spark,
            {"runtime": self.runtime_cfg},
        )

        read_format = self.runtime_cfg.get("write_format", "parquet")
        jsc = self.spark._jsc
        jvm = self.spark.sparkContext._jvm
        conf = jsc.hadoopConfiguration()
        base_path = jvm.org.apache.hadoop.fs.Path(self.base_raw)
        fs = base_path.getFileSystem(conf)
        if not fs.exists(base_path):
            return {"status": "error", "error": "raw_path_missing"}
        status_list = fs.listStatus(base_path)
        chunk_paths: List[str] = []
        for st in status_list:
            if st.isDirectory():
                name = st.getPath().getName()
                if name.startswith("load_date="):
                    chunk_paths.append(st.getPath().toString())
        if not chunk_paths:
            chunk_paths = [self.base_raw]
        chunk_paths.sort()

        try:
            df_raw = self.spark.read.format(read_format).load(chunk_paths)
        except Exception as exc:  # pragma: no cover - defensive
            raise RuntimeError(f"raw_read_failed: {exc}") from exc
        if df_raw.rdd.isEmpty():
            return {"status": "error", "error": "raw_empty"}

        df_augmented = self._apply_derived_columns(df_raw)
        pk_cols = list(self.table_cfg.get("primary_keys", []))
        incr_col = self.table_cfg.get("incremental_column")
        order_cols: List[str] = []
        if incr_col:
            order_cols.append(incr_col)
        if "load_timestamp" in df_augmented.columns:
            order_cols.append("load_timestamp")
        if pk_cols:
            df_clean = IcebergHelper.dedup(df_augmented, pk_cols, order_cols)
        else:
            df_clean = df_augmented.dropDuplicates()
        total_rows = df_clean.count()
        if total_rows == 0:
            return {"status": "error", "error": "raw_empty"}

        table_initialized = False
        tgt_schema = None
        ident = IcebergHelper._identifier(self.schema, self.table)
        full_name = f"{catalog}.{database}.{ident}"
        backup_suffix = datetime.now().strftime("%Y%m%d%H%M%S")
        backup_ident = f"{ident}__backup_{backup_suffix}"
        backup_name = f"{catalog}.{database}.{backup_ident}"
        table_exists = True
        try:
            self.spark.table(full_name)
        except AnalysisException:
            table_exists = False

        try:
            if table_exists:
                self.spark.sql(f"ALTER TABLE {full_name} RENAME TO {backup_name}")
            else:
                self.spark.sql(f"DROP TABLE IF EXISTS {full_name}")

            IcebergHelper.ensure_table(
                self.spark,
                {"runtime": self.runtime_cfg},
                self.schema,
                self.table,
                df_clean,
                inter_cfg.get("partition_col", "load_date"),
                {"spec": self._partition_spec},
            )
            tgt_schema = self.spark.table(full_name).schema
            ordered_cols = [field.name for field in tgt_schema.fields]
            df_clean.select([col(c) for c in ordered_cols]).writeTo(full_name).append()
            table_initialized = True
        except Exception as exc:
            if table_exists:
                try:
                    self.spark.sql(f"ALTER TABLE {backup_name} RENAME TO {full_name}")
                except Exception:  # pragma: no cover
                    logger.error(
                        "maintenance_restore_failed",
                        schema=self.schema,
                        table=self.table,
                        backup_table=backup_name,
                    )
            return {"status": "error", "error": str(exc)}

        if table_exists and not keep_backup:
            try:
                self.spark.sql(f"DROP TABLE IF EXISTS {backup_name}")
            except Exception:  # pragma: no cover
                logger.warn(
                    "maintenance_backup_drop_failed",
                    schema=self.schema,
                    table=self.table,
                    backup_table=backup_name,
                )

        return {
            "status": "rebuild",
            "merged_rows": total_rows,
            "backup_table": backup_name if table_exists and keep_backup else None,
        }
