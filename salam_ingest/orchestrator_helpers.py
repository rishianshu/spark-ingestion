from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional, Tuple

from pyspark.sql import SparkSession

from .io.filesystem import HDFSOutbox
from .events import emit_log
from .notification import Heartbeat
from .staging import TimeAwareBufferedSink
from .state import BufferedState, SingleStoreState


def validate_config(cfg: Dict[str, Any]) -> None:
    def _validate_source_filter(table_cfg: Dict[str, Any]) -> None:
        filt = table_cfg.get("source_filter")
        if filt is None:
            return
        if isinstance(filt, str):
            return
        if isinstance(filt, (list, tuple)):
            if not all(isinstance(item, str) for item in filt):
                raise ValueError("tables[].source_filter entries must be strings")
            return
        raise ValueError("tables[].source_filter must be a string or list of strings")

    def _validate_partition_spec_entries(entries: Any, context: str) -> None:
        if entries is None:
            return
        if not isinstance(entries, list):
            raise ValueError(f"{context}.spec must be a list when provided")
        for entry in entries:
            if not isinstance(entry, dict):
                raise ValueError(f"{context}.spec entries must be objects")
            field = entry.get("field")
            if not field or not isinstance(field, str):
                raise ValueError(f"{context}.spec entries require a string 'field'")
            transform = entry.get("transform", "identity")
            if not isinstance(transform, str):
                raise ValueError(f"{context}.spec.transform must be a string")
            norm = transform.lower()
            base = norm.split(":", 1)[0]
            allowed = {
                "identity",
                "year",
                "years",
                "month",
                "months",
                "day",
                "days",
                "bucket",
                "truncate",
            }
            if base not in allowed:
                raise ValueError(f"Unsupported partition transform '{transform}' in {context}")
            if base in {"bucket", "truncate"}:
                if ":" not in norm:
                    raise ValueError(f"{context}.spec.transform '{transform}' must include a parameter (e.g. bucket:16)")
                param = norm.split(":", 1)[1]
                if not param or not param.isdigit():
                    raise ValueError(f"{context}.spec.transform '{transform}' parameter must be an integer")

    def _validate_derived_columns(entries: Any, context: str) -> None:
        if entries is None:
            return
        if not isinstance(entries, list):
            raise ValueError(f"{context}.derived_columns must be a list when provided")
        for entry in entries:
            if not isinstance(entry, dict):
                raise ValueError(f"{context}.derived_columns entries must be objects")
            name = entry.get("name")
            expr = entry.get("expr")
            if not name or not isinstance(name, str):
                raise ValueError(f"{context}.derived_columns entries require a string 'name'")
            if not expr or not isinstance(expr, str):
                raise ValueError(f"{context}.derived_columns entries require a string 'expr'")

    def _validate_merge_filter(entry: Any, context: str) -> None:
        if entry is None:
            return
        if isinstance(entry, str):
            return
        if isinstance(entry, dict):
            expr = entry.get("expr")
            if expr is None or not isinstance(expr, str):
                raise ValueError(f"{context}.merge_filter.expr must be a string")
            return
        raise ValueError(f"{context}.merge_filter must be a string or object with an 'expr' field")

    for key in ["jdbc", "runtime", "tables"]:
        if key not in cfg:
            raise ValueError(f"Missing config key: {key}")
    runtime = cfg["runtime"]
    for key in ["raw_root", "final_root", "timezone"]:
        if key not in runtime:
            raise ValueError(f"Missing runtime.{key}")
    if runtime.get("state_backend", "singlestore") == "singlestore":
        ss = runtime.get("state", {}).get("singlestore")
        if not ss:
            raise ValueError("runtime.state.singlestore required")
        required = [
            "ddlEndpoint",
            "user",
            "password",
            "database",
            "eventsTable",
            "watermarksTable",
            "sourceId",
        ]
        for key in required:
            if key not in ss:
                raise ValueError(f"Missing singlestore.{key}")
    metadata_cfg = cfg.get("metadata")
    if metadata_cfg is not None and not isinstance(metadata_cfg, dict):
        raise ValueError("metadata must be an object when provided")
    if isinstance(metadata_cfg, dict):
        policy_cfg = metadata_cfg.get("schema_policy") or metadata_cfg.get("schemaPolicy")
        if policy_cfg is not None and not isinstance(policy_cfg, dict):
            raise ValueError("metadata.schema_policy must be an object when provided")
        if policy_cfg:
            ignored_cols = policy_cfg.get("ignored_columns") or policy_cfg.get("ignoredColumns")
            if ignored_cols is not None and not isinstance(ignored_cols, (list, tuple, set, str)):
                raise ValueError("metadata.schema_policy.ignored_columns must be a list or string")

    orchestration_cfg = runtime.get("orchestration")
    if orchestration_cfg is not None and not isinstance(orchestration_cfg, dict):
        raise ValueError("runtime.orchestration must be an object when provided")
    if isinstance(orchestration_cfg, dict):
        mode = str(orchestration_cfg.get("mode", "plain_cron")).lower()
        allowed_modes = {"plain_cron", "external_scheduler", "temporal", "informatica", "airflow"}
        if mode not in allowed_modes:
            raise ValueError(f"Unsupported orchestration mode: {mode}")
        if mode == "plain_cron":
            cron_cfg = orchestration_cfg.get("cron", {})
            if cron_cfg and not isinstance(cron_cfg, dict):
                raise ValueError("runtime.orchestration.cron must be an object")
        if mode in {"external_scheduler", "informatica", "airflow"}:
            external_cfg = orchestration_cfg.get("external", {})
            if external_cfg and not isinstance(external_cfg, dict):
                raise ValueError("runtime.orchestration.external must be an object")
        if mode == "temporal":
            temporal_cfg = orchestration_cfg.get("temporal", {})
            if not isinstance(temporal_cfg, dict):
                raise ValueError("runtime.orchestration.temporal must be an object")
            required = ["namespace", "task_queue", "workflow"]
            missing = [key for key in required if not temporal_cfg.get(key)]
            if missing:
                raise ValueError(f"runtime.orchestration.temporal missing keys: {', '.join(missing)}")

    inter_cfg = runtime.get("intermediate", {})
    if inter_cfg.get("enabled"):
        if "catalog" not in inter_cfg:
            raise ValueError("runtime.intermediate.catalog required when intermediate.enabled is true")
        catalog_type = str(inter_cfg.get("catalog_type", "hadoop")).lower()
        if catalog_type not in {"hadoop", "hive"}:
            raise ValueError("runtime.intermediate.catalog_type must be 'hadoop' or 'hive'")
        if catalog_type == "hadoop" and not inter_cfg.get("warehouse"):
            raise ValueError("runtime.intermediate.warehouse required for hadoop catalogs")
        if catalog_type == "hive" and not inter_cfg.get("metastore_uri"):
            raise ValueError("runtime.intermediate.metastore_uri required for hive catalogs")

    partition_defaults = runtime.get("intermediate", {}).get("partition_defaults")
    if partition_defaults is not None:
        if not isinstance(partition_defaults, dict):
            raise ValueError("runtime.intermediate.partition_defaults must be an object when provided")
        _validate_partition_spec_entries(partition_defaults.get("spec"), "runtime.intermediate.partition_defaults")
        _validate_derived_columns(partition_defaults.get("derived_columns"), "runtime.intermediate.partition_defaults")
        _validate_merge_filter(partition_defaults.get("merge_filter"), "runtime.intermediate.partition_defaults")

    for tbl in cfg.get("tables", []):
        if not isinstance(tbl, dict):
            continue
        _validate_source_filter(tbl)
        table_intermediate = (tbl.get("intermediate") or {})
        if table_intermediate and not isinstance(table_intermediate, dict):
            raise ValueError("tables[].intermediate must be an object when provided")
        partition_cfg = table_intermediate.get("partition")
        if partition_cfg is not None:
            if not isinstance(partition_cfg, dict):
                raise ValueError("tables[].intermediate.partition must be an object when provided")
            spec_mode = partition_cfg.get("spec_mode")
            if spec_mode is not None and str(spec_mode).lower() not in {"append", "replace"}:
                raise ValueError("tables[].intermediate.partition.spec_mode must be 'append' or 'replace'")
            _validate_partition_spec_entries(partition_cfg.get("spec"), "tables[].intermediate.partition")
            _validate_derived_columns(partition_cfg.get("derived_columns"), "tables[].intermediate.partition")
        _validate_merge_filter(table_intermediate.get("merge_filter"), "tables[].intermediate")


def suggest_singlestore_ddl(logger, cfg: Dict[str, Any]) -> None:
    ss = cfg["runtime"]["state"]["singlestore"]
    events = ss["eventsTable"]
    wm = ss["watermarksTable"]
    db = ss["database"]
    emit_log(
        emitter=None,
        level="INFO",
        msg="singlestore_recommended_ddl",
        logger=logger,
        events=f"ALTER TABLE {db}.{events} ADD COLUMN IF NOT EXISTS ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP;",
        wm=(
            f"ALTER TABLE {db}.{wm} ADD COLUMN IF NOT EXISTS updated_at "
            "TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP;"
        ),
        metadata=f"ALTER TABLE {db}.{events} ADD COLUMN IF NOT EXISTS metadata_json TEXT;",
    )


def filter_tables(tables: Iterable[Dict[str, Any]], only_tables: Optional[str]) -> List[Dict[str, Any]]:
    if not only_tables:
        return list(tables)
    allow = {s.strip().lower() for s in only_tables.split(",") if s.strip()}

    def allow_tbl(table_cfg: Dict[str, Any]) -> bool:
        key = f"{table_cfg['schema']}.{table_cfg['table']}".lower()
        return key in allow

    return [tbl for tbl in tables if allow_tbl(tbl)]


def build_state_components(
    spark: SparkSession,
    cfg: Dict[str, Any],
    logger,
) -> Tuple[BufferedState, Optional[TimeAwareBufferedSink], Optional[HDFSOutbox]]:
    runtime = cfg["runtime"]
    backend = runtime.get("state_backend", "singlestore")
    if backend != "singlestore":
        base_dir = runtime["state"]["hdfs"]["dir"]
        spark.range(1).write.mode("overwrite").parquet(str(base_dir))
        raise NotImplementedError("HDFSState not included in this file.")

    base_state = SingleStoreState(spark, runtime["state"]["singlestore"])
    outbox = HDFSOutbox(spark, runtime["hdfs_outbox_root"]) if runtime.get("hdfs_outbox_root") else None
    if outbox:
        outbox.restore_all(base_state, logger)
    sink = TimeAwareBufferedSink(
        base_state,
        logger,
        outbox,
        flush_every=runtime.get("state_flush_every", 50),
        flush_age_seconds=runtime.get("state_flush_age_seconds", 120),
    )
    state = BufferedState(
        base_state=base_state,
        source_id=runtime["state"]["singlestore"]["sourceId"],
        flush_every=int(runtime.get("state_flush_every", 50)),
        outbox=outbox,
        time_sink=sink,
        logger=logger,
    )
    return state, sink, outbox


def build_heartbeat(logger, cfg: Dict[str, Any], sink: Optional[TimeAwareBufferedSink], args) -> Heartbeat:
    return Heartbeat(
        logger,
        interval_sec=int(cfg["runtime"].get("heartbeat_seconds", getattr(args, "heartbeat_seconds", 120))),
        time_sink=sink,
    )


def summarize_run(results, errors) -> None:
    print("\n=== SUMMARY ===")
    for res in results:
        print(res)
    if errors:
        print("\n=== ERRORS ===")
        for table_name, err in errors:
            print(table_name, err)
