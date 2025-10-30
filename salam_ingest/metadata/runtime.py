from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, Mapping, Optional

from salam_ingest.common import PrintLogger
from salam_ingest.events import emit_log
from salam_ingest.endpoints.base import MetadataCapableEndpoint
from salam_ingest.metadata.cache import MetadataCacheConfig, MetadataCacheManager
from salam_ingest.metadata.core import JsonFileMetadataRepository, MetadataTarget
from salam_ingest.metadata.consumers import PrecisionGuardrailEvaluator
from salam_ingest.metadata.services import MetadataCollectionService, MetadataJob, MetadataServiceConfig
from salam_ingest.metadata.schema import (
    SchemaDriftPolicy,
    SchemaDriftValidator,
    SchemaSnapshot,
    SchemaSnapshotColumn,
)
from salam_ingest.metadata.utils import safe_upper, to_serializable
from salam_ingest.tools.base import ExecutionTool
from pyspark.sql import SparkSession


def _build_metadata_configs(
    cfg: Dict[str, Any],
    logger: PrintLogger,
    spark: Optional[SparkSession] = None,
) -> tuple[MetadataServiceConfig, MetadataCacheManager, str]:
    meta_cfg = cfg.get("metadata", cfg.get("catalog", {})) or {}
    runtime_cfg = cfg.get("runtime", {})
    jdbc_cfg = cfg.get("jdbc", {})
    dialect = (jdbc_cfg.get("dialect") or "default").lower()

    cache_root = meta_cfg.get("cache_path") or meta_cfg.get("root") or "cache/catalog"
    ttl_hours = int(meta_cfg.get("ttl_hours", meta_cfg.get("ttlHours", 24)))
    enabled = bool(meta_cfg.get("enabled", True))
    source_id = (
        meta_cfg.get("source_id")
        or meta_cfg.get("source")
        or runtime_cfg.get("job_name")
        or dialect
    )
    cache_cfg = MetadataCacheConfig(
        cache_path=cache_root,
        ttl_hours=ttl_hours,
        enabled=enabled,
        source_id=str(source_id).lower().replace(" ", "_"),
    )
    cache_manager = MetadataCacheManager(cache_cfg, logger, spark)

    endpoint_defaults = meta_cfg.get("endpoint") if isinstance(meta_cfg.get("endpoint"), dict) else {}
    service_cfg = MetadataServiceConfig(endpoint_defaults=endpoint_defaults)
    return service_cfg, cache_manager, str(source_id)


def _extract_value(source: Any, *keys: str) -> Any:
    if isinstance(source, Mapping):
        for key in keys:
            if key in source:
                return source[key]
    for key in keys:
        if hasattr(source, key):
            return getattr(source, key)
    return None


def _snapshot_columns_from_payload(payload: Mapping[str, Any]) -> Dict[str, SchemaSnapshotColumn]:
    columns: Dict[str, SchemaSnapshotColumn] = {}
    raw_columns: Iterable[Any] = payload.get("schema_fields") or payload.get("columns") or []
    for entry in raw_columns:
        name = _extract_value(entry, "name", "column_name")
        if not name:
            continue
        col = SchemaSnapshotColumn(
            name=str(name),
            data_type=_extract_value(entry, "data_type", "type", "dataType"),
            nullable=_extract_value(entry, "nullable"),
            precision=_extract_value(entry, "precision", "data_precision"),
            scale=_extract_value(entry, "scale", "data_scale"),
        )
        columns[col.name.lower()] = col
    return columns


def _build_schema_snapshot(record) -> Optional[SchemaSnapshot]:
    if record is None:
        return None
    payload = record.payload
    if isinstance(payload, Mapping):
        payload_dict: Mapping[str, Any] = payload
    else:
        payload_serialized = to_serializable(payload)
        payload_dict = payload_serialized if isinstance(payload_serialized, Mapping) else {}
    columns = _snapshot_columns_from_payload(payload_dict)
    namespace = safe_upper(record.target.namespace)
    entity = safe_upper(record.target.entity)
    collected_at = payload_dict.get("collected_at") or payload_dict.get("produced_at")
    version = record.version or payload_dict.get("version") or payload_dict.get("version_hint")
    return SchemaSnapshot(
        namespace=namespace,
        entity=entity,
        columns=columns,
        version=version,
        collected_at=collected_at,
        raw=payload,
    )


def collect_metadata(
    cfg: Dict[str, Any],
    tables: List[Dict[str, Any]],
    tool: Optional[ExecutionTool],
    logger: PrintLogger,
) -> None:
    """Collect metadata snapshots for the provided tables using their endpoints."""

    # Imported lazily to avoid circular dependency during module import
    from salam_ingest.endpoints.factory import EndpointFactory

    spark = getattr(tool, "spark", None) if tool is not None else SparkSession.getActiveSession()
    service_cfg, cache_manager, default_namespace = _build_metadata_configs(cfg, logger, spark)
    metadata_service = MetadataCollectionService(service_cfg, cache_manager, logger)

    if not cache_manager.cfg.enabled:
        emit_log(None, level="INFO", msg="metadata_collection_disabled", logger=logger)
        return
    if tool is None:
        emit_log(None, level="WARN", msg="metadata_collection_skipped", reason="no_execution_tool", logger=logger)
        return

    jobs: List[MetadataJob] = []
    for tbl in tables:
        try:
            endpoint = EndpointFactory.build_source(cfg, tbl, tool)
        except Exception as exc:  # pragma: no cover - defensive logging
            emit_log(
                None,
                level="WARN",
                msg="metadata_endpoint_build_failed",
                schema=tbl.get("schema"),
                dataset=tbl.get("table"),
                error=str(exc),
                logger=logger,
            )
            continue
        if not isinstance(endpoint, MetadataCapableEndpoint):
            emit_log(
                None,
                level="INFO",
                msg="metadata_capability_missing",
                schema=tbl.get("schema"),
                dataset=tbl.get("table"),
                logger=logger,
            )
            continue
        namespace = safe_upper(str(tbl.get("schema") or tbl.get("namespace") or default_namespace))
        entity = safe_upper(str(tbl.get("table") or tbl.get("dataset") or tbl.get("name") or tbl.get("entity") or "unknown"))
        target = MetadataTarget(namespace=namespace, entity=entity)
        jobs.append(MetadataJob(target=target, artifact=tbl, endpoint=endpoint))

    if not jobs:
        return

    try:
        metadata_service.run(jobs)
    except Exception as exc:  # pragma: no cover - defensive logging
        emit_log(None, level="WARN", msg="metadata_collection_failed", error=str(exc), logger=logger)


@dataclass
class MetadataAccess:
    cache_manager: MetadataCacheManager
    repository: JsonFileMetadataRepository
    precision_guardrail: Optional[PrecisionGuardrailEvaluator] = None
    guardrail_defaults: Dict[str, Any] = field(default_factory=dict)
    schema_policy: SchemaDriftPolicy = field(default_factory=SchemaDriftPolicy)
    schema_validator: SchemaDriftValidator = field(default_factory=SchemaDriftValidator)

    def snapshot_for(self, schema: str, table: str) -> Optional[SchemaSnapshot]:
        target = MetadataTarget(namespace=safe_upper(schema), entity=safe_upper(table))
        record = self.repository.latest(target)
        return _build_schema_snapshot(record)


def build_metadata_access(cfg: Dict[str, Any], logger: PrintLogger) -> Optional[MetadataAccess]:
    """Prepare repository and evaluators for metadata consumers."""

    meta_conf = cfg.get("metadata") or {}
    spark = getattr(logger, "spark", None) or SparkSession.getActiveSession()
    _, cache_manager, _ = _build_metadata_configs(cfg, logger, spark)
    if not cache_manager.cfg.enabled:
        return None
    repository = JsonFileMetadataRepository(cache_manager)
    guardrail = PrecisionGuardrailEvaluator(repository)
    guardrail_defaults = meta_conf.get("guardrails", {})
    policy_cfg = meta_conf.get("schema_policy") or meta_conf.get("schemaPolicy")
    schema_policy = SchemaDriftPolicy.from_config(policy_cfg)
    schema_validator = SchemaDriftValidator(schema_policy)
    return MetadataAccess(
        cache_manager=cache_manager,
        repository=repository,
        precision_guardrail=guardrail,
        guardrail_defaults=guardrail_defaults,
        schema_policy=schema_policy,
        schema_validator=schema_validator,
    )
