from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Sequence


@dataclass(frozen=True)
class SchemaDriftPolicy:
    """Policy controls for metadata-aware schema validation."""

    require_snapshot: bool = False
    allow_new_columns: bool = True
    allow_missing_columns: bool = True
    allow_type_mismatch: bool = True
    warn_only: bool = False
    ignored_columns: Sequence[str] = ("load_timestamp", "run_id")

    @staticmethod
    def from_config(config: Optional[Dict[str, Any]]) -> "SchemaDriftPolicy":
        cfg = dict(config or {})
        ignored = cfg.get("ignored_columns") or cfg.get("ignoredColumns") or []
        if isinstance(ignored, str):
            ignored = [ignored]
        ignored = tuple(str(col).strip() for col in ignored if str(col).strip())
        return SchemaDriftPolicy(
            require_snapshot=bool(cfg.get("require_snapshot", cfg.get("requireSnapshot", False))),
            allow_new_columns=bool(cfg.get("allow_new_columns", cfg.get("allowNewColumns", True))),
            allow_missing_columns=bool(cfg.get("allow_missing_columns", cfg.get("allowMissingColumns", True))),
            allow_type_mismatch=bool(cfg.get("allow_type_mismatch", cfg.get("allowTypeMismatch", True))),
            warn_only=bool(cfg.get("warn_only", cfg.get("warnOnly", False))),
            ignored_columns=ignored if ignored else ("load_timestamp", "run_id"),
        )

    def merge(self, override: Optional["SchemaDriftPolicy"]) -> "SchemaDriftPolicy":
        if override is None:
            return self
        return SchemaDriftPolicy(
            require_snapshot=override.require_snapshot if override.require_snapshot != self.require_snapshot else self.require_snapshot,
            allow_new_columns=override.allow_new_columns if override.allow_new_columns != self.allow_new_columns else self.allow_new_columns,
            allow_missing_columns=override.allow_missing_columns if override.allow_missing_columns != self.allow_missing_columns else self.allow_missing_columns,
            allow_type_mismatch=override.allow_type_mismatch if override.allow_type_mismatch != self.allow_type_mismatch else self.allow_type_mismatch,
            warn_only=override.warn_only if override.warn_only != self.warn_only else self.warn_only,
            ignored_columns=override.ignored_columns if override.ignored_columns != self.ignored_columns else self.ignored_columns,
        )

    def clone_with(
        self,
        *,
        require_snapshot: Optional[bool] = None,
        allow_new_columns: Optional[bool] = None,
        allow_missing_columns: Optional[bool] = None,
        allow_type_mismatch: Optional[bool] = None,
        warn_only: Optional[bool] = None,
        ignored_columns: Optional[Sequence[str]] = None,
    ) -> "SchemaDriftPolicy":
        return SchemaDriftPolicy(
            require_snapshot=require_snapshot if require_snapshot is not None else self.require_snapshot,
            allow_new_columns=allow_new_columns if allow_new_columns is not None else self.allow_new_columns,
            allow_missing_columns=allow_missing_columns if allow_missing_columns is not None else self.allow_missing_columns,
            allow_type_mismatch=allow_type_mismatch if allow_type_mismatch is not None else self.allow_type_mismatch,
            warn_only=warn_only if warn_only is not None else self.warn_only,
            ignored_columns=tuple(ignored_columns) if ignored_columns is not None else self.ignored_columns,
        )


@dataclass
class SchemaSnapshotColumn:
    name: str
    data_type: Optional[str] = None
    nullable: Optional[bool] = None
    precision: Optional[int] = None
    scale: Optional[int] = None


@dataclass
class SchemaSnapshot:
    namespace: str
    entity: str
    columns: Dict[str, SchemaSnapshotColumn]
    version: Optional[str] = None
    collected_at: Optional[str] = None
    raw: Any = None


@dataclass
class SchemaDriftResult:
    snapshot: Optional[SchemaSnapshot]
    new_columns: List[str] = field(default_factory=list)
    missing_columns: List[str] = field(default_factory=list)
    type_mismatches: List[Dict[str, str]] = field(default_factory=list)

    def has_errors(self, policy: SchemaDriftPolicy) -> bool:
        if policy.warn_only:
            return False
        if not policy.allow_new_columns and self.new_columns:
            return True
        if not policy.allow_missing_columns and self.missing_columns:
            return True
        if not policy.allow_type_mismatch and self.type_mismatches:
            return True
        return False

    def warnings(self, policy: SchemaDriftPolicy) -> Dict[str, Sequence[str]]:
        warnings: Dict[str, Sequence[str]] = {}
        if policy.allow_new_columns and self.new_columns:
            warnings["new_columns"] = tuple(self.new_columns)
        if policy.allow_missing_columns and self.missing_columns:
            warnings["missing_columns"] = tuple(self.missing_columns)
        if policy.allow_type_mismatch and self.type_mismatches:
            warnings["type_mismatches"] = tuple(self._render_type_mismatches())
        return warnings

    def _render_type_mismatches(self) -> Iterable[str]:
        for mismatch in self.type_mismatches:
            column = mismatch.get("column")
            expected = mismatch.get("expected")
            observed = mismatch.get("observed")
            yield f"{column}:{expected}->{observed}"


class SchemaValidationError(RuntimeError):
    """Raised when schema drift violates the configured policy."""

    def __init__(self, message: str, result: SchemaDriftResult) -> None:
        super().__init__(message)
        self.result = result


class SchemaDriftValidator:
    """Compare Spark dataframe schema with catalog metadata snapshots."""

    def __init__(self, default_policy: Optional[SchemaDriftPolicy] = None) -> None:
        self.default_policy = default_policy or SchemaDriftPolicy()

    def validate(
        self,
        *,
        snapshot: Optional[SchemaSnapshot],
        dataframe_schema: Any,
        policy: Optional[SchemaDriftPolicy] = None,
    ) -> Optional[SchemaDriftResult]:
        policy = policy or self.default_policy
        if snapshot is None:
            if policy.require_snapshot:
                raise SchemaValidationError("Metadata snapshot required but not found", SchemaDriftResult(snapshot=None))
            return None
        if dataframe_schema is None or not hasattr(dataframe_schema, "fields"):
            return SchemaDriftResult(snapshot=snapshot)

        ignore = {name.lower() for name in policy.ignored_columns}
        snapshot_cols = snapshot.columns
        snapshot_keys = {name for name in snapshot_cols.keys() if name not in ignore}
        df_fields = self._iter_fields(dataframe_schema)
        df_keys = {name for name, _ in df_fields if name not in ignore}

        new_columns = sorted(df_keys - snapshot_keys)
        missing_columns = sorted(snapshot_keys - df_keys)

        type_mismatches: List[Dict[str, str]] = []
        common = snapshot_keys & df_keys
        for key in sorted(common):
            snapshot_col = snapshot_cols.get(key)
            field = next((field for name, field in df_fields if name == key), None)
            if field is None or snapshot_col is None:
                continue
            expected = self._normalize_type(snapshot_col.data_type)
            observed = self._spark_type(field)
            if not self._types_equivalent(expected, observed):
                type_mismatches.append(
                    {
                        "column": snapshot_col.name,
                        "expected": expected or "unknown",
                        "observed": observed or "unknown",
                    }
                )

        result = SchemaDriftResult(
            snapshot=snapshot,
            new_columns=new_columns,
            missing_columns=missing_columns,
            type_mismatches=type_mismatches,
        )
        if result.has_errors(policy):
            raise SchemaValidationError("Schema drift violates policy", result)
        return result

    def _iter_fields(self, schema: Any) -> List[tuple[str, Any]]:
        fields_attr = getattr(schema, "fields", [])
        results: List[tuple[str, Any]] = []
        for field in fields_attr:
            name = getattr(field, "name", None)
            if not name:
                continue
            results.append((str(name).lower(), field))
        return results

    def _spark_type(self, field: Any) -> Optional[str]:
        if field is None:
            return None
        dtype = getattr(field, "dataType", None)
        if hasattr(dtype, "simpleString"):
            try:
                return str(dtype.simpleString()).lower()
            except Exception:  # pragma: no cover - defensive
                return str(dtype).lower()
        if dtype is not None:
            return str(dtype).lower()
        return None

    def _normalize_type(self, data_type: Optional[str]) -> Optional[str]:
        if data_type is None:
            return None
        return str(data_type).strip().lower()

    def _types_equivalent(self, expected: Optional[str], observed: Optional[str]) -> bool:
        if not expected or not observed:
            return True
        if expected == observed:
            return True
        if expected in observed or observed in expected:
            return True
        numeric_markers = {"number", "numeric", "decimal", "float", "double"}
        if any(marker in expected for marker in numeric_markers) and any(marker in observed for marker in numeric_markers):
            return True
        string_markers = {"char", "varchar", "string", "text"}
        if any(marker in expected for marker in string_markers) and any(marker in observed for marker in string_markers):
            return True
        if expected.startswith("timestamp") and observed.startswith("timestamp"):
            return True
        if expected.startswith("date") and observed.startswith("date"):
            return True
        return False
