from types import SimpleNamespace
import sys
import types

import pytest

if "pyspark" not in sys.modules:  # minimal stubs for pyspark imports used in strategies
    ps = types.ModuleType("pyspark")
    sql = types.ModuleType("pyspark.sql")
    functions = types.ModuleType("pyspark.sql.functions")
    functions.lit = lambda value: value
    sql.functions = functions
    ps.sql = sql
    sys.modules["pyspark"] = ps
    sys.modules["pyspark.sql"] = sql
    sys.modules["pyspark.sql.functions"] = functions

from salam_ingest.metadata.schema import (
    SchemaDriftPolicy,
    SchemaDriftResult,
    SchemaDriftValidator,
    SchemaSnapshot,
    SchemaSnapshotColumn,
    SchemaValidationError,
)
from salam_ingest.strategies import ExecutionContext


class _FakeType:
    def __init__(self, simple: str) -> None:
        self._simple = simple

    def simpleString(self) -> str:
        return self._simple


class _FakeField:
    def __init__(self, name: str, dtype: str, nullable: bool = True) -> None:
        self.name = name
        self.dataType = _FakeType(dtype)
        self.nullable = nullable


def _make_snapshot(columns):
    col_map = {
        name.lower(): SchemaSnapshotColumn(name=name, data_type=dtype, nullable=True)
        for name, dtype in columns.items()
    }
    return SchemaSnapshot(namespace="TEST", entity="TABLE", columns=col_map, version="v1", collected_at="iso", raw={})


def _make_schema(fields):
    return SimpleNamespace(fields=[_FakeField(name, dtype) for name, dtype in fields.items()])


class _StubDataFrame:
    def __init__(self, columns):
        self._columns = [(name, dtype) for name, dtype in columns]
        self._refresh_schema()

    def _refresh_schema(self):
        self.schema = SimpleNamespace(fields=[_FakeField(name, dtype) for name, dtype in self._columns])

    @property
    def columns(self):
        return [name for name, _ in self._columns]

    def withColumn(self, name, _value):  # pragma: no cover - simple stub
        if name in self.columns:
            return self
        new_df = _StubDataFrame(self._columns + [(name, "string")])
        return new_df



def test_validator_accepts_matching_schema():
    snapshot = _make_snapshot({"ID": "NUMBER", "NAME": "VARCHAR2"})
    schema = _make_schema({"id": "decimal(10,0)", "name": "string"})
    validator = SchemaDriftValidator(SchemaDriftPolicy(require_snapshot=True))

    result = validator.validate(snapshot=snapshot, dataframe_schema=schema, policy=None)

    assert isinstance(result, SchemaDriftResult)
    assert not result.new_columns
    assert not result.missing_columns
    assert not result.type_mismatches


def test_validator_raises_on_new_column_when_not_allowed():
    snapshot = _make_snapshot({"ID": "NUMBER"})
    schema = _make_schema({"id": "decimal(10,0)", "extra": "string"})
    policy = SchemaDriftPolicy(require_snapshot=True, allow_new_columns=False)
    validator = SchemaDriftValidator(policy)

    with pytest.raises(SchemaValidationError) as exc:
        validator.validate(snapshot=snapshot, dataframe_schema=schema, policy=policy)

    assert exc.value.result.new_columns == ["extra"]


def test_validator_warns_on_type_mismatch_when_allowed():
    snapshot = _make_snapshot({"ID": "NUMBER"})
    schema = _make_schema({"id": "string"})
    policy = SchemaDriftPolicy(require_snapshot=True, allow_type_mismatch=True)
    validator = SchemaDriftValidator(policy)

    result = validator.validate(snapshot=snapshot, dataframe_schema=schema, policy=policy)

    assert isinstance(result, SchemaDriftResult)
    assert result.type_mismatches


class _DummyLogger:
    def log(self, *_args, **_kwargs):  # pragma: no cover - logging stub
        pass


class _StubMetadataAccess:
    def __init__(self, snapshot, policy):
        self._snapshot = snapshot
        self.schema_policy = policy
        self.schema_validator = SchemaDriftValidator(policy)

    def snapshot_for(self, _schema, _table):
        return self._snapshot


def test_execution_context_extends_missing_columns_when_disallowed():
    snapshot = SchemaSnapshot(
        namespace="TEST",
        entity="TABLE",
        columns={
            "id": SchemaSnapshotColumn(name="ID", data_type="NUMBER"),
            "name": SchemaSnapshotColumn(name="NAME", data_type="VARCHAR2"),
        },
        version="v1",
        collected_at="iso",
    )
    policy = SchemaDriftPolicy(require_snapshot=True, allow_missing_columns=False)
    metadata_access = _StubMetadataAccess(snapshot, policy)
    context = ExecutionContext(metadata_access=metadata_access)
    df = _StubDataFrame([("ID", "decimal(10,0)")])

    updated_df, result = context.validate_schema(schema="TEST", table="TABLE", dataframe=df, logger=_DummyLogger())

    assert "NAME" in updated_df.columns
    assert result is not None
