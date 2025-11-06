from __future__ import annotations

from typing import Any, Dict, Iterable, Optional

try:
    from sqlalchemy import create_engine, text
    from sqlalchemy.engine import Engine
except ImportError as exc:  # pragma: no cover - dependency guard
    raise RuntimeError("SQLAlchemy support requires the 'sqlalchemy' package") from exc

from .base import ExecutionTool, QueryRequest, WriteRequest


class SQLAlchemyTool(ExecutionTool):
    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def query(self, request: QueryRequest):
        sql = request.options.get("sql") or request.options.get("dbtable")
        if not sql:
            raise ValueError("SQLAlchemyTool requires 'sql' in options")
        with self._engine.connect() as conn:
            result = conn.execute(text(sql))
            rows = [dict(row._mapping) for row in result]
        return rows

    def query_scalar(self, request: QueryRequest):
        rows = self.query(request)
        if not rows:
            return None
        first = rows[0]
        if len(first) == 1:
            return next(iter(first.values()))
        return first

    def write_dataset(self, request: WriteRequest) -> None:  # pragma: no cover - not used in recon
        raise NotImplementedError("SQLAlchemyTool does not support write operations")

    def write_text(self, path: str, content: str) -> None:  # pragma: no cover
        raise NotImplementedError("SQLAlchemyTool does not support write_text")

    def execute_sql(self, sql: str, params: Optional[Dict[str, Any]] = None) -> Iterable[Dict[str, Any]]:
        with self._engine.connect() as conn:
            result = conn.execute(text(sql), params or {})
            return [dict(row._mapping) for row in result]

    @classmethod
    def from_config(cls, cfg: Dict[str, Any]) -> "SQLAlchemyTool":
        runtime = cfg.get("runtime", {})
        sa_cfg = runtime.get("sqlalchemy") or {}
        url = sa_cfg.get("url")
        if not url:
            raise ValueError("runtime.sqlalchemy.url must be provided for SQLAlchemy tool")
        engine = create_engine(url, **{k: v for k, v in sa_cfg.items() if k != "url"})
        return cls(engine)

    def stop(self) -> None:
        if self._engine:
            self._engine.dispose()
