from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Sequence


@dataclass(frozen=True)
class SelectItem:
    expression: str
    alias: Optional[str] = None

    def render(self) -> str:
        return f"{self.expression} AS {self.alias}" if self.alias else self.expression


@dataclass(frozen=True)
class OrderItem:
    expression: str
    descending: bool = False

    def render(self) -> str:
        suffix = " DESC" if self.descending else ""
        return f"{self.expression}{suffix}"


@dataclass(frozen=True)
class QueryPlan:
    selects: Sequence[SelectItem]
    source: Optional[str] = None
    filters: Sequence[str] = field(default_factory=list)
    group_by: Sequence[str] = field(default_factory=list)
    having: Sequence[str] = field(default_factory=list)
    order_by: Sequence[OrderItem] = field(default_factory=list)
    limit: Optional[int] = None

    def with_filter(self, predicate: Optional[str]) -> "QueryPlan":
        if not predicate:
            return self
        return QueryPlan(
            selects=self.selects,
            source=self.source,
            filters=(*self.filters, predicate),
            group_by=self.group_by,
            having=self.having,
            order_by=self.order_by,
            limit=self.limit,
        )


@dataclass
class ResultRow:
    values: Dict[str, Any]

    def get(self, key: str, default: Any = None) -> Any:
        return self.values.get(key, default)


@dataclass
class QueryResult:
    rows: List[ResultRow]

    @classmethod
    def from_records(cls, records: Iterable[Dict[str, Any]]) -> "QueryResult":
        return cls(rows=[ResultRow(values=dict(record)) for record in records])

    @classmethod
    def from_scalar(cls, alias: str, value: Any) -> "QueryResult":
        return cls(rows=[ResultRow(values={alias: value})])

    def scalar(self, alias: Optional[str] = None) -> Any:
        if not self.rows:
            return None
        first = self.rows[0].values
        if alias:
            if alias in first:
                return first[alias]
            lowered = alias.lower()
            for key, value in first.items():
                if key.lower() == lowered:
                    return value
            return None
        if len(first) != 1:
            raise ValueError("Scalar access requires alias when multiple columns are present")
        return next(iter(first.values()))

    def to_dicts(self) -> List[Dict[str, Any]]:
        return [dict(row.values) for row in self.rows]
