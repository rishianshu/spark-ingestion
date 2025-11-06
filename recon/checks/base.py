from __future__ import annotations

import abc
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Type

from ..context import ReconContext
from ..results import ReconCheckResult


@dataclass
class CheckThreshold:
    absolute: float = 0.0
    percent: Optional[float] = None

    @classmethod
    def from_config(cls, cfg: Dict[str, Any]) -> "CheckThreshold":
        if not isinstance(cfg, dict):
            return cls()
        abs_val = cfg.get("absolute")
        pct_val = cfg.get("percent")
        absolute = float(abs_val) if abs_val is not None else 0.0
        percent = float(pct_val) if pct_val is not None else None
        return cls(absolute=absolute, percent=percent)


class CheckRegistry:
    """Registry of available reconciliation checks."""

    def __init__(self) -> None:
        self._by_type: Dict[str, Type["ReconCheck"]] = {}

    def register(self, check_cls: Type["ReconCheck"]) -> None:
        check_type = check_cls.type_name()
        self._by_type[check_type] = check_cls

    def get(self, check_type: str) -> Optional[Type["ReconCheck"]]:
        return self._by_type.get(check_type.lower())

    def all(self) -> Dict[str, Type["ReconCheck"]]:
        return dict(self._by_type)


registry = CheckRegistry()


class ReconCheck(abc.ABC):
    """Base class for reconciliation checks."""

    def __init__(self, context: ReconContext, cfg: Dict[str, Any]) -> None:
        self.context = context
        self.cfg = cfg or {}
        self.threshold = CheckThreshold.from_config(self.cfg.get("threshold", {}))
        self.name = str(self.cfg.get("name") or self.cfg.get("type") or self.type_name()).strip()
        if not self.name:
            self.name = self.type_name()

    @classmethod
    def type_name(cls) -> str:
        return getattr(cls, "_TYPE", cls.__name__.lower())

    def run(self) -> ReconCheckResult:
        try:
            outcome = self._execute()
        except Exception as exc:  # pragma: no cover - defensive
            return ReconCheckResult(
                schema=self.context.table_cfg.get("schema"),
                table=self.context.table_cfg.get("table"),
                check_name=self.name,
                check_type=self.type_name(),
                status="error",
                detail=str(exc),
            )
        outcome.check_type = self.type_name()
        outcome.check_name = self.name
        return outcome

    @abc.abstractmethod
    def _execute(self) -> ReconCheckResult:
        ...

