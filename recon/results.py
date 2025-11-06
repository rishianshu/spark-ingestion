from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List


@dataclass
class ReconCheckResult:
    schema: str | None
    table: str | None
    status: str
    check_type: str | None = None
    check_name: str | None = None
    detail: Any = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "schema": self.schema,
            "table": self.table,
            "check_type": self.check_type,
            "check_name": self.check_name,
            "status": self.status,
            "detail": self.detail,
        }


@dataclass
class ReconRunSummary:
    total: int
    passed: int
    failed: int
    warned: int
    errors: int

    @classmethod
    def from_results(cls, results: Iterable[ReconCheckResult]) -> "ReconRunSummary":
        total = passed = failed = warned = errors = 0
        for result in results:
            total += 1
            status = (result.status or "").lower()
            if status == "pass":
                passed += 1
            elif status in {"warn", "warning"}:
                warned += 1
            elif status in {"error", "skipped"}:
                errors += 1
            else:
                failed += 1
        return cls(total=total, passed=passed, failed=failed, warned=warned, errors=errors)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "status": "completed",
            "summary": {
                "total": self.total,
                "passed": self.passed,
                "failed": self.failed,
                "warned": self.warned,
                "errors": self.errors,
            },
        }

