from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

STATUS_PASS = "PASS"
STATUS_WARN = "WARN"
STATUS_FAIL = "FAIL"
VALID_STATUSES = {STATUS_PASS, STATUS_WARN, STATUS_FAIL}


@dataclass
class CheckResult:
    name: str
    status: str
    summary: str
    details: list[str] = field(default_factory=list)
    metrics: dict[str, Any] = field(default_factory=dict)
    duration_ms: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "status": self.status,
            "summary": self.summary,
            "details": self.details,
            "metrics": self.metrics,
            "duration_ms": self.duration_ms,
        }

