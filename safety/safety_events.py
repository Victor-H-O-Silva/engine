from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass(frozen=True)
class SafetyEvent:
    code: str
    decision: str
    message: str
    details: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "code": self.code,
            "decision": self.decision,
            "message": self.message,
            "details": dict(self.details or {}),
        }


def allow(code: str, message: str, details: Optional[Dict[str, Any]] = None) -> SafetyEvent:
    return SafetyEvent(code=code, decision="ALLOW", message=message, details=details or {})


def block(code: str, message: str, details: Optional[Dict[str, Any]] = None) -> SafetyEvent:
    return SafetyEvent(code=code, decision="BLOCK", message=message, details=details or {})
