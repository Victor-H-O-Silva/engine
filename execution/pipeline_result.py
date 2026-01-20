from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass(frozen=True)
class PipelineResult:
    pipeline_name: str
    run_id: str
    attempt: int
    intent_hash: str
    status: str
    started_ts_utc: str
    finished_ts_utc: str
    error_message: Optional[str] = None
    error_class: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "pipeline_name": self.pipeline_name,
            "run_id": self.run_id,
            "attempt": int(self.attempt),
            "intent_hash": self.intent_hash,
            "status": self.status,
            "started_ts_utc": self.started_ts_utc,
            "finished_ts_utc": self.finished_ts_utc,
            "error_message": self.error_message,
            "error_class": self.error_class,
        }
