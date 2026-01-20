from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass(frozen=True)
class ExecutionRequest:
    env: str
    mode: str
    window_start_utc: str
    window_end_utc: str

    pipelines: Optional[List[str]] = None
    tags: Optional[List[str]] = None

    params: Optional[Dict[str, Any]] = None
    pipeline_params: Optional[Dict[str, Dict[str, Any]]] = None
    trigger: Optional[Dict[str, Any]] = None

    attempt: Optional[int] = None
    allow_destructive: bool = False
    dry_run: bool = False

    failure_policy: str = "STOP"
    max_concurrency: int = 1

    depends_on: Optional[Dict[str, List[str]]] = None
