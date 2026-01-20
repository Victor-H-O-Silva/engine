from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass(frozen=True)
class PipelineExecutionResult:
    pipeline_name: str
    status: str
    run_id: str
    attempt: int
    intent_hash: str
    started_ts_utc: str
    finished_ts_utc: str
    error_message: Optional[str]
    error_class: Optional[str]


@dataclass(frozen=True)
class ExecutionResult:
    overall_status: str
    started_ts_utc: str
    finished_ts_utc: str

    env: str
    mode: str
    window_start_utc: str
    window_end_utc: str

    failure_policy: str
    max_concurrency: int

    total: int
    succeeded: int
    failed: int
    failed_precondition: int

    results: List[PipelineExecutionResult]
    raw_executor_result: Dict[str, Any]