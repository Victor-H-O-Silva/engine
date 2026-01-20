from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict

from pipeline_runtime.execution.pipeline_executor import execute_pipelines
from pipeline_runtime.orchestration.execution_request import ExecutionRequest
from pipeline_runtime.orchestration.execution_result import (
    ExecutionResult,
    PipelineExecutionResult,
)
from pipeline_runtime.orchestration.pipeline_selector import select_pipelines
from pipeline_runtime.orchestration.failure_policy import should_stop_execution
from pipeline_runtime.orchestration.dependency_resolver import resolve_dependencies
from pipeline_runtime.performance.controls import (
    PerformanceController,
    parse_performance_controls,
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_request(payload: Any) -> ExecutionRequest:
    if isinstance(payload, str):
        obj = json.loads(payload)
    elif isinstance(payload, dict):
        obj = payload
    else:
        raise ValueError("payload must be dict or JSON string")

    return ExecutionRequest(**obj)


def run_execution(payload: Any) -> Dict[str, Any]:
    started = _utc_now()
    req = _load_request(payload)

    spark = None
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
    except Exception:
        pass

    controller = PerformanceController(
        spark=spark,
        run_history_table_fqn="",
    )

    controls = parse_performance_controls(req.params or {})
    
    def _as_bool(v: Any) -> bool:
        if isinstance(v, bool):
            return v
        s = str(v).strip().lower()
        return s in ("1", "true", "yes", "y")
    
    def _parse_iso(s: str) -> datetime:
        t = str(s).strip()
        if t.endswith("Z"):
            t = t[:-1] + "+00:00"
        dt = datetime.fromisoformat(t)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)

    chunks = controller.compute_window_chunks(
        controls=controls,
        start_ts=_parse_iso(req.window_start_utc),
        end_ts_excl=_parse_iso(req.window_end_utc),
    )

    selected = select_pipelines(pipelines=req.pipelines, tags=req.tags)
    ordered = resolve_dependencies(selected, req.depends_on) if req.depends_on else selected

    results = []
    raw_results = []

    total = succeeded = failed = failed_precondition = 0

    for chunk in chunks:
        for p in ordered:
            total += 1

            exec_out = execute_pipelines(
                pipelines=[p],
                env=req.env,
                mode=req.mode,
                window_start_utc=chunk["start_ts"].isoformat(),
                window_end_utc=chunk["end_ts_excl"].isoformat(),
                params_json=json.dumps(req.params or {}),
                trigger_json=json.dumps(req.trigger or {}),
                attempt=req.attempt,
                allow_destructive=_as_bool(req.allow_destructive),
                dry_run=_as_bool(req.dry_run),
                pipeline_params_json=json.dumps(req.pipeline_params or {}),
            )

            raw_results.append(exec_out)
            r = exec_out["results"][0]

            status = r["status"]
            if status == "SUCCESS":
                succeeded += 1
            elif status == "FAILED_PRECONDITION":
                failed_precondition += 1
                failed += 1
            else:
                failed += 1

            results.append(
                PipelineExecutionResult(
                    pipeline_name=r["pipeline_name"],
                    status=status,
                    run_id=r["run_id"],
                    attempt=r["attempt"],
                    intent_hash=r["intent_hash"],
                    started_ts_utc=r["started_ts_utc"],
                    finished_ts_utc=r["finished_ts_utc"],
                    error_message=r.get("error_message"),
                    error_class=r.get("error_class"),
                )
            )

            if should_stop_execution(req.failure_policy, status):
                break

        if should_stop_execution(req.failure_policy, "FAILED" if failed else "SUCCESS") and failed:
            break

    finished = _utc_now()

    return ExecutionResult(
        overall_status="SUCCESS" if failed == 0 else "FAILED",
        started_ts_utc=started,
        finished_ts_utc=finished,
        env=req.env,
        mode=req.mode,
        window_start_utc=req.window_start_utc,
        window_end_utc=req.window_end_utc,
        failure_policy=req.failure_policy,
        max_concurrency=req.max_concurrency,
        total=total,
        succeeded=succeeded,
        failed=failed,
        failed_precondition=failed_precondition,
        results=results,
        raw_executor_result={"pipelines": raw_results},
    ).__dict__