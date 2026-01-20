from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple, Union

from pyspark.sql import SparkSession

from pipeline_runtime.contracts.run_context import ContractConfig, RunContext, build_run_context
from pipeline_runtime.execution.pipeline_result import PipelineResult
from pipeline_runtime.execution.pipeline_registry import default_registry
from pipeline_runtime.execution.pipeline_runner import PipelineRunner
from pipeline_runtime.state.state_paths import state_tables
from pipeline_runtime.state.state_store import StateStore
from pipeline_runtime.performance.controls import (
    PerformanceController,
    parse_performance_controls,
)
from pipeline_runtime.performance.errors import ConcurrencyLimitExceededError


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _safe_load_json(s: Optional[str]) -> Any:
    if s is None:
        return None
    t = str(s).strip()
    if t == "":
        return None
    return json.loads(t)


def _compact_json(obj: Any) -> str:
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False, sort_keys=True)


def _normalize_pipeline_list(pipelines: Union[str, List[str], Tuple[str, ...]]) -> List[str]:
    if isinstance(pipelines, str):
        p = pipelines.strip()
        if p == "":
            return []
        return [p]
    return [str(x).strip() for x in pipelines if str(x).strip() != ""]


def _merge_params(global_params: Dict[str, Any], per_pipeline_params: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    out = dict(global_params or {})
    if per_pipeline_params:
        for k, v in per_pipeline_params.items():
            out[k] = v
    return out


def execute_pipelines(
    pipelines: Union[str, List[str], Tuple[str, ...]],
    env: str,
    mode: str,
    window_start_utc: str,
    window_end_utc: str,
    params_json: Optional[str] = None,
    trigger_json: Optional[str] = None,
    framework_version: str = "0.1.0-workspace",
    log_timezone: str = "America/Sao_Paulo",
    dry_run: Union[bool, str] = False,
    allow_destructive: Union[bool, str] = False,
    attempt: Union[int, str, None] = None,
    pipeline_params_json: Optional[str] = None,
    cfg: Optional[ContractConfig] = None,
    *,
    state_enabled: bool = True,
    state_schema: str = "pipeline_runtime",
    state_catalog: str = "brewdat_uc_people_dev",
    state_allow_schema_create: bool = False,
    state_table_properties_json: Optional[str] = None,
    lock_name: str = "pipeline_run",
    lock_ttl_seconds: int = 6 * 60 * 60,
    watermark_name: str = "window_end_utc",
) -> Dict[str, Any]:
    cfg = cfg or ContractConfig()
    pipeline_list = _normalize_pipeline_list(pipelines)
    if not pipeline_list:
        raise ValueError("pipelines must be a non-empty name or list of names")

    global_params = _safe_load_json(params_json) if params_json else {}
    if global_params is None:
        global_params = {}
    if not isinstance(global_params, dict):
        raise ValueError("params_json must decode to a JSON object")

    trigger = _safe_load_json(trigger_json) if trigger_json else {}
    if trigger is None:
        trigger = {}
    if not isinstance(trigger, dict):
        raise ValueError("trigger_json must decode to a JSON object")

    per_pipeline_params_map = _safe_load_json(pipeline_params_json) if pipeline_params_json else {}
    if per_pipeline_params_map is None:
        per_pipeline_params_map = {}
    if not isinstance(per_pipeline_params_map, dict):
        raise ValueError("pipeline_params_json must decode to a JSON object mapping pipeline_name -> params object")

    attempt_val = 1
    if attempt is not None and str(attempt).strip() != "":
        attempt_val = int(str(attempt).strip())
    if attempt_val < 1:
        attempt_val = 1

    tblprops = _safe_load_json(state_table_properties_json) if state_table_properties_json else {}
    if tblprops is None:
        tblprops = {}
    if not isinstance(tblprops, dict):
        raise ValueError("state_table_properties_json must decode to a JSON object mapping string->string")

    state_store = None
    if state_enabled:
        spark = SparkSession.getActiveSession()
        if spark is None:
            spark = SparkSession.builder.getOrCreate()

        tables = state_tables(catalog=state_catalog, schema=state_schema)
        controller = PerformanceController(
            spark=spark,
            run_history_table_fqn=tables.run_history,
        )
        
        state_store = StateStore(
            spark=spark,
            tables=tables,
            table_properties={str(k): str(v) for k, v in tblprops.items()},
            allow_schema_create=bool(state_allow_schema_create),
        )

    started = _now_utc()
    results: List[PipelineResult] = []

    reg = default_registry()
    runner = PipelineRunner(
        state_store=state_store,
        lock_name=lock_name,
        lock_ttl_seconds=lock_ttl_seconds,
        watermark_name=watermark_name,
        commit_watermark_on_success=True,
    )

    for p in pipeline_list:
        per_started = _now_utc()
        try:
            pp = per_pipeline_params_map.get(p)
            if pp is not None and not isinstance(pp, dict):
                raise ValueError("pipeline_params_json must map each pipeline_name to a JSON object")

            merged = _merge_params(global_params, pp)
            args = {
                "run_id": str(uuid.uuid4()),
                "attempt": str(attempt_val),
                "env": env,
                "pipeline_name": p,
                "mode": mode,
                "window_start_utc": window_start_utc,
                "window_end_utc": window_end_utc,
                "params_json": _compact_json(merged),
                "trigger_json": _compact_json(trigger),
                "framework_version": framework_version,
                "log_timezone": log_timezone,
                "dry_run": str(dry_run).lower(),
                "allow_destructive": str(allow_destructive).lower(),
            }

            rc: RunContext = build_run_context(args, cfg=cfg)

            controls = parse_performance_controls(rc.params)

            conf_scope = controller.apply_spark_conf(controls)
            try:
                controller.enforce_concurrency(
                    controls=controls,
                    pipeline_name=rc.pipeline_name,
                    orchestrator_name=rc.trigger.get("orchestrator", "databricks"),
                )

                pipeline = reg.get(rc.pipeline_name)

                rr = runner.run(pipeline, rc)

            finally:
                conf_scope.close()


            results.append(
                PipelineResult(
                    pipeline_name=rc.pipeline_name,
                    run_id=rc.run_id,
                    attempt=rc.attempt,
                    intent_hash=rc.intent_hash,
                    status=rr.status,
                    started_ts_utc=rr.started_ts_utc,
                    finished_ts_utc=rr.finished_ts_utc,
                    error_message=rr.error_message,
                    error_class=rr.error_class,
                )
            )
        except Exception as e:
            per_finished = _now_utc()
            results.append(
                PipelineResult(
                    pipeline_name=p,
                    run_id="",
                    attempt=attempt_val,
                    intent_hash="",
                    status="FAILED_PRECONDITION",
                    started_ts_utc=per_started.isoformat(),
                    finished_ts_utc=per_finished.isoformat(),
                    error_message=str(e),
                    error_class=type(e).__name__,
                )
            )

    finished = _now_utc()

    return {
        "started_ts_utc": started.isoformat(),
        "finished_ts_utc": finished.isoformat(),
        "env": env,
        "mode": mode,
        "window_start_utc": window_start_utc,
        "window_end_utc": window_end_utc,
        "attempt": attempt_val,
        "pipelines_requested": pipeline_list,
        "results": [r.to_dict() for r in results],
    }