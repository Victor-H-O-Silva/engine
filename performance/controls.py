from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from pipeline_runtime.performance.models import (
    ConcurrencyPolicy,
    PerformanceControls,
    PerformanceDecision,
    SparkConfPolicy,
    WindowChunkingPolicy,
)
from pipeline_runtime.performance.spark_conf import SparkConfManager, SparkConfSnapshot
from pipeline_runtime.performance.window_chunking import split_window
from pipeline_runtime.performance.concurrency import ConcurrencyGuard, ConcurrencyState


_DEFAULT_ALLOW_PREFIXES = (
    "spark.sql.",
    "spark.databricks.delta.",
    "spark.databricks.io.",
    "spark.databricks.optimizer.",
    "spark.sql.adaptive.",
    "spark.sql.shuffle.",
)

_DEFAULT_DENY_PREFIXES = (
    "spark.hadoop.",
    "spark.driver.",
    "spark.executor.",
    "spark.master",
    "spark.local",
    "spark.ui.",
)

_DEFAULT_DENY_EXACT = (
    "spark.sql.warehouse.dir",
)


def _as_tuple_str(x: Any) -> Tuple[str, ...]:
    if x is None:
        return tuple()
    if isinstance(x, (list, tuple)):
        return tuple(str(v) for v in x)
    return (str(x),)


def parse_performance_controls(params_json: Dict[str, Any]) -> PerformanceControls:
    perf = params_json.get("performance") or {}

    spark_conf = perf.get("spark_conf") or {}
    allow_prefixes = _as_tuple_str(perf.get("spark_conf_allow_prefixes") or _DEFAULT_ALLOW_PREFIXES)
    deny_prefixes = _as_tuple_str(perf.get("spark_conf_deny_prefixes") or _DEFAULT_DENY_PREFIXES)
    deny_exact = _as_tuple_str(perf.get("spark_conf_deny_exact") or _DEFAULT_DENY_EXACT)

    spark_policy = SparkConfPolicy(
        conf={str(k): str(v) for k, v in spark_conf.items()},
        allow_keys_prefixes=allow_prefixes,
        deny_keys_prefixes=deny_prefixes,
        deny_exact_keys=deny_exact,
    )

    conc = perf.get("concurrency") or {}
    conc_policy = ConcurrencyPolicy(
        enabled=bool(conc.get("enabled", False)),
        max_active=int(conc.get("max_active", 0)),
        scope=str(conc.get("scope", "pipeline")),
        scope_key=(str(conc.get("scope_key")) if conc.get("scope_key") is not None else None),
        active_statuses=_as_tuple_str(conc.get("active_statuses") or ("RUNNING", "LOCKED", "STARTED")),
    )

    chunk = perf.get("window_chunking") or {}
    chunk_policy = WindowChunkingPolicy(
        enabled=bool(chunk.get("enabled", False)),
        unit=str(chunk.get("unit", "days")),
        size=int(chunk.get("size", 1)),
        align_to_unit_boundary=bool(chunk.get("align_to_unit_boundary", True)),
    )

    return PerformanceControls(
        spark_conf=spark_policy,
        concurrency=conc_policy,
        window_chunking=chunk_policy,
    )


@dataclass
class SparkConfScope:
    manager: SparkConfManager
    snapshot: Optional[SparkConfSnapshot]

    def close(self) -> None:
        if self.snapshot is not None:
            self.manager.restore(self.snapshot)


class PerformanceController:
    def __init__(self, spark, run_history_table_fqn: str):
        self._spark = spark
        self._run_history_fqn = run_history_table_fqn

    def apply_spark_conf(self, controls: PerformanceControls) -> SparkConfScope:
        mgr = SparkConfManager(
            spark=self._spark,
            allow_keys_prefixes=controls.spark_conf.allow_keys_prefixes,
            deny_keys_prefixes=controls.spark_conf.deny_keys_prefixes,
            deny_exact_keys=controls.spark_conf.deny_exact_keys,
        )
        snap = None
        if controls.spark_conf.conf:
            snap = mgr.apply(controls.spark_conf.conf)
        return SparkConfScope(manager=mgr, snapshot=snap)

    def enforce_concurrency(
        self,
        controls: PerformanceControls,
        pipeline_name: str,
        orchestrator_name: str,
    ) -> Optional[ConcurrencyState]:
        if not controls.concurrency.enabled:
            return None

        scope = controls.concurrency.scope
        if scope == "pipeline":
            scope_key = pipeline_name
        elif scope == "orchestrator":
            scope_key = orchestrator_name
        elif scope == "custom":
            if not controls.concurrency.scope_key:
                scope_key = pipeline_name
            else:
                scope_key = controls.concurrency.scope_key
        else:
            scope_key = pipeline_name

        guard = ConcurrencyGuard(spark=self._spark, run_history_table_fqn=self._run_history_fqn)
        return guard.enforce(
            scope=scope,
            scope_key=scope_key,
            max_active=controls.concurrency.max_active,
            active_statuses=controls.concurrency.active_statuses,
        )

    def compute_window_chunks(
        self,
        controls: PerformanceControls,
        start_ts: Optional[datetime],
        end_ts_excl: Optional[datetime],
    ) -> List[Dict[str, Any]]:
        if not controls.window_chunking.enabled:
            return [{"start_ts": start_ts, "end_ts_excl": end_ts_excl, "chunk_index": 0, "chunk_count": 1}]

        if start_ts is None or end_ts_excl is None:
            return [{"start_ts": start_ts, "end_ts_excl": end_ts_excl, "chunk_index": 0, "chunk_count": 1}]

        if start_ts.tzinfo is None:
            start_ts = start_ts.replace(tzinfo=timezone.utc)
        if end_ts_excl.tzinfo is None:
            end_ts_excl = end_ts_excl.replace(tzinfo=timezone.utc)

        windows = split_window(
            start_ts=start_ts,
            end_ts_excl=end_ts_excl,
            unit=controls.window_chunking.unit,
            size=controls.window_chunking.size,
            align_to_unit_boundary=controls.window_chunking.align_to_unit_boundary,
        )
        n = len(windows)
        return [
            {"start_ts": w.start_ts, "end_ts_excl": w.end_ts_excl, "chunk_index": i, "chunk_count": n}
            for i, w in enumerate(windows)
        ]

    def decide(
        self,
        controls: PerformanceControls,
        pipeline_name: str,
        orchestrator_name: str,
        start_ts: Optional[datetime],
        end_ts_excl: Optional[datetime],
    ) -> PerformanceDecision:
        conc_state = self.enforce_concurrency(
            controls=controls,
            pipeline_name=pipeline_name,
            orchestrator_name=orchestrator_name,
        )

        chunks = self.compute_window_chunks(
            controls=controls,
            start_ts=start_ts,
            end_ts_excl=end_ts_excl,
        )

        return PerformanceDecision(
            applied_spark_conf=dict(controls.spark_conf.conf),
            concurrency_scope=(conc_state.scope if conc_state else None),
            concurrency_scope_key=(conc_state.scope_key if conc_state else None),
            concurrency_active_count=(conc_state.active if conc_state else None),
            concurrency_max_active=(conc_state.max_active if conc_state else None),
            chunk_count=len(chunks),
            chunk_unit=(controls.window_chunking.unit if controls.window_chunking.enabled else None),
            chunk_size=(controls.window_chunking.size if controls.window_chunking.enabled else None),
            diagnostics={},
        )