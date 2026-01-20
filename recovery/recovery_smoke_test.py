# Databricks notebook source
import sys, importlib

sys.path.insert(0, "/Workspace/Users/victor.oliveira-ext4@ab-inbev.com")

# COMMAND ----------

import uuid
from datetime import datetime, timezone

from pyspark.sql import functions as F

from pipeline_runtime.state.state_paths import StateTables
from pipeline_runtime.state.state_store import StateStore
from pipeline_runtime.execution.pipeline_runner import PipelineRunner
from pipeline_runtime.contracts.run_context import build_run_context, RunContext
from pipeline_runtime.execution.pipeline import Pipeline

# COMMAND ----------

CATALOG = "brewdat_uc_people_dev"
SCHEMA = "pipeline_runtime"

RUN_HISTORY = f"{CATALOG}.{SCHEMA}.run_history"
WATERMARKS = f"{CATALOG}.{SCHEMA}.watermarks"
LOCKS = f"{CATALOG}.{SCHEMA}.locks"

TABLES = StateTables(
    catalog=CATALOG,
    schema=SCHEMA,
    run_history=RUN_HISTORY,
    watermarks=WATERMARKS,
    locks=LOCKS,
)

state = StateStore(
    spark=spark,
    tables=TABLES,
    allow_schema_create=False,
)

state.ensure_tables()
print("StateStore tables OK:", TABLES.run_history, TABLES.watermarks, TABLES.locks)

# COMMAND ----------

def _assert(cond, msg):
    if not cond:
        raise AssertionError(msg)

def _utc_now():
    return datetime.now(timezone.utc)

def _cleanup_pipeline_state(pipeline_name: str, lock_name: str = "pipeline_run"):
    spark.sql(f"DELETE FROM {TABLES.locks} WHERE pipeline_name = '{pipeline_name}' AND lock_name = '{lock_name}'")
    spark.sql(f"DELETE FROM {TABLES.watermarks} WHERE pipeline_name = '{pipeline_name}'")
    spark.sql(f"DELETE FROM {TABLES.run_history} WHERE pipeline_name = '{pipeline_name}'")

def _mk_rc(
    *,
    pipeline_name: str,
    env: str = "dev",
    mode: str = "incremental",
    window_start: str = "2026-01-01T00:00:00+00:00",
    window_end: str = "2026-01-02T00:00:00+00:00",
    params: dict | None = None,
    trigger: dict | None = None,
    attempt: int = 1,
    allow_destructive: bool = False,
    dry_run: bool = False,
):
    return RunContext.create(
        run_id=str(uuid.uuid4()),
        attempt=attempt,
        env=env,
        pipeline_name=pipeline_name,
        mode=mode,
        window_start_utc=window_start,
        window_end_utc=window_end,
        params_json=params or {},
        trigger_json=trigger or {},
        allow_destructive=allow_destructive,
        dry_run=dry_run,
        framework_version="test",
    )

# COMMAND ----------

class CountingPipeline(Pipeline):
    def __init__(self, name: str, behavior: str, counters: dict):
        self._name = name
        self._behavior = behavior
        self._counters = counters

    @property
    def name(self) -> str:
        return self._name

    def extract(self, rc: RunContext):
        self._counters["extract"] = self._counters.get("extract", 0) + 1
        if self._behavior == "fail_in_extract":
            raise RuntimeError("boom_extract")
        return {"x": 1}

    def transform(self, rc: RunContext, data):
        self._counters["transform"] = self._counters.get("transform", 0) + 1
        if self._behavior == "fail_in_transform":
            raise RuntimeError("boom_transform")
        return data

    def load(self, rc: RunContext, data):
        self._counters["load"] = self._counters.get("load", 0) + 1
        if self._behavior == "fail_in_load":
            raise RuntimeError("boom_load")
        return None

# COMMAND ----------

def _mk_runner(lock_ttl_seconds: int = 120):
    return PipelineRunner(
        state_store=state,
        lock_name="pipeline_run",
        lock_ttl_seconds=lock_ttl_seconds,
        watermark_name="window_end_utc",
        commit_watermark_on_success=True,
    )

# COMMAND ----------

def _assert(cond, msg):
    if not cond:
        raise AssertionError(msg)

def _cleanup_pipeline_state(pipeline_name: str, lock_name: str = "pipeline_run"):
    spark.sql(f"DELETE FROM {TABLES.locks} WHERE pipeline_name = '{pipeline_name}' AND lock_name = '{lock_name}'")
    spark.sql(f"DELETE FROM {TABLES.watermarks} WHERE pipeline_name = '{pipeline_name}'")
    spark.sql(f"DELETE FROM {TABLES.run_history} WHERE pipeline_name = '{pipeline_name}'")

def _mk_rc(
    *,
    pipeline_name: str,
    env: str = "dev",
    mode: str = "incremental",
    window_start_utc: str = "2026-01-01 00:00:00",
    window_end_utc: str = "2026-01-02 00:00:00",
    params: dict | None = None,
    trigger: dict | None = None,
    attempt: int = 1,
    allow_destructive: bool = False,
    dry_run: bool = False,
    framework_version: str = "test",
    log_timezone: str = "America/Sao_Paulo",
):
    cfg = ContractConfig()
    args = {
        "run_id": str(uuid.uuid4()),
        "attempt": str(int(attempt)),
        "env": env,
        "pipeline_name": pipeline_name,
        "mode": mode,
        "window_start_utc": window_start_utc,
        "window_end_utc": window_end_utc,
        "params_json": json.dumps(params or {}, separators=(",", ":"), ensure_ascii=False, sort_keys=True),
        "trigger_json": json.dumps(trigger or {}, separators=(",", ":"), ensure_ascii=False, sort_keys=True),
        "framework_version": framework_version,
        "log_timezone": log_timezone,
        "dry_run": str(bool(dry_run)).lower(),
        "allow_destructive": str(bool(allow_destructive)).lower(),
    }
    return build_run_context(args, cfg=cfg)


# COMMAND ----------

def test_success_commits_watermark():
    pipeline_name = f"p8_success_{uuid.uuid4().hex[:8]}"
    _cleanup_pipeline_state(pipeline_name)

    counters = {}
    p = CountingPipeline(name=pipeline_name, behavior="success", counters=counters)
    rc = _mk_rc(pipeline_name=pipeline_name)

    runner = _mk_runner()
    rr = runner.run(p, rc)

    _assert(rr.status == "SUCCESS", f"expected SUCCESS, got {rr.status}")
    _assert(counters.get("extract", 0) == 1, "extract not called exactly once")
    _assert(counters.get("transform", 0) == 1, "transform not called exactly once")
    _assert(counters.get("load", 0) == 1, "load not called exactly once")

    wm = state.get_watermark(pipeline_name, "window_end_utc")
    _assert(wm is not None, "watermark missing after success")
    _assert(wm[0] == rc.window_end_utc.isoformat(), f"watermark_value mismatch: {wm[0]} vs {rc.window_end_utc.isoformat()}")
    _assert(wm[2] == rc.run_id, f"watermark run_id mismatch: {wm[2]} vs {rc.run_id}")

    rh = (
        spark.table(TABLES.run_history)
        .where(F.col("pipeline_name") == pipeline_name)
        .where(F.col("run_id") == rc.run_id)
        .select("status")
        .limit(1)
        .collect()
    )
    _assert(len(rh) == 1, "run_history row missing")
    _assert(rh[0]["status"] == "SUCCESS", f"run_history status mismatch: {rh[0]['status']}")

test_success_commits_watermark()
print("PASS test_success_commits_watermark")

# COMMAND ----------

def test_lock_not_acquired_metrics_present():
    pipeline_name = f"p8_lockbusy_{uuid.uuid4().hex[:8]}"
    _cleanup_pipeline_state(pipeline_name)

    runner = _mk_runner(lock_ttl_seconds=600)

    other_owner = "other_" + str(uuid.uuid4())
    state.try_acquire_lock(
        pipeline_name=pipeline_name,
        lock_name="pipeline_run",
        owner_run_id=other_owner,
        ttl_seconds=600,
    )

    counters = {}
    p = CountingPipeline(name=pipeline_name, behavior="success", counters=counters)
    rc = _mk_rc(pipeline_name=pipeline_name, attempt=1)

    rr = runner.run(p, rc)

    _assert(rr.status == "FAILED_PRECONDITION", f"expected FAILED_PRECONDITION, got {rr.status}")
    _assert(rr.error_class == "LockNotAcquired", f"expected LockNotAcquired, got {rr.error_class}")

    _assert("lock_acquired" in rr.metrics, "missing metrics.lock_acquired (did you add it in the lock-not-acquired block?)")
    _assert(rr.metrics["lock_acquired"] is False, f"expected lock_acquired False, got {rr.metrics['lock_acquired']}")
    _assert(rr.metrics.get("lock_owner_run_id") is not None, "expected lock_owner_run_id not None")
    _assert(rr.metrics.get("lock_expires_at") is not None, "expected lock_expires_at not None")

    lock_row = (
        spark.table(TABLES.locks)
        .where(F.col("pipeline_name") == pipeline_name)
        .where(F.col("lock_name") == "pipeline_run")
        .select("owner_run_id", "expires_at")
        .limit(1)
        .collect()
    )
    _assert(len(lock_row) == 1, "expected lock row to exist")
    actual_owner = lock_row[0]["owner_run_id"]

    _assert(
        rr.metrics.get("lock_owner_run_id") == actual_owner,
        f"metrics.lock_owner_run_id must match locks.owner_run_id. metrics={rr.metrics.get('lock_owner_run_id')} table={actual_owner}",
    )

    _assert(
        actual_owner != rc.run_id,
        f"lock owner must not be the current run_id when lock_acquired is False. owner={actual_owner} run_id={rc.run_id}",
    )

    _assert(counters.get("extract", 0) == 0, "pipeline should not start when lock not acquired")
    _assert(counters.get("transform", 0) == 0, "pipeline should not start when lock not acquired")
    _assert(counters.get("load", 0) == 0, "pipeline should not start when lock not acquired")

test_lock_not_acquired_metrics_present()
print("PASS test_lock_not_acquired_metrics_present")

# COMMAND ----------

def test_heartbeat_failure_locklost():
    pipeline_name = f"p8_hbfail_{uuid.uuid4().hex[:8]}"
    _cleanup_pipeline_state(pipeline_name)

    counters = {}
    p = CountingPipeline(name=pipeline_name, behavior="success", counters=counters)

    rc = _mk_rc(
        pipeline_name=pipeline_name,
        params={"lock_heartbeat_seconds": 1},
    )

    runner = _mk_runner(lock_ttl_seconds=600)

    orig = state.heartbeat_lock
    def _hb_fail(*args, **kwargs):
        return False
    state.heartbeat_lock = _hb_fail

    try:
        rr = runner.run(p, rc)
    finally:
        state.heartbeat_lock = orig

    _assert(rr.status == "FAILED_PRECONDITION", f"expected FAILED_PRECONDITION, got {rr.status}")
    _assert(rr.error_class == "LockLost", f"expected LockLost, got {rr.error_class}")
    _assert("Lock heartbeat failed" in (rr.error_message or ""), f"unexpected error_message: {rr.error_message}")

    _assert(counters.get("extract", 0) == 0, "pipeline should not start when heartbeat fails")
    _assert(counters.get("transform", 0) == 0, "pipeline should not start when heartbeat fails")
    _assert(counters.get("load", 0) == 0, "pipeline should not start when heartbeat fails")

    wm = state.get_watermark(pipeline_name, "window_end_utc")
    _assert(wm is None, "watermark should not exist after LockLost")

test_heartbeat_failure_locklost()
print("PASS test_heartbeat_failure_locklost")


# COMMAND ----------

def test_resume_if_safe_skips_execution():
    pipeline_name = f"p8_resume_{uuid.uuid4().hex[:8]}"
    _cleanup_pipeline_state(pipeline_name)

    runner = _mk_runner(lock_ttl_seconds=600)

    counters1 = {}
    p1 = CountingPipeline(name=pipeline_name, behavior="success", counters=counters1)
    rc1 = _mk_rc(pipeline_name=pipeline_name)

    rr1 = runner.run(p1, rc1)
    _assert(rr1.status == "SUCCESS", f"first run expected SUCCESS, got {rr1.status}")

    wm1 = state.get_watermark(pipeline_name, "window_end_utc")
    _assert(wm1 is not None, "watermark missing after first success")
    _assert(wm1[2] == rc1.run_id, "watermark run_id must equal first run_id")

    counters2 = {}
    p2 = CountingPipeline(name=pipeline_name, behavior="success", counters=counters2)

    rc2 = _mk_rc(
        pipeline_name=pipeline_name,
        env=rc1.env,
        mode=rc1.mode.value,
        window_start_utc=rc1.window_start_utc.isoformat(),
        window_end_utc=rc1.window_end_utc.isoformat(),
        params={"recovery_mode": "RESUME_IF_SAFE"},
        attempt=2,
    )

    rr2 = runner.run(p2, rc2)

    _assert(rr2.status == "SUCCESS", f"resume run expected SUCCESS, got {rr2.status}")
    _assert(rr2.metrics.get("recovery_skipped") is True, "expected metrics.recovery_skipped True")
    _assert(rr2.metrics.get("recovery_prior_run_id") == rc1.run_id, f"expected prior_run_id={rc1.run_id}, got {rr2.metrics.get('recovery_prior_run_id')}")

    _assert(counters2.get("extract", 0) == 0, "extract should not be called when resumed")
    _assert(counters2.get("transform", 0) == 0, "transform should not be called when resumed")
    _assert(counters2.get("load", 0) == 0, "load should not be called when resumed")

    wm2 = state.get_watermark(pipeline_name, "window_end_utc")
    _assert(wm2 is not None, "watermark missing after resume")
    _assert(wm2[0] == rc1.window_end_utc.isoformat(), "watermark value must remain unchanged after resume")
    _assert(wm2[2] == rc1.run_id, "watermark run_id must remain the first run_id after resume")

test_resume_if_safe_skips_execution()
print("PASS test_resume_if_safe_skips_execution")