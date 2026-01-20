# Databricks notebook source
import sys, importlib

sys.path.insert(0, "/Workspace/Users/victor.oliveira-ext4@ab-inbev.com")

# COMMAND ----------

import json
import uuid
from datetime import datetime, timezone, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pipeline_runtime.execution.pipeline_executor import execute_pipelines
from pipeline_runtime.execution.pipeline_registry import default_registry
from pipeline_runtime.orchestration.execution_entrypoint import run_execution
from pipeline_runtime.state.state_paths import state_tables
from pipeline_runtime.performance.controls import PerformanceController, parse_performance_controls
from pipeline_runtime.performance.spark_conf import SparkConfManager
from pipeline_runtime.performance.errors import (
    SparkConfDeniedError,
    ConcurrencyLimitExceededError,
)
from pipeline_runtime.performance.concurrency import ConcurrencyGuard


# COMMAND ----------

spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()

# COMMAND ----------

def _dt(s: str) -> datetime:
    d = datetime.fromisoformat(s.replace("Z", "+00:00"))
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)
    return d.astimezone(timezone.utc)

def _iso(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()

def _assert_eq(a, b):
    assert a == b, (a, b)

# COMMAND ----------

params = {
    "performance": {
        "window_chunking": {
            "enabled": True,
            "unit": "days",
            "size": 7,
            "align_to_unit_boundary": True,
        }
    }
}

controls = parse_performance_controls(params)
controller = PerformanceController(spark=spark, run_history_table_fqn="")

start = _dt("2026-01-01T00:00:00+00:00")
end = _dt("2026-01-15T00:00:00+00:00")

chunks_a = controller.compute_window_chunks(controls=controls, start_ts=start, end_ts_excl=end)
chunks_b = controller.compute_window_chunks(controls=controls, start_ts=start, end_ts_excl=end)

_assert_eq(chunks_a, chunks_b)

len(chunks_a), chunks_a[0], chunks_a[-1]

# COMMAND ----------

_assert_eq(len(chunks_a), 2)
_assert_eq(chunks_a[0]["start_ts"], start)
_assert_eq(chunks_a[0]["end_ts_excl"], _dt("2026-01-08T00:00:00+00:00"))
_assert_eq(chunks_a[1]["start_ts"], _dt("2026-01-08T00:00:00+00:00"))
_assert_eq(chunks_a[1]["end_ts_excl"], end)

# COMMAND ----------

conf_key = "spark.sql.shuffle.partitions"

orig = None
try:
    orig = spark.conf.get(conf_key)
except Exception:
    orig = None

override = {"spark.sql.shuffle.partitions": "123"}

mgr = SparkConfManager(
    spark=spark,
    allow_keys_prefixes=("spark.sql.",),
    deny_keys_prefixes=(),
    deny_exact_keys=(),
)

snap = mgr.apply(override)

during = spark.conf.get(conf_key)
_assert_eq(during, "123")

mgr.restore(snap)

after = None
try:
    after = spark.conf.get(conf_key)
except Exception:
    after = None

_assert_eq(after, orig)

orig, during, after

# COMMAND ----------

deny_test = {"spark.driver.memory": "1g"}

mgr2 = SparkConfManager(
    spark=spark,
    allow_keys_prefixes=("spark.sql.", "spark.databricks."),
    deny_keys_prefixes=("spark.driver.", "spark.executor.", "spark.hadoop."),
    deny_exact_keys=(),
)

thrown = None
try:
    mgr2.apply(deny_test)
except Exception as e:
    thrown = e

type(thrown).__name__, str(thrown)[:200]

# COMMAND ----------

deny_test = {"spark.driver.memory": "1g"}

mgr2 = SparkConfManager(
    spark=spark,
    allow_keys_prefixes=("spark.sql.", "spark.databricks."),
    deny_keys_prefixes=("spark.driver.", "spark.executor.", "spark.hadoop."),
    deny_exact_keys=(),
)

thrown = None
try:
    mgr2.apply(deny_test)
except Exception as e:
    thrown = e

type(thrown).__name__, str(thrown)[:200]

# COMMAND ----------

assert isinstance(thrown, SparkConfDeniedError), type(thrown)

# COMMAND ----------

tmp_run_history = "tmp_run_history_phase9"

data = [
    ("pipeline", "p1", "RUNNING"),
    ("pipeline", "p1", "STARTED"),
    ("pipeline", "p2", "RUNNING"),
    ("orchestrator", "databricks", "RUNNING"),
]

df = spark.createDataFrame(data, ["concurrency_scope", "concurrency_scope_key", "status"])
df.createOrReplaceTempView(tmp_run_history)

guard = ConcurrencyGuard(spark=spark, run_history_table_fqn=tmp_run_history)

thrown = None
try:
    guard.enforce(
        scope="pipeline",
        scope_key="p1",
        max_active=2,
        active_statuses=("RUNNING", "LOCKED", "STARTED"),
    )
except Exception as e:
    thrown = e

type(thrown).__name__, str(thrown)[:200]

# COMMAND ----------

assert isinstance(thrown, ConcurrencyLimitExceededError), type(thrown)

# COMMAND ----------

state = guard.enforce(
    scope="pipeline",
    scope_key="p2",
    max_active=2,
    active_statuses=("RUNNING", "LOCKED", "STARTED"),
)
state
_assert_eq(state.active, 1)
_assert_eq(state.max_active, 2)

# COMMAND ----------

from pipeline_runtime.orchestration.failure_policy import should_stop_execution

ordered = ["pA", "pB"]
chunks = [
    {"start_ts": _dt("2026-01-01T00:00:00+00:00"), "end_ts_excl": _dt("2026-01-08T00:00:00+00:00")},
    {"start_ts": _dt("2026-01-08T00:00:00+00:00"), "end_ts_excl": _dt("2026-01-15T00:00:00+00:00")},
]

def simulate(statuses_by_call, failure_policy):
    total = succeeded = failed = failed_precondition = 0
    i = 0
    for _chunk in chunks:
        for _p in ordered:
            total += 1
            status = statuses_by_call[i]
            i += 1
            if status == "SUCCESS":
                succeeded += 1
            elif status == "FAILED_PRECONDITION":
                failed_precondition += 1
                failed += 1
            else:
                failed += 1

            if should_stop_execution(failure_policy, status):
                return {
                    "total": total,
                    "succeeded": succeeded,
                    "failed": failed,
                    "failed_precondition": failed_precondition,
                }
    return {
        "total": total,
        "succeeded": succeeded,
        "failed": failed,
        "failed_precondition": failed_precondition,
    }

# COMMAND ----------

out_stop = simulate(
    statuses_by_call=["FAILED_PRECONDITION", "SUCCESS", "SUCCESS", "SUCCESS"],
    failure_policy="STOP",
)
out_stop

# COMMAND ----------

_assert_eq(out_stop["total"], 1)
_assert_eq(out_stop["failed_precondition"], 1)

# COMMAND ----------

out_cont = simulate(
    statuses_by_call=["FAILED_PRECONDITION", "SUCCESS", "SUCCESS", "SUCCESS"],
    failure_policy="CONTINUE",
)
out_cont

# COMMAND ----------

_assert_eq(out_cont["total"], len(chunks) * len(ordered))
_assert_eq(out_cont["failed_precondition"], 1)
_assert_eq(out_cont["succeeded"], 3)