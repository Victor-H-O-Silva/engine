# Databricks notebook source
import sys
import uuid
sys.path.insert(0, "/Workspace/Users/victor.oliveira-ext4@ab-inbev.com")

from pipeline_runtime.state.state_paths import state_tables, StateTables
from pipeline_runtime.state.state_store import StateStore
from pyspark.sql import functions as F

# COMMAND ----------

CATALOG = "brewdat_uc_people_dev"
SCHEMA = "pipeline_runtime"

suffix = uuid.uuid4().hex[:10]
pipeline_name = f"smoke_state_{suffix}"
run_id_1 = f"run_{suffix}_1"
run_id_2 = f"run_{suffix}_2"
wm_name = "wm_source_ts"
lock_name = "main"

tables = state_tables(catalog=CATALOG, schema=SCHEMA)
store = StateStore(spark, tables)

def table_exists_fqn(fqn: str) -> bool:
    parts = fqn.split(".")
    if len(parts) != 3:
        raise ValueError(f"Expected catalog.schema.table, got: {fqn}")
    c, s, t = parts
    q = f"""
    SELECT 1
    FROM system.information_schema.tables
    WHERE table_catalog = '{c}'
      AND table_schema  = '{s}'
      AND table_name    = '{t}'
    LIMIT 1
    """
    return len(spark.sql(q).collect()) > 0

assert table_exists_fqn(tables.run_history), f"Missing table: {tables.run_history}"
assert table_exists_fqn(tables.watermarks), f"Missing table: {tables.watermarks}"
assert table_exists_fqn(tables.locks), f"Missing table: {tables.locks}"


store.record_run_start(
    pipeline_name=pipeline_name,
    run_id=run_id_1,
    attempt=1,
    env="dev",
    mode="incremental",
    framework_version="smoke",
    params={"k": "v", "suffix": suffix},
    actor="smoke_test",
)

store.record_run_start(
    pipeline_name=pipeline_name,
    run_id=run_id_1,
    attempt=1,
    env="dev",
    mode="incremental",
    framework_version="smoke",
    params={"k": "v", "suffix": suffix},
    actor="smoke_test",
)

rh = (
    spark.table(tables.run_history)
    .where((F.col("pipeline_name") == pipeline_name) & (F.col("run_id") == run_id_1))
    .select("status", "started_at", "ended_at", "params_json")
    .collect()
)

assert len(rh) == 1
assert rh[0]["status"] == "RUNNING"
assert rh[0]["started_at"] is not None
assert rh[0]["ended_at"] is None
assert rh[0]["params_json"] is not None

store.record_run_end(
    pipeline_name=pipeline_name,
    run_id=run_id_1,
    status="SUCCESS",
    metrics={"rows_written": 0, "ok": True},
)

rh2 = (
    spark.table(tables.run_history)
    .where((F.col("pipeline_name") == pipeline_name) & (F.col("run_id") == run_id_1))
    .select("status", "ended_at", "metrics_json")
    .collect()
)

assert len(rh2) == 1
assert rh2[0]["status"] == "SUCCESS"
assert rh2[0]["ended_at"] is not None
assert rh2[0]["metrics_json"] is not None

wm0 = store.get_watermark(pipeline_name, wm_name)
assert wm0 is None

store.upsert_watermark(
    pipeline_name=pipeline_name,
    watermark_name=wm_name,
    watermark_value="100",
    run_id=run_id_1,
)

wm1 = store.get_watermark(pipeline_name, wm_name)
assert wm1 is not None
assert wm1[0] == "100"
assert wm1[2] == run_id_1

store.upsert_watermark(
    pipeline_name=pipeline_name,
    watermark_name=wm_name,
    watermark_value="200",
    run_id=run_id_2,
)

wm2 = store.get_watermark(pipeline_name, wm_name)
assert wm2 is not None
assert wm2[0] == "200"
assert wm2[2] == run_id_2

wm_rows = (
    spark.table(tables.watermarks)
    .where((F.col("pipeline_name") == pipeline_name) & (F.col("watermark_name") == wm_name))
    .count()
)
assert wm_rows == 1

acq1 = store.try_acquire_lock(pipeline_name, lock_name, run_id_1, ttl_seconds=120)
assert acq1.acquired is True
assert acq1.owner_run_id == run_id_1
assert acq1.expires_at is not None

acq2 = store.try_acquire_lock(pipeline_name, lock_name, run_id_2, ttl_seconds=120)
assert acq2.acquired is False
assert acq2.owner_run_id == run_id_1

hb1 = store.heartbeat_lock(pipeline_name, lock_name, run_id_1, ttl_seconds=120)
assert hb1 is True

hb2 = store.heartbeat_lock(pipeline_name, lock_name, run_id_2, ttl_seconds=120)
assert hb2 is False

store.release_lock(pipeline_name, lock_name, run_id_1)

acq3 = store.try_acquire_lock(pipeline_name, lock_name, run_id_2, ttl_seconds=120)
assert acq3.acquired is True
assert acq3.owner_run_id == run_id_2

store.release_lock(pipeline_name, lock_name, run_id_2)

lk_rows = (
    spark.table(tables.locks)
    .where((F.col("pipeline_name") == pipeline_name) & (F.col("lock_name") == lock_name))
    .count()
)
assert lk_rows == 0

spark.sql(f"DELETE FROM {tables.run_history} WHERE pipeline_name = '{pipeline_name}'")
spark.sql(f"DELETE FROM {tables.watermarks} WHERE pipeline_name = '{pipeline_name}'")
spark.sql(f"DELETE FROM {tables.locks} WHERE pipeline_name = '{pipeline_name}'")

print("STATE SMOKE TEST PASSED:", tables.schema, pipeline_name)
