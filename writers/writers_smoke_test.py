# Databricks notebook source
import sys
sys.path.insert(0, "/Workspace/Users/victor.oliveira-ext4@ab-inbev.com")

# COMMAND ----------

from __future__ import annotations

import json
import uuid
from typing import Any, Dict, Optional

from pyspark.sql import functions as F

from pipeline_runtime.contracts.run_context import ContractConfig, build_run_context
from pipeline_runtime.writers.table_ref import TableRef
from pipeline_runtime.writers.delta_merge_writer import DeltaMergeWriter
from pipeline_runtime.writers.delta_append_writer import DeltaAppendWriter
from pipeline_runtime.writers.delta_snapshot_writer import DeltaSnapshotWriter
from pipeline_runtime.writers.writer_errors import PreconditionFailed, DestructiveOperationNotAllowed

# COMMAND ----------

CATALOG = "brewdat_uc_people_dev"
SCHEMA = "pipeline_runtime"

cfg = ContractConfig()


def assert_equal(a: Any, b: Any, msg: str = "") -> None:
    if a != b:
        raise AssertionError(f"{msg} expected={b} got={a}")


def assert_true(v: Any, msg: str = "") -> None:
    if not v:
        raise AssertionError(msg or "assert_true failed")


def drop_table_if_exists(fqn: str) -> None:
    spark.sql(f"DROP TABLE IF EXISTS {fqn}")

# COMMAND ----------

def mk_rc(
    *,
    pipeline_name: str,
    mode: str,
    env: str = "dev",
    allow_destructive: bool = False,
    dry_run: bool = False,
    params: Optional[Dict[str, Any]] = None,
) -> Any:
    args: Dict[str, Any] = {
        "run_id": str(uuid.uuid4()),
        "attempt": "1",
        "env": env,
        "pipeline_name": pipeline_name,
        "mode": mode,
        "window_start_utc": "2026-01-01T00:00:00Z",
        "window_end_utc": "2026-01-02T00:00:00Z",
        "params_json": "{}",
        "trigger_json": '{"orchestrator":"smoke_test"}',
        "framework_version": "0.1.0-smoke",
        "log_timezone": "America/Sao_Paulo",
        "allow_destructive": "true" if allow_destructive else "false",
        "dry_run": "true" if dry_run else "false",
    }
    if params is not None:
        args["params_json"] = json.dumps(params)
    return build_run_context(args, cfg=cfg)
suffix = uuid.uuid4().hex[:10]

merge_target = TableRef(CATALOG, SCHEMA, f"writers_smoke_merge_{suffix}")
append_target = TableRef(CATALOG, SCHEMA, f"writers_smoke_append_{suffix}")
snap_target = TableRef(CATALOG, SCHEMA, f"writers_smoke_snapshot_{suffix}")

drop_table_if_exists(merge_target.fqn)
drop_table_if_exists(append_target.fqn)
drop_table_if_exists(snap_target.fqn)

merge_writer = DeltaMergeWriter(spark=spark)
append_writer = DeltaAppendWriter(spark=spark)
snap_writer = DeltaSnapshotWriter(spark=spark)



# COMMAND ----------

rc_merge = mk_rc(pipeline_name=f"writers_smoke_merge_{suffix}", mode="incremental")

df1 = spark.createDataFrame(
    [(1, "a"), (2, "b"), (3, "c")],
    ["id", "val"],
)

r1 = merge_writer.write(
    rc=rc_merge,
    df=df1,
    target=merge_target,
    key_cols=["id"],
)

c1 = spark.table(merge_target.fqn).count()
assert_equal(c1, 3, "MERGE initial count")

r2 = merge_writer.write(
    rc=rc_merge,
    df=df1,
    target=merge_target,
    key_cols=["id"],
)

c2 = spark.table(merge_target.fqn).count()
assert_equal(c2, 3, "MERGE rerun count (idempotent)")

df2 = spark.createDataFrame(
    [(2, "bb"), (4, "d")],
    ["id", "val"],
)

r3 = merge_writer.write(
    rc=rc_merge,
    df=df2,
    target=merge_target,
    key_cols=["id"],
)

rows = {r["id"]: r["val"] for r in spark.table(merge_target.fqn).select("id", "val").collect()}
assert_equal(rows[2], "bb", "MERGE update")
assert_equal(rows[4], "d", "MERGE insert")
assert_equal(len(rows), 4, "MERGE final distinct ids")

# COMMAND ----------

rc_merge_dry = mk_rc(pipeline_name=f"writers_smoke_merge_{suffix}", mode="incremental", dry_run=True)
df_dry = spark.createDataFrame([(999, "zzz")], ["id", "val"])
r_dry = merge_writer.write(
    rc=rc_merge_dry,
    df=df_dry,
    target=merge_target,
    key_cols=["id"],
)
assert_true(r_dry.output_metadata.get("dry_run") is True, "MERGE dry_run flag missing")

c_dry = spark.table(merge_target.fqn).count()
assert_equal(c_dry, 4, "MERGE dry_run must not change table")

# COMMAND ----------

drop_table_if_exists(append_target.fqn)

rc_append = mk_rc(pipeline_name=f"writers_smoke_append_{suffix}", mode="append")

df_app = spark.createDataFrame(
    [(10, "x"), (11, "y")],
    ["id", "val"],
)

r1a = append_writer.write(
    rc=rc_append,
    df=df_app,
    target=append_target,
    dedup_key_cols=None,
)

c1a = spark.table(append_target.fqn).count()
assert_equal(c1a, 4, "APPEND no-dedup first call (create+append)")

r2a = append_writer.write(
    rc=rc_append,
    df=df_app,
    target=append_target,
    dedup_key_cols=None,
)

c2a = spark.table(append_target.fqn).count()
assert_equal(c2a, 6, "APPEND no-dedup second call duplicates again")

# COMMAND ----------

drop_table_if_exists(append_target.fqn)

r1b = append_writer.write(
    rc=rc_append,
    df=df_app,
    target=append_target,
    dedup_key_cols=["id"],
)

c1b = spark.table(append_target.fqn).count()
assert_equal(c1b, 2, "APPEND dedup first call should create with 2 rows")

r2b = append_writer.write(
    rc=rc_append,
    df=df_app,
    target=append_target,
    dedup_key_cols=["id"],
)

c2b = spark.table(append_target.fqn).count()
assert_equal(c2b, 2, "APPEND dedup second call should remain 2 rows")

# COMMAND ----------

# DBTITLE 1,append
drop_table_if_exists(append_target.fqn)

rc_append = mk_rc(pipeline_name=f"writers_smoke_append_{suffix}", mode="append")

df_app = spark.createDataFrame(
    [(10, "x"), (11, "y")],
    ["id", "val"],
)

r1a = append_writer.write(
    rc=rc_append,
    df=df_app,
    target=append_target,
    dedup_key_cols=None,
)

c1a = spark.table(append_target.fqn).count()
assert_equal(c1a, 4, "APPEND no-dedup first call (create+append)")

r2a = append_writer.write(
    rc=rc_append,
    df=df_app,
    target=append_target,
    dedup_key_cols=None,
)

c2a = spark.table(append_target.fqn).count()
assert_equal(c2a, 6, "APPEND no-dedup second call duplicates again")


drop_table_if_exists(append_target.fqn)

r1b = append_writer.write(
    rc=rc_append,
    df=df_app,
    target=append_target,
    dedup_key_cols=["id"],
)

c1b = spark.table(append_target.fqn).count()
assert_equal(c1b, 2, "APPEND dedup first call should create with 2 rows")

r2b = append_writer.write(
    rc=rc_append,
    df=df_app,
    target=append_target,
    dedup_key_cols=["id"],
)

c2b = spark.table(append_target.fqn).count()
assert_equal(c2b, 2, "APPEND dedup second call should remain 2 rows")


# COMMAND ----------

# DBTITLE 1,snap

drop_table_if_exists(snap_target.fqn)

rc_snap_block = mk_rc(
    pipeline_name=f"writers_smoke_snap_{suffix}_snap",
    mode="snapshot",
    env="dev",
    allow_destructive=False,
)

blocked = False
try:
    snap_writer.write(
        rc=rc_snap_block,
        df=spark.createDataFrame([(1, "a")], ["id", "val"]),
        target=snap_target,
        partition_cols=None,
        replace_where=None,
        require_snapshot_token=True,
    )
except DestructiveOperationNotAllowed:
    blocked = True

assert_true(blocked, "SNAPSHOT should block when allow_destructive=false")

rc_snap_ok = mk_rc(
    pipeline_name=f"writers_smoke_snap_{suffix}_snap",
    mode="snapshot",
    env="dev",
    allow_destructive=True,
)

df_s1 = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "val"])

r_s1 = snap_writer.write(
    rc=rc_snap_ok,
    df=df_s1,
    target=snap_target,
    partition_cols=None,
    replace_where=None,
    require_snapshot_token=True,
)

c_s1 = spark.table(snap_target.fqn).count()
assert_equal(c_s1, 2, "SNAPSHOT initial write count")

df_s2 = spark.createDataFrame([(2, "bb"), (3, "c")], ["id", "val"])

r_s2 = snap_writer.write(
    rc=rc_snap_ok,
    df=df_s2,
    target=snap_target,
    partition_cols=None,
    replace_where=None,
    require_snapshot_token=True,
)

rows_s = {r["id"]: r["val"] for r in spark.table(snap_target.fqn).select("id", "val").collect()}
assert_equal(len(rows_s), 2, "SNAPSHOT overwrite should replace content")
assert_equal(rows_s[2], "bb", "SNAPSHOT overwrite value")
assert_equal(rows_s[3], "c", "SNAPSHOT overwrite insert")

drop_table_if_exists(merge_target.fqn)
drop_table_if_exists(append_target.fqn)
drop_table_if_exists(snap_target.fqn)

print("WRITERS SMOKE TEST PASSED:", CATALOG, SCHEMA, suffix)