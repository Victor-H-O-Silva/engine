# Databricks notebook source
import sys, importlib

sys.path.insert(0, "/Workspace/Users/victor.oliveira-ext4@ab-inbev.com")

# COMMAND ----------

from dataclasses import replace
from pyspark.sql import functions as F
import uuid
from datetime import datetime, timezone

from pipeline_runtime.orchestration.execution_entrypoint import run_execution
from pipeline_runtime.safety.safety_config import SafetyConfig
from pipeline_runtime.safety.safety_rails import (
    enforce_snapshot_overwrite_policy,
    enforce_append_dedup_policy,
)
from pipeline_runtime.writers.writer_errors import PreconditionFailed


def _mk_rc(env: str, params: dict | None = None):
    class RC:
        def __init__(self, env, params):
            self.env = env
            self.params = params or {}
    return RC(env=env, params=params or {})


def _mk_target_table(fqn: str):
    spark.sql(f"DROP TABLE IF EXISTS {fqn}")
    df = (
        spark.range(0, 1000)
        .withColumn("p", (F.col("id") % 50).cast("int"))
        .withColumn("v", F.col("id").cast("int"))
    )
    (df.write.format("delta").mode("overwrite").partitionBy("p").saveAsTable(fqn))


def _assert_raises(fn, contains: str):
    try:
        fn()
    except Exception as e:
        s = str(e)
        if contains not in s:
            raise AssertionError(f"Expected exception containing '{contains}', got: {s}")
        return
    raise AssertionError("Expected exception but none was raised")


cfg = SafetyConfig.default()

catalog = "brewdat_uc_people_dev"
schema = "pipeline_runtime"
tbl = f"zz_phase7_smoke_{str(uuid.uuid4()).replace('-', '')[:10]}"
target_fqn = f"{catalog}.{schema}.{tbl}"

_mk_target_table(target_fqn)


def test_snapshot_replacewhere_required_blocks():
    rc = _mk_rc(env="qa", params={})
    def _call():
        enforce_snapshot_overwrite_policy(
            spark=spark,
            rc=rc,
            target_fqn=target_fqn,
            partition_cols=["p"],
            replace_where=None,
            cfg=cfg,
        )
    _assert_raises(_call, "Snapshot overwrite requires replaceWhere")

test_snapshot_replacewhere_required_blocks()


def test_snapshot_full_overwrite_dev_allowed_with_param():
    rc = _mk_rc(env="dev", params={cfg.allow_snapshot_without_replacewhere_param_key: True})
    events, affected = enforce_snapshot_overwrite_policy(
        spark=spark,
        rc=rc,
        target_fqn=target_fqn,
        partition_cols=["p"],
        replace_where=None,
        cfg=cfg,
    )
    assert affected == -1
    assert len(events) >= 1
    assert any(e.code == "SNAPSHOT_FULL_OVERWRITE_ALLOWED" for e in events)

test_snapshot_full_overwrite_dev_allowed_with_param()


def test_snapshot_max_partitions_exceeded_blocks():
    tight_cfg = replace(cfg, max_partitions_overwrite_dev=1, max_partitions_overwrite_qa=1, max_partitions_overwrite_prod=1)
    rc = _mk_rc(env="dev", params={})
    def _call():
        enforce_snapshot_overwrite_policy(
            spark=spark,
            rc=rc,
            target_fqn=target_fqn,
            partition_cols=["p"],
            replace_where="p in (1,2,3)",  
            cfg=tight_cfg,
        )
    _assert_raises(_call, "Snapshot overwrite exceeds max affected partitions")

test_snapshot_max_partitions_exceeded_blocks()


def test_append_dedup_required_blocks_in_qa():
    rc = _mk_rc(env="qa", params={})
    def _call():
        enforce_append_dedup_policy(
            rc=rc,
            cfg=cfg,
            dedup_key_cols=None,
        )
    _assert_raises(_call, "Append writer requires dedup_key_cols")

test_append_dedup_required_blocks_in_qa()


def test_append_dedup_ok_in_qa_with_keys():
    rc = _mk_rc(env="qa", params={})
    events = enforce_append_dedup_policy(
        rc=rc,
        cfg=cfg,
        dedup_key_cols=["id"],
    )
    assert any(e.code == "APPEND_DEDUP_POLICY_OK" for e in events)

test_append_dedup_ok_in_qa_with_keys()


print("ALL PASS (Safety rails smoke)")
