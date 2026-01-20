# Databricks notebook source
import sys, importlib

sys.path.insert(0, "/Workspace/Users/victor.oliveira-ext4@ab-inbev.com")

import pipeline_runtime.execution.pipeline_executor as pe
importlib.reload(pe)


# COMMAND ----------

from datetime import datetime, timezone
import uuid

from pyspark.sql import functions as F

from pipeline_runtime.contracts.run_context import ContractConfig, build_run_context
from pipeline_runtime.execution.pipeline_runner import PipelineRunner
from pipeline_runtime.state.state_paths import state_tables
from pipeline_runtime.state.state_store import StateStore

# COMMAND ----------

out = pe.execute_pipelines(
    pipelines=["ppl_meta_inc_hst", "sls_ordrs_app_hst"],
    env="dev",
    mode="incremental",
    window_start_utc="2026-01-12T00:00:00Z",
    window_end_utc="2026-01-13T00:00:00Z",
    params_json='{"lookback_days":7}',
    trigger_json='{"orchestrator":"databricks","job_id":"123","workspace_id":"ws-dev-01"}',
    attempt="3",
)
display(out)


# COMMAND ----------

out2 = pe.execute_pipelines(
    pipelines=["ppl_meta_inc_hst", "sls_ordrs_full_snap"],
    env="prod",
    mode="snapshot",
    window_start_utc="2026-01-12T00:00:00Z",
    window_end_utc="2026-01-13T00:00:00Z",
    params_json='{"lookback_days":7}',
    trigger_json='{"orchestrator":"databricks","job_id":"123"}',
    allow_destructive="true",
    attempt=1,
)
display(out2)


# COMMAND ----------

out3 = pe.execute_pipelines(
    pipelines=["ppl_meta_inc_hst", "sls_ordrs_app_hst"],
    env="dev",
    mode="incremental",
    window_start_utc="2026-01-12T00:00:00Z",
    window_end_utc="2026-01-13T00:00:00Z",
    params_json='{"lookback_days":7,"common":"x"}',
    pipeline_params_json='{"sls_ordrs_app_hst":{"batch_id":"2026-01-13-001","common":"y"}}',
    trigger_json='{"orchestrator":"databricks"}',
)
display(out3)


# COMMAND ----------

import sys, importlib
sys.path.insert(0, "/Workspace/Users/victor.oliveira-ext4@ab-inbev.com")

import pipeline_runtime.execution.pipeline_registry as pr
import pipeline_runtime.execution.pipeline_runner as rr
import pipeline_runtime.execution.pipeline_executor as pe

importlib.reload(pr)
importlib.reload(rr)
importlib.reload(pe)


# COMMAND ----------

from pipeline_runtime.execution.pipeline import PipelineOutput

class ToySuccess:
    name = "ppl_meta_inc_hst"
    def validate_input(self, rc): 
        return None
    def extract(self, rc):
        return [1, 2, 3]
    def transform(self, rc, data):
        return data
    def load(self, rc, data):
        return PipelineOutput(row_count=len(data), metadata={"toy": "ok"})
    def validate_output(self, rc, out):
        return None

class ToyFail:
    name = "sls_ordrs_app_hst"
    def validate_input(self, rc):
        return None
    def extract(self, rc):
        raise RuntimeError("boom in extract")
    def transform(self, rc, data):
        return data
    def load(self, rc, data):
        return PipelineOutput(row_count=0, metadata={})
    def validate_output(self, rc, out):
        return None

reg = pr.default_registry()
reg.register(ToySuccess())
reg.register(ToyFail())

print(list(reg.list_names()))


# COMMAND ----------

out = pe.execute_pipelines(
    pipelines=["ppl_meta_inc_hst", "sls_ordrs_app_hst"],
    env="dev",
    mode="incremental",
    window_start_utc="2026-01-12T00:00:00Z",
    window_end_utc="2026-01-13T00:00:00Z",
    params_json='{"lookback_days":7}',
    trigger_json='{"orchestrator":"databricks","job_id":"123"}',
    attempt="1",
)
display(out)

# COMMAND ----------

out2 = pe.execute_pipelines(
    pipelines=["unknown_pipeline_x"],
    env="dev",
    mode="incremental",
    window_start_utc="2026-01-12T00:00:00Z",
    window_end_utc="2026-01-13T00:00:00Z",
    params_json='{}',
    trigger_json='{"orchestrator":"databricks"}',
)
display(out2)

# COMMAND ----------

# DBTITLE 1,---
out = pe.execute_pipelines(
    pipelines=["ppl_meta_inc_hst", "sls_ordrs_app_hst"],
    env="dev",
    mode="incremental",
    window_start_utc="2026-01-12T00:00:00Z",
    window_end_utc="2026-01-13T00:00:00Z",
    params_json='{}',
    trigger_json='{"orchestrator":"databricks"}',
    attempt="1",
)
display(out)
