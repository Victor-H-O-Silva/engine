# Databricks notebook source
import sys, importlib, os
sys.path.insert(0, "/Workspace/Users/victor.oliveira-ext4@ab-inbev.com")

import pipeline_runtime.contracts.run_context as rc
importlib.reload(rc)

ContractConfig = rc.ContractConfig
build_run_context = rc.build_run_context

# COMMAND ----------

cfg = ContractConfig()

base_args = {
    "env": "dev",
    "pipeline_name": "ppl_meta_inc_hst",
    "mode": "incremental",
    "window_start_utc": "2026-01-12T00:00:00Z",
    "window_end_utc": "2026-01-13T00:00:00Z",
    "params_json": '{"lookback_days":7}',
    "trigger_json": '{"orchestrator":"databricks","job_id":"123","workflow_run_id":"456"}',
    "framework_version": "0.1.0-workspace",
    "log_timezone": "America/Sao_Paulo",
    "dry_run": "false",
    "allow_destructive": "false",
}


# COMMAND ----------

rc_inc = build_run_context(base_args, cfg=cfg)
display(rc_inc.to_dict())


# COMMAND ----------

bk_args = dict(base_args)
bk_args["mode"] = "backfill"
bk_args["pipeline_name"] = "ppl_meta_bkfl_hst"

rc_bk = build_run_context(bk_args, cfg=cfg)
display(rc_bk.to_dict())

# COMMAND ----------

app_args = dict(base_args)
app_args["mode"] = "append"
app_args["pipeline_name"] = "sls_ordrs_app_hst"
app_args["params_json"] = '{"batch_id":"2026-01-13-001","source_version":"v42"}'

rc_app = build_run_context(app_args, cfg=cfg)
display(rc_app.to_dict())

# COMMAND ----------

snap_args = dict(base_args)
snap_args["mode"] = "snapshot"
snap_args["env"] = "prod"
snap_args["pipeline_name"] = "sls_ordrs_full_snap"
snap_args["allow_destructive"] = "true"

rc_snap = build_run_context(snap_args, cfg=cfg)
display(rc_snap.to_dict())

# COMMAND ----------

try:
    bad = dict(snap_args)
    bad["pipeline_name"] = "sls_ordrs_full"
    build_run_context(bad, cfg=cfg)
    raise Exception("FAILED: expected snapshot allowlist failure but it passed")
except Exception as e:
    print("EXPECTED FAILURE (snapshot allowlist token):", str(e))


# COMMAND ----------

try:
    bad2 = dict(snap_args)
    bad2["allow_destructive"] = "false"
    build_run_context(bad2, cfg=cfg)
    raise Exception("FAILED: expected allow_destructive failure but it passed")
except Exception as e:
    print("EXPECTED FAILURE (prod snapshot requires allow_destructive):", str(e))


# COMMAND ----------

try:
    bad3 = dict(base_args)
    bad3["window_end_utc"] = "2026-01-12T00:00:00Z"
    build_run_context(bad3, cfg=cfg)
    raise Exception("FAILED: expected window order failure but it passed")
except Exception as e:
    print("EXPECTED FAILURE (window order):", str(e))


# COMMAND ----------

try:
    bad4 = dict(base_args)
    bad4["env"] = "prd"
    build_run_context(bad4, cfg=cfg)
    raise Exception("FAILED: expected env allowlist failure but it passed")
except Exception as e:
    print("EXPECTED FAILURE (env allowlist):", str(e))


# COMMAND ----------

a1 = dict(base_args)
a1["run_id"] = "run-1"
rc1 = build_run_context(a1, cfg=cfg)

a2 = dict(base_args)
a2["run_id"] = "run-2"
rc2 = build_run_context(a2, cfg=cfg)

print("intent_hash same for same intent:", rc1.intent_hash == rc2.intent_hash)
print("run_id differs:", rc1.run_id, rc2.run_id)

a3 = dict(base_args)
a3["run_id"] = "run-3"
a3["window_end_utc"] = "2026-01-14T00:00:00Z"
rc3 = build_run_context(a3, cfg=cfg)

print("intent_hash changes with window:", rc1.intent_hash != rc3.intent_hash)


# COMMAND ----------

try:
    huge = dict(base_args)
    huge["params_json"] = '{"x":"' + ("a" * (cfg.max_param_bytes + 10)) + '"}'
    build_run_context(huge, cfg=cfg)
    raise Exception("FAILED: expected params size failure but it passed")
except Exception as e:
    print("EXPECTED FAILURE (params size guard):", str(e))


# COMMAND ----------

cfg = ContractConfig()

t1 = dict(base_args)
t1["run_id"] = "run-attempt-1"
t1["attempt"] = "1"
rc1 = build_run_context(t1, cfg=cfg)

t2 = dict(base_args)
t2["run_id"] = "run-attempt-2"
t2["attempt"] = "2"
rc2 = build_run_context(t2, cfg=cfg)

print("attempt 1:", rc1.attempt)
print("attempt 2:", rc2.attempt)
print("intent_hash same across attempts:", rc1.intent_hash == rc2.intent_hash)


# COMMAND ----------

cfg = ContractConfig()

t3 = dict(base_args)
t3["run_id"] = "run-trigger"
t3["attempt"] = "1"
t3["trigger_json"] = '{"orchestrator":"databricks","job_id":"123","workspace_id":"ws-dev-01","repo_ref":"refs/heads/feature/runcontext","git_sha":"abc123","notebook_path":"/Repos/org/repo/notebooks/01_contracts/run_context_smoke_test"}'

rc3 = build_run_context(t3, cfg=cfg)

print(rc3.trigger.get("workspace_id"))
print(rc3.trigger.get("repo_ref"))
print(rc3.trigger.get("git_sha"))
print(rc3.trigger.get("notebook_path"))

d = rc3.to_dict()
print('"workspace_id":"ws-dev-01"' in d["trigger_json"])
print('"repo_ref":"refs/heads/feature/runcontext"' in d["trigger_json"])

