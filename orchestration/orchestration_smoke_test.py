# Databricks notebook source
import sys
sys.path.insert(0, "/Workspace/Users/victor.oliveira-ext4@ab-inbev.com")
import uuid
from datetime import datetime, timezone

from pipeline_runtime.orchestration.execution_entrypoint import run_execution

# COMMAND ----------

def _utc_iso():
    return datetime.now(timezone.utc).isoformat()


def _make_exec_out(pipeline_name, status):
    now = _utc_iso()
    base = {
        "pipeline_name": pipeline_name,
        "status": status,
        "run_id": str(uuid.uuid4()),
        "attempt": 1,
        "intent_hash": f"ih_{pipeline_name}",
        "started_ts_utc": now,
        "finished_ts_utc": now,
        "error_message": None,
        "error_class": None,
    }
    if status != "SUCCESS":
        base["error_message"] = f"boom:{pipeline_name}"
        base["error_class"] = "RuntimeError"
    return {"results": [base]}


def _base_payload(**overrides):
    payload = {
        "env": "dev",
        "mode": "INCREMENTAL",
        "window_start_utc": "2026-01-01T00:00:00Z",
        "window_end_utc": "2026-01-02T00:00:00Z",
        "pipelines": ["ok"],
        "tags": None,
        "params": None,
        "pipeline_params": None,
        "trigger": None,
        "attempt": 1,
        "allow_destructive": False,
        "dry_run": False,
        "failure_policy": "STOP",
        "max_concurrency": 1,
        "depends_on": None,
    }
    payload.update(overrides)
    return payload


def _assert(cond, msg):
    if not cond:
        raise AssertionError(msg)


def _assert_execution_result_shape(res):
    expected_keys = {
        "overall_status",
        "started_ts_utc",
        "finished_ts_utc",
        "env",
        "mode",
        "window_start_utc",
        "window_end_utc",
        "failure_policy",
        "max_concurrency",
        "total",
        "succeeded",
        "failed",
        "failed_precondition",
        "results",
        "raw_executor_result",
    }
    _assert(set(res.keys()) == expected_keys, f"ExecutionResult keys mismatch. got={set(res.keys())}")

    _assert(isinstance(res["results"], list), "ExecutionResult.results must be a list")
    for r in res["results"]:
        for attr in [
            "pipeline_name",
            "status",
            "run_id",
            "attempt",
            "intent_hash",
            "started_ts_utc",
            "finished_ts_utc",
            "error_message",
            "error_class",
        ]:
            _assert(hasattr(r, attr), f"PipelineExecutionResult missing attr: {attr}")


def _patch_execute_pipelines():
    from pipeline_runtime.orchestration import execution_entrypoint as ep

    orig = ep.execute_pipelines

    def _fake_execute_pipelines(
        *,
        pipelines,
        env,
        mode,
        window_start_utc,
        window_end_utc,
        params_json,
        trigger_json,
        attempt,
        allow_destructive,
        dry_run,
    ):
        p = pipelines[0]
        status = "SUCCESS"
        if p.startswith("fail_pre"):
            status = "FAILED_PRECONDITION"
        elif p.startswith("fail"):
            status = "FAILED"
        return _make_exec_out(p, status)

    ep.execute_pipelines = _fake_execute_pipelines
    return orig


def _unpatch_execute_pipelines(orig):
    from pipeline_runtime.orchestration import execution_entrypoint as ep
    ep.execute_pipelines = orig


def test_phase6_success_single_pipeline():
    payload = _base_payload(pipelines=["ok_one"], failure_policy="STOP")
    res = run_execution(payload)

    _assert_execution_result_shape(res)

    _assert(res["overall_status"] == "SUCCESS", f"expected SUCCESS, got {res['overall_status']}")
    _assert(res["total"] == 1, f"expected total=1, got {res['total']}")
    _assert(res["succeeded"] == 1, f"expected succeeded=1, got {res['succeeded']}")
    _assert(res["failed"] == 0, f"expected failed=0, got {res['failed']}")
    _assert(res["failed_precondition"] == 0, f"expected failed_precondition=0, got {res['failed_precondition']}")

    _assert(len(res["results"]) == 1, f"expected 1 result, got {len(res['results'])}")
    r0 = res["results"][0]
    _assert(r0.pipeline_name == "ok_one", f"expected pipeline_name ok_one, got {r0.pipeline_name}")
    _assert(r0.status == "SUCCESS", f"expected status SUCCESS, got {r0.status}")
    _assert(r0.error_message is None, f"expected error_message None, got {r0.error_message}")
    _assert(r0.error_class is None, f"expected error_class None, got {r0.error_class}")

    _assert(isinstance(res["raw_executor_result"], dict), "raw_executor_result must be a dict")
    _assert("pipelines" in res["raw_executor_result"], "raw_executor_result must contain pipelines")
    _assert(len(res["raw_executor_result"]["pipelines"]) == 1, "raw_executor_result.pipelines length mismatch")


def test_phase6_stop_policy_stops_on_failed():
    payload = _base_payload(
        pipelines=["ok_before", "fail_mid", "ok_after"],
        failure_policy="STOP",
    )
    res = run_execution(payload)

    _assert_execution_result_shape(res)

    _assert(res["overall_status"] == "FAILED", f"expected FAILED, got {res['overall_status']}")
    _assert(res["total"] == 2, f"expected total=2, got {res['total']}")
    _assert(res["succeeded"] == 1, f"expected succeeded=1, got {res['succeeded']}")
    _assert(res["failed"] == 1, f"expected failed=1, got {res['failed']}")
    _assert(res["failed_precondition"] == 0, f"expected failed_precondition=0, got {res['failed_precondition']}")

    names = [r.pipeline_name for r in res["results"]]
    _assert(names == ["ok_before", "fail_mid"], f"expected ['ok_before','fail_mid'], got {names}")
    _assert(res["results"][1].status == "FAILED", f"expected FAILED, got {res['results'][1].status}")


def test_phase6_continue_policy_runs_all():
    payload = _base_payload(
        pipelines=["ok_before", "fail_mid", "ok_after"],
        failure_policy="CONTINUE",
    )
    res = run_execution(payload)

    _assert_execution_result_shape(res)

    _assert(res["overall_status"] == "FAILED", f"expected FAILED, got {res['overall_status']}")
    _assert(res["total"] == 3, f"expected total=3, got {res['total']}")
    _assert(res["succeeded"] == 2, f"expected succeeded=2, got {res['succeeded']}")
    _assert(res["failed"] == 1, f"expected failed=1, got {res['failed']}")
    _assert(res["failed_precondition"] == 0, f"expected failed_precondition=0, got {res['failed_precondition']}")

    names = [r.pipeline_name for r in res["results"]]
    _assert(names == ["ok_before", "fail_mid", "ok_after"], f"expected all pipelines, got {names}")


def test_phase6_stop_policy_stops_on_failed_precondition():
    payload = _base_payload(
        pipelines=["ok_before", "fail_pre_mid", "ok_after"],
        failure_policy="STOP",
    )
    res = run_execution(payload)

    _assert_execution_result_shape(res)

    _assert(res["overall_status"] == "FAILED", f"expected FAILED, got {res['overall_status']}")
    _assert(res["total"] == 2, f"expected total=2, got {res['total']}")
    _assert(res["succeeded"] == 1, f"expected succeeded=1, got {res['succeeded']}")
    _assert(res["failed"] == 1, f"expected failed=1, got {res['failed']}")
    _assert(res["failed_precondition"] == 1, f"expected failed_precondition=1, got {res['failed_precondition']}")

    names = [r.pipeline_name for r in res["results"]]
    _assert(names == ["ok_before", "fail_pre_mid"], f"expected stop at precondition fail, got {names}")
    _assert(res["results"][1].status == "FAILED_PRECONDITION", f"expected FAILED_PRECONDITION, got {res['results'][1].status}")


def test_phase6_dependency_ordering():
    payload = _base_payload(
        pipelines=["C", "A"],
        depends_on={"C": ["B"], "B": ["A"]},
        failure_policy="STOP",
    )
    res = run_execution(payload)

    _assert_execution_result_shape(res)

    _assert(res["overall_status"] == "SUCCESS", f"expected SUCCESS, got {res['overall_status']}")
    _assert(res["total"] == 3, f"expected total=3, got {res['total']}")
    _assert(res["succeeded"] == 3, f"expected succeeded=3, got {res['succeeded']}")
    _assert(res["failed"] == 0, f"expected failed=0, got {res['failed']}")
    _assert(res["failed_precondition"] == 0, f"expected failed_precondition=0, got {res['failed_precondition']}")

    names = [r.pipeline_name for r in res["results"]]
    _assert(names == ["A", "B", "C"], f"expected dependency order A->B->C, got {names}")


def run_phase6_orchestrator_smoke():
    orig = _patch_execute_pipelines()
    try:
        tests = [
            test_phase6_success_single_pipeline,
            test_phase6_stop_policy_stops_on_failed,
            test_phase6_continue_policy_runs_all,
            test_phase6_stop_policy_stops_on_failed_precondition,
            test_phase6_dependency_ordering,
        ]
        out = []
        for t in tests:
            t()
            out.append(f"PASS  {t.__name__}")
        print("\n".join(out))
        print(f"\nALL PASS ({len(out)} tests)")
    finally:
        _unpatch_execute_pipelines(orig)


run_phase6_orchestrator_smoke()