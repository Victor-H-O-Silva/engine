from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from pipeline_runtime.contracts.run_context import RunContext
from pipeline_runtime.execution.pipeline import Pipeline, PipelineOutput
from pipeline_runtime.recovery.recovery_config import RecoveryConfig
from pipeline_runtime.recovery.recovery_mode import parse_recovery_mode
from pipeline_runtime.state.state_store import StateStore


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


class LockLost(Exception):
    pass


@dataclass(frozen=True)
class RunnerResult:
    status: str
    started_ts_utc: str
    finished_ts_utc: str
    metrics: Dict[str, Any]
    error_message: Optional[str] = None
    error_class: Optional[str] = None


class PipelineRunner:
    def __init__(
        self,
        *,
        state_store: Optional[StateStore] = None,
        lock_name: str = "pipeline_run",
        lock_ttl_seconds: int = 6 * 60 * 60,
        watermark_name: str = "window_end_utc",
        commit_watermark_on_success: bool = True,
    ) -> None:
        self._state_store = state_store
        self._lock_name = str(lock_name).strip() or "pipeline_run"
        self._lock_ttl_seconds = int(lock_ttl_seconds)
        self._watermark_name = str(watermark_name).strip() or "window_end_utc"
        self._commit_watermark_on_success = bool(commit_watermark_on_success)
        self._recovery_cfg = RecoveryConfig()

    def _heartbeat_or_raise(self, rc: RunContext, metrics: Dict[str, Any]) -> None:
        if self._state_store is None:
            return

        hb_seconds = self._recovery_cfg.default_lock_heartbeat_seconds
        if isinstance(rc.params, dict):
            v = rc.params.get(self._recovery_cfg.lock_heartbeat_seconds_param_key)
            if v is not None and str(v).strip() != "":
                hb_seconds = int(str(v).strip())

        if hb_seconds <= 0:
            return

        now = _now_utc()
        last = metrics.get("_last_heartbeat_ts_utc")
        if last is not None:
            try:
                last_dt = datetime.fromisoformat(str(last))
                elapsed = (now - last_dt).total_seconds()
                if elapsed < hb_seconds:
                    return
            except Exception:
                pass

        ok = self._state_store.heartbeat_lock(
            pipeline_name=rc.pipeline_name,
            lock_name=self._lock_name,
            owner_run_id=rc.run_id,
            ttl_seconds=self._lock_ttl_seconds,
        )
        metrics["_last_heartbeat_ts_utc"] = now.isoformat()
        if not ok:
            raise LockLost(f"Lock heartbeat failed; lock is no longer owned by run_id={rc.run_id}")

    def _should_resume_if_safe(self, rc: RunContext) -> Optional[str]:
        if self._state_store is None:
            return None
        if not isinstance(rc.params, dict):
            return None

        rm = parse_recovery_mode(rc.params.get(self._recovery_cfg.recovery_mode_param_key))
        if rm.value != "RESUME_IF_SAFE":
            return None

        prior = self._state_store.get_last_success_by_intent(
            pipeline_name=rc.pipeline_name,
            env=rc.env,
            intent_hash=rc.intent_hash,
        )
        if prior is None:
            return None

        if self._recovery_cfg.resume_requires_watermark_commit:
            committed = self._state_store.watermark_committed_for_run(
                pipeline_name=rc.pipeline_name,
                watermark_name=self._watermark_name,
                watermark_value=rc.window_end_utc.isoformat(),
                run_id=prior.run_id,
            )
            if not committed:
                return None

        return prior.run_id

    def run(self, pipeline: Pipeline, rc: RunContext) -> RunnerResult:
        started = _now_utc()

        metrics: Dict[str, Any] = {
            "pipeline_name": rc.pipeline_name,
            "run_id": rc.run_id,
            "attempt": rc.attempt,
            "intent_hash": rc.intent_hash,
            "mode": rc.mode.value,
            "env": rc.env,
            "window_start_utc": rc.window_start_utc.isoformat(),
            "window_end_utc": rc.window_end_utc.isoformat(),
            "dry_run": bool(rc.dry_run),
            "allow_destructive": bool(rc.allow_destructive),
        }

        lock_acquired = False

        try:
            if self._state_store is not None:
                lr = self._state_store.try_acquire_lock(
                    pipeline_name=rc.pipeline_name,
                    lock_name=self._lock_name,
                    owner_run_id=rc.run_id,
                    ttl_seconds=self._lock_ttl_seconds,
                )
                lock_acquired = bool(lr.acquired)
                if not lock_acquired:
                    metrics["lock_acquired"] = False
                    metrics["lock_owner_run_id"] = lr.owner_run_id
                    metrics["lock_expires_at"] = lr.expires_at.isoformat() if lr.expires_at else None

                    finished = _now_utc()
                    return RunnerResult(
                        status="FAILED_PRECONDITION",
                        started_ts_utc=started.isoformat(),
                        finished_ts_utc=finished.isoformat(),
                        metrics=metrics,
                        error_message=f"Lock not acquired. owner_run_id={lr.owner_run_id} expires_at={lr.expires_at}",
                        error_class="LockNotAcquired",
                    )

                actor = None
                if isinstance(rc.trigger, dict):
                    actor = rc.trigger.get("actor") or rc.trigger.get("creator_user_name") or rc.trigger.get("run_as")

                self._state_store.record_run_start(
                    pipeline_name=rc.pipeline_name,
                    run_id=rc.run_id,
                    attempt=rc.attempt,
                    env=rc.env,
                    mode=rc.mode.value,
                    framework_version=rc.framework_version,
                    trigger_ts=rc.run_ts_utc,
                    params=rc.params,
                    actor=actor,
                )

                self._heartbeat_or_raise(rc, metrics)

                prior_run_id = self._should_resume_if_safe(rc)
                if prior_run_id is not None:
                    metrics[self._recovery_cfg.resume_emit_metric_key] = True
                    metrics[self._recovery_cfg.resume_prior_run_id_metric_key] = prior_run_id

                    finished = _now_utc()
                    self._state_store.record_run_end(
                        pipeline_name=rc.pipeline_name,
                        run_id=rc.run_id,
                        status="SUCCESS",
                        metrics=metrics,
                        error=None,
                        ended_at=finished,
                    )
                    return RunnerResult(
                        status="SUCCESS",
                        started_ts_utc=started.isoformat(),
                        finished_ts_utc=finished.isoformat(),
                        metrics=metrics,
                        error_message=None,
                        error_class=None,
                    )

            if hasattr(pipeline, "validate_input"):
                pipeline.validate_input(rc)

            self._heartbeat_or_raise(rc, metrics)
            data = pipeline.extract(rc)

            self._heartbeat_or_raise(rc, metrics)
            transformed = pipeline.transform(rc, data)

            self._heartbeat_or_raise(rc, metrics)
            out: PipelineOutput = pipeline.load(rc, transformed)

            if hasattr(pipeline, "validate_output"):
                pipeline.validate_output(rc, out)

            if out is not None:
                if getattr(out, "row_count", None) is not None:
                    metrics["row_count"] = out.row_count
                if getattr(out, "metadata", None) is not None:
                    metrics["output_metadata"] = out.metadata

            finished = _now_utc()

            if self._state_store is not None:
                if (
                    self._commit_watermark_on_success
                    and (not rc.dry_run)
                    and rc.mode.value != "snapshot"
                ):
                    self._state_store.upsert_watermark(
                        pipeline_name=rc.pipeline_name,
                        watermark_name=self._watermark_name,
                        watermark_value=rc.window_end_utc.isoformat(),
                        watermark_ts=rc.window_end_utc,
                        run_id=rc.run_id,
                    )

                self._state_store.record_run_end(
                    pipeline_name=rc.pipeline_name,
                    run_id=rc.run_id,
                    status="SUCCESS",
                    metrics=metrics,
                    error=None,
                    ended_at=finished,
                )

            return RunnerResult(
                status="SUCCESS",
                started_ts_utc=started.isoformat(),
                finished_ts_utc=finished.isoformat(),
                metrics=metrics,
                error_message=None,
                error_class=None,
            )

        except LockLost as e:
            finished = _now_utc()
            if self._state_store is not None:
                self._state_store.record_run_end(
                    pipeline_name=rc.pipeline_name,
                    run_id=rc.run_id,
                    status="FAILED_PRECONDITION",
                    metrics=metrics,
                    error={"message": str(e), "class": type(e).__name__},
                    ended_at=finished,
                )
            return RunnerResult(
                status="FAILED_PRECONDITION",
                started_ts_utc=started.isoformat(),
                finished_ts_utc=finished.isoformat(),
                metrics=metrics,
                error_message=str(e),
                error_class=type(e).__name__,
            )

        except Exception as e:
            finished = _now_utc()

            if self._state_store is not None:
                self._state_store.record_run_end(
                    pipeline_name=rc.pipeline_name,
                    run_id=rc.run_id,
                    status="FAILED",
                    metrics=metrics,
                    error={"message": str(e), "class": type(e).__name__},
                    ended_at=finished,
                )

            return RunnerResult(
                status="FAILED",
                started_ts_utc=started.isoformat(),
                finished_ts_utc=finished.isoformat(),
                metrics=metrics,
                error_message=str(e),
                error_class=type(e).__name__,
            )

        finally:
            if self._state_store is not None and lock_acquired:
                self._state_store.release_lock(
                    pipeline_name=rc.pipeline_name,
                    lock_name=self._lock_name,
                    owner_run_id=rc.run_id,
                )