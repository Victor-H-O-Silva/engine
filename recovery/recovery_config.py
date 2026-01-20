from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RecoveryConfig:
    recovery_mode_param_key: str = "recovery_mode"
    lock_heartbeat_seconds_param_key: str = "lock_heartbeat_seconds"

    default_lock_heartbeat_seconds: int = 5 * 60

    resume_requires_watermark_commit: bool = True

    resume_emit_metric_key: str = "recovery_skipped"
    resume_prior_run_id_metric_key: str = "recovery_prior_run_id"
