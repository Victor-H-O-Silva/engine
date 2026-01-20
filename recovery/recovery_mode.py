from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class RecoveryMode:
    value: str

    @staticmethod
    def retry_intent() -> "RecoveryMode":
        return RecoveryMode("RETRY_INTENT")

    @staticmethod
    def replay_window() -> "RecoveryMode":
        return RecoveryMode("REPLAY_WINDOW")

    @staticmethod
    def resume_if_safe() -> "RecoveryMode":
        return RecoveryMode("RESUME_IF_SAFE")

    @staticmethod
    def none() -> "RecoveryMode":
        return RecoveryMode("NONE")


def parse_recovery_mode(v: Optional[str]) -> RecoveryMode:
    if v is None:
        return RecoveryMode.none()
    s = str(v).strip().upper()
    if s == "":
        return RecoveryMode.none()
    if s in {"RETRY_INTENT", "RETRY"}:
        return RecoveryMode.retry_intent()
    if s in {"REPLAY_WINDOW", "REPLAY"}:
        return RecoveryMode.replay_window()
    if s in {"RESUME_IF_SAFE", "RESUME"}:
        return RecoveryMode.resume_if_safe()
    return RecoveryMode(s)