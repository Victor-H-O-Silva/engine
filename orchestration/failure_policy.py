from __future__ import annotations


def should_stop_execution(policy: str, status: str) -> bool:
    policy = (policy or "STOP").upper()
    if policy == "CONTINUE":
        return False
    return status in ("FAILED", "FAILED_PRECONDITION")