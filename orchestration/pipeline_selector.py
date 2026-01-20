from __future__ import annotations
from typing import List, Optional, Sequence
from pipeline_runtime.execution.pipeline_registry import PipelineRegistry, default_registry


def select_pipelines(
    *,
    pipelines: Optional[Sequence[str]],
    tags: Optional[Sequence[str]],
    registry: Optional[PipelineRegistry] = None,
) -> List[str]:
    reg = registry or default_registry()

    if pipelines:
        return [p for p in pipelines if p]

    if tags:
        if hasattr(reg, "list_by_tags"):
            return reg.list_by_tags(tags)
        raise ValueError("tags selection requested but registry does not support tags")

    raise ValueError("either pipelines or tags must be provided")