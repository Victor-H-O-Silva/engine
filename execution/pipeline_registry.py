from __future__ import annotations

from typing import Dict, Iterable, Optional

from pipeline_runtime.execution.pipeline import Pipeline


class PipelineRegistry:
    def __init__(self) -> None:
        self._pipelines: Dict[str, Pipeline] = {}

    def register(self, pipeline: Pipeline) -> None:
        name = getattr(pipeline, "name", None)
        if name is None or str(name).strip() == "":
            raise ValueError("pipeline.name must be set and non-empty")
        key = str(name).strip().lower()
        if key in self._pipelines:
            raise ValueError(f"pipeline already registered: {key}")
        self._pipelines[key] = pipeline

    def get(self, name: str) -> Pipeline:
        key = str(name).strip().lower()
        p = self._pipelines.get(key)
        if p is None:
            raise KeyError(f"pipeline not found: {key}")
        return p

    def list_names(self) -> Iterable[str]:
        return sorted(self._pipelines.keys())


_DEFAULT_REGISTRY: Optional[PipelineRegistry] = None


def default_registry() -> PipelineRegistry:
    global _DEFAULT_REGISTRY
    if _DEFAULT_REGISTRY is None:
        _DEFAULT_REGISTRY = PipelineRegistry()
    return _DEFAULT_REGISTRY
