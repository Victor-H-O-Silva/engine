from __future__ import annotations
from typing import Dict, List, Sequence


def resolve_dependencies(
    pipelines: Sequence[str],
    depends_on: Dict[str, List[str]],
) -> List[str]:
    visited, temp, result = set(), set(), []

    def visit(p):
        if p in visited:
            return
        if p in temp:
            raise ValueError("cycle detected in depends_on")
        temp.add(p)
        for d in depends_on.get(p, []):
            visit(d)
        temp.remove(p)
        visited.add(p)
        result.append(p)

    for p in pipelines:
        visit(p)

    return result