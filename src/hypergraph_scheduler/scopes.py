from __future__ import annotations

import json
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

from hypergraph_scheduler.paths import DOCS_DIR


@dataclass(frozen=True)
class ScopeDefinition:
    scope_id: str
    display_name: str
    input_dir: Path
    graph_path: Path
    model_path: Path
    artifact_prefix: str
    seed_edge_sensor_map: list[tuple[str, str, str]]

    def raw_table_name(self, suffix: str) -> str:
        return f"raw_{self.scope_id}_{suffix}"

    def view_name(self, suffix: str) -> str:
        return f"{self.scope_id}_{suffix}"


def _find_single_input_file(input_dir: Path, suffix: str) -> Path:
    matches = sorted(input_dir.glob(f"*{suffix}"))
    if len(matches) != 1:
        raise FileNotFoundError(f"Expected exactly one '*{suffix}' file in {input_dir}, found {len(matches)}")
    return matches[0]


def _load_scope_definition(metadata_path: Path) -> ScopeDefinition:
    payload = json.loads(metadata_path.read_text(encoding="utf-8"))
    input_dir = metadata_path.parent

    return ScopeDefinition(
        scope_id=payload["scope_id"],
        display_name=payload["display_name"],
        input_dir=input_dir,
        graph_path=_find_single_input_file(input_dir, "_dag_dependencies.json"),
        model_path=_find_single_input_file(input_dir, "_schedule_optimization_model.json"),
        artifact_prefix=payload.get("artifact_prefix", payload["scope_id"]),
        seed_edge_sensor_map=[
            (entry["from_dag_id"], entry["to_dag_id"], entry["sensor_task_id"])
            for entry in payload.get("seed_edge_sensor_map", [])
        ],
    )


@lru_cache(maxsize=1)
def discover_scopes() -> tuple[ScopeDefinition, ...]:
    metadata_paths = sorted(DOCS_DIR.glob("*_inputs/scope.json"))
    return tuple(_load_scope_definition(path) for path in metadata_paths)


def get_scope(scope_id: str) -> ScopeDefinition:
    for scope in discover_scopes():
        if scope.scope_id == scope_id:
            return scope
    raise KeyError(f"Unknown scope: {scope_id}")