from dataclasses import dataclass
from pathlib import Path

from hypergraph_scheduler.paths import DUCKDB_DIR


@dataclass(frozen=True)
class DuckDBConfig:
    database_path: Path = DUCKDB_DIR / "hypergraph_scheduler.duckdb"


DEFAULT_DUCKDB_CONFIG = DuckDBConfig()
