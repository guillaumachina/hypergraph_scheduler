from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
DUCKDB_DIR = DATA_DIR / "duckdb"
SQL_DIR = PROJECT_ROOT / "sql"
ARTIFACTS_DIR = PROJECT_ROOT / "artifacts"
DOCS_DIR = PROJECT_ROOT / "docs"
RECOMMENDATION_ENGINE_INPUTS_DIR = DOCS_DIR / "recommendation_engine_inputs"
