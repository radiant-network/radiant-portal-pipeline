import pathlib

from airflow.models import Param

NAMESPACE = "radiant"

ICEBERG_COMMON_DAG_PARAMS = {
    "iceberg_catalog": Param(
        default="iceberg_catalog",
        description="The iceberg catalog to use.",
        type="string",
    ),
    "iceberg_database": Param(
        default="iceberg_database",
        description="The iceberg database to use.",
        type="string",
    ),
}

ICEBERG_COMMON_TASK_PARAMS = {
    "iceberg_catalog": "{{ params.iceberg_catalog }}",
    "iceberg_database": "{{ params.iceberg_database }}",
}

DEFAULT_ARGS = {"owner": "radiant"}

DAGS_DIR = pathlib.Path(__file__).parent
SQL_DIR = DAGS_DIR / "sql"

# This is required because docs are read at DAG parse time, not a execution time.
_dags_dir = pathlib.Path(__file__).parent
DOCS_DIR = pathlib.Path(_dags_dir / "docs")


def load_docs_md(file_name: str) -> str:
    """
    Load the markdown file from the docs directory.
    """
    with open(str(DOCS_DIR / file_name)) as f:
        return f.read()
