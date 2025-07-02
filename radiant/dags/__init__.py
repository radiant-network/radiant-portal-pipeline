import os
import pathlib

NAMESPACE = "radiant"
ICEBERG_NAMESPACE = os.getenv("RADIANT_ICEBERG_NAMESPACE", "radiant")

DEFAULT_ARGS = {"owner": "radiant"}

DAGS_DIR = pathlib.Path(__file__).parent
SQL_DIR = pathlib.Path("sql")

# This is required because docs are read at DAG parse time, not a execution time.
DOCS_DIR = pathlib.Path(DAGS_DIR / "docs")


def load_docs_md(file_name: str) -> str:
    """
    Load the markdown file from the docs directory.
    """
    with open(str(DOCS_DIR / file_name)) as f:
        return f.read()
