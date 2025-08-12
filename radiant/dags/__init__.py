import os
import pathlib

from airflow.decorators import task

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


@task(task_id="get_iceberg_namespace", task_display_name="[PyOp] Get Iceberg Namespace")
def get_namespace():
    """Get the Iceberg namespace from conf or use the one defined in environment variable or use default."""
    from airflow.operators.python import get_current_context

    context = get_current_context()
    dag_conf = context["dag_run"].conf or {}
    return dag_conf.get("RADIANT_ICEBERG_NAMESPACE", ICEBERG_NAMESPACE)
