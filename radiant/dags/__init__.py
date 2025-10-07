import os
import pathlib
from dataclasses import dataclass

from airflow.decorators import task
from airflow.models import Variable

NAMESPACE = "radiant"
ICEBERG_NAMESPACE = os.getenv("RADIANT_ICEBERG_NAMESPACE", "radiant")

DEFAULT_ARGS = {"owner": "radiant"}

DAGS_DIR = pathlib.Path(__file__).parent
SQL_DIR = pathlib.Path("sql")

# This is required because docs are read at DAG parse time, not a execution time.
DOCS_DIR = pathlib.Path(DAGS_DIR / "docs")

IS_AWS = os.environ.get("IS_AWS", "false").lower() == "true"


def parse_list(env_val):
    return [v.strip() for v in env_val.split(",") if v.strip()]


@dataclass(frozen=True)
class ECSEnv:
    ECS_CLUSTER: str | None = None
    ECS_SUBNETS: list[str] | None = None
    ECS_SECURITY_GROUPS: list[str] | None = None

    def __post_init__(self):
        object.__setattr__(self, "ECS_CLUSTER", Variable.get("AWS_ECS_CLUSTER"))
        object.__setattr__(self, "ECS_SUBNETS", parse_list(Variable.get("AWS_ECS_SUBNETS")))
        object.__setattr__(self, "ECS_SECURITY_GROUPS", parse_list(Variable.get("AWS_ECS_SECURITY_GROUPS")))


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
