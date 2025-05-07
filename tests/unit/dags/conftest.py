import pytest
from airflow.models import DagBag

from radiant.dags import DAGS_DIR


@pytest.fixture
def dag_bag():
    return DagBag(dag_folder=str(DAGS_DIR), include_examples=False)
