import pytest
from airflow.models import DagBag

from radiant.dags import DAGS_DIR


@pytest.fixture(scope="session")
def dag_bag():
    """Parsed once per session: parsing every DAG file is the slow part of these tests.

    Tests share the DAG objects, so a test must not mutate them.
    """
    return DagBag(dag_folder=str(DAGS_DIR))
