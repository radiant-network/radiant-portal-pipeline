import pytest
from airflow.exceptions import ParamValidationError

DAG_ID = "radiant-import-tenant-gene-panel"


def test_dag_is_importable(dag_bag):
    assert dag_bag.get_dag(DAG_ID) is not None
    assert not dag_bag.import_errors


def test_it_is_manual_and_serial(dag_bag):
    """Each run replaces the tenant's full set of panels: nothing may trigger it on its own, and two
    runs at once would race on which file wins."""
    dag = dag_bag.get_dag(DAG_ID)
    assert dag.schedule_interval is None
    assert dag.max_active_runs == 1
    assert "manual" in dag.tags


def test_one_task_retried_because_the_upload_is_idempotent(dag_bag):
    dag = dag_bag.get_dag(DAG_ID)
    assert dag.task_ids == ["upload_gene_panels"]
    assert dag.get_task("upload_gene_panels").retries == 2


def test_params_are_tenant_file_and_strict(dag_bag):
    dag = dag_bag.get_dag(DAG_ID)
    assert set(dag.params) == {"tenant", "gene_panel_filepath", "strict"}
    assert dag.params["strict"] is False


def test_a_blank_tenant_is_refused(dag_bag):
    param = dag_bag.get_dag(DAG_ID).params.get_param("tenant")
    assert param.resolve("radiant") == "radiant"
    with pytest.raises(ParamValidationError):
        param.resolve("   ")
