import pytest
from airflow.exceptions import ParamValidationError

DAG_ID = "radiant-nextflow-cnv-postprocessing"


def test_dag_is_importable(dag_bag):
    assert dag_bag.get_dag(DAG_ID) is not None
    assert not dag_bag.import_errors


def test_dag_contains_the_driver_and_cleanup_tasks(dag_bag):
    dag = dag_bag.get_dag(DAG_ID)
    assert set(dag.task_ids) == {"run_cnv_postprocessing", "cleanup_work"}
    assert dag.validate() is None


def test_cleanup_only_runs_after_a_successful_pipeline(dag_bag):
    """The scratch is what `-resume` reads; cleaning up after a failure would make every
    retry a full re-run."""
    dag = dag_bag.get_dag(DAG_ID)
    cleanup = dag.get_task("cleanup_work")
    assert cleanup.trigger_rule == "all_success"
    assert {t.task_id for t in cleanup.upstream_list} == {"run_cnv_postprocessing"}


def test_dag_exposes_exactly_the_two_per_run_values(dag_bag):
    """Everything run-invariant belongs in the nextflow-cnv-params ConfigMap, not here."""
    dag = dag_bag.get_dag(DAG_ID)
    assert set(dag.params) == {"input", "outdir"}


def test_input_is_required_and_outdir_is_not(dag_bag):
    dag = dag_bag.get_dag(DAG_ID)
    assert dag.params["outdir"] == ""
    with pytest.raises(ParamValidationError):
        dag.params.validate()


def test_retries_are_configured_so_resume_is_reachable(dag_bag):
    assert dag_bag.get_dag(DAG_ID).default_args["retries"] >= 1


def test_one_driver_at_a_time(dag_bag):
    assert dag_bag.get_dag(DAG_ID).max_active_runs == 1


def test_run_tag_is_namespaced_away_from_the_other_launchers(dag_bag):
    """RUN_TAG drives the Nextflow workDir, the launch dir and the default outdir, and
    Airflow run ids are only unique *within* a DAG. Three launchers share one filesystem."""
    dag = dag_bag.get_dag(DAG_ID)
    for task_id in ("run_cnv_postprocessing", "cleanup_work"):
        env = {e.name: e.value for e in dag.get_task(task_id).env_vars}
        assert env["RUN_TAG"].startswith("cnv-")
