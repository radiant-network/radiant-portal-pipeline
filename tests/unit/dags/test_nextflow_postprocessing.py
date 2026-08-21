import pytest

DAG_ID = "radiant-nextflow-postprocessing"


def test_dag_is_importable(dag_bag):
    assert dag_bag.get_dag(DAG_ID) is not None
    assert not dag_bag.import_errors


def test_dag_contains_the_driver_and_cleanup_tasks(dag_bag):
    dag = dag_bag.get_dag(DAG_ID)
    assert set(dag.task_ids) == {"run_postprocessing", "cleanup_work"}
    assert dag.validate() is None


def test_cleanup_only_runs_after_a_successful_pipeline(dag_bag):
    """The scratch is what `-resume` reads. If cleanup ran after a failure, every
    Airflow retry would become a full re-run instead of resuming."""
    dag = dag_bag.get_dag(DAG_ID)
    cleanup = dag.get_task("cleanup_work")
    assert cleanup.trigger_rule == "all_success"
    assert {t.task_id for t in cleanup.upstream_list} == {"run_postprocessing"}


def test_dag_exposes_exactly_input_and_outdir(dag_bag):
    """Guards against param creep: everything run-invariant belongs in the
    nextflow-params ConfigMap, not here."""
    dag = dag_bag.get_dag(DAG_ID)
    assert set(dag.params) == {"input", "outdir"}


def test_input_is_required_and_outdir_is_not(dag_bag):
    dag = dag_bag.get_dag(DAG_ID)
    # outdir defaults to empty, which the driver reads as "derive from the run tag".
    assert dag.params["outdir"] == ""
    with pytest.raises(Exception):
        dag.params.validate()


def test_retries_are_configured_so_resume_is_reachable(dag_bag):
    """RUN_TAG is derived from run_id precisely so a retry re-enters the same
    Nextflow launch dir. DEFAULT_ARGS carries only `owner` and Airflow defaults
    retries to 0, so without an explicit override there would be no retry to
    resume from and the whole mechanism would be dead code."""
    dag = dag_bag.get_dag(DAG_ID)
    assert dag.default_args["retries"] >= 1


def test_only_one_driver_runs_at_a_time(dag_bag):
    """Concurrent runs would share the same FSx workspace."""
    dag = dag_bag.get_dag(DAG_ID)
    assert dag.max_active_runs == 1
