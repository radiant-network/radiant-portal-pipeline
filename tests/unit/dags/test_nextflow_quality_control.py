import pytest
from airflow.exceptions import ParamValidationError

DAG_ID = "radiant-nextflow-quality-control"


def test_dag_is_importable(dag_bag):
    assert dag_bag.get_dag(DAG_ID) is not None
    assert not dag_bag.import_errors


def test_dag_contains_the_driver_and_cleanup_tasks(dag_bag):
    dag = dag_bag.get_dag(DAG_ID)
    assert set(dag.task_ids) == {"run_quality_control", "cleanup_work"}
    assert dag.validate() is None


def test_cleanup_only_runs_after_a_successful_pipeline(dag_bag):
    """The scratch is what `-resume` reads. If cleanup ran after a failure, every
    Airflow retry would become a full re-run."""
    dag = dag_bag.get_dag(DAG_ID)
    cleanup = dag.get_task("cleanup_work")
    assert cleanup.trigger_rule == "all_success"
    assert {t.task_id for t in cleanup.upstream_list} == {"run_quality_control"}


def test_dag_exposes_exactly_the_three_per_run_values(dag_bag):
    """Guards against param creep: everything run-invariant belongs in the
    nextflow-qc-params ConfigMap, not here."""
    dag = dag_bag.get_dag(DAG_ID)
    assert set(dag.params) == {"input", "dragen_metrics_dir", "outdir"}


def test_input_and_dragen_metrics_dir_are_required_and_outdir_is_not(dag_bag):
    """dragen_metrics_dir is optional upstream but required here: it is what selects
    DRAGEN-metrics mode, and omitting it would silently launch a full BAM_QC/VCF_QC
    recompute instead of failing."""
    dag = dag_bag.get_dag(DAG_ID)
    # outdir defaults to empty, which the driver reads as "derive from the run tag".
    assert dag.params["outdir"] == ""
    with pytest.raises(ParamValidationError):
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


def test_run_tag_is_namespaced_away_from_the_postprocessing_dag(dag_bag):
    """RUN_TAG drives the Nextflow workDir, the launch dir and the default outdir,
    but Airflow run ids are only unique *within* a DAG -- and this DAG can run
    concurrently with radiant-nextflow-postprocessing. Sharing a launch dir is how a
    resume cache gets corrupted, and either cleanup_work would delete the other's
    scratch."""
    dag = dag_bag.get_dag(DAG_ID)
    for task_id in ("run_quality_control", "cleanup_work"):
        env = {e.name: e.value for e in dag.get_task(task_id).env_vars}
        assert env["RUN_TAG"].startswith("qc-")
