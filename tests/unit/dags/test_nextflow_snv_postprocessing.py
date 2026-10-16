import pytest
from airflow.exceptions import ParamValidationError

DAG_ID = "radiant-nextflow-snv-postprocessing"


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


def test_dag_exposes_exactly_input_outdir_and_the_params_set(dag_bag):
    """Guards against param creep: everything run-invariant belongs in a params ConfigMap, not
    here. `params_set` only picks which one."""
    dag = dag_bag.get_dag(DAG_ID)
    assert set(dag.params) == {"input", "outdir", "params_set"}


@pytest.mark.parametrize(
    ("params_set", "configmap"),
    [("germline", "nextflow-snv-params"), ("somatic", "nextflow-snv-somatic-params")],
)
def test_the_params_set_picks_the_configmap(dag_bag, params_set, configmap):
    dag = dag_bag.get_dag(DAG_ID)
    assert dag.params["params_set"] == "germline"
    assert dag.params.get_param("params_set").schema["enum"] == ["germline", "somatic"]
    run = dag.get_task("run_postprocessing")
    template = {v.name: v for v in run.volumes}["nextflow-params"].config_map.name
    assert run.render_template(template, {"params": {"params_set": params_set}}) == configmap


def test_the_somatic_run_shares_the_germline_config_and_script(dag_bag):
    """Same `nextflow-snv-cfg`, script and image: the same VEP image, cache and flags."""
    run = dag_bag.get_dag(DAG_ID).get_task("run_postprocessing")
    volumes = {v.name: v for v in run.volumes}
    assert volumes["nextflow-cfg"].config_map.name == "nextflow-snv-cfg"
    assert "snv-post-processing" in run.arguments[0]


def test_input_is_required_and_outdir_is_not(dag_bag):
    dag = dag_bag.get_dag(DAG_ID)
    # outdir defaults to empty, which the driver reads as "derive from the run tag".
    assert dag.params["outdir"] == ""
    # `input` has no default, so validating the whole set is what surfaces it as missing.
    with pytest.raises(ParamValidationError):
        dag.params.validate()


def test_retries_are_configured_so_resume_is_reachable(dag_bag):
    """RUN_TAG is derived from run_id precisely so a retry re-enters the same
    Nextflow launch dir. DEFAULT_ARGS carries only `owner` and Airflow defaults
    retries to 0, so without an explicit override there would be no retry to
    resume from and the whole mechanism would be dead code."""
    dag = dag_bag.get_dag(DAG_ID)
    assert dag.default_args["retries"] >= 1


def test_a_germline_and_a_somatic_driver_can_run_at_once(dag_bag):
    """Everything a run writes is keyed by RUN_TAG, so two drivers cannot collide. Two, not more:
    the germline cases DAG keeps germline runs one at a time, a somatic run is the other."""
    dag = dag_bag.get_dag(DAG_ID)
    assert dag.max_active_runs == 2
