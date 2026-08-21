import pytest
from airflow.exceptions import ParamValidationError

DAG_ID = "radiant-nextflow-postprocessing-cases"
PIPELINE_DAG_ID = "radiant-nextflow-postprocessing"


def test_dag_is_importable(dag_bag):
    assert dag_bag.get_dag(DAG_ID) is not None
    assert not dag_bag.import_errors


def test_dag_contains_the_six_stages(dag_bag):
    dag = dag_bag.get_dag(DAG_ID)
    assert set(dag.task_ids) == {
        "fetch_members",
        "fetch_phenotypes",
        "resolve_cases",
        "generate_inputs",
        "run_pipeline",
        "collect_outputs",
        "register_tasks",
    }
    assert dag.validate() is None


def test_the_pipeline_runs_between_generating_inputs_and_collecting_outputs(dag_bag):
    """Collecting before the pipeline has run would list an empty prefix; generating after
    it would feed it the previous run's samplesheet."""
    dag = dag_bag.get_dag(DAG_ID)
    run_pipeline = dag.get_task("run_pipeline")
    assert {t.task_id for t in run_pipeline.upstream_list} == {"generate_inputs"}
    assert "run_pipeline" in {t.task_id for t in dag.get_task("collect_outputs").upstream_list}


def test_registration_is_last_and_sees_both_the_cases_and_the_outputs(dag_bag):
    dag = dag_bag.get_dag(DAG_ID)
    register = dag.get_task("register_tasks")
    assert {t.task_id for t in register.upstream_list} == {"resolve_cases", "collect_outputs"}
    assert register.downstream_list == []


def test_dag_asks_for_nothing_it_can_work_out_itself(dag_bag):
    """Guards against param creep. The storage roots are environment and the working paths
    are derived from the run, so a fresh prefix per run is structural; and `cases.id` is a
    single-column primary key over one shared clinical schema, so the tenant is read off
    the cases rather than typed in -- leaving intent, and only intent, as parameters."""
    dag = dag_bag.get_dag(DAG_ID)
    assert set(dag.params) == {"case_ids", "dry_run"}


def test_registration_is_a_dry_run_unless_asked_otherwise(dag_bag):
    """The batch report names every failure with its code and path, which is much better
    triage than an HTTP status -- so the safe pass is the default."""
    dag = dag_bag.get_dag(DAG_ID)
    assert dag.params["dry_run"] is True


def test_case_ids_is_required(dag_bag):
    """It has no default, so a run that omits it is rejected at trigger time rather than
    resolving to an empty set and quietly doing nothing."""
    dag = dag_bag.get_dag(DAG_ID)
    with pytest.raises(ParamValidationError):
        dag.params.validate()


def test_it_triggers_the_pipeline_dag_rather_than_duplicating_it(dag_bag):
    """Driver-pod, resume and cleanup behaviour stay in one place."""
    dag = dag_bag.get_dag(DAG_ID)
    run_pipeline = dag.get_task("run_pipeline")
    assert run_pipeline.trigger_dag_id == PIPELINE_DAG_ID
    assert run_pipeline.wait_for_completion
    # A WGS run takes hours; `_defer` is where TriggerDagRunOperator keeps `deferrable`.
    assert run_pipeline._defer


def test_the_child_run_id_is_pinned_to_this_run(dag_bag):
    """Auto-generated, a retry of this DAG would create a fresh child run with a fresh
    Nextflow launch dir, so `-resume` would find nothing and the retry would be a full
    re-run -- hours on WGS."""
    dag = dag_bag.get_dag(DAG_ID)
    run_pipeline = dag.get_task("run_pipeline")
    assert "generate_inputs" in run_pipeline.trigger_run_id
    assert run_pipeline.reset_dag_run


def test_only_one_run_at_a_time(dag_bag):
    """The pipeline DAG is max_active_runs=1 because concurrent drivers share the FSx
    workspace; queueing here keeps that visible on the parent."""
    dag = dag_bag.get_dag(DAG_ID)
    assert dag.max_active_runs == 1


def test_the_queries_resolve_from_the_sql_search_path(dag_bag):
    """`parameters` is a template field and the DAG renders natively, which is what lets
    the case-id list survive as a list rather than a string."""
    dag = dag_bag.get_dag(DAG_ID)
    assert dag.render_template_as_native_obj
    # DagBag resolves template files at parse time, so `sql` is already the query itself.
    assert "{{ mapping.clinical_case }}" in dag.get_task("fetch_members").sql
    assert "{{ mapping.starrocks_hpo_term }}" in dag.get_task("fetch_phenotypes").sql
    for task_id in ("fetch_members", "fetch_phenotypes"):
        assert set(dag.get_task(task_id).parameters) == {"case_ids"}
