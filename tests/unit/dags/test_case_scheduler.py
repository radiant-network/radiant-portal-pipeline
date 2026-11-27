from airflow.utils.trigger_rule import TriggerRule

DAG_ID = "radiant-case-scheduler"

CASES_DAG_IDS = {
    "trigger_germline_snv_postprocessing": "radiant-nextflow-snv-postprocessing-cases",
    "trigger_germline_cnv_postprocessing": "radiant-nextflow-cnv-postprocessing-cases",
    "trigger_somatic_snv_annotation": "radiant-nextflow-somatic-snv-annotation-cases",
    "trigger_quality_control": "radiant-nextflow-quality-control-cases",
}
EXCLUDED_TENANTS_CONF = {"excluded_tenants": "{{ params.excluded_tenants }}"}


def _upstream(dag, task_id):
    return {t.task_id for t in dag.get_task(task_id).upstream_list}


def test_dag_is_importable(dag_bag):
    assert dag_bag.get_dag(DAG_ID) is not None
    assert not dag_bag.import_errors


def test_it_only_starts_the_pipelines_then_the_import(dag_bag):
    """It decides nothing and moves no status: each pipeline DAG decides and moves its own cases."""
    assert set(dag_bag.get_dag(DAG_ID).task_ids) == {*CASES_DAG_IDS, "trigger_import", "watcher"}


def test_it_is_the_daily_entry_point_one_run_at_a_time(dag_bag):
    dag = dag_bag.get_dag(DAG_ID)
    assert dag.dag_display_name == "Radiant - Case Scheduler"
    assert dag.schedule == "@daily"
    assert dag.max_active_runs == 1
    assert not dag.catchup
    assert dag.params.validate() is not None


def test_the_pipeline_dags_have_no_schedule_of_their_own(dag_bag):
    for dag_id in (*CASES_DAG_IDS.values(), "radiant-import", "radiant-case-evaluation"):
        assert dag_bag.get_dag(dag_id).schedule is None


def test_the_pipeline_dags_are_given_the_run_s_block_list_and_no_items(dag_bag):
    dag = dag_bag.get_dag(DAG_ID)
    for task_id, cases_dag_id in CASES_DAG_IDS.items():
        assert dag.get_task(task_id).trigger_dag_id == cases_dag_id
        assert dag.get_task(task_id).conf == EXCLUDED_TENANTS_CONF
        assert "excluded_tenants" in dag_bag.get_dag(cases_dag_id).params
    assert dag.get_task("trigger_import").conf == EXCLUDED_TENANTS_CONF | {"notify": "{{ params.notify }}"}


def test_the_block_list_defaults_to_the_env_var(dag_bag, monkeypatch):
    """One env var for the scheduler and the DAGs it starts, read when the DAGs are parsed."""
    from radiant.dags import excluded_tenants_from_env

    monkeypatch.setenv("RADIANT_ETL_EXCLUDED_TENANTS", " radiant , qlin,")
    assert excluded_tenants_from_env() == ["radiant", "qlin"]
    monkeypatch.delenv("RADIANT_ETL_EXCLUDED_TENANTS")
    assert excluded_tenants_from_env() == []
    for dag_id in (DAG_ID, *CASES_DAG_IDS.values(), "radiant-import", "radiant-case-evaluation"):
        assert dag_bag.get_dag(dag_id).params["excluded_tenants"] == []


def test_the_pipelines_run_whatever_the_others_did(dag_bag):
    dag = dag_bag.get_dag(DAG_ID)
    for task_id in (*CASES_DAG_IDS, "trigger_import"):
        assert dag.get_task(task_id).trigger_rule == TriggerRule.ALL_DONE


def test_the_import_follows_post_processing_whatever_its_outcome(dag_bag):
    """It loads the annotation tasks SNV post-processing, somatic annotation and CNV post-processing
    (the slivar VCF, RAD-206) register. Quality control is not upstream of it yet."""
    dag = dag_bag.get_dag(DAG_ID)
    assert dag.get_task("trigger_import").trigger_dag_id == "radiant-import"
    assert _upstream(dag, "trigger_import") == {
        "trigger_germline_snv_postprocessing",
        "trigger_germline_cnv_postprocessing",
        "trigger_somatic_snv_annotation",
    }


def test_every_child_run_is_waited_for_and_pinned_to_this_run(dag_bag):
    """A retry resets the same child run, so Nextflow's `-resume` finds its launch dir, and the journal rows of the
    run share one run tag."""
    dag = dag_bag.get_dag(DAG_ID)
    for task_id in (*CASES_DAG_IDS, "trigger_import"):
        trigger = dag.get_task(task_id)
        assert trigger.wait_for_completion
        assert trigger.reset_dag_run
        assert trigger.deferrable
        assert trigger.trigger_run_id == "{{ sanitize_run_tag(run_id) }}"


def test_the_pinned_run_id_is_not_a_reserved_scheduled_id(dag_bag):
    """Airflow 3.2 refuses a triggered run whose id starts with `scheduled__`."""
    sanitize = dag_bag.get_dag(DAG_ID).user_defined_macros["sanitize_run_tag"]
    assert not sanitize("scheduled__2026-10-08T00:00:00+00:00").startswith("scheduled__")


def test_a_failed_step_fails_the_run(dag_bag):
    """The all_done chain would otherwise end green over a failed pipeline."""
    dag = dag_bag.get_dag(DAG_ID)
    watcher = dag.get_task("watcher")
    assert watcher.trigger_rule == TriggerRule.ONE_FAILED
    assert _upstream(dag, "watcher") == {*CASES_DAG_IDS, "trigger_import"}


def test_params(dag_bag):
    assert set(dag_bag.get_dag(DAG_ID).params) == {"excluded_tenants", "notify"}
