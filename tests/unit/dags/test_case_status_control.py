from airflow.models.mappedoperator import MappedOperator
from airflow.utils.trigger_rule import TriggerRule

DAG_ID = "radiant-case-status-control"

CASES_DAG_IDS = {
    "trigger_snv_postprocessing": "radiant-nextflow-snv-postprocessing-cases",
    "trigger_cnv_postprocessing": "radiant-nextflow-cnv-postprocessing-cases",
    "trigger_quality_control": "radiant-nextflow-quality-control-cases",
}
DISCOVERY = {
    "discover_snv_cases": "radiant-nextflow-snv-postprocessing-cases",
    "discover_cnv_cases": "radiant-nextflow-cnv-postprocessing-cases",
    "discover_qc_cases": "radiant-nextflow-quality-control-cases",
}


def _upstream(dag, task_id):
    return {t.task_id for t in dag.get_task(task_id).upstream_list}


def test_dag_is_importable(dag_bag):
    assert dag_bag.get_dag(DAG_ID) is not None
    assert not dag_bag.import_errors


def test_dag_contains_the_expected_stages(dag_bag):
    dag = dag_bag.get_dag(DAG_ID)
    assert set(dag.task_ids) == {
        "discover_snv_cases",
        "discover_cnv_cases",
        "discover_qc_cases",
        "discover_import_cases",
        "fetch_case_statuses",
        "discover_cases",
        "set_processing",
        "trigger_snv_postprocessing",
        "trigger_cnv_postprocessing",
        "trigger_quality_control",
        "trigger_import",
        "rediscover_snv_cases",
        "rediscover_cnv_cases",
        "rediscover_qc_cases",
        "rediscover_import_cases",
        "refresh_case_statuses",
        "select_ready_cases",
        "evaluate_cases",
        "watcher",
    }
    assert dag.validate() is None


def test_it_is_the_daily_entry_point_one_run_at_a_time(dag_bag):
    dag = dag_bag.get_dag(DAG_ID)
    assert dag.dag_display_name == "Radiant - Case Status Control"
    assert dag.schedule_interval == "@daily"
    assert dag.max_active_runs == 1
    assert not dag.catchup
    assert dag.params.validate() is not None


def test_the_cases_dags_have_no_schedule_of_their_own(dag_bag):
    """Scheduled on their own they would process cases without moving their status, and race
    this DAG. They stay manually runnable for a targeted rerun."""
    for dag_id in CASES_DAG_IDS.values():
        assert dag_bag.get_dag(dag_id).schedule_interval is None


def test_discovery_uses_each_cases_dag_query_and_allow_list(dag_bag):
    """Only count what the DAG would actually run: same query, same tenant allow-list."""
    dag = dag_bag.get_dag(DAG_ID)
    for task_id, cases_dag_id in DISCOVERY.items():
        discover = dag.get_task(task_id)
        cases_dag = dag_bag.get_dag(cases_dag_id)
        assert discover.sql == cases_dag.get_task("discover_scope").sql
        assert discover.params["tenants"] == cases_dag.params["tenants"]
        assert discover.params["task_ids"] == []
    assert "staging_sequencing_experiment_delta" in dag.get_task("discover_import_cases").sql


def test_discover_cases_reads_every_discovery_and_the_statuses(dag_bag):
    dag = dag_bag.get_dag(DAG_ID)
    assert _upstream(dag, "discover_cases") == {*DISCOVERY, "discover_import_cases", "fetch_case_statuses"}
    assert "{{ mapping.clinical_case }}" in dag.get_task("fetch_case_statuses").sql


def test_status_changes_are_mapped_once_per_tenant(dag_bag):
    dag = dag_bag.get_dag(DAG_ID)
    for task_id in ("set_processing", "evaluate_cases"):
        assert isinstance(dag.get_task(task_id), MappedOperator)
    assert _upstream(dag, "set_processing") == {"discover_cases"}
    assert _upstream(dag, "evaluate_cases") == {"select_ready_cases"}


def test_the_pipelines_run_in_parallel_even_if_setting_statuses_failed(dag_bag):
    """A portal outage, or a day with no submitted case, must not hold the data back."""
    dag = dag_bag.get_dag(DAG_ID)
    for task_id, cases_dag_id in CASES_DAG_IDS.items():
        trigger = dag.get_task(task_id)
        assert trigger.trigger_dag_id == cases_dag_id
        assert trigger.trigger_rule == TriggerRule.ALL_DONE
        assert _upstream(dag, task_id) == {"set_processing"}


def test_the_import_follows_post_processing_whatever_its_outcome(dag_bag):
    dag = dag_bag.get_dag(DAG_ID)
    trigger = dag.get_task("trigger_import")
    assert trigger.trigger_dag_id == "radiant-import"
    assert trigger.trigger_rule == TriggerRule.ALL_DONE
    assert _upstream(dag, "trigger_import") == {"trigger_snv_postprocessing"}


def test_evaluation_waits_for_every_pipeline_and_runs_whatever_their_outcome(dag_bag):
    dag = dag_bag.get_dag(DAG_ID)
    second_pass = ["refresh_case_statuses", *(f"re{task_id}" for task_id in DISCOVERY), "rediscover_import_cases"]
    for task_id in second_pass:
        assert dag.get_task(task_id).trigger_rule == TriggerRule.ALL_DONE
        assert _upstream(dag, task_id) == {"trigger_import", "trigger_cnv_postprocessing", "trigger_quality_control"}
    # The statuses are read again: the pipelines and the import have run since the first read.
    assert dag.get_task("refresh_case_statuses").sql == dag.get_task("fetch_case_statuses").sql
    assert _upstream(dag, "select_ready_cases") == set(second_pass)


def test_evaluation_asks_the_same_work_waiting_question_as_discovery(dag_bag):
    """A case still pending anywhere after the pipelines stays processing; the two passes must
    not drift, or a case would be moved on with a pipeline left to run, or never at all."""
    dag = dag_bag.get_dag(DAG_ID)
    for name in ("snv", "cnv", "qc", "import"):
        first, second = dag.get_task(f"discover_{name}_cases"), dag.get_task(f"rediscover_{name}_cases")
        assert second.sql == first.sql
        assert second.params == first.params
        assert second.parameters == first.parameters


def test_every_child_run_is_waited_for_and_pinned_to_this_run(dag_bag):
    """A retry resets the same child run, so Nextflow's `-resume` finds its launch dir."""
    dag = dag_bag.get_dag(DAG_ID)
    for task_id in (*CASES_DAG_IDS, "trigger_import"):
        trigger = dag.get_task(task_id)
        assert trigger.wait_for_completion
        assert trigger.reset_dag_run
        assert trigger._defer
        assert trigger.trigger_run_id == "{{ sanitize_run_tag(run_id) }}"


def test_the_pinned_run_id_is_not_a_reserved_scheduled_id(dag_bag):
    """Airflow 3.2 refuses a triggered run whose id starts with `scheduled__`."""
    dag = dag_bag.get_dag(DAG_ID)
    sanitize = dag.user_defined_macros["sanitize_run_tag"]
    assert not sanitize("scheduled__2026-10-08T00:00:00+00:00").startswith("scheduled__")


def test_a_failed_step_fails_the_run(dag_bag):
    """The all_done chain would otherwise end green over a failed pipeline."""
    dag = dag_bag.get_dag(DAG_ID)
    watcher = dag.get_task("watcher")
    assert watcher.trigger_rule == TriggerRule.ONE_FAILED
    assert watcher.downstream_list == []
    assert _upstream(dag, "watcher") == set(dag.task_ids) - {"watcher", "discover_cases", "select_ready_cases"}


def test_the_status_allow_list_does_not_shadow_the_discovery_one(dag_bag):
    """A run conf key `tenants` would override every discovery task's own allow-list."""
    assert set(dag_bag.get_dag(DAG_ID).params) == {"status_tenants"}
