from airflow.models.mappedoperator import MappedOperator
from airflow.utils.task_group import MappedTaskGroup
from airflow.utils.trigger_rule import TriggerRule

DAG_ID = "radiant-case-scheduler"

CASES_DAG_IDS = {
    "trigger_germline_snv_postprocessing": "radiant-nextflow-snv-postprocessing-cases",
    "trigger_germline_cnv_postprocessing": "radiant-nextflow-cnv-postprocessing-cases",
    "trigger_somatic_snv_annotation": "radiant-nextflow-somatic-snv-annotation-cases",
    "trigger_quality_control": "radiant-nextflow-quality-control-cases",
}
DISCOVERY = {
    "discover_germline_snv_cases": "radiant-nextflow-snv-postprocessing-cases",
    "discover_germline_cnv_cases": "radiant-nextflow-cnv-postprocessing-cases",
    "discover_somatic_snv_cases": "radiant-nextflow-somatic-snv-annotation-cases",
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
        "discover_germline_snv_cases",
        "discover_germline_cnv_cases",
        "discover_somatic_snv_cases",
        "discover_qc_cases",
        "discover_import_cases",
        "fetch_case_statuses",
        "discover_cases",
        "set_processing",
        "trigger_germline_snv_postprocessing",
        "trigger_germline_cnv_postprocessing",
        "trigger_somatic_snv_annotation",
        "trigger_quality_control",
        "trigger_import",
        "rediscover_germline_snv_cases",
        "rediscover_germline_cnv_cases",
        "rediscover_somatic_snv_cases",
        "rediscover_qc_cases",
        "rediscover_import_cases",
        "refresh_case_statuses",
        "select_ready_cases",
        "evaluate_cases.set_in_progress",
        "evaluate_cases.create_case_group",
        "evaluate_cases.notify_labs",
        "watcher",
    }
    assert dag.validate() is None


def test_it_is_the_daily_entry_point_one_run_at_a_time(dag_bag):
    dag = dag_bag.get_dag(DAG_ID)
    assert dag.dag_display_name == "Radiant - Case Scheduler"
    assert dag.schedule_interval == "@daily"
    assert dag.max_active_runs == 1
    assert not dag.catchup
    assert dag.params.validate() is not None


def test_the_cases_dags_have_no_schedule_of_their_own(dag_bag):
    """Scheduled on their own they would process cases without moving their status, and race
    this DAG. They stay manually runnable for a targeted rerun."""
    for dag_id in CASES_DAG_IDS.values():
        assert dag_bag.get_dag(dag_id).schedule_interval is None


def test_discovery_uses_each_cases_dag_query_and_the_run_s_block_list(dag_bag):
    """Only count what the DAG would actually run: same query, same excluded tenants."""
    dag = dag_bag.get_dag(DAG_ID)
    for task_id, cases_dag_id in DISCOVERY.items():
        discover = dag.get_task(task_id)
        assert discover.sql == dag_bag.get_dag(cases_dag_id).get_task("discover_scope").sql
        assert discover.parameters == {"excluded_tenants": "{{ params.excluded_tenants }}"}
        # Inherited from the DAG, not set per task: a run conf `excluded_tenants` must reach it.
        assert discover.params["excluded_tenants"] == dag.params["excluded_tenants"]
        assert discover.params["task_ids"] == []
    import_sql = dag.get_task("discover_import_cases").sql
    assert "staging_sequencing_experiment_delta" in import_sql
    # A row leaves the delta once inserted: the ones `import_part` has not loaded are still pending.
    assert "se.updated_at >= COALESCE(se.ingested_at, '1970-01-01 00:00:00') OR se.deleted" in import_sql


def test_the_cases_dags_are_given_the_run_s_block_list(dag_bag):
    """So each leaves out exactly the tenants discovery left out: a case discovery counted but the
    DAG excluded would sit in `processing` with nothing running for it."""
    dag = dag_bag.get_dag(DAG_ID)
    for task_id, cases_dag_id in CASES_DAG_IDS.items():
        assert dag.get_task(task_id).conf == {"excluded_tenants": "{{ params.excluded_tenants }}"}
        assert "excluded_tenants" in dag_bag.get_dag(cases_dag_id).params
    assert not dag.get_task("trigger_import").conf


def test_the_block_list_defaults_to_the_env_var(dag_bag, monkeypatch):
    """One env var for the scheduler and the three cases DAGs, read when the DAGs are parsed."""
    from radiant.dags import excluded_tenants_from_env

    monkeypatch.setenv("RADIANT_ETL_EXCLUDED_TENANTS", " radiant , qlin,")
    assert excluded_tenants_from_env() == ["radiant", "qlin"]
    monkeypatch.delenv("RADIANT_ETL_EXCLUDED_TENANTS")
    assert excluded_tenants_from_env() == []
    for dag_id in (DAG_ID, *CASES_DAG_IDS.values()):
        assert dag_bag.get_dag(dag_id).params["excluded_tenants"] == []


def test_discover_cases_reads_every_discovery_and_the_statuses(dag_bag):
    dag = dag_bag.get_dag(DAG_ID)
    assert _upstream(dag, "discover_cases") == {*DISCOVERY, "discover_import_cases", "fetch_case_statuses"}
    assert "{{ mapping.clinical_case }}" in dag.get_task("fetch_case_statuses").sql


def test_status_changes_are_mapped_once_per_tenant(dag_bag):
    dag = dag_bag.get_dag(DAG_ID)
    assert isinstance(dag.get_task("set_processing"), MappedOperator)
    assert _upstream(dag, "set_processing") == {"discover_cases"}
    assert isinstance(dag.task_group_dict["evaluate_cases"], MappedTaskGroup)
    assert _upstream(dag, "evaluate_cases.set_in_progress") == {"select_ready_cases"}


def test_the_labs_are_emailed_per_tenant_once_its_cases_reach_in_progress(dag_bag):
    """After the status change, so the manifest holds every pipeline's results. One chain per
    tenant in a mapped group: one tenant skipped (403) or failed must not hold back the others."""
    dag = dag_bag.get_dag(DAG_ID)
    assert _upstream(dag, "evaluate_cases.create_case_group") == {
        "evaluate_cases.set_in_progress",
        "select_ready_cases",
    }
    assert _upstream(dag, "evaluate_cases.notify_labs") == {"evaluate_cases.create_case_group", "select_ready_cases"}
    assert dag.params["notify"] is True


def test_the_pipelines_run_in_parallel_even_if_setting_statuses_failed(dag_bag):
    """A portal outage, or a day with no submitted case, must not hold the data back."""
    dag = dag_bag.get_dag(DAG_ID)
    for task_id, cases_dag_id in CASES_DAG_IDS.items():
        trigger = dag.get_task(task_id)
        assert trigger.trigger_dag_id == cases_dag_id
        assert trigger.trigger_rule == TriggerRule.ALL_DONE
        assert _upstream(dag, task_id) == {"set_processing"}


def test_the_import_follows_post_processing_whatever_its_outcome(dag_bag):
    """It loads the annotation tasks SNV post-processing and somatic annotation register. CNV
    post-processing stays outside: the import does not read its outputs."""
    dag = dag_bag.get_dag(DAG_ID)
    trigger = dag.get_task("trigger_import")
    assert trigger.trigger_dag_id == "radiant-import"
    assert trigger.trigger_rule == TriggerRule.ALL_DONE
    assert _upstream(dag, "trigger_import") == {
        "trigger_germline_snv_postprocessing",
        "trigger_somatic_snv_annotation",
    }


def test_evaluation_waits_for_every_pipeline_and_runs_whatever_their_outcome(dag_bag):
    dag = dag_bag.get_dag(DAG_ID)
    second_pass = ["refresh_case_statuses", *(f"re{task_id}" for task_id in DISCOVERY), "rediscover_import_cases"]
    for task_id in second_pass:
        assert dag.get_task(task_id).trigger_rule == TriggerRule.ALL_DONE
        assert _upstream(dag, task_id) == {
            "trigger_import",
            "trigger_germline_cnv_postprocessing",
            "trigger_quality_control",
        }
    # The statuses are read again: the pipelines and the import have run since the first read.
    assert dag.get_task("refresh_case_statuses").sql == dag.get_task("fetch_case_statuses").sql
    assert _upstream(dag, "select_ready_cases") == set(second_pass)


def test_evaluation_asks_the_same_work_waiting_question_as_discovery(dag_bag):
    """A case still pending anywhere after the pipelines stays processing; the two passes must
    not drift, or a case would be moved on with a pipeline left to run, or never at all."""
    dag = dag_bag.get_dag(DAG_ID)
    for name in ("germline_snv", "germline_cnv", "somatic_snv", "qc", "import"):
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
    # The tenant chains reach it through their last task: a failure earlier in a chain leaves
    # `notify_labs` upstream_failed, which `one_failed` counts.
    assert _upstream(dag, "watcher") == set(dag.task_ids) - {
        "watcher",
        "discover_cases",
        "select_ready_cases",
        "evaluate_cases.set_in_progress",
        "evaluate_cases.create_case_group",
    }


def test_params(dag_bag):
    assert set(dag_bag.get_dag(DAG_ID).params) == {"excluded_tenants", "notify"}


def _run_select_ready_cases(dag, monkeypatch, cases_with_work):
    """`select_ready_cases` with no work left anywhere, reading `cases_with_work` from discovery."""
    from radiant.dags import case_scheduler

    pulls = []

    class TaskInstance:
        def xcom_pull(self, task_ids, key):
            pulls.append((task_ids, key))
            return cases_with_work

    context = {"ti": TaskInstance(), "params": {"excluded_tenants": []}}
    monkeypatch.setattr("airflow.operators.python.get_current_context", lambda: context)
    monkeypatch.setattr(case_scheduler, "_cases_with_work", lambda *_: set())
    status_rows = [
        {"case_id": 1, "tenant_code": "radiant", "status_code": "processing", "has_variants": True},
        {"case_id": 2, "tenant_code": "radiant", "status_code": "in_progress", "has_variants": True},
    ]
    batches = dag.get_task("select_ready_cases").python_callable([], [], [], [], status_rows)
    assert pulls == [("discover_cases", case_scheduler.CASES_WITH_WORK_KEY)]
    return batches


def test_the_cases_this_run_processed_are_notified_whatever_their_status(dag_bag, monkeypatch):
    """Not only the cases moved to in_progress: one already past it gets its new results too."""
    batches = _run_select_ready_cases(dag_bag.get_dag(DAG_ID), monkeypatch, cases_with_work=[2])
    assert batches == [{"tenant": "radiant", "case_ids": [1], "processed_case_ids": [2]}]


def test_a_failed_discovery_still_moves_the_cases_to_in_progress(dag_bag, monkeypatch):
    """Pulled, not passed: discovery failing costs the extra emails only, not the status changes."""
    batches = _run_select_ready_cases(dag_bag.get_dag(DAG_ID), monkeypatch, cases_with_work=None)
    assert batches == [{"tenant": "radiant", "case_ids": [1], "processed_case_ids": []}]
