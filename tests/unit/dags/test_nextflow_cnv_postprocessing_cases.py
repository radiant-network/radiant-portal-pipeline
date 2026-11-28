from airflow.sdk.definitions.mappedoperator import MappedOperator

DAG_ID = "radiant-nextflow-cnv-postprocessing-cases"
PIPELINE_DAG_ID = "radiant-nextflow-cnv-postprocessing"


def test_dag_is_importable(dag_bag):
    assert dag_bag.get_dag(DAG_ID) is not None
    assert not dag_bag.import_errors


def test_dag_contains_the_expected_stages(dag_bag):
    dag = dag_bag.get_dag(DAG_ID)
    assert set(dag.task_ids) == {
        "fetch_facts",
        "decide",
        "journal",
        "list_processing",
        "set_processing",
        "has_cases",
        "fetch_case_members",
        "select_cases",
        "fetch_phenotypes",
        "resolve_cases",
        "generate_inputs",
        "run_pipeline",
        "collect_outputs",
        "list_tenants",
        "register_tasks",
        "journal_outcomes",
    }
    assert dag.validate() is None


def test_it_runs_the_cases_it_decided(dag_bag):
    """It decides every candidate, journals it, moves the cases to run to `processing`, then runs them."""
    dag = dag_bag.get_dag(DAG_ID)
    # The control DAG starts it; a schedule of its own would race it.
    assert dag.schedule is None
    assert set(dag.params) == {"case_ids", "excluded_tenants", "dry_run"}
    assert dag.params["case_ids"] == []
    assert dag.params["dry_run"] is False
    fetch = dag.get_task("fetch_case_members")
    assert {t.task_id for t in fetch.upstream_list} == {"has_cases", "set_processing"}
    assert fetch.parameters == {"case_ids": "{{ ti.xcom_pull(task_ids='decide')['run_case_ids'] }}"}
    # A failed status move does not hold the data back.
    assert fetch.trigger_rule == "all_done"
    assert "-- One row per" in dag.get_task("fetch_facts").sql
    assert "%(case_ids)s" in fetch.sql
    assert "{{ mapping.starrocks_current_germline_gcnv }}" in fetch.sql
    assert "annotated" not in fetch.sql
    assert set(fetch.parameters) == {"case_ids"}
    assert {t.task_id for t in dag.get_task("select_cases").upstream_list} == {"fetch_case_members"}


def test_the_pipeline_run_is_pinned_to_the_generated_inputs(dag_bag):
    dag = dag_bag.get_dag(DAG_ID)
    run_pipeline = dag.get_task("run_pipeline")
    assert run_pipeline.trigger_dag_id == PIPELINE_DAG_ID
    assert "generate_inputs" in run_pipeline.trigger_run_id
    assert run_pipeline.wait_for_completion is True
    assert run_pipeline.reset_dag_run is True
    assert {t.task_id for t in run_pipeline.upstream_list} == {"generate_inputs"}
    assert "run_pipeline" in {t.task_id for t in dag.get_task("collect_outputs").upstream_list}


def test_phenotypes_are_fetched_for_the_selected_cases_only(dag_bag):
    dag = dag_bag.get_dag(DAG_ID)
    fetch = dag.get_task("fetch_phenotypes")
    assert {t.task_id for t in fetch.upstream_list} == {"select_cases"}
    assert "select_cases" in fetch.parameters["case_ids"]


def test_registration_is_last_and_mapped_over_tenants(dag_bag):
    dag = dag_bag.get_dag(DAG_ID)
    register = dag.get_task("register_tasks")
    assert isinstance(register, MappedOperator)
    assert {t.task_id for t in register.downstream_list} == {"journal_outcomes"}
    assert {t.task_id for t in register.upstream_list} == {"list_tenants", "collect_outputs", "resolve_cases"}
    assert register.partial_kwargs.get("max_active_tis_per_dagrun") == 1


def test_the_outcomes_are_journaled_last_and_do_not_decide_the_run_state(dag_bag):
    """A teardown: a failed pipeline or registration is a `failed` marker, and still fails the run."""
    outcomes = dag_bag.get_dag(DAG_ID).get_task("journal_outcomes")
    assert {t.task_id for t in outcomes.upstream_list} == {"select_cases", "register_tasks"}
    assert outcomes.downstream_list == []
    assert outcomes.is_teardown
    assert not outcomes.on_failure_fail_dagrun
    assert outcomes.retries == 2


def test_only_one_run_at_a_time(dag_bag):
    assert dag_bag.get_dag(DAG_ID).max_active_runs == 1


def test_the_queries_resolve_from_the_sql_search_path(dag_bag):
    dag = dag_bag.get_dag(DAG_ID)
    assert dag.template_searchpath == ["/opt/airflow/dags/radiant/dags/sql"]
    assert dag.render_template_as_native_obj is True
