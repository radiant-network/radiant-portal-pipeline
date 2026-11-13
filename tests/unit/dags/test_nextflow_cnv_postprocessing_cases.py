from airflow.sdk.definitions.mappedoperator import MappedOperator

DAG_ID = "radiant-nextflow-cnv-postprocessing-cases"
PIPELINE_DAG_ID = "radiant-nextflow-cnv-postprocessing"


def test_dag_is_importable(dag_bag):
    assert dag_bag.get_dag(DAG_ID) is not None
    assert not dag_bag.import_errors


def test_dag_contains_the_expected_stages(dag_bag):
    dag = dag_bag.get_dag(DAG_ID)
    assert set(dag.task_ids) == {
        "discover_scope",
        "select_cases",
        "fetch_phenotypes",
        "resolve_cases",
        "generate_inputs",
        "run_pipeline",
        "collect_outputs",
        "list_tenants",
        "register_tasks",
    }
    assert dag.validate() is None


def test_it_finds_its_own_work(dag_bag):
    dag = dag_bag.get_dag(DAG_ID)
    # The control DAG starts it; a schedule of its own would race it.
    assert dag.schedule is None
    assert dag.get_task("discover_scope").upstream_list == []
    assert set(dag.params) == {"task_ids", "excluded_tenants", "dry_run"}
    assert dag.params["task_ids"] == []
    assert dag.params["dry_run"] is False


def test_discovery_reads_the_cnv_facts(dag_bag):
    """A Python task over `read_facts`: the facts of the cases the scheduler decided to run."""
    discover = dag_bag.get_dag(DAG_ID).get_task("discover_scope")
    assert discover.python_callable.__name__ == "discover_scope"


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
    assert register.downstream_list == []
    assert {t.task_id for t in register.upstream_list} == {"list_tenants", "collect_outputs", "resolve_cases"}
    assert register.partial_kwargs.get("max_active_tis_per_dagrun") == 1


def test_only_one_run_at_a_time(dag_bag):
    assert dag_bag.get_dag(DAG_ID).max_active_runs == 1


def test_the_queries_resolve_from_the_sql_search_path(dag_bag):
    dag = dag_bag.get_dag(DAG_ID)
    assert dag.template_searchpath == ["/opt/airflow/dags/radiant/dags/sql"]
    assert dag.render_template_as_native_obj is True
