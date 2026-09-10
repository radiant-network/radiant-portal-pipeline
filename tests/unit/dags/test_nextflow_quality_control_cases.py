from airflow.models.mappedoperator import MappedOperator

DAG_ID = "radiant-nextflow-quality-control-cases"
PIPELINE_DAG_ID = "radiant-nextflow-quality-control"


def test_dag_is_importable(dag_bag):
    assert dag_bag.get_dag(DAG_ID) is not None
    assert not dag_bag.import_errors


def test_dag_contains_the_expected_stages(dag_bag):
    dag = dag_bag.get_dag(DAG_ID)
    assert set(dag.task_ids) == {
        "discover_scope",
        "select_cases",
        "locate_metrics",
        "group_cases",
        "generate_inputs",
        "run_pipeline",
        "collect_outputs",
        "list_tenants",
        "register_tasks",
    }
    assert dag.validate() is None


def test_it_finds_its_own_work(dag_bag):
    dag = dag_bag.get_dag(DAG_ID)
    assert dag.schedule_interval == "@daily"
    assert dag.get_task("discover_scope").upstream_list == []
    assert dag.params["task_ids"] == []
    assert dag.params["dry_run"] is False


def test_discovery_uses_the_qc_query(dag_bag):
    """Airflow resolves the `.sql` template into its text at parse time, so check the query
    itself: the anti-join must be on the QC task type, not the annotation one."""
    sql = dag_bag.get_dag(DAG_ID).get_task("discover_scope").sql
    assert "quality_control_metrics" in sql
    assert "radiant_germline_annotation" not in sql


def test_one_launcher_run_per_metrics_directory(dag_bag):
    """`--dragen_metrics_dir` is one directory per Nextflow run, so both the samplesheet and
    the trigger are mapped over the groups `locate_metrics` produced."""
    dag = dag_bag.get_dag(DAG_ID)
    assert isinstance(dag.get_task("generate_inputs"), MappedOperator)
    run_pipeline = dag.get_task("run_pipeline")
    assert isinstance(run_pipeline, MappedOperator)
    assert run_pipeline.partial_kwargs["trigger_dag_id"] == PIPELINE_DAG_ID
    assert run_pipeline.partial_kwargs["wait_for_completion"] is True
    assert run_pipeline.partial_kwargs["reset_dag_run"] is True
    assert run_pipeline.partial_kwargs["deferrable"] is True
    assert {t.task_id for t in run_pipeline.upstream_list} == {"generate_inputs"}


def test_the_pipeline_runs_between_generating_inputs_and_collecting_outputs(dag_bag):
    dag = dag_bag.get_dag(DAG_ID)
    assert "run_pipeline" in {t.task_id for t in dag.get_task("collect_outputs").upstream_list}
    assert {t.task_id for t in dag.get_task("generate_inputs").upstream_list} == {"locate_metrics", "group_cases"}


def test_registration_is_last_and_mapped_over_tenants(dag_bag):
    dag = dag_bag.get_dag(DAG_ID)
    register = dag.get_task("register_tasks")
    assert isinstance(register, MappedOperator)
    assert register.downstream_list == []
    assert {t.task_id for t in register.upstream_list} == {"list_tenants", "collect_outputs", "locate_metrics"}
    assert register.partial_kwargs.get("max_active_tis_per_dagrun") == 1


def test_only_one_run_at_a_time(dag_bag):
    assert dag_bag.get_dag(DAG_ID).max_active_runs == 1


def test_the_queries_resolve_from_the_sql_search_path(dag_bag):
    dag = dag_bag.get_dag(DAG_ID)
    assert dag.template_searchpath == ["/opt/airflow/dags/radiant/dags/sql"]
    assert dag.render_template_as_native_obj is True
