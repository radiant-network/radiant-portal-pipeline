from airflow.sdk.definitions.mappedoperator import MappedOperator

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
    assert dag.params["task_ids"] == []
    assert dag.params["dry_run"] is False


def test_discovery_uses_the_qc_query(dag_bag):
    """Airflow resolves the `.sql` template into its text at parse time, so check the query
    itself: the anti-join must be on the QC task type, not the annotation one."""
    sql = dag_bag.get_dag(DAG_ID).get_task("discover_scope").sql
    assert "quality_control_metrics" in sql
    assert "radiant_germline_annotation" not in sql


def test_one_launcher_run_for_every_case(dag_bag):
    """The pipeline reads each sample's DRAGEN metrics from its samplesheet row, so nothing
    splits the cases: one samplesheet, one child run."""
    dag = dag_bag.get_dag(DAG_ID)
    assert not isinstance(dag.get_task("generate_inputs"), MappedOperator)
    run_pipeline = dag.get_task("run_pipeline")
    assert not isinstance(run_pipeline, MappedOperator)
    assert run_pipeline.trigger_dag_id == PIPELINE_DAG_ID
    assert run_pipeline.wait_for_completion is True
    assert run_pipeline.reset_dag_run is True
    # `_defer` is where TriggerDagRunOperator keeps `deferrable`.
    assert run_pipeline.deferrable is True
    assert {t.task_id for t in run_pipeline.upstream_list} == {"generate_inputs"}


def test_the_child_run_gets_only_the_samplesheet_and_the_outdir(dag_bag):
    """No `dragen_metrics_dir`: the launcher no longer has that param."""
    run_pipeline = dag_bag.get_dag(DAG_ID).get_task("run_pipeline")
    assert set(run_pipeline.conf) == {"input", "outdir"}
    assert "generate_inputs" in run_pipeline.trigger_run_id


def test_the_pipeline_runs_between_generating_inputs_and_collecting_outputs(dag_bag):
    dag = dag_bag.get_dag(DAG_ID)
    assert "run_pipeline" in {t.task_id for t in dag.get_task("collect_outputs").upstream_list}
    assert {t.task_id for t in dag.get_task("generate_inputs").upstream_list} == {"resolve_cases"}


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
