from airflow.models.mappedoperator import MappedOperator

DAG_ID = "radiant-nextflow-postprocessing-cases"
PIPELINE_DAG_ID = "radiant-nextflow-postprocessing"


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
        "list_tenants",
        "generate_inputs",
        "run_pipeline",
        "collect_outputs",
        "register_tasks",
    }
    assert dag.validate() is None


def test_it_finds_its_own_work(dag_bag):
    """The point of the whole thing: a scheduled run is given nothing and discovers the
    cases that have been aligned but never annotated."""
    dag = dag_bag.get_dag(DAG_ID)
    assert dag.schedule_interval == "@daily"
    assert dag.get_task("discover_scope").upstream_list == []
    # No default would make a scheduled run fail validation at trigger time.
    assert dag.params["task_ids"] == []


def test_phenotypes_are_fetched_for_the_selected_cases_only(dag_bag):
    """They used to run in parallel off the same param. Discovery now decides the case ids,
    so this one waits -- and asks for exactly the cases that survived selection."""
    dag = dag_bag.get_dag(DAG_ID)
    fetch = dag.get_task("fetch_phenotypes")
    assert {t.task_id for t in fetch.upstream_list} == {"select_cases"}
    assert "select_cases" in fetch.parameters["case_ids"]


def test_the_pipeline_runs_between_generating_inputs_and_collecting_outputs(dag_bag):
    """Collecting before the pipeline has run would list an empty prefix; generating after
    it would feed it the previous run's samplesheet."""
    dag = dag_bag.get_dag(DAG_ID)
    run_pipeline = dag.get_task("run_pipeline")
    assert {t.task_id for t in run_pipeline.upstream_list} == {"generate_inputs"}
    assert "run_pipeline" in {t.task_id for t in dag.get_task("collect_outputs").upstream_list}


def test_registration_is_last_and_mapped_over_tenants(dag_bag):
    """PATCH appends, so one task looping over tenants would double-register everything
    that had already succeeded when a retry replayed it. Mapping gives per-tenant retry."""
    dag = dag_bag.get_dag(DAG_ID)
    register = dag.get_task("register_tasks")
    assert {t.task_id for t in register.upstream_list} == {"resolve_cases", "collect_outputs", "list_tenants"}
    assert register.downstream_list == []
    assert isinstance(register, MappedOperator)


def test_dag_asks_for_nothing_it_can_work_out_itself(dag_bag):
    """Guards against param creep. Storage roots are environment and working paths are
    derived from the run; `cases.id` is a single-column primary key over one shared clinical
    schema, so the tenant is read off the cases. What is left is intent: which tasks (none,
    normally), where writes are permitted, and whether to write at all."""
    dag = dag_bag.get_dag(DAG_ID)
    assert set(dag.params) == {"task_ids", "tenants", "dry_run"}


def test_a_scheduled_run_actually_writes(dag_bag):
    """A dry run leaves every case eligible, so scheduling one would re-run the pipeline
    over the same cases every night and register nothing, for ever."""
    dag = dag_bag.get_dag(DAG_ID)
    assert dag.params["dry_run"] is False


def test_params_validate_with_no_input(dag_bag):
    """A scheduled run supplies no conf at all, so the defaults have to be a valid run."""
    dag = dag_bag.get_dag(DAG_ID)
    assert dag.params.validate() is not None


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
    """This is the entire concurrency story. A run that overruns a day makes the next queue;
    discovery happens at the start of a run, so the queued one re-queries after the previous
    has registered and never picks the same case up twice."""
    dag = dag_bag.get_dag(DAG_ID)
    assert dag.max_active_runs == 1


def test_the_queries_resolve_from_the_sql_search_path(dag_bag):
    """`parameters` is a template field and the DAG renders natively, which is what lets a
    list of ids survive as a list rather than a string."""
    dag = dag_bag.get_dag(DAG_ID)
    assert dag.render_template_as_native_obj
    # DagBag resolves template files at parse time, so `sql` is already the query itself.
    assert "{{ mapping.clinical_case }}" in dag.get_task("discover_scope").sql
    assert "{{ mapping.starrocks_hpo_term }}" in dag.get_task("fetch_phenotypes").sql
    assert set(dag.get_task("discover_scope").parameters) == {"task_ids", "tenants"}
    assert set(dag.get_task("fetch_phenotypes").parameters) == {"case_ids"}
