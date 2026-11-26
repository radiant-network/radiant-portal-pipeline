from airflow.sdk.definitions.mappedoperator import MappedOperator

DAG_ID = "radiant-nextflow-quality-control-cases"
PIPELINE_DAG_ID = "radiant-nextflow-quality-control"


def test_dag_is_importable(dag_bag):
    assert dag_bag.get_dag(DAG_ID) is not None
    assert not dag_bag.import_errors


def test_dag_contains_the_expected_stages(dag_bag):
    dag = dag_bag.get_dag(DAG_ID)
    assert set(dag.task_ids) == {
        "has_cases",
        "fetch_germline_members",
        "fetch_somatic_members",
        "select_cases",
        "resolve_cases",
        "generate_inputs",
        "run_pipeline",
        "collect_outputs",
        "list_tenants",
        "register_tasks",
        "journal_outcomes",
    }
    assert dag.validate() is None


def test_it_runs_the_named_cases_and_decides_nothing(dag_bag):
    """The scheduler names the cases it decided `run` for; none named runs nothing."""
    dag = dag_bag.get_dag(DAG_ID)
    # The control DAG starts it; a schedule of its own would race it.
    assert dag.schedule is None
    assert set(dag.params) == {"case_ids", "excluded_tenants", "dry_run"}
    assert dag.params["case_ids"] == []
    assert dag.params["dry_run"] is False


def test_the_members_are_fetched_per_case_type_for_the_named_cases(dag_bag):
    """No anti-join: a named case runs, QC'd before or not."""
    dag = dag_bag.get_dag(DAG_ID)
    germline, somatic = dag.get_task("fetch_germline_members"), dag.get_task("fetch_somatic_members")
    assert "pc.case_type = 'germline'" in germline.sql
    assert "pc.case_type = 'somatic'" in somatic.sql
    for fetch in (germline, somatic):
        assert {t.task_id for t in fetch.upstream_list} == {"has_cases"}
        assert "%(case_ids)s" in fetch.sql
        assert "quality_control_metrics" not in fetch.sql
        assert set(fetch.parameters) == {"case_ids"}
    assert {t.task_id for t in dag.get_task("select_cases").upstream_list} == {
        "fetch_germline_members",
        "fetch_somatic_members",
    }


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


def test_each_pipeline_journals_its_own_cases(dag_bag, monkeypatch):
    """A germline case is a `quality_control` outcome, a somatic one a `quality_control_somatic` outcome."""
    from radiant.tasks.decision import Decision
    from radiant.tasks.journal import Pipeline

    selections = {
        "quality_control": {"case_ids": [3], "members": [{"case_id": 3, "tenant_code": "radiant"}], "excluded": []},
        "quality_control_somatic": {
            "case_ids": [23],
            "members": [{"case_id": 23, "tenant_code": "other"}],
            "excluded": [],
        },
    }

    pulls = {
        "select_cases": selections,
        "register_tasks": [{"tenant": "radiant", "report": {}}],
        "collect_outputs": {"CA3": {"json": {"url": "s3://out/CA3.json"}}},
    }

    class TaskInstance:
        def xcom_pull(self, task_ids, key=None):
            return pulls[task_ids]

    recorded = []
    context = {"ti": TaskInstance(), "params": {"dry_run": False}, "run_id": "manual__1"}
    monkeypatch.setattr("airflow.sdk.get_current_context", lambda: context)
    monkeypatch.setattr("radiant.tasks.journal.record", lambda entries, run_id: recorded.extend(entries) or entries)
    monkeypatch.setattr("radiant.tasks.nextflow.lineage.output_task_ids", lambda urls: {"s3://out/CA3.json": 71})
    assert dag_bag.get_dag(DAG_ID).get_task("journal_outcomes").python_callable() == 2
    # The succeeded case names the QC task registered for it.
    assert [(e.case_id, e.pipeline, e.decision, e.output_task_id) for e in recorded] == [
        (3, Pipeline.QUALITY_CONTROL, Decision.SUCCEEDED, 71),
        (23, Pipeline.QUALITY_CONTROL_SOMATIC, Decision.FAILED, None),
    ]


def test_only_one_run_at_a_time(dag_bag):
    assert dag_bag.get_dag(DAG_ID).max_active_runs == 1


def test_the_queries_resolve_from_the_sql_search_path(dag_bag):
    dag = dag_bag.get_dag(DAG_ID)
    assert dag.template_searchpath == ["/opt/airflow/dags/radiant/dags/sql"]
    assert dag.render_template_as_native_obj is True
