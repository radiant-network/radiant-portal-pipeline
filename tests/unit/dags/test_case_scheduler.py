from airflow.sdk.definitions.mappedoperator import MappedOperator
from airflow.sdk.definitions.taskgroup import MappedTaskGroup
from airflow.utils.trigger_rule import TriggerRule

DAG_ID = "radiant-case-scheduler"

CASES_DAG_IDS = {
    "trigger_germline_snv_postprocessing": "radiant-nextflow-snv-postprocessing-cases",
    "trigger_germline_cnv_postprocessing": "radiant-nextflow-cnv-postprocessing-cases",
    "trigger_somatic_snv_annotation": "radiant-nextflow-somatic-snv-annotation-cases",
    "trigger_quality_control": "radiant-nextflow-quality-control-cases",
}
PIPELINE_TASKS = (*CASES_DAG_IDS, "trigger_import")


def _upstream(dag, task_id):
    return {t.task_id for t in dag.get_task(task_id).upstream_list}


def test_dag_is_importable(dag_bag):
    assert dag_bag.get_dag(DAG_ID) is not None
    assert not dag_bag.import_errors


def test_dag_contains_the_expected_stages(dag_bag):
    dag = dag_bag.get_dag(DAG_ID)
    assert set(dag.task_ids) == {
        "discover_cases",
        "set_processing",
        "trigger_germline_snv_postprocessing",
        "trigger_germline_cnv_postprocessing",
        "trigger_somatic_snv_annotation",
        "trigger_quality_control",
        "trigger_import",
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
    assert dag.schedule == "@daily"
    assert dag.max_active_runs == 1
    assert not dag.catchup
    assert dag.params.validate() is not None


def test_the_cases_dags_have_no_schedule_of_their_own(dag_bag):
    """Scheduled on their own they would process cases without moving their status, and race
    this DAG. They stay manually runnable for a targeted rerun."""
    for dag_id in CASES_DAG_IDS.values():
        assert dag_bag.get_dag(dag_id).schedule is None


def test_the_pipelines_are_one_group(dag_bag):
    """Folded in the graph: the four "from Cases" DAGs, then the import."""
    group = dag_bag.get_dag(DAG_ID).task_group_dict["pipelines"]
    assert set(group.children) == set(PIPELINE_TASKS)


def test_both_passes_read_the_facts_themselves(dag_bag):
    """No SQL task left: each pass reads every pipeline's facts and the statuses in Python."""
    dag = dag_bag.get_dag(DAG_ID)
    assert _upstream(dag, "discover_cases") == set()
    assert not [t for t in dag.tasks if hasattr(t, "sql")]


def test_the_cases_dags_are_given_the_run_s_block_list(dag_bag):
    """So each leaves out exactly the tenants discovery left out: a case discovery counted but the
    DAG excluded would sit in `processing` with nothing running for it."""
    dag = dag_bag.get_dag(DAG_ID)
    for task_id, cases_dag_id in CASES_DAG_IDS.items():
        assert dag.get_task(task_id).conf == {
            "excluded_tenants": "{{ params.excluded_tenants }}",
            "decision_run_id": "{{ sanitize_run_tag(run_id) }}",
        }
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
    """It loads the annotation tasks SNV post-processing, somatic annotation and CNV post-processing
    (the slivar VCF, RAD-206) register."""
    dag = dag_bag.get_dag(DAG_ID)
    trigger = dag.get_task("trigger_import")
    assert trigger.trigger_dag_id == "radiant-import"
    assert trigger.trigger_rule == TriggerRule.ALL_DONE
    assert _upstream(dag, "trigger_import") == {
        "trigger_germline_snv_postprocessing",
        "trigger_germline_cnv_postprocessing",
        "trigger_somatic_snv_annotation",
    }


def test_evaluation_waits_for_every_pipeline_and_stops_if_one_failed(dag_bag):
    """A failed import, CNV post-processing or quality control leaves the second pass, and so the
    status changes and the emails after it, upstream_failed."""
    dag = dag_bag.get_dag(DAG_ID)
    assert dag.get_task("select_ready_cases").trigger_rule == TriggerRule.NONE_FAILED
    assert _upstream(dag, "select_ready_cases") == {
        "trigger_import",
        "trigger_germline_cnv_postprocessing",
        "trigger_quality_control",
    }
    for name in ("set_in_progress", "create_case_group", "notify_labs"):
        assert dag.get_task(f"evaluate_cases.{name}").trigger_rule == TriggerRule.ALL_SUCCESS


def test_every_child_run_is_waited_for_and_pinned_to_this_run(dag_bag):
    """A retry resets the same child run, so Nextflow's `-resume` finds its launch dir."""
    dag = dag_bag.get_dag(DAG_ID)
    for task_id in PIPELINE_TASKS:
        trigger = dag.get_task(task_id)
        assert trigger.wait_for_completion
        assert trigger.reset_dag_run
        assert trigger.deferrable
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
    # Every pipeline, not only the group's last ones: the import runs `all_done` over a failed one.
    # The tenant chains reach it through their last task: a failure earlier in a chain leaves
    # `notify_labs` upstream_failed, which `one_failed` counts.
    assert _upstream(dag, "watcher") == {
        "discover_cases",
        "set_processing",
        *PIPELINE_TASKS,
        "select_ready_cases",
        "evaluate_cases.notify_labs",
    }


def test_params(dag_bag):
    assert set(dag_bag.get_dag(DAG_ID).params) == {"excluded_tenants", "notify"}


RUN_ID = "scheduled__2026-11-12T00:00:00+00:00"
RUN_TAG = "scheduled-2026-11-12T00-00-00-00-00"


def _context(task_instance) -> dict:
    from types import SimpleNamespace

    return {
        "ti": task_instance,
        "params": {"excluded_tenants": []},
        "run_id": RUN_ID,
        "dag_run": SimpleNamespace(conf={}),
    }


def _feed(monkeypatch, status_rows, **rows):
    """What both passes read, without StarRocks: the discovery rows by pipeline (`germline_snv`,
    `germline_cnv`, `somatic_snv`, `qc`, `import`) and the case statuses."""
    discovery = {p: rows.get(p, []) for p in ("germline_snv", "germline_cnv", "somatic_snv", "qc", "import")}
    monkeypatch.setattr("radiant.tasks.nextflow.decision.read_discovery", lambda conf=None: discovery)
    monkeypatch.setattr("radiant.tasks.nextflow.decision.read_rows", lambda *args, **kwargs: status_rows)


def _fake_log(monkeypatch, *entries) -> list:
    """`pipeline_decision_log` in memory: `write_decisions` appends `(run_id, stage, decision)`,
    `read_run_units` reads the `run` case rows back. `entries` are already in it."""
    from radiant.tasks.nextflow import decision

    log = list(entries)

    def write(decisions, run_id, stage, conf=None):
        # A unit's state is the rows of its last write: one pass, one (run_id, stage).
        last: dict[tuple, tuple] = {}
        for r, st, d in log:
            unit = (d.pipeline, d.case_id, d.unit_id)
            if last.get(unit, ((None, None), []))[0] != (r, st):
                last[unit] = ((r, st), [])
            last[unit][1].append(d.model_dump())
        state = [row for _, rows in last.values() for row in rows]
        log.extend((run_id, stage, d) for d in decision.changed(decisions, state))

    def read(run_id, pipeline, conf=None, stage=decision.DISCOVER):
        return {
            (d.unit_id, d.case_id)
            for r, st, d in log
            if (r, st, d.pipeline, d.scope, d.decision) == (run_id, stage, pipeline, "case", decision.RUN)
        }

    monkeypatch.setattr(decision, "write_decisions", write)
    monkeypatch.setattr(decision, "read_run_units", read)
    return log


def _run(pipeline, case_id, stage="discover", unit_id=None):
    """A `run` case row of this run, as the log holds it."""
    from radiant.tasks.nextflow.decision import Decision

    return RUN_TAG, stage, Decision(pipeline=pipeline, scope="case", case_id=case_id, unit_id=unit_id, decision="run")


def test_the_children_follow_this_run_s_decisions(dag_bag):
    """Each "from Cases" DAG gets the run whose decisions it launches, and the block list."""
    dag = dag_bag.get_dag(DAG_ID)
    for task_id in (
        "trigger_germline_snv_postprocessing",
        "trigger_germline_cnv_postprocessing",
        "trigger_somatic_snv_annotation",
        "trigger_quality_control",
    ):
        conf = dag.get_task(task_id).conf
        assert conf["decision_run_id"] == "{{ sanitize_run_tag(run_id) }}"
        assert conf["excluded_tenants"] == "{{ params.excluded_tenants }}"
    assert not dag.get_task("trigger_import").conf


def _in_progress_after(dag, monkeypatch, *entries):
    """`select_ready_cases`, with no work left anywhere, then the plan its tenants' tasks read."""
    from radiant.dags import case_scheduler

    context = _context(None)
    monkeypatch.setattr("airflow.sdk.get_current_context", lambda: context)
    _fake_log(monkeypatch, *entries)
    status_rows = [
        {"case_id": 1, "tenant_code": "radiant", "status_code": "processing", "has_variants": True},
        {"case_id": 2, "tenant_code": "radiant", "status_code": "in_progress", "has_variants": True},
    ]
    _feed(monkeypatch, status_rows)
    assert dag.get_task("select_ready_cases").python_callable() == ["radiant"]
    return case_scheduler._in_progress_plan(context)


def test_the_cases_this_run_processed_are_notified_whatever_their_status(dag_bag, monkeypatch):
    """Not only the cases moved to in_progress: one already past it gets its new results too. The work
    this run had is read from the log, not passed along."""
    plan = _in_progress_after(dag_bag.get_dag(DAG_ID), monkeypatch, _run("import", 2))
    assert plan == [{"tenant": "radiant", "case_ids": [1], "processed_case_ids": [2]}]


def test_a_failed_discovery_still_moves_the_cases_to_in_progress(dag_bag, monkeypatch):
    """No `discover` row in the log: the status changes go out, only the extra emails do not."""
    plan = _in_progress_after(dag_bag.get_dag(DAG_ID), monkeypatch)
    assert plan == [{"tenant": "radiant", "case_ids": [1], "processed_case_ids": []}]


def test_the_mapped_tasks_get_the_tenant_only(dag_bag):
    """The cases of each tenant are read back from the log and the statuses, not passed in XCom."""
    import inspect

    dag = dag_bag.get_dag(DAG_ID)
    parameters = {
        task_id: list(inspect.signature(dag.get_task(task_id).python_callable).parameters)
        for task_id in (
            "set_processing",
            "evaluate_cases.set_in_progress",
            "evaluate_cases.create_case_group",
            "evaluate_cases.notify_labs",
        )
    }
    assert parameters == {
        "set_processing": ["tenant"],
        "evaluate_cases.set_in_progress": ["tenant"],
        # The portal's answer: which cases did move.
        "evaluate_cases.create_case_group": ["tenant", "results"],
        "evaluate_cases.notify_labs": ["tenant"],
    }


def test_somatic_candidates_count_as_work_in_both_passes(dag_bag, monkeypatch):
    """A somatic SNV candidate makes its case work waiting before the pipelines, and keeps it in
    processing after them."""
    from radiant.dags import case_scheduler
    from tests.unit.nextflow.somatic_snv.conftest import calling_task_rows

    context = _context(None)
    monkeypatch.setattr("airflow.sdk.get_current_context", lambda: context)
    log = _fake_log(monkeypatch)
    dag = dag_bag.get_dag(DAG_ID)
    somatic_rows = calling_task_rows(case_id=23)

    submitted = [{"case_id": 23, "tenant_code": "radiant", "status_code": "submitted", "has_variants": False}]
    _feed(monkeypatch, submitted, somatic_snv=somatic_rows)
    assert dag.get_task("discover_cases").python_callable() == ["radiant"]
    assert case_scheduler._processing_plan(context) == [{"tenant": "radiant", "case_ids": [23]}]

    processing = [{"case_id": 23, "tenant_code": "radiant", "status_code": "processing", "has_variants": True}]
    _feed(monkeypatch, processing, somatic_snv=somatic_rows)
    assert dag.get_task("select_ready_cases").python_callable() == []

    # Both passes are logged under the run, the calling task as the unit.
    assert [(stage, d.pipeline, d.unit_id, d.case_id, d.decision) for run_id, stage, d in log] == [
        ("discover", "somatic_snv", 70, 23, "run"),
        ("after", "somatic_snv", 70, 23, "run"),
    ]
    assert {run_id for run_id, _, _ in log} == {RUN_TAG}


def test_germline_and_somatic_quality_control_rows_both_count_as_work(dag_bag, monkeypatch):
    """The two QC queries are decided together: either one makes its case work waiting."""
    from radiant.dags import case_scheduler
    from tests.unit.nextflow.qc.conftest import member_row

    context = _context(None)
    monkeypatch.setattr("airflow.sdk.get_current_context", lambda: context)
    _fake_log(monkeypatch)
    submitted = [
        {"case_id": case_id, "tenant_code": "radiant", "status_code": "submitted", "has_variants": False}
        for case_id in (3, 23)
    ]
    qc_rows = [member_row(case_id=3), member_row(case_id=23, case_type="somatic", affected_status="unknown")]
    _feed(monkeypatch, submitted, qc=qc_rows)
    dag_bag.get_dag(DAG_ID).get_task("discover_cases").python_callable()
    assert case_scheduler._processing_plan(context) == [{"tenant": "radiant", "case_ids": [3, 23]}]


def test_a_settled_case_is_logged_once_and_the_run_s_case_on_each_change(dag_bag, monkeypatch):
    """Every `run` is written; the case annotated long ago is `done` once, then nothing; the run's case
    goes `run` then `done`."""
    from tests.unit.nextflow.germline_snv.conftest import member_row

    context = _context(None)
    monkeypatch.setattr("airflow.sdk.get_current_context", lambda: context)
    log = _fake_log(monkeypatch)
    dag = dag_bag.get_dag(DAG_ID)
    annotated_elsewhere = member_row(case_id=2, patient_id=200, seq_id=600, annotated=True)

    submitted = [{"case_id": 1, "tenant_code": "radiant", "status_code": "submitted", "has_variants": False}]
    _feed(monkeypatch, submitted, germline_snv=[member_row(case_id=1), annotated_elsewhere])
    dag.get_task("discover_cases").python_callable()

    processing = [{"case_id": 1, "tenant_code": "radiant", "status_code": "processing", "has_variants": True}]
    _feed(monkeypatch, processing, germline_snv=[member_row(case_id=1, annotated=True), annotated_elsewhere])
    dag.get_task("select_ready_cases").python_callable()
    # A second pass with the same facts writes nothing: no `run`, no change.
    dag.get_task("select_ready_cases").python_callable()

    assert [(stage, d.case_id, d.decision) for _, stage, d in log] == [
        ("discover", 1, "run"),
        ("discover", 2, "done"),
        ("after", 1, "done"),
    ]
