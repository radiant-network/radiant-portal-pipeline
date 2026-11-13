from airflow.sdk.definitions.mappedoperator import MappedOperator
from airflow.sdk.definitions.taskgroup import MappedTaskGroup
from airflow.utils.trigger_rule import TriggerRule

from radiant.dags import DAGS_DIR

DAG_ID = "radiant-case-scheduler"

CASES_DAG_IDS = {
    "trigger_germline_snv_postprocessing": "radiant-nextflow-snv-postprocessing-cases",
    "trigger_germline_cnv_postprocessing": "radiant-nextflow-cnv-postprocessing-cases",
    "trigger_somatic_snv_annotation": "radiant-nextflow-somatic-snv-annotation-cases",
    "trigger_quality_control": "radiant-nextflow-quality-control-cases",
}
# Each discovery task: the cases DAG and the task of it that runs the same query.
DISCOVERY = {
    "discover_germline_snv_cases": ("radiant-nextflow-snv-postprocessing-cases", "discover_scope"),
    "discover_germline_cnv_cases": ("radiant-nextflow-cnv-postprocessing-cases", "discover_scope"),
    "discover_somatic_snv_cases": ("radiant-nextflow-somatic-snv-annotation-cases", "discover_scope"),
    "discover_qc_cases": ("radiant-nextflow-quality-control-cases", "discover_germline"),
    "discover_qc_somatic_cases": ("radiant-nextflow-quality-control-cases", "discover_somatic"),
}
# The discoveries that read facts and decide in Python.
FACT_QUERIES = {"discover_germline_snv_cases": "germline_snv_facts_select.sql"}


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
        "discover_qc_somatic_cases",
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
        "rediscover_qc_somatic_cases",
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
    assert dag.schedule == "@daily"
    assert dag.max_active_runs == 1
    assert not dag.catchup
    assert dag.params.validate() is not None


def test_the_cases_dags_have_no_schedule_of_their_own(dag_bag):
    """Scheduled on their own they would process cases without moving their status, and race
    this DAG. They stay manually runnable for a targeted rerun."""
    for dag_id in CASES_DAG_IDS.values():
        assert dag_bag.get_dag(dag_id).schedule is None


def test_discovery_uses_each_cases_dag_query_and_the_run_s_block_list(dag_bag):
    """Only count what the DAG would actually run: same query, same excluded tenants."""
    dag = dag_bag.get_dag(DAG_ID)
    for task_id, (cases_dag_id, cases_task_id) in DISCOVERY.items():
        discover = dag.get_task(task_id)
        # Inherited from the DAG, not set per task: a run conf `excluded_tenants` must reach it.
        assert discover.params["excluded_tenants"] == dag.params["excluded_tenants"]
        if task_id in FACT_QUERIES:
            # The child reads the same file from Python, narrowed to the decided cases.
            assert discover.sql.strip() == (DAGS_DIR / "sql" / "clinical" / FACT_QUERIES[task_id]).read_text().strip()
            # A fact query takes no parameter: the block list is applied in Python.
            assert not discover.parameters
            assert "task_ids" not in discover.params
            continue
        assert discover.sql == dag_bag.get_dag(cases_dag_id).get_task(cases_task_id).sql
        assert discover.parameters == {"excluded_tenants": "{{ params.excluded_tenants }}"}
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


def test_discover_cases_reads_every_discovery_and_the_statuses(dag_bag):
    dag = dag_bag.get_dag(DAG_ID)
    assert _upstream(dag, "discover_cases") == {*DISCOVERY, "discover_import_cases", "fetch_case_statuses"}
    assert "{{ mapping.starrocks_pipeline_case }}" in dag.get_task("fetch_case_statuses").sql


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
    second_pass = ["refresh_case_statuses", *(f"re{task_id}" for task_id in DISCOVERY), "rediscover_import_cases"]
    for task_id in second_pass:
        assert dag.get_task(task_id).trigger_rule == TriggerRule.NONE_FAILED
        assert _upstream(dag, task_id) == {
            "trigger_import",
            "trigger_germline_cnv_postprocessing",
            "trigger_quality_control",
        }
    # The statuses are read again: the pipelines and the import have run since the first read.
    assert dag.get_task("refresh_case_statuses").sql == dag.get_task("fetch_case_statuses").sql
    assert _upstream(dag, "select_ready_cases") == set(second_pass)
    evaluation = ("set_in_progress", "create_case_group", "notify_labs")
    for task_id in ("select_ready_cases", *(f"evaluate_cases.{name}" for name in evaluation)):
        assert dag.get_task(task_id).trigger_rule == TriggerRule.ALL_SUCCESS


def test_evaluation_asks_the_same_work_waiting_question_as_discovery(dag_bag):
    """A case still pending anywhere after the pipelines stays processing; the two passes must
    not drift, or a case would be moved on with a pipeline left to run, or never at all."""
    dag = dag_bag.get_dag(DAG_ID)
    for name in ("germline_snv", "germline_cnv", "somatic_snv", "qc", "qc_somatic", "import"):
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


def _record_writes(monkeypatch, run_units=None) -> list:
    """`write_decisions` and `read_run_units` without StarRocks: the `(run_id, stage, decisions)`
    of each write; `run_units` are the units read back as decided `run`, by pipeline."""
    written = []
    monkeypatch.setattr(
        "radiant.tasks.nextflow.decision.write_decisions",
        lambda decisions, run_id, stage, conf=None: written.append((run_id, stage, decisions)),
    )
    monkeypatch.setattr(
        "radiant.tasks.nextflow.decision.read_run_units",
        lambda run_id, pipeline, conf=None: (run_units or {}).get(pipeline, set()),
    )
    return written


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


def _run_select_ready_cases(dag, monkeypatch, cases_with_work):
    """`select_ready_cases` with no work left anywhere, reading `cases_with_work` from discovery."""
    from radiant.dags import case_scheduler

    pulls = []

    class TaskInstance:
        def xcom_pull(self, task_ids, key):
            pulls.append((task_ids, key))
            return cases_with_work

    context = _context(TaskInstance())
    monkeypatch.setattr("airflow.sdk.get_current_context", lambda: context)
    # No discovery row: nothing decided, nothing waiting. The decisions are not written anywhere.
    _record_writes(monkeypatch)
    status_rows = [
        {"case_id": 1, "tenant_code": "radiant", "status_code": "processing", "has_variants": True},
        {"case_id": 2, "tenant_code": "radiant", "status_code": "in_progress", "has_variants": True},
    ]
    batches = dag.get_task("select_ready_cases").python_callable(
        germline_snv_rows=[],
        germline_cnv_rows=[],
        somatic_snv_rows=[],
        qc_rows=[],
        qc_somatic_rows=[],
        import_rows=[],
        status_rows=status_rows,
    )
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


def test_somatic_candidates_count_as_work_in_both_passes(dag_bag, monkeypatch):
    """Each discovery input reaches `_cases_with_work` in its own place: a somatic SNV candidate
    makes its case work waiting before the pipelines, and keeps it in processing after them."""
    from tests.unit.nextflow.somatic_snv.conftest import calling_task_rows

    pushed = {}

    class TaskInstance:
        def xcom_push(self, key, value):
            pushed[key] = value

        def xcom_pull(self, task_ids, key):
            return pushed.get(key)

    context = _context(TaskInstance())
    monkeypatch.setattr("airflow.sdk.get_current_context", lambda: context)
    written = _record_writes(monkeypatch)
    dag = dag_bag.get_dag(DAG_ID)
    inputs = {
        "germline_snv_rows": [],
        "germline_cnv_rows": [],
        "somatic_snv_rows": calling_task_rows(case_id=23),
        "qc_rows": [],
        "qc_somatic_rows": [],
        "import_rows": [],
    }

    submitted = [{"case_id": 23, "tenant_code": "radiant", "status_code": "submitted", "has_variants": False}]
    batches = dag.get_task("discover_cases").python_callable(**inputs, status_rows=submitted)
    assert batches == [{"tenant": "radiant", "case_ids": [23]}]

    processing = [{"case_id": 23, "tenant_code": "radiant", "status_code": "processing", "has_variants": True}]
    assert dag.get_task("select_ready_cases").python_callable(**inputs, status_rows=processing) == []

    # Both passes are logged under the run, the calling task as the unit.
    assert [(run_id, stage) for run_id, stage, _ in written] == [(RUN_TAG, "discover"), (RUN_TAG, "after")]
    for _, _, decisions in written:
        assert [(d.pipeline, d.unit_id, d.case_id, d.decision) for d in decisions if d.scope == "case"] == [
            ("somatic_snv", 70, 23, "run")
        ]


def test_germline_and_somatic_quality_control_rows_both_count_as_work(dag_bag, monkeypatch):
    """The two QC discoveries reach `_cases_with_work` together: either one makes its case work waiting."""
    from tests.unit.nextflow.qc.conftest import member_row

    class TaskInstance:
        def xcom_push(self, key, value):
            pass

    context = _context(TaskInstance())
    monkeypatch.setattr("airflow.sdk.get_current_context", lambda: context)
    _record_writes(monkeypatch)
    submitted = [
        {"case_id": case_id, "tenant_code": "radiant", "status_code": "submitted", "has_variants": False}
        for case_id in (3, 23)
    ]
    discover = dag_bag.get_dag(DAG_ID).get_task("discover_cases")
    batches = discover.python_callable(
        germline_snv_rows=[],
        germline_cnv_rows=[],
        somatic_snv_rows=[],
        qc_rows=[member_row(case_id=3)],
        qc_somatic_rows=[member_row(case_id=23, case_type="somatic", affected_status="unknown")],
        import_rows=[],
        status_rows=submitted,
    )
    assert batches == [{"tenant": "radiant", "case_ids": [3, 23]}]


def test_a_germline_case_the_run_annotated_is_logged_done_after_and_only_then(dag_bag, monkeypatch):
    """`done` at `after` for the unit decided `run` at `discover`; never at `discover`."""
    from tests.unit.nextflow.germline_snv.conftest import member_row

    class TaskInstance:
        def xcom_push(self, key, value):
            pass

        def xcom_pull(self, task_ids, key):
            return [1]

    context = _context(TaskInstance())
    monkeypatch.setattr("airflow.sdk.get_current_context", lambda: context)
    written = _record_writes(monkeypatch, run_units={"germline_snv": {(None, 1)}})
    dag = dag_bag.get_dag(DAG_ID)
    empty = {"germline_cnv_rows": [], "somatic_snv_rows": [], "qc_rows": [], "qc_somatic_rows": [], "import_rows": []}
    annotated_elsewhere = member_row(case_id=2, patient_id=200, seq_id=600, annotated=True)

    submitted = [{"case_id": 1, "tenant_code": "radiant", "status_code": "submitted", "has_variants": False}]
    rows = [member_row(case_id=1), annotated_elsewhere]
    dag.get_task("discover_cases").python_callable(germline_snv_rows=rows, **empty, status_rows=submitted)

    processing = [{"case_id": 1, "tenant_code": "radiant", "status_code": "processing", "has_variants": True}]
    rows = [member_row(case_id=1, annotated=True), annotated_elsewhere]
    dag.get_task("select_ready_cases").python_callable(germline_snv_rows=rows, **empty, status_rows=processing)

    logged = [(stage, d.case_id, d.decision) for _, stage, decisions in written for d in decisions]
    assert logged == [("discover", 1, "run"), ("after", 1, "done")]
