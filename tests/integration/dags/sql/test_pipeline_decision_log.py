"""`pipeline_decision_log` over a real StarRocks + clinical Postgres.

The decisions are the ones `decide` takes on the seeds, from the real pending queries; they are written with
the insert `write_decisions` runs, then read back with the query a child uses and the "why case X" query of the
spec. Seed case 1 already carries a `quality_control_metrics` task, so it is not a QC candidate; case 16 is a
trio ready for QC.
"""

import datetime
import os

import jinja2
import pytest

from radiant.dags import DAGS_DIR
from radiant.tasks.nextflow.decision import DISCOVER, QC, RUN, changed, decide, run_units
from tests.integration.dags.sql.test_sequencing_experiment_select_delta import (  # noqa: F401
    sequencing_experiment_tables,
)

pytestmark = pytest.mark.usefixtures("pipeline_views")

_SQL = os.path.join(DAGS_DIR, "sql")
RUN_ID = "scheduled-2026-11-12T00-00-00-00-00"
# One value per pass, as `write_decisions` sets it.
DECIDED_AT = datetime.datetime.now(datetime.UTC).replace(tzinfo=None, microsecond=0)
TRIO_CASE_ID = 16


def _render(path, mapping, params=None, **variables):
    with open(os.path.join(_SQL, path)) as f:
        return jinja2.Template(f.read()).render(
            mapping=mapping, params=params or {"task_ids": [], "excluded_tenants": []}, **variables
        )


def _rows(session, sql, args=None):
    with session.cursor() as cursor:
        cursor.execute(sql, args)
        columns = [d[0] for d in cursor.description]
        return [dict(zip(columns, row, strict=False)) for row in cursor.fetchall()]


@pytest.fixture(scope="module")
def decision_log(starrocks_session, radiant_mapping, sequencing_experiment_tables):  # noqa: F811
    """The table, and the seeds' decisions written once under RUN_ID at stage `discover`."""
    table = radiant_mapping["starrocks_pipeline_decision_log"]
    with starrocks_session.cursor() as cursor:
        cursor.execute(_render("radiant/init/pipeline_decision_log_create_table.sql", radiant_mapping))
        cursor.execute(_render("radiant/init/case_pipeline_state_create_table.sql", radiant_mapping))
        cursor.execute(f"TRUNCATE TABLE {table}")

    pending = {
        name: _rows(starrocks_session, _render(f"clinical/{name}_select.sql", radiant_mapping))
        for name in (
            "germline_snv_facts",
            "germline_cnv_facts",
            "somatic_snv_facts",
            "qc_germline_facts",
            "qc_somatic_facts",
        )
    }
    import_rows = _rows(starrocks_session, _render("radiant/import_pending_case_select.sql", radiant_mapping))
    decisions = decide(
        pending["germline_snv_facts"],
        pending["germline_cnv_facts"],
        pending["somatic_snv_facts"],
        [*pending["qc_germline_facts"], *pending["qc_somatic_facts"]],
        import_rows,
    )
    with starrocks_session.cursor() as cursor:
        cursor.executemany(
            _render("radiant/pipeline_decision_log_insert.sql", radiant_mapping),
            [{**d.model_dump(), "run_id": RUN_ID, "stage": DISCOVER, "decided_at": DECIDED_AT} for d in decisions],
        )
    yield decisions
    with starrocks_session.cursor() as cursor:
        cursor.execute(f"TRUNCATE TABLE {table}")


def test_every_decision_is_written(decision_log, starrocks_session, radiant_mapping):
    table = radiant_mapping["starrocks_pipeline_decision_log"]
    (count,) = _rows(starrocks_session, f"SELECT COUNT(*) AS n FROM {table} WHERE run_id = %(r)s", {"r": RUN_ID})
    assert count["n"] == len(decision_log) > 0


def test_a_child_reads_back_the_units_decided_run(decision_log, starrocks_session, radiant_mapping):
    rows = _rows(
        starrocks_session,
        _render("radiant/pipeline_decision_run_units_select.sql", radiant_mapping),
        {"run_id": RUN_ID, "pipeline": QC, "stage": DISCOVER},
    )
    assert {(r["unit_id"], r["case_id"]) for r in rows} == run_units(decision_log, QC)
    assert (None, TRIO_CASE_ID) in {(r["unit_id"], r["case_id"]) for r in rows}


def test_why_case_x_is_one_query(decision_log, starrocks_session, radiant_mapping):
    table = radiant_mapping["starrocks_pipeline_decision_log"]
    rows = _rows(
        starrocks_session,
        f"SELECT stage, pipeline, scope, decision, reason FROM {table} "
        "WHERE case_id = %(case_id)s ORDER BY decided_at DESC, pipeline",
        {"case_id": TRIO_CASE_ID},
    )
    assert ("discover", "qc", "case", "run", None) in {tuple(r.values()) for r in rows}


def test_decided_at_is_utc(decision_log, starrocks_session, radiant_mapping):
    table = radiant_mapping["starrocks_pipeline_decision_log"]
    (row,) = _rows(
        starrocks_session,
        f"SELECT ABS(TIMESTAMPDIFF(MINUTE, MAX(decided_at), UTC_TIMESTAMP())) AS drift FROM {table}",
    )
    assert int(row["drift"]) <= 5


def test_the_import_cases_come_with_a_reason(sequencing_experiment_tables, starrocks_session, radiant_mapping):  # noqa: F811
    rows = _rows(starrocks_session, _render("radiant/import_pending_case_select.sql", radiant_mapping))
    assert rows
    assert {r["reason"] for r in rows} <= {"new_or_changed", "not_loaded", "deleted"}


def _state(starrocks_session, radiant_mapping):
    return _rows(starrocks_session, _render("radiant/case_pipeline_state_select.sql", radiant_mapping))


def test_the_state_is_the_last_write_of_each_unit(decision_log, starrocks_session, radiant_mapping):
    """A later write of the trio's QC replaces its state; the other units keep the first write."""
    table = radiant_mapping["starrocks_pipeline_decision_log"]
    view = radiant_mapping["starrocks_case_pipeline_state"]
    later = {
        "run_id": "later-run",
        "stage": "after",
        "pipeline": QC,
        "case_id": TRIO_CASE_ID,
        "scope": "case",
        "tenant_code": "radiant",
        "patient_id": None,
        "seq_id": None,
        "task_id": None,
        "unit_id": None,
        "decision": "excluded",
        "reason": "ambiguous_cram",
        "detail": None,
        "decided_at": DECIDED_AT + datetime.timedelta(hours=1),
    }
    with starrocks_session.cursor() as cursor:
        cursor.execute(_render("radiant/pipeline_decision_log_insert.sql", radiant_mapping), later)
    try:
        rows = _rows(
            starrocks_session,
            f"SELECT run_id, pipeline, decision FROM {view} WHERE case_id = %(c)s AND scope = 'case'",
            {"c": TRIO_CASE_ID},
        )
        by_pipeline = {r["pipeline"]: (r["run_id"], r["decision"]) for r in rows}
        assert by_pipeline[QC] == ("later-run", "excluded")
        assert all(run_id == RUN_ID for p, (run_id, _) in by_pipeline.items() if p != QC)
        assert len(rows) == len(by_pipeline)
    finally:
        with starrocks_session.cursor() as cursor:
            cursor.execute(f"DELETE FROM {table} WHERE run_id = 'later-run'")


def test_a_second_identical_pass_writes_the_runs_only(decision_log, starrocks_session, radiant_mapping):
    """The state read back from StarRocks compares equal to the same decisions: only the work is written again."""
    kept = changed(decision_log, _state(starrocks_session, radiant_mapping))
    assert kept
    assert {(d.pipeline, d.case_id, d.unit_id) for d in kept} == {
        (d.pipeline, d.case_id, d.unit_id) for d in decision_log if d.scope == "case" and d.decision == RUN
    }


def test_the_discovery_reads_the_live_cases_only(decision_log, starrocks_session, radiant_mapping):
    """With the seeds' decisions as the state: a case comes back when it is not settled for the pipeline, or its
    status still moves; a settled case stays out, with its state."""
    statuses = {
        r["case_id"]: r["status_code"]
        for r in _rows(starrocks_session, _render("clinical/case_status_select.sql", radiant_mapping))
    }
    settled_decisions = {"done"}
    for pipeline, sql_file in (
        ("germline_snv", "germline_snv_facts_select.sql"),
        (QC, "qc_germline_facts_select.sql"),
    ):
        unsettled = {
            d.case_id
            for d in decision_log
            if d.pipeline == pipeline
            and d.scope == "case"
            and d.decision not in settled_decisions
            and d.reason != "no_trigger_document"
        }
        every = {r["case_id"] for r in _rows(starrocks_session, _render(f"clinical/{sql_file}", radiant_mapping))}
        live = {
            r["case_id"]
            for r in _rows(starrocks_session, _render(f"clinical/{sql_file}", radiant_mapping, live_only=True))
        }
        expected = {c for c in every if c in unsettled or statuses.get(c) in ("submitted", "processing")}
        assert live == expected, pipeline
        assert live < every, pipeline
