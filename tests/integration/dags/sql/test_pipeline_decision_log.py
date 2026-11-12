"""`pipeline_decision_log` over a real StarRocks + clinical Postgres.

The decisions are the ones `decide` takes on the seeds, from the real pending queries; they are written with
the insert `write_decisions` runs, then read back with the query a child uses and the "why case X" query of the
spec. Seed case 1 already carries a `quality_control_metrics` task, so it is not a QC candidate; case 16 is a
trio ready for QC.
"""

import os

import jinja2
import pytest

from radiant.dags import DAGS_DIR
from radiant.tasks.nextflow.decision import DISCOVER, QC, decide, run_units
from tests.integration.dags.sql.test_sequencing_experiment_select_delta import (  # noqa: F401
    sequencing_experiment_tables,
)

pytestmark = pytest.mark.usefixtures("pipeline_views")

_SQL = os.path.join(DAGS_DIR, "sql")
RUN_ID = "scheduled-2026-11-12T00-00-00-00-00"
TRIO_CASE_ID = 16


def _render(path, mapping, params=None):
    with open(os.path.join(_SQL, path)) as f:
        return jinja2.Template(f.read()).render(
            mapping=mapping, params=params or {"task_ids": [], "excluded_tenants": []}
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
        cursor.execute(f"TRUNCATE TABLE {table}")

    pending = {
        name: _rows(starrocks_session, _render(f"clinical/{name}_select.sql", radiant_mapping))
        for name in (
            "pending_annotation",
            "pending_cnv_annotation",
            "pending_somatic_annotation",
            "pending_quality_control",
            "pending_quality_control_somatic",
        )
    }
    import_rows = _rows(starrocks_session, _render("radiant/import_pending_case_select.sql", radiant_mapping))
    decisions = decide(
        pending["pending_annotation"],
        pending["pending_cnv_annotation"],
        pending["pending_somatic_annotation"],
        [*pending["pending_quality_control"], *pending["pending_quality_control_somatic"]],
        import_rows,
    )
    with starrocks_session.cursor() as cursor:
        cursor.executemany(
            _render("radiant/pipeline_decision_log_insert.sql", radiant_mapping),
            [{**d.model_dump(), "run_id": RUN_ID, "stage": DISCOVER} for d in decisions],
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
        {"run_id": RUN_ID, "pipeline": QC},
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
