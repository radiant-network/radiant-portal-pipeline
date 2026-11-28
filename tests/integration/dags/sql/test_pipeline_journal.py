"""`pipeline_journal` and its `pipeline_state` view over a real StarRocks: written and read back by `journal`."""

import datetime

import jinja2
import pytest

from radiant.dags import DAGS_DIR
from radiant.dags.case_release import PIPELINE_STATE_SQL
from radiant.tasks.decision import Decision, Reason, ReasonCode
from radiant.tasks.journal import Entry, Pipeline, read_state, to_write, write
from radiant.tasks.nextflow.lineage import OUTPUT_TASK_SQL

_INIT_DIR = DAGS_DIR / "sql" / "radiant" / "init"
J1 = datetime.datetime(2026, 11, 1, 2, 0, 0, 1)
J2 = datetime.datetime(2026, 11, 2, 2, 0, 0, 2)

ALIGNMENT = Reason(code=ReasonCode.PENDING_ALIGNMENT)
CALLER = Reason(code=ReasonCode.UNSUPPORTED_CALLER, detail="it's GATK")


def _entry(case_id, decision, *reasons, pipeline=Pipeline.GERMLINE_SNV, item_key=""):
    return Entry(case_id=case_id, pipeline=pipeline, item_key=item_key, decision=decision, reasons=reasons)


@pytest.fixture
def journal(starrocks_session, radiant_mapping):
    with starrocks_session.cursor() as cursor:
        for name in ("pipeline_journal", "pipeline_state"):
            sql = (_INIT_DIR / f"{name}_create_table.sql").read_text()
            cursor.execute(jinja2.Template(sql).render(mapping=radiant_mapping))
        cursor.execute(f"TRUNCATE TABLE {radiant_mapping['starrocks_pipeline_journal']}")
        yield cursor
        cursor.execute(f"TRUNCATE TABLE {radiant_mapping['starrocks_pipeline_journal']}")


def test_the_state_is_the_last_row_of_each_item(journal, radiant_mapping):
    write(
        journal,
        radiant_mapping,
        [
            _entry(1, Decision.WAIT, ALIGNMENT),
            _entry(2, Decision.BLOCKED, CALLER, pipeline=Pipeline.GERMLINE_CNV),
            _entry(3, Decision.RUN, pipeline=Pipeline.SOMATIC_SNV, item_key="70"),
        ],
        "scheduled__j1",
        J1,
    )
    write(journal, radiant_mapping, [_entry(1, Decision.RUN)], "scheduled__j2", J2)

    assert read_state(journal, radiant_mapping, Pipeline.GERMLINE_SNV) == {
        (1, "germline_snv", ""): _entry(1, Decision.RUN)
    }
    assert read_state(journal, radiant_mapping, Pipeline.GERMLINE_CNV) == {
        (2, "germline_cnv", ""): _entry(2, Decision.BLOCKED, CALLER, pipeline=Pipeline.GERMLINE_CNV)
    }
    assert list(read_state(journal, radiant_mapping, Pipeline.SOMATIC_SNV)) == [(3, "somatic_snv", "70")]


def test_an_unchanged_decision_read_back_is_not_written_again(journal, radiant_mapping):
    wait = _entry(1, Decision.WAIT, ALIGNMENT)
    write(journal, radiant_mapping, [wait], "scheduled__j1", J1)

    assert to_write([wait], read_state(journal, radiant_mapping, Pipeline.GERMLINE_SNV)) == []


def test_the_state_shows_the_task_a_run_registered(journal, radiant_mapping):
    write(journal, radiant_mapping, [_entry(1, Decision.RUN)], "scheduled__j1", J1)
    succeeded = Entry(case_id=1, pipeline=Pipeline.GERMLINE_SNV, decision=Decision.SUCCEEDED, output_task_id=103)
    write(journal, radiant_mapping, [succeeded], "scheduled__j1", J2)

    journal.execute(
        f"SELECT run_id, decision, output_task_id FROM {radiant_mapping['starrocks_pipeline_state']} WHERE case_id = 1"
    )
    assert journal.fetchall() == (("scheduled-j1", "succeeded", 103),)


def test_a_task_is_found_back_from_the_url_of_its_output(postgres_clinical_seeds, starrocks_session, radiant_mapping):
    """Task 63, the seeded germline SNV annotation of case 1, from its annotated VCF."""
    with starrocks_session.cursor() as cursor:
        cursor.execute(
            f"SELECT d.url FROM {radiant_mapping['clinical_document']} d "
            f"JOIN {radiant_mapping['clinical_task_has_document']} thd ON thd.document_id = d.id "
            "WHERE thd.task_id = 63 AND thd.type = 'output' AND d.format_code = 'vcf'"
        )
        [(url,)] = cursor.fetchall()
        cursor.execute(
            jinja2.Template(OUTPUT_TASK_SQL).render(mapping=radiant_mapping), {"urls": [url, "s3://unknown.vcf.gz"]}
        )
        assert cursor.fetchall() == ((url, 63),)


def test_the_release_reads_the_items_of_its_cases(journal, radiant_mapping):
    write(
        journal,
        radiant_mapping,
        [
            _entry(1, Decision.RUN),
            _entry(1, Decision.WAIT, ALIGNMENT, pipeline=Pipeline.GERMLINE_CNV),
            _entry(2, Decision.RUN),
        ],
        "scheduled__j1",
        J1,
    )
    journal.execute(jinja2.Template(PIPELINE_STATE_SQL).render(mapping=radiant_mapping), {"case_ids": [1]})
    assert sorted(journal.fetchall()) == [(1, "germline_cnv", "", "wait"), (1, "germline_snv", "", "run")]
