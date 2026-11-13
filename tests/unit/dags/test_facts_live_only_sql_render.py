"""The scheduler's discovery reads the live cases only (`live_only`); the "from Cases" DAGs never pass it."""

import re

import jinja2
import pytest

from radiant.dags import DAGS_DIR
from radiant.tasks.data.radiant_tables import get_radiant_mapping

_CLINICAL_SQL = DAGS_DIR / "sql" / "clinical"
_CONF = {"RADIANT_TABLES_DATABASE": "radiant"}
# Each fact query and the pipeline whose state settles its cases.
_FACTS = {
    "germline_snv_facts_select.sql": "germline_snv",
    "germline_cnv_facts_select.sql": "germline_cnv",
    "qc_germline_facts_select.sql": "qc",
    "qc_somatic_facts_select.sql": "qc",
    "somatic_snv_facts_select.sql": "somatic_snv",
}


def _render(filename: str, **variables) -> str:
    text = (_CLINICAL_SQL / filename).read_text()
    sql = jinja2.Template(text, undefined=jinja2.StrictUndefined).render(
        mapping=get_radiant_mapping(_CONF), **variables
    )
    return "\n".join(line for line in sql.splitlines() if not line.lstrip().startswith("--"))


@pytest.mark.parametrize("filename", sorted(_FACTS))
def test_without_live_only_every_case_comes_back(filename):
    sql = _render(filename)
    assert "live_case" not in sql
    assert "case_pipeline_state" not in sql


@pytest.mark.parametrize("filename", sorted(_FACTS))
def test_live_only_keeps_the_cases_with_work_moving_or_unsettled(filename):
    sql = _render(filename, live_only=True)
    settled = sql.split("settled_case AS (")[1].split("live_case AS (")[0]
    assert "FROM radiant.case_pipeline_state" in settled
    assert f"WHERE pipeline = '{_FACTS[filename]}'" in settled
    assert "decision = 'done' OR reason = 'no_trigger_document'" in settled
    live = sql.split("live_case AS (")[1]
    assert "pc.status_code IN ('submitted', 'processing')" in live
    assert "OR s.case_id IS NULL" in live
    # The filter narrows the cases, never the members of a case.
    assert re.search(r"AND (pc|rc)\.case_id IN \(SELECT case_id FROM live_case\)", sql)


@pytest.mark.parametrize("filename", sorted(_FACTS))
def test_no_stray_percent_with_live_only(filename):
    sql = _render(filename, live_only=True, case_ids=[1])
    assert "%" not in re.sub(r"%\(\w+\)s", "", sql).replace("%%", "")
