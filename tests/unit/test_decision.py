import re
from pathlib import Path

import pytest

from radiant.tasks.decision import Decision, Reason, ReasonCode, from_query

SQL_DIR = Path(__file__).parents[2] / "radiant" / "dags" / "sql" / "clinical"


def _query_codes() -> set[str]:
    codes = set()
    for path in SQL_DIR.glob("pending_*_select.sql"):
        codes |= set(re.findall(r"THEN '([a-z_]+)'", path.read_text()))
    return codes


def test_each_code_leads_to_one_scheduler_decision():
    assert {code: code.decision for code in ReasonCode} == {
        ReasonCode.PENDING_SEQUENCING: Decision.WAIT,
        ReasonCode.PENDING_ALIGNMENT: Decision.WAIT,
        ReasonCode.MISSING_INPUT: Decision.BLOCKED,
        ReasonCode.AMBIGUOUS_INPUT: Decision.BLOCKED,
        ReasonCode.MISSING_ALIQUOT: Decision.BLOCKED,
        ReasonCode.PROBAND_COUNT: Decision.BLOCKED,
        ReasonCode.UNSUPPORTED_SAMPLES: Decision.BLOCKED,
        ReasonCode.UNSUPPORTED_STRATEGY: Decision.BLOCKED,
        ReasonCode.UNSUPPORTED_CALLER: Decision.BLOCKED,
        ReasonCode.NO_PROJECT_CODE: Decision.BLOCKED,
        ReasonCode.TENANT_EXCLUDED: Decision.SKIP,
    }


def test_every_code_has_a_label():
    assert all(code.label for code in ReasonCode)


def test_every_code_a_fact_query_returns_is_known():
    codes = _query_codes()
    assert codes, f"no reason code found in {SQL_DIR}"
    for code in codes:
        from_query(code)


@pytest.mark.parametrize(
    ("query_code", "reason"),
    [
        ("no_gvcf", Reason(code=ReasonCode.MISSING_INPUT, detail="gvcf")),
        ("ambiguous_dragen_metrics", Reason(code=ReasonCode.AMBIGUOUS_INPUT, detail="metrics")),
        ("pending_alignment", Reason(code=ReasonCode.PENDING_ALIGNMENT)),
    ],
)
def test_a_query_code_maps_to_a_shared_code_and_its_detail(query_code, reason):
    assert from_query(query_code) == reason


def test_an_unknown_query_code_raises():
    with pytest.raises(ValueError):
        from_query("no_such_reason")


def test_the_text_fills_in_the_detail():
    reason = Reason(code=ReasonCode.MISSING_INPUT, detail="cram")
    assert reason.text == "its current alignment published no cram document"
    assert str(reason) == "missing_input:cram"
    assert reason.decision == Decision.BLOCKED
