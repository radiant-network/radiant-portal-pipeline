import pytest

from radiant.tasks.decision import Decision, Reason, ReasonCode, from_query

# Every code the pipelines' `query_reason` rules return, per trigger document.
_QUERY_CODES = {
    "pending_sequencing",
    "pending_alignment",
    "no_project_code",
    "tenant_excluded",
    *(f"no_{document}" for document in ("gvcf", "gcnv", "cram", "metrics", "ssnv")),
    *(f"ambiguous_{document}" for document in ("gvcf", "gcnv", "cram", "dragen_metrics", "ssnv")),
}


def test_each_code_leads_to_one_scheduler_decision():
    assert {code: code.decision for code in ReasonCode} == {
        ReasonCode.FIRST_RUN: Decision.RUN,
        ReasonCode.NEW_SEQUENCING: Decision.RUN,
        ReasonCode.RETRY: Decision.RUN,
        ReasonCode.TASK_DELETED: Decision.RUN,
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


@pytest.mark.parametrize("code", sorted(_QUERY_CODES))
def test_every_code_the_fact_rules_return_is_known(code):
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
    assert (
        reason.text == "has no cram: a document its current alignment did not publish, or a value the import requires"
    )
    assert str(reason) == "missing_input:cram"
    assert reason.decision == Decision.BLOCKED


def test_only_a_member_reason_names_the_member():
    assert Reason(code=ReasonCode.PENDING_ALIGNMENT).about(101).patient_id == 101
    assert Reason(code=ReasonCode.TENANT_EXCLUDED).about(101).patient_id is None
