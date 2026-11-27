import datetime

from radiant.tasks.decision import Decision, Reason, ReasonCode
from radiant.tasks.importing.resolve import decide, outcome_entries
from radiant.tasks.journal import Pipeline
from radiant.tasks.starrocks.partition import SequencingExperimentPartitionAssigner

_DATE = datetime.datetime(2026, 1, 1)


def fact_row(case_id=1, task_id=10, seq_id=100, state="new", **overrides):
    """One row of `import_facts_select.sql`."""
    staged = state != "new"
    return {
        "state": state,
        "deleted": False,
        "case_id": case_id,
        "seq_id": seq_id,
        "task_id": task_id,
        "task_type": "radiant_germline_annotation",
        "analysis_type": "germline",
        "aliquot": f"aliquot-{seq_id}",
        "patient_id": seq_id,
        "experimental_strategy": "wgs",
        "tenant_code": "radiant",
        "request_priority": "routine",
        "vcf_filepath": "s3://vcf/a.vcf.gz",
        "cnv_vcf_filepath": None,
        "exomiser_filepath": None,
        "sex": "male",
        "family_id": case_id,
        "family_role": "proband",
        "affected_status": "affected",
        "histology_type": None,
        "created_at": _DATE,
        "updated_at": _DATE,
        "part": 0 if staged else None,
        "ingested_at": datetime.datetime(1970, 1, 1) if staged else None,
        "patient_part": None,
        "seq_part": None,
        "case_part": None,
        "family_part": None,
        "max_part": None,
        "max_count": None,
    } | overrides


def _by_item(decision):
    return {(e.case_id, e.item_key): e for e in decision.entries}


def test_a_new_row_runs_for_the_first_time_and_is_staged():
    decision = decide([fact_row()])
    [entry] = decision.entries
    assert (entry.pipeline, entry.item_key, entry.decision) == (Pipeline.IMPORT, "10", Decision.RUN)
    assert entry.reasons == (Reason(code=ReasonCode.FIRST_RUN),)
    assert [r["task_id"] for r in decision.new_rows] == [10]
    assert decision.staged_rows == []
    # The new rows go to the partition assigner as they are.
    assert len(SequencingExperimentPartitionAssigner().assign_partitions(decision.new_rows)) == 1


def test_a_staged_row_not_loaded_is_a_retry():
    decision = decide([fact_row(state="staged")])
    assert decision.entries[0].reasons == (Reason(code=ReasonCode.RETRY),)
    assert decision.new_rows == []
    assert [r["task_id"] for r in decision.staged_rows] == [10]


def test_a_deleted_task_runs_to_purge_its_data_and_is_flagged():
    decision = decide([fact_row(state="staged", deleted=1), fact_row(case_id=2, state="staged", deleted=1)])
    assert {e.reasons for e in decision.entries} == {(Reason(code=ReasonCode.TASK_DELETED),)}
    assert decision.deleted_task_ids == [10]
    assert all(r["deleted"] is True for r in decision.staged_rows)


def test_a_row_missing_a_required_value_blocks_its_task_in_every_case_with_new_rows():
    rows = [
        fact_row(case_id=1, seq_id=100),
        fact_row(case_id=1, seq_id=101, sex=None),
        fact_row(case_id=2, seq_id=101, sex=None),
        # Staged already: valid, it still loads.
        fact_row(case_id=3, seq_id=102, state="staged"),
        # Another task is not held back.
        fact_row(case_id=4, task_id=11),
    ]
    decision = decide(rows)
    by_item = _by_item(decision)
    missing = (Reason(code=ReasonCode.MISSING_INPUT, detail="sex", patient_id=101),)
    assert by_item[(1, "10")].decision == by_item[(2, "10")].decision == Decision.BLOCKED
    assert by_item[(1, "10")].reasons == by_item[(2, "10")].reasons == missing
    assert by_item[(3, "10")].reasons == (Reason(code=ReasonCode.RETRY),)
    assert by_item[(4, "11")].decision == Decision.RUN
    assert [r["task_id"] for r in decision.new_rows] == [11]
    assert [r["case_id"] for r in decision.staged_rows] == [3]


def test_an_unknown_strategy_blocks_the_task():
    [entry] = decide([fact_row(experimental_strategy="rna")]).entries
    assert entry.decision == Decision.BLOCKED
    assert entry.reasons == (Reason(code=ReasonCode.UNSUPPORTED_STRATEGY, detail="rna"),)


def test_an_item_still_in_the_facts_after_the_import_failed():
    decided = decide([fact_row(case_id=1), fact_row(case_id=2, task_id=11), fact_row(case_id=3, task_id=12, sex=None)])
    entries = [e.model_dump(mode="json") for e in decided.entries]
    outcomes = outcome_entries(entries, [fact_row(case_id=2, task_id=11, state="staged")])
    # The blocked item did not run: no outcome.
    assert {(e.case_id, e.item_key): e.decision for e in outcomes} == {
        (1, "10"): Decision.SUCCEEDED,
        (2, "11"): Decision.FAILED,
    }
