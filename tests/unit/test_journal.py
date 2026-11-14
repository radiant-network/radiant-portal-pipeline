import datetime

from radiant.tasks.decision import Decision, Reason, ReasonCode
from radiant.tasks.journal import Entry, Pipeline, _dump_reasons, _load_reasons, record, to_write, write

ALIGNMENT = Reason(code=ReasonCode.PENDING_ALIGNMENT)
NO_GVCF = Reason(code=ReasonCode.MISSING_INPUT, detail="gvcf")


def entry(decision, *reasons, case_id=812, pipeline=Pipeline.GERMLINE_SNV, item_key=""):
    return Entry(case_id=case_id, pipeline=pipeline, item_key=item_key, decision=decision, reasons=reasons)


def state(*entries):
    return {e.key: e for e in entries}


def test_a_first_decision_is_written():
    wait = entry(Decision.WAIT, ALIGNMENT)
    assert to_write([wait], {}) == [wait]


def test_the_same_decision_and_reasons_are_not_written_again():
    wait = entry(Decision.WAIT, ALIGNMENT)
    assert to_write([wait], state(wait)) == []


def test_run_is_written_on_every_pass():
    run = entry(Decision.RUN)
    assert to_write([run], state(run)) == [run]


def test_an_outcome_after_run_is_written():
    succeeded = entry(Decision.SUCCEEDED)
    assert to_write([succeeded], state(entry(Decision.RUN))) == [succeeded]


def test_a_new_decision_is_written():
    blocked = entry(Decision.BLOCKED, NO_GVCF)
    assert to_write([blocked], state(entry(Decision.WAIT, ALIGNMENT))) == [blocked]


def test_a_changed_reason_or_detail_is_written():
    last = state(entry(Decision.BLOCKED, NO_GVCF))
    other_detail = entry(Decision.BLOCKED, Reason(code=ReasonCode.MISSING_INPUT, detail="cram"))
    one_more = entry(Decision.BLOCKED, NO_GVCF, Reason(code=ReasonCode.MISSING_ALIQUOT))
    assert to_write([other_detail], last) == [other_detail]
    assert to_write([one_more], last) == [one_more]


def test_the_order_of_the_reasons_does_not_matter():
    aliquot = Reason(code=ReasonCode.MISSING_ALIQUOT)
    assert (
        to_write([entry(Decision.BLOCKED, aliquot, NO_GVCF)], state(entry(Decision.BLOCKED, NO_GVCF, aliquot))) == []
    )


def test_items_are_independent():
    wait = entry(Decision.WAIT, ALIGNMENT)
    others = [
        entry(Decision.WAIT, ALIGNMENT, case_id=813),
        entry(Decision.WAIT, ALIGNMENT, pipeline=Pipeline.GERMLINE_CNV),
        entry(Decision.WAIT, ALIGNMENT, item_key="70"),
    ]
    assert to_write(others, state(wait)) == others


def test_reasons_round_trip_through_json():
    reasons = (NO_GVCF, Reason(code=ReasonCode.UNSUPPORTED_CALLER, detail="it's GATK", patient_id=101))
    assert _load_reasons(_dump_reasons(reasons)) == reasons
    assert _dump_reasons(()) is None
    assert _load_reasons(None) == ()


class _Cursor:
    def __init__(self):
        self.calls = []

    def executemany(self, sql, rows):
        self.calls.append((sql, rows))


def test_write_sends_one_row_per_entry_with_one_timestamp():
    cursor = _Cursor()
    at = datetime.datetime(2026, 11, 13, 2, 0, 0, 123456)
    write(
        cursor,
        {"starrocks_pipeline_journal": "pipeline_journal"},
        [entry(Decision.RUN), entry(Decision.WAIT, ALIGNMENT, case_id=9)],
        "scheduled__x",
        at,
    )
    [(sql, rows)] = cursor.calls
    assert "INSERT INTO pipeline_journal" in sql
    assert [tuple(row.values()) for row in rows] == [
        (812, "germline_snv", "", at, "scheduled__x", "run", None),
        (9, "germline_snv", "", at, "scheduled__x", "wait", '[{"code": "pending_alignment", "detail": ""}]'),
    ]


def test_nothing_to_write_sends_nothing():
    cursor = _Cursor()
    write(cursor, {}, [], "scheduled__x")
    assert cursor.calls == []


class _Connection:
    def __init__(self, state_rows):
        self.cursor_ = _StateCursor(state_rows)
        self.committed = False

    def cursor(self):
        return self.cursor_

    def commit(self):
        self.committed = True


class _StateCursor(_Cursor):
    description = [("case_id",), ("pipeline",), ("item_key",), ("decision",), ("reasons",)]

    def __init__(self, state_rows):
        super().__init__()
        self.state_rows = state_rows

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def execute(self, sql, params):
        assert params == {"pipeline": "germline_snv"}

    def fetchall(self):
        return self.state_rows


def test_record_writes_only_what_changed_and_commits(monkeypatch):
    from airflow.hooks.base import BaseHook

    connection = _Connection([(812, "germline_snv", "", "wait", '[{"code": "pending_alignment", "detail": ""}]')])
    hook = type("Hook", (), {"get_conn": lambda self: connection})()
    monkeypatch.setattr(BaseHook, "get_connection", lambda conn_id: type("C", (), {"get_hook": lambda self: hook})())

    unchanged, new = entry(Decision.WAIT, ALIGNMENT), entry(Decision.RUN, case_id=9)
    assert record([unchanged, new], "scheduled__x") == [new]
    [(_, rows)] = connection.cursor_.calls
    assert [row["case_id"] for row in rows] == [9]
    assert connection.committed


def test_the_member_is_in_the_json_only_when_named():
    assert _dump_reasons((Reason(code=ReasonCode.PENDING_SEQUENCING, patient_id=101), NO_GVCF)) == (
        '[{"code": "pending_sequencing", "detail": "", "patient_id": 101}, '
        '{"code": "missing_input", "detail": "gvcf"}]'
    )


def test_the_same_code_for_another_member_is_a_change():
    last = state(entry(Decision.WAIT, Reason(code=ReasonCode.PENDING_SEQUENCING, patient_id=101)))
    mother = entry(Decision.WAIT, Reason(code=ReasonCode.PENDING_SEQUENCING, patient_id=102))
    assert to_write([mother], last) == [mother]
