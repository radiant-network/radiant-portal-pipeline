"""What the import does with each (case, task) of `import_facts_select.sql`, and what came of it.

One journal item per (case, task), its `item_key` the task. Every candidate runs, unless a new row of its task lacks a
value the import requires: the task's new rows are then blocked in every case, since the import loads a task from all
its rows.
"""

from pathlib import Path

import jinja2
from pydantic import BaseModel

from radiant.tasks.decision import Decision, Reason, ReasonCode, decision_of
from radiant.tasks.journal import Entry, Pipeline
from radiant.tasks.starrocks.partition import SequencingDeltaCommon, SequencingExperimentPartitionAssigner

_FACTS_SQL = Path(__file__).parents[2] / "dags" / "sql" / "clinical" / "import_facts_select.sql"

NEW = "new"

# The columns a new row must have to be staged: the required fields of the staging model.
REQUIRED = tuple(name for name, field in SequencingDeltaCommon.model_fields.items() if field.is_required())


class ImportDecision(BaseModel):
    entries: list[Entry] = []
    # The new rows to stage, and the staged rows to load, of the items decided `run`.
    new_rows: list[dict] = []
    staged_rows: list[dict] = []
    deleted_task_ids: list[int] = []


def decide(fact_rows: list[dict]) -> ImportDecision:
    blocked = _blocked_tasks(fact_rows)
    by_item: dict[tuple[int, int], list[dict]] = {}
    for row in fact_rows:
        by_item.setdefault((row["case_id"], row["task_id"]), []).append(row)

    decision = ImportDecision()
    for (case_id, task_id), rows in sorted(by_item.items()):
        # A staged row is already valid: only an item with new rows waits for its task to be fixed.
        has_new = any(r["state"] == NEW for r in rows)
        reasons = (blocked.get(task_id) if has_new else None) or (_run_reason(rows),)
        entry = Entry(
            case_id=case_id,
            pipeline=Pipeline.IMPORT,
            item_key=str(task_id),
            decision=decision_of(reasons),
            reasons=reasons,
        )
        decision.entries.append(entry)
        if entry.decision != Decision.RUN:
            continue
        decision.new_rows += [r for r in rows if r["state"] == NEW]
        decision.staged_rows += [{**r, "deleted": bool(r["deleted"])} for r in rows if r["state"] != NEW]
    decision.deleted_task_ids = sorted({r["task_id"] for r in decision.staged_rows if r["deleted"]})
    return decision


def outcome_entries(entries: list[dict], fact_rows_after: list[dict]) -> list[Entry]:
    """`failed` for each item decided `run` that the facts still hold after the import, `succeeded` for the others."""
    pending = {(r["case_id"], str(r["task_id"])) for r in fact_rows_after}
    return [
        Entry(
            case_id=e.case_id,
            pipeline=Pipeline.IMPORT,
            item_key=e.item_key,
            decision=Decision.FAILED if (e.case_id, e.item_key) in pending else Decision.SUCCEEDED,
        )
        for e in map(Entry.model_validate, entries)
        if e.decision == Decision.RUN
    ]


def cases_with_new_data(entries: list[dict], outcomes: list[Entry]) -> list[int]:
    """The cases with an item `succeeded` that was decided `first_run` or `retry`: data to analyse. A purge
    (`task_deleted`) brings none."""
    loading = {
        (e.case_id, e.item_key)
        for e in map(Entry.model_validate, entries)
        if e.decision == Decision.RUN and {r.code for r in e.reasons} & {ReasonCode.FIRST_RUN, ReasonCode.RETRY}
    }
    return sorted(
        {o.case_id for o in outcomes if o.decision == Decision.SUCCEEDED and (o.case_id, o.item_key) in loading}
    )


def fetch_facts(conf: dict | None = None) -> list[dict]:
    """The facts, over the Airflow StarRocks connection."""
    from airflow.hooks.base import BaseHook

    from radiant.tasks.data.radiant_tables import get_radiant_mapping

    sql = jinja2.Template(_FACTS_SQL.read_text()).render(mapping=get_radiant_mapping(conf))
    connection = BaseHook.get_connection("starrocks_conn").get_hook().get_conn()
    with connection.cursor() as cursor:
        cursor.execute(sql)
        columns = [d[0] for d in cursor.description]
        return [dict(zip(columns, values, strict=True)) for values in cursor.fetchall()]


def _run_reason(rows: list[dict]) -> Reason:
    if any(r["deleted"] for r in rows):
        return Reason(code=ReasonCode.TASK_DELETED)
    if any(r["state"] != NEW for r in rows):
        return Reason(code=ReasonCode.RETRY)
    return Reason(code=ReasonCode.FIRST_RUN)


def _blocked_tasks(fact_rows: list[dict]) -> dict[int, tuple[Reason, ...]]:
    """The reasons of each task one of whose new rows cannot be staged."""
    blocked: dict[int, dict[Reason, None]] = {}
    for row in fact_rows:
        if row["state"] != NEW:
            continue
        reasons = [
            Reason(code=ReasonCode.MISSING_INPUT, detail=column, patient_id=row["patient_id"])
            for column in REQUIRED
            if row.get(column) is None
        ]
        strategy = row["experimental_strategy"]
        if strategy is not None and strategy.lower() not in SequencingExperimentPartitionAssigner.SEQUENCING_TYPES:
            reasons.append(Reason(code=ReasonCode.UNSUPPORTED_STRATEGY, detail=strategy))
        for reason in reasons:
            blocked.setdefault(row["task_id"], {})[reason] = None
    return {task_id: tuple(reasons) for task_id, reasons in blocked.items()}
