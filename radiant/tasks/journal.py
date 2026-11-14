"""The pipeline journal: per item, the decision taken and its reasons, written only when it changes.

An item is `(case_id, pipeline, item_key)`. `run` is written on every pass, so each run lists all its work; any
other decision is written only when it, or its reasons, differ from the item's last row, whatever the run.
"""

import datetime
import json
from enum import StrEnum
from pathlib import Path

import jinja2
from pydantic import BaseModel, ConfigDict

from radiant.tasks.decision import Decision, Reason

_SQL_DIR = Path(__file__).parents[1] / "dags" / "sql" / "radiant"

ItemKey = tuple[int, str, str]


class Pipeline(StrEnum):
    GERMLINE_SNV = "germline_snv"
    GERMLINE_CNV = "germline_cnv"
    QUALITY_CONTROL = "quality_control"
    QUALITY_CONTROL_SOMATIC = "quality_control_somatic"
    SOMATIC_SNV = "somatic_snv"
    IMPORT = "import"
    CASE_STATUS = "case_status"


class Entry(BaseModel):
    """One item's decision and its reasons."""

    model_config = ConfigDict(frozen=True)

    case_id: int
    pipeline: Pipeline
    item_key: str = ""
    decision: Decision
    reasons: tuple[Reason, ...] = ()

    @property
    def key(self) -> ItemKey:
        return self.case_id, self.pipeline, self.item_key


def to_write(entries: list[Entry], state: dict[ItemKey, Entry]) -> list[Entry]:
    """The entries the journal does not already hold as the item's last row. `run` is always written."""
    return [e for e in entries if e.decision == Decision.RUN or _changed(e, state.get(e.key))]


def write(cursor, mapping: dict, entries: list[Entry], run_id: str, decided_at: datetime.datetime | None = None):
    """Append `entries` to the journal, all with the same `decided_at` (UTC, naive)."""
    if not entries:
        return
    decided_at = decided_at or datetime.datetime.now(datetime.UTC).replace(tzinfo=None)
    rows = [
        (e.case_id, e.pipeline, e.item_key, decided_at, run_id, e.decision, _dump_reasons(e.reasons)) for e in entries
    ]
    cursor.executemany(_render("pipeline_journal_insert.sql", mapping), rows)


def read_state(cursor, mapping: dict, pipeline: Pipeline) -> dict[ItemKey, Entry]:
    """The last row of every item of `pipeline`, by item key."""
    cursor.execute(_render("pipeline_state_select.sql", mapping), {"pipeline": str(pipeline)})
    columns = [d[0] for d in cursor.description]
    state = {}
    for values in cursor.fetchall():
        row = dict(zip(columns, values, strict=True))
        entry = Entry(
            case_id=row["case_id"],
            pipeline=row["pipeline"],
            item_key=row["item_key"],
            decision=row["decision"],
            reasons=_load_reasons(row["reasons"]),
        )
        state[entry.key] = entry
    return state


def record(entries: list[Entry], run_id: str, conf: dict | None = None) -> list[Entry]:
    """Write to the journal what changed in `entries`, over the Airflow StarRocks connection; return it."""
    from airflow.hooks.base import BaseHook

    from radiant.tasks.data.radiant_tables import get_radiant_mapping

    mapping = get_radiant_mapping(conf)
    connection = BaseHook.get_connection("starrocks_conn").get_hook().get_conn()
    with connection.cursor() as cursor:
        state = {}
        for pipeline in {e.pipeline for e in entries}:
            state |= read_state(cursor, mapping, pipeline)
        changed = to_write(entries, state)
        write(cursor, mapping, changed, run_id)
    connection.commit()
    return changed


def _changed(entry: Entry, last: Entry | None) -> bool:
    # Reasons are compared as a set: their order carries no meaning.
    return last is None or (entry.decision, set(entry.reasons)) != (last.decision, set(last.reasons))


def _dump_reasons(reasons: tuple[Reason, ...]) -> str | None:
    return json.dumps([r.model_dump(mode="json") for r in reasons]) if reasons else None


def _load_reasons(text: str | None) -> tuple[Reason, ...]:
    return tuple(Reason(**r) for r in json.loads(text)) if text else ()


def _render(name: str, mapping: dict) -> str:
    return jinja2.Template((_SQL_DIR / name).read_text()).render(mapping=mapping)
