"""Format of an upgrade note: one YAML file per PR in ``upgrade-notes/``, named after the ticket.

Example (``upgrade-notes/RAD-266.yaml``)::

    actions:
      - when: after
        do: Delete the somatic rows of the germline tasks in the staging table.
        preconditions:
          - No radiant-import or import_part run is active.
        refs:
          - radiant/dags/sql/radiant/migrations/RAD-266_germline_tasks_out_of_somatic_cases.sql
        check: Query 0 of the migration file returns no row.
"""

import re
from pathlib import PurePosixPath
from typing import Annotated, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, StringConstraints, ValidationError

NOTES_DIR = "upgrade-notes"

# The ticket key, with an optional suffix when a ticket has more than one PR: RAD-171.yaml, RAD-171-2.yaml
NOTE_FILE_NAME = re.compile(r"^(?P<ticket>[A-Z][A-Z0-9]*-\d+)(?:-[a-z0-9]+)?\.yaml$")

NonEmptyStr = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]


class Action(BaseModel):
    """One operator action of an upgrade."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    when: Literal["before", "after"] = Field(description="Before or after the deploy of the release.")
    do: NonEmptyStr = Field(description="What to do.")
    preconditions: list[NonEmptyStr] = Field(default_factory=list, description="For example: no import running.")
    refs: list[NonEmptyStr] = Field(
        default_factory=list, description="The migration file or the DAG the action refers to. No SQL copy."
    )
    check: NonEmptyStr = Field(description="How to check that the action worked.")


class UpgradeNote(BaseModel):
    """The content of one note file."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    actions: list[Action] = Field(min_length=1)


class NoteError(ValueError):
    """A note file that does not match the format. The message names the file."""


def is_note_path(path: str) -> bool:
    """True for a file directly in ``upgrade-notes/`` with a ``.yaml`` or ``.yml`` extension."""
    p = PurePosixPath(path)
    return p.parent == PurePosixPath(NOTES_DIR) and p.suffix in (".yaml", ".yml")


def ticket_of(path: str) -> str:
    """The note's label: its file name without the extension (``RAD-171``, ``RAD-171-2``)."""
    return PurePosixPath(path).stem


def parse_note(path: str, text: str) -> UpgradeNote:
    """Validate the file name and the content of one note.

    Raises:
        NoteError: the name or the content does not match the format.
    """
    name = PurePosixPath(path).name
    if not NOTE_FILE_NAME.match(name):
        raise NoteError(
            f"{path}: the file name must be the ticket key, with an optional lowercase suffix, and end in .yaml "
            f"(for example RAD-171.yaml or RAD-171-2.yaml)"
        )
    try:
        data = yaml.safe_load(text)
    except yaml.YAMLError as e:
        raise NoteError(f"{path}: not valid YAML: {e}") from e
    try:
        return UpgradeNote.model_validate(data)
    except ValidationError as e:
        details = "; ".join(f"{'.'.join(str(x) for x in err['loc']) or '(root)'}: {err['msg']}" for err in e.errors())
        raise NoteError(f"{path}: {details}") from e
