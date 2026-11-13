import pytest

from scripts.upgrade_guide.notes import NoteError, is_note_path, parse_note, ticket_of

VALID = """actions:
  - when: before
    do: Pause radiant-case-scheduler.
    preconditions:
      - No radiant-import run is active.
    refs:
      - radiant-case-scheduler
    check: The DAG shows as paused.
  - when: after
    do: Run the migration in each tenant database.
    refs:
      - radiant/dags/sql/radiant/migrations/RAD-1_x.sql
    check: Query 0 returns no row.
"""


def test_valid_note():
    note = parse_note("upgrade-notes/RAD-1.yaml", VALID)
    assert [a.when for a in note.actions] == ["before", "after"]
    assert note.actions[0].preconditions == ["No radiant-import run is active."]
    assert note.actions[1].preconditions == []
    assert note.actions[1].refs == ["radiant/dags/sql/radiant/migrations/RAD-1_x.sql"]


@pytest.mark.parametrize(
    "text, message",
    [
        ("actions:\n  - when: after\n    check: x\n", "actions.0.do"),
        ("actions:\n  - when: after\n    do: x\n", "actions.0.check"),
        ("actions:\n  - do: x\n    check: y\n", "actions.0.when"),
        ("actions:\n  - when: during\n    do: x\n    check: y\n", "actions.0.when"),
        ("actions:\n  - when: after\n    do: '  '\n    check: y\n", "actions.0.do"),
        ("actions:\n  - when: after\n    do: x\n    check: y\n    owner: me\n", "actions.0.owner"),
        ("actions: []\n", "actions"),
        ("", "(root)"),
        ("actions: [\n", "not valid YAML"),
    ],
)
def test_invalid_note(text, message):
    with pytest.raises(NoteError) as e:
        parse_note("upgrade-notes/RAD-1.yaml", text)
    assert "upgrade-notes/RAD-1.yaml" in str(e.value)
    assert message in str(e.value)


@pytest.mark.parametrize("name", ["RAD-171.yaml", "RAD-171-2.yaml", "SJRA-1811-b.yaml"])
def test_valid_file_names(name):
    parse_note(f"upgrade-notes/{name}", VALID)


@pytest.mark.parametrize("name", ["rad-171.yaml", "RAD-171.yml", "notes.yaml", "RAD-171_x.yaml", "RAD-171-X.yaml"])
def test_invalid_file_names(name):
    with pytest.raises(NoteError, match="file name"):
        parse_note(f"upgrade-notes/{name}", VALID)


def test_is_note_path():
    assert is_note_path("upgrade-notes/RAD-1.yaml")
    assert is_note_path("upgrade-notes/RAD-1.yml")
    assert not is_note_path("upgrade-notes/README.md")
    assert not is_note_path("upgrade-notes/sub/RAD-1.yaml")
    assert not is_note_path("other/RAD-1.yaml")


def test_ticket_of():
    assert ticket_of("upgrade-notes/RAD-171-2.yaml") == "RAD-171-2"
