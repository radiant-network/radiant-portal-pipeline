import pytest

from scripts.upgrade_guide.__main__ import main
from scripts.upgrade_guide.check import ChangedFile, check_pr, is_hidden_type, parse_name_status, sensitive_values
from tests.unit.upgrade_guide.conftest import note

MIGRATION = "radiant/dags/sql/radiant/migrations/RAD-9_x.sql"
NOTE_PATH = "upgrade-notes/RAD-9.yaml"


def files(*pairs):
    return [ChangedFile(status, path) for status, path in pairs]


def test_plain_pr_passes():
    assert check_pr("fix: RAD-9 typo", "", files(("M", "radiant/dags/import_part.py")), {}) == []


def test_migration_without_note_fails():
    errors = check_pr("fix: RAD-9 x", "", files(("A", MIGRATION)), {})
    assert len(errors) == 1
    assert MIGRATION in errors[0]


def test_migration_with_note_passes():
    assert check_pr("fix: RAD-9 x", "", files(("A", MIGRATION), ("A", NOTE_PATH)), {NOTE_PATH: note()}) == []


def test_migration_with_edited_note_passes():
    assert check_pr("fix: RAD-9 x", "", files(("M", MIGRATION), ("M", NOTE_PATH)), {NOTE_PATH: note()}) == []


@pytest.mark.parametrize(
    "path", ["radiant/dags/sql/clinical/migrations/x.sql", "radiant/dags/sql/open_data/migrations/sub/x.sql"]
)
def test_any_migrations_folder(path):
    assert check_pr("fix: x", "", files(("M", path)), {})


def test_breaking_title_without_note_fails():
    errors = check_pr("feat!: RAD-9 drop a column", "", files(("M", "a.py")), {})
    assert any("breaking change" in e for e in errors)


def test_breaking_scope_title_without_note_fails():
    assert check_pr("feat(cnv)!: RAD-9 x", "", files(("M", "a.py")), {})


def test_breaking_footer_without_note_fails():
    body = "## Context\n\nText.\n\nBREAKING CHANGE: the column is gone.\n"
    assert any("breaking change" in e for e in check_pr("feat: RAD-9 x", body, files(("M", "a.py")), {}))


def test_breaking_with_note_passes():
    assert check_pr("feat!: RAD-9 x", "", files(("M", "a.py"), ("A", NOTE_PATH)), {NOTE_PATH: note()}) == []


def test_breaking_word_in_text_is_not_a_footer():
    assert check_pr("feat: x", "No BREAKING CHANGE: here, mid-line.", files(("M", "a.py")), {}) == []


def test_deleted_note_fails():
    errors = check_pr("fix: x", "", files(("D", NOTE_PATH)), {})
    assert any("deletes an upgrade note" in e for e in errors)


def test_note_in_hidden_type_fails():
    errors = check_pr("chore: RAD-9 x", "", files(("A", NOTE_PATH)), {NOTE_PATH: note()})
    assert any("opens no release" in e for e in errors)


def test_edit_note_in_hidden_type_passes():
    assert check_pr("docs: RAD-9 correct the note", "", files(("M", NOTE_PATH)), {NOTE_PATH: note()}) == []


@pytest.mark.parametrize(
    "title, hidden",
    [("feat: x", False), ("fix(a): x", False), ("perf: x", False), ("revert: x", False), ("chore!: x", False),
     ("chore: x", True), ("docs: x", True), ("ci(release): x", True), ("not conventional", False)],
)  # fmt: skip
def test_is_hidden_type(title, hidden):
    assert is_hidden_type(title) is hidden


def test_invalid_note_fails():
    errors = check_pr("fix: x", "", files(("A", NOTE_PATH)), {NOTE_PATH: "actions:\n  - when: during\n"})
    assert any(e.startswith(f"{NOTE_PATH}:") for e in errors)


def test_readme_in_notes_folder_is_not_a_note():
    assert check_pr("docs: x", "", files(("M", "upgrade-notes/README.md")), {}) == []


@pytest.mark.parametrize(
    "path", ["upgrade-notes/sub/RAD-9.yaml", "upgrade-notes/RAD-9.YAML", "upgrade-notes/RAD-9.md"]
)
def test_misplaced_note_fails(path):
    errors = check_pr("fix: x", "", files(("A", path)), {})
    assert any(e.startswith(f"{path}: not an upgrade note") for e in errors)


def test_deleted_misplaced_file_passes():
    assert check_pr("fix: x", "", files(("D", "upgrade-notes/sub/RAD-9.yaml")), {}) == []


@pytest.mark.parametrize(
    "value, kind",
    [
        ("Connect to 10.12.0.4 first.", "IP address"),
        ("Connect to 10.12.0.4.", "IP address: 10.12.0.4"),
        ("Grant arn:aws:s3:::prod-datalake/x.", "S3 ARN with a real bucket name: prod-datalake"),
        ("Host fe80::1ff:fe23:4567:890a.", "IP address"),
        ("See https://starrocks.internal.example/x.", "allowlist: starrocks.internal.example"),
        ("Use jdbc:mysql://db.example:9030/x.", "allowlist: db.example"),
        ("Role arn:aws:iam::123456789012:role/x.", "account number: 123456789012"),
        ("Copy s3://prod-datalake/x.", "real bucket name: prod-datalake"),
        ("Copy s3a://prod-datalake/x.", "real bucket name: prod-datalake"),
    ],
)
def test_sensitive_values_found(value, kind):
    found = sensitive_values(value)
    assert any(kind in f for f in found), found


@pytest.mark.parametrize(
    "value",
    [
        "See https://docs.starrocks.io/docs/sql-reference/ and https://github.com/radiant-network/x.",
        "Copy s3://<bucket>/x to <shared_db>.",
        "Grant arn:aws:s3:::<bucket>/x.",
        "See radiant/tasks/starrocks/operator.py:142.",
        "Release v1.12.3, commit a53742c0123456789012ab, 14400 s, 10:30:00.",
        "Run std::sort, then version 1.2.3.4.5.",
    ],
)
def test_sensitive_values_clean(value):
    assert sensitive_values(value) == []


def test_sensitive_note_fails():
    text = note(do="Copy s3://prod-datalake/x.")
    errors = check_pr("fix: x", "", files(("A", NOTE_PATH)), {NOTE_PATH: text})
    assert any("prod-datalake" in e for e in errors)


def test_parse_name_status():
    assert parse_name_status("A\tupgrade-notes/RAD-1.yaml\nM\ta.py\nD\tb.py\n") == files(
        ("A", "upgrade-notes/RAD-1.yaml"), ("M", "a.py"), ("D", "b.py")
    )


def test_cli_check_on_repo(repo, tmp_path, capsys):
    base = repo.commit("feat: base (#1)", {"a.txt": "a"})
    repo.git("checkout", "-q", "-b", "feature")
    head = repo.commit("fix: RAD-9 x", {MIGRATION: "-- WHERE / WHEN"})
    body = tmp_path / "body.md"
    body.write_text("")
    args = ["--repo", str(repo.path), "check", "--base", base, "--head", head, "--title", "fix: RAD-9 x"]
    assert main([*args, "--body-file", str(body)]) == 1
    assert MIGRATION in capsys.readouterr().err

    head = repo.commit("fix: RAD-9 note", {NOTE_PATH: note()})
    args[args.index("--head") + 1] = head
    assert main(args) == 0

    head = repo.commit("fix: rename", {"upgrade-notes/RAD-9-2.yaml": note()}, delete=[NOTE_PATH])
    args[args.index("--head") + 1] = head
    assert main(args) == 0  # Not deleted relative to the base: the PR adds RAD-9-2 only
