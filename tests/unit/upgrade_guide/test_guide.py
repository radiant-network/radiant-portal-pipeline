import pytest

from scripts.upgrade_guide.__main__ import main
from scripts.upgrade_guide.guide import (
    Git,
    GuideError,
    build_guide,
    render_guide,
    render_preview,
    render_release_section,
)
from tests.unit.upgrade_guide.conftest import note


@pytest.fixture
def history(repo):
    """v1.0.0 has no notes folder; v1.1.0 adds it; then notes over v1.2.0, v1.3.0 and main.

    - v1.1.0: the notes folder, no note
    - v1.2.0: RAD-1 (#11, after), then RAD-2 (#12, before + after)
    - v1.3.0: no note
    - after v1.3.0: RAD-4 (#14, before), not released
    """
    repo.commit("feat: first (#1)", {"a.txt": "a"})
    repo.tag("v1.0.0")
    repo.commit("feat: RAD-0 notes folder (#10)", {"upgrade-notes/README.md": "notes"})
    repo.tag("v1.1.0")
    repo.commit("fix: RAD-1 first (#11)", {"upgrade-notes/RAD-1.yaml": note("after", "Do one.")})
    repo.commit(
        "feat: RAD-2 second (#12)",
        {
            "upgrade-notes/RAD-2.yaml": (
                "actions:\n"
                "  - when: before\n    do: Pause.\n    preconditions: [No import running.]\n    check: Paused.\n"
                "  - when: after\n    do: Migrate.\n    refs: [radiant/dags/sql/radiant/migrations/RAD-2.sql]\n"
                "    check: Done.\n"
            )
        },
    )
    repo.tag("v1.2.0")
    repo.commit("fix: nothing to do (#13)", {"b.txt": "b"})
    repo.tag("v1.3.0")
    repo.commit("feat: RAD-4 next (#14)", {"upgrade-notes/RAD-4.yaml": note("before", "Do four.")})
    return repo


def guide_text(repo, frm, to, **kwargs):
    return render_guide(build_guide(Git(repo.path), frm, to, **kwargs))


def test_release_order_grouping_version_and_pr(history):
    text = guide_text(history, "v1.1.0", "v1.3.0")
    assert text.index("## v1.2.0") < text.index("## v1.3.0")
    v12 = text[text.index("## v1.2.0") : text.index("## v1.3.0")]
    assert v12.index("### Before the deploy") < v12.index("### After the deploy")
    before = v12[v12.index("### Before") : v12.index("### After")]
    after = v12[v12.index("### After") :]
    assert "**RAD-2** (v1.2.0, PR #12): Pause." in before
    assert "Preconditions: No import running." in before
    assert "RAD-1" not in before
    # Merge order: RAD-1 merged before RAD-2
    assert after.index("**RAD-1** (v1.2.0, PR #11): Do one.") < after.index("**RAD-2** (v1.2.0, PR #12): Migrate.")
    assert "Refs: `radiant/dags/sql/radiant/migrations/RAD-2.sql`" in after
    assert "Check: Done." in after
    assert "RAD-4" not in text
    assert "Warning" not in text


def test_release_with_no_note(history):
    text = guide_text(history, "v1.2.0", "v1.3.0")
    assert "## v1.3.0\n\nNo upgrade action." in text
    assert "## v1.2.0" not in text


def test_from_must_be_older(history):
    with pytest.raises(GuideError, match="v1.3.0 is not older than v1.2.0"):
        build_guide(Git(history.path), "v1.3.0", "v1.2.0")
    with pytest.raises(GuideError, match="v1.2.0 is not older than v1.2.0"):
        build_guide(Git(history.path), "v1.2.0", "v1.2.0")


@pytest.mark.parametrize("frm, to, bad", [("v9.9.9", "v1.2.0", "v9.9.9"), ("v1.1.0", "v1.9.0", "v1.9.0")])
def test_unknown_tag(history, frm, to, bad):
    with pytest.raises(GuideError, match=f"{bad} is not a release tag"):
        build_guide(Git(history.path), frm, to)


def test_unknown_ref(history):
    with pytest.raises(GuideError, match="mian is not a release tag"):
        build_guide(Git(history.path), "v1.1.0", "mian")


def test_warning_before_first_tag_with_notes(history):
    guide = build_guide(Git(history.path), "v1.0.0", "v1.2.0")
    assert len(guide.warnings) == 1
    assert "The releases v1.0.1 have no upgrade notes" not in guide.warnings[0]
    assert "v1.1.0 is the first release with the upgrade-notes/ folder" in guide.warnings[0]
    text = render_guide(guide)
    assert "> **Warning:**" in text
    assert "## v1.1.0\n\nNo upgrade action." in text


def test_warning_when_no_tag_has_notes(repo):
    repo.commit("feat: first (#1)")
    repo.tag("v1.0.0")
    repo.commit("feat: second (#2)")
    repo.tag("v1.0.1")
    guide = build_guide(Git(repo.path), "v1.0.0", "v1.0.1")
    assert "No release up to v1.0.1 has upgrade notes" in guide.warnings[0]
    assert "No upgrade action." in render_guide(guide)


def test_no_warning_for_next_release_only(repo):
    repo.commit("feat: first (#1)")
    repo.tag("v1.0.0")
    repo.commit("fix: RAD-8 (#2)", {"upgrade-notes/RAD-8.yaml": note(do="Eight.")})
    guide = build_guide(Git(repo.path), "v1.0.0", "HEAD")
    assert guide.warnings == []
    assert "**RAD-8** (Next release, PR #2): Eight." in render_guide(guide)


def test_edited_note_shows_new_text(history):
    history.commit("docs: RAD-1 correct the note (#15)", {"upgrade-notes/RAD-1.yaml": note("after", "Do one, fixed.")})
    text = guide_text(history, "v1.1.0", "v1.2.0")
    assert "**RAD-1** (v1.2.0, PR #11): Do one, fixed." in text
    assert "Do one.\n" not in text


def test_notes_text_from_notes_ref(history):
    old = history.git("rev-parse", "v1.2.0").strip()
    history.commit("docs: RAD-1 correct the note (#15)", {"upgrade-notes/RAD-1.yaml": note("after", "Do one, fixed.")})
    text = guide_text(history, "v1.1.0", "v1.2.0", notes_ref=old)
    assert "Do one." in text
    assert "fixed" not in text


def test_unreleased_notes_to_branch(history):
    text = guide_text(history, "v1.3.0", "main", next_version="v1.4.0")
    assert text.startswith("# Upgrade guide from v1.3.0 to v1.4.0")
    assert "## v1.4.0" in text
    assert "**RAD-4** (v1.4.0, PR #14): Do four." in text


def test_unreleased_includes_releases_between(history):
    text = guide_text(history, "v1.1.0", "HEAD")
    assert [line for line in text.splitlines() if line.startswith("## ")] == [
        "## v1.2.0",
        "## v1.3.0",
        "## Next release",
    ]


def test_commit_without_pr_number(repo):
    repo.commit("feat: notes (#1)", {"upgrade-notes/README.md": "x"})
    repo.tag("v1.0.0")
    sha = repo.commit("fix: RAD-5 direct push", {"upgrade-notes/RAD-5.yaml": note()})
    repo.tag("v1.0.1")
    assert f"(v1.0.1, commit {sha[:7]})" in guide_text(repo, "v1.0.0", "v1.0.1")


def test_semver_order_not_lexical(repo):
    repo.commit("feat: notes (#1)", {"upgrade-notes/README.md": "x"})
    repo.tag("v1.0.9")
    repo.commit("fix: RAD-6 (#2)", {"upgrade-notes/RAD-6.yaml": note(do="Six.")})
    repo.tag("v1.0.10")
    text = guide_text(repo, "v1.0.9", "v1.0.10")
    assert "**RAD-6** (v1.0.10, PR #2): Six." in text


def test_release_section(history):
    text = render_release_section(Git(history.path), "v1.2.0")
    assert text.startswith("## Upgrade guide\n")
    assert "**RAD-2** (v1.2.0, PR #12): Pause." in text
    assert "RAD-4" not in text
    assert render_release_section(Git(history.path), "v1.3.0") == "## Upgrade guide\n\nNo upgrade action.\n"


def test_invalid_note_on_main_fails(history):
    history.commit("fix: RAD-7 (#16)", {"upgrade-notes/RAD-7.yaml": "actions: []\n"})
    with pytest.raises(ValueError, match="upgrade-notes/RAD-7.yaml"):
        build_guide(Git(history.path), "v1.1.0", "v1.2.0")


def test_cli_guide(history, capsys):
    assert main(["--repo", str(history.path), "guide", "--from", "v1.0.0", "--to", "v1.2.0"]) == 0
    out, err = capsys.readouterr()
    assert out.startswith("# Upgrade guide from v1.0.0 to v1.2.0")
    assert "warning:" in err


def test_cli_error_names_version(history, capsys):
    assert main(["--repo", str(history.path), "guide", "--from", "v1.2.0", "--to", "v1.1.0"]) == 2
    assert "v1.2.0 is not older than v1.1.0" in capsys.readouterr().err


def test_cli_release_section(history, capsys):
    assert main(["--repo", str(history.path), "release-section", "--tag", "v1.2.0"]) == 0
    assert capsys.readouterr().out.startswith("## Upgrade guide")


def test_preview_renders_notes_of_the_pr():
    text = render_preview(
        {
            "upgrade-notes/RAD-9.yaml": note("before", "Pause."),
            "upgrade-notes/RAD-8.yaml": note("after", "Migrate."),
        },
        pr="42",
    )
    assert text.startswith("## Upgrade guide preview\n")
    assert text.index("### Before the deploy") < text.index("### After the deploy")
    assert "**RAD-9** (next release, PR #42): Pause." in text
    assert "**RAD-8** (next release, PR #42): Migrate." in text


def test_preview_no_note():
    assert "This PR adds or changes no upgrade note." in render_preview({})


def test_preview_lists_invalid_notes():
    text = render_preview({"upgrade-notes/RAD-9.yaml": "actions: []\n", "upgrade-notes/RAD-8.yaml": note()})
    assert "`upgrade-notes/RAD-9.yaml`: not valid" in text
    assert "**RAD-8** (next release): Run the migration." in text


def test_cli_preview(repo, capsys):
    base = repo.commit("feat: base (#1)", {"a.txt": "a"})
    head = repo.commit("fix: RAD-9 x", {"upgrade-notes/RAD-9.yaml": note(do="Nine."), "b.txt": "b"})
    assert main(["--repo", str(repo.path), "preview", "--base", base, "--head", head, "--pr", "7"]) == 0
    assert "**RAD-9** (next release, PR #7): Nine." in capsys.readouterr().out


def test_render_escapes_placeholders_and_lists_preconditions():
    text = render_preview(
        {
            "upgrade-notes/RAD-9.yaml": (
                "actions:\n  - when: after\n    do: Run it in every <tenant>_tenant database.\n"
                "    preconditions: [One., Two.]\n    refs: [<shared_db>]\n    check: A & B.\n"
            )
        }
    )
    assert "every &lt;tenant&gt;_tenant database." in text
    assert "  - Preconditions:\n    - One.\n    - Two." in text
    assert "`<shared_db>`" in text  # Code spans keep the text as is
    assert "Check: A &amp; B." in text
