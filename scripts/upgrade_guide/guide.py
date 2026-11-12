"""Builds the upgrade guide between two releases from the notes in ``upgrade-notes/`` and the ``vX.Y.Z`` tags.

- The text of each note comes from ``notes_ref`` (``HEAD``: in CI, the tip of ``main``), so a corrected note reaches
  the guide of a release already made.
- The release of a note is the first ``vX.Y.Z`` tag that contains the commit that added the note file (the PR's
  squash commit). A note in no tag waits for the next release.
- The merge order of the notes is the order of these commits in the history.
"""

import re
import subprocess
from dataclasses import dataclass, field
from pathlib import Path

from scripts.upgrade_guide.notes import NOTES_DIR, Action, NoteError, is_note_path, parse_note, ticket_of

TAG_PATTERN = re.compile(r"^v(\d+)\.(\d+)\.(\d+)$")
# The PR number that GitHub appends to a squash commit subject
PR_NUMBER = re.compile(r"\(#(\d+)\)\s*$")

NO_ACTION = "No upgrade action."
WHEN_HEADINGS = (("before", "Before the deploy"), ("after", "After the deploy"))


class GuideError(Exception):
    """An input the guide cannot be built from. The message names the version."""


@dataclass(frozen=True)
class NoteRecord:
    """One note file with the commit that added it."""

    path: str
    ticket: str
    actions: tuple[Action, ...]
    commit: str
    order: int  # Position of the commit in the history: smaller is merged earlier
    pr: str | None  # "#123", or None when the subject has no PR number
    release: str | None  # First tag that contains the commit, None when not released yet


@dataclass
class Release:
    """One section of the guide."""

    version: str
    notes: list[NoteRecord] = field(default_factory=list)


class Git:
    """The few git commands the guide needs, in one repo."""

    def __init__(self, repo: str | Path = "."):
        self.repo = str(repo)

    def run(self, *args: str, check: bool = True) -> subprocess.CompletedProcess:
        return subprocess.run(
            ["git", "-C", self.repo, *args], check=check, capture_output=True, text=True, encoding="utf-8"
        )

    def out(self, *args: str) -> str:
        return self.run(*args).stdout

    def rev(self, ref: str) -> str | None:
        """The commit of ``ref``, or None when it is not a ref."""
        res = self.run("rev-parse", "--verify", "--quiet", f"{ref}^{{commit}}", check=False)
        return res.stdout.strip() if res.returncode == 0 else None

    def is_ancestor(self, older: str, newer: str) -> bool:
        return self.run("merge-base", "--is-ancestor", older, newer, check=False).returncode == 0

    def release_tags(self) -> list[str]:
        """The ``vX.Y.Z`` tags, oldest version first."""
        tags = [t for t in self.out("tag", "--list", "v*").split() if TAG_PATTERN.match(t)]
        return sorted(tags, key=version_key)


def version_key(tag: str) -> tuple[int, int, int]:
    m = TAG_PATTERN.match(tag)
    if not m:
        raise GuideError(f"{tag} is not a vX.Y.Z version")
    return int(m[1]), int(m[2]), int(m[3])


def load_notes(git: Git, notes_ref: str) -> list[NoteRecord]:
    """Every note on ``notes_ref`` with its commit, PR and release, in merge order."""
    listing = git.out("ls-tree", "--name-only", notes_ref, "--", f"{NOTES_DIR}/").split("\n")
    paths = [p for p in listing if p and is_note_path(p)]
    if not paths:
        return []

    order = {sha: i for i, sha in enumerate(git.out("rev-list", "--topo-order", "--reverse", notes_ref).split())}
    records = []
    for path in paths:
        note = parse_note(path, git.out("show", f"{notes_ref}:{path}"))
        # The oldest commit that added the file. The PR check forbids deleting a note, so there is one.
        added = git.out("log", "--diff-filter=A", "--format=%H%x09%s", notes_ref, "--", path).strip().split("\n")
        sha, _, subject = added[-1].partition("\t")
        pr = PR_NUMBER.search(subject)
        containing = [t for t in git.out("tag", "--contains", sha, "--list", "v*").split() if TAG_PATTERN.match(t)]
        records.append(
            NoteRecord(
                path=path,
                ticket=ticket_of(path),
                actions=tuple(note.actions),
                commit=sha,
                order=order[sha],
                pr=f"#{pr[1]}" if pr else None,
                release=min(containing, key=version_key) if containing else None,
            )
        )
    return sorted(records, key=lambda r: (r.order, r.path))


def first_tag_with_notes(git: Git, tags: list[str]) -> str | None:
    """The oldest tag whose tree has the ``upgrade-notes/`` folder."""
    for tag in tags:
        if git.out("ls-tree", "--name-only", tag, "--", NOTES_DIR).strip():
            return tag
    return None


@dataclass
class Guide:
    from_version: str
    to_version: str
    releases: list[Release]
    warnings: list[str]


def build_guide(
    git: Git, from_version: str, to: str, notes_ref: str = "HEAD", next_version: str | None = None
) -> Guide:
    """The releases after ``from_version`` up to ``to`` included.

    ``to`` is a ``vX.Y.Z`` tag, or a branch or commit (``HEAD``, ``main``): then the guide ends with the notes not
    released yet, in a section named ``next_version`` (or "Next release").
    """
    tags = git.release_tags()
    if from_version not in tags:
        raise GuideError(f"{from_version} is not a release tag (vX.Y.Z)")

    to_sha = None
    if TAG_PATTERN.match(to):
        if to not in tags:
            raise GuideError(f"{to} is not a release tag (vX.Y.Z)")
        if version_key(from_version) >= version_key(to):
            raise GuideError(f"{from_version} is not older than {to}")
        in_range = [t for t in tags if version_key(from_version) < version_key(t) <= version_key(to)]
        unreleased = None
    else:
        to_sha = git.rev(to)
        if to_sha is None:
            raise GuideError(f"{to} is not a release tag (vX.Y.Z), nor a branch or a commit")
        if not git.is_ancestor(from_version, to_sha):
            raise GuideError(f"{from_version} is not older than {to}")
        in_range = [t for t in tags if version_key(t) > version_key(from_version) and git.is_ancestor(t, to_sha)]
        unreleased = Release(version=next_version or "Next release")

    releases = {t: Release(version=t) for t in in_range}
    for record in load_notes(git, notes_ref):
        if record.release in releases:
            releases[record.release].notes.append(record)
        elif record.release is None and to_sha and git.is_ancestor(record.commit, to_sha):
            unreleased.notes.append(record)

    warnings = []
    first = first_tag_with_notes(git, tags)
    if first is None:
        # Before the first tag with notes, only the section of the next release (``to`` not a tag) can hold notes
        if in_range:
            warnings.append(
                f"No release up to {in_range[-1]} has upgrade notes: the actions of the releases after "
                f"{from_version} up to {in_range[-1]} are not listed. Read their changelog and migration files."
            )
    elif version_key(from_version) < version_key(first):
        without = [t for t in in_range if version_key(t) < version_key(first)]
        listed = f"The releases {', '.join(without)} have no upgrade notes and " if without else ""
        warnings.append(
            f"{listed}{first} is the first release with the upgrade-notes/ folder: a PR merged before the folder "
            f"existed has no note. Read the changelog and the migration files of these releases."
        )

    ordered = [releases[t] for t in in_range] + ([unreleased] if unreleased else [])
    return Guide(from_version=from_version, to_version=next_version or to, releases=ordered, warnings=warnings)


def _text(value: str) -> str:
    """A note text as Markdown.

    GitHub drops ``<tenant>`` and the other placeholders as unknown HTML tags, and reads ``mother_*, father_*`` as
    emphasis.
    """
    return value.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("*", "\\*")


# The end of a sentence: a period, then a capital or a digit. "e.g. the" and "init_iceberg_table.py" do not split.
SENTENCE_END = re.compile(r"(?<=[.!?])\s+(?=[A-Z0-9])")


def split_do(do: str) -> tuple[str, str]:
    """The step title (the first sentence of ``do``) and the rest of the text."""
    parts = SENTENCE_END.split(do.strip(), maxsplit=1)
    return parts[0], parts[1] if len(parts) > 1 else ""


def render_actions(release: Release, heading: str) -> list[str]:
    """The actions of one release as numbered steps, grouped by ``when``, in merge order.

    Each step: the first sentence of ``do`` in bold, the ticket and the PR, the rest of ``do``, then the
    preconditions, the check and the refs. The numbers run on from "Before" to "After", so a step has one number in
    the release. ``heading`` prefixes the group titles.
    """
    lines = []
    number = 0
    for when, title in WHEN_HEADINGS:
        items = [(note, action) for note in release.notes for action in note.actions if action.when == when]
        if not items:
            continue
        lines += [f"{heading} {title}", ""]
        for note, action in items:
            number += 1
            source = [note.ticket]
            if note.pr:
                source.append(note.pr)
            elif note.commit:
                source.append(note.commit[:7])
            step_title, rest = split_do(action.do)
            lines.append(f"{number}. **{_text(step_title)}** · {' · '.join(source)}")
            if rest:
                lines.append(f"   {_text(rest)}")
            if len(action.preconditions) == 1:
                lines.append(f"   - **Before you start:** {_text(action.preconditions[0])}")
            elif action.preconditions:
                lines.append("   - **Before you start:**")
                lines += [f"     - {_text(p)}" for p in action.preconditions]
            lines.append(f"   - **Check:** {_text(action.check)}")
            if action.refs:
                lines.append(f"   - **Refs:** {', '.join(f'`{r}`' for r in action.refs)}")
            lines.append("")
    if not lines:
        lines = [NO_ACTION, ""]
    return lines


def _count(releases: list[Release]) -> str:
    """ "15 actions: 6 before the deploy, 9 after" over the releases of a guide."""
    actions = [a for r in releases for n in r.notes for a in n.actions]
    if not actions:
        return "no upgrade action"
    before = sum(a.when == "before" for a in actions)
    plural = "s" if len(actions) > 1 else ""
    return f"{len(actions)} action{plural}: {before} before the deploy, {len(actions) - before} after"


def render_preview(notes: dict[str, str], pr: str | None = None) -> str:
    """How the notes a PR adds or changes will show in the guide of the next release, for the PR's job summary.

    Args:
        notes: the path and the text, at the PR head, of each note the PR adds or changes.
        pr: the PR number, without ``#``.
    """
    lines = ["## Upgrade guide preview", ""]
    if not notes:
        return "\n".join([*lines, "This PR adds or changes no upgrade note.", ""])
    records, invalid = [], []
    for path in sorted(notes):
        try:
            note = parse_note(path, notes[path])
        except NoteError:
            invalid.append(path)
            continue
        records.append(
            NoteRecord(
                path=path,
                ticket=ticket_of(path),
                actions=tuple(note.actions),
                commit="",
                order=0,
                pr=f"#{pr}" if pr else None,
                release=None,
            )
        )
    lines += [
        "The steps of this PR's notes, as the release PR and the guide will show them once merged.",
        "",
    ]
    if records:
        lines += render_actions(Release(version="next release", notes=records), "###")
    for path in invalid:
        lines += [f"- `{path}`: not valid, see the errors of the check.", ""]
    return "\n".join(lines).rstrip() + "\n"


def render_guide(guide: Guide) -> str:
    """The guide, in one ``##`` section: it goes into a PR description, a job summary or a terminal."""
    lines = [f"## Upgrade guide: {guide.from_version} → {guide.to_version}", ""]
    if guide.releases:
        lines += [
            f"{len(guide.releases)} release{'s' if len(guide.releases) > 1 else ''}, {_count(guide.releases)}. "
            f"Do the steps in order, release by release.",
            "",
        ]
    for warning in guide.warnings:
        lines += ["> [!WARNING]", f"> {_text(warning)}", ""]
    if not guide.releases:
        lines += ["No release in this range.", ""]
    for release in guide.releases:
        lines += [f"### {release.version}", ""]
        lines += render_actions(release, "####")
    return "\n".join(lines).rstrip() + "\n"


def render_release_section(git: Git, tag: str, notes_ref: str = "HEAD") -> str:
    """The "Upgrade guide" section of the GitHub release of ``tag``."""
    tags = git.release_tags()
    if tag not in tags:
        raise GuideError(f"{tag} is not a release tag (vX.Y.Z)")
    release = Release(version=tag, notes=[n for n in load_notes(git, notes_ref) if n.release == tag])
    lines = ["## Upgrade guide", ""] + render_actions(release, "###")
    return "\n".join(lines).rstrip() + "\n"
