"""Rules of the PR check: when a PR needs an upgrade note, and what a note must not hold.

The notes are public (the guide goes to the job summary, the release PR and the GitHub release), so a note must hold
no address, account or bucket of a deployment. Placeholders follow the migration files: ``<shared_db>``,
``s3://<bucket>/...``.
"""

import ipaddress
import re
from dataclasses import dataclass

from scripts.upgrade_guide.notes import NOTES_DIR, NoteError, is_note_path, parse_note

MIGRATION_PATH = re.compile(r"^radiant/dags/sql/(?:.+/)?migrations/.+$")

# Header of a conventional commit, as pr-title.yml and commit-subject.yml read it
TITLE = re.compile(r"^(?P<type>[A-Za-z0-9_]+)(?:\([^)]*\))?(?P<breaking>!)?: .+")
# The types that open a release PR by themselves (release-please `simple`); the others are hidden
RELEASED_TYPES = frozenset({"feat", "fix", "perf", "revert"})
BREAKING_FOOTER = re.compile(r"^BREAKING[ -]CHANGE: ", re.MULTILINE)

# Public hosts a note may link to: GitHub and the docs of the tools the ETL runs. Exact host names.
ALLOWED_HOSTS = frozenset(
    {
        "github.com",
        "docs.github.com",
        "docs.starrocks.io",
        "airflow.apache.org",
        "iceberg.apache.org",
        "py.iceberg.apache.org",
        "www.nextflow.io",
        "nextflow.io",
        "docs.seqera.io",
        "docs.getdbt.com",
        "docs.aws.amazon.com",
        "kubernetes.io",
    }
)

URL = re.compile(r"\b[a-zA-Z][a-zA-Z0-9+.-]*://[^\s'\"<>`]+")
# Not inside a longer dotted number (version 1.2.3.4.5), but a final period of a sentence is allowed
IPV4 = re.compile(r"(?<![\w.])(?:\d{1,3}\.){3}\d{1,3}(?!\w|\.\d)")
IPV6_CANDIDATE = re.compile(r"(?<![\w:])[0-9A-Fa-f]{0,4}(?::[0-9A-Fa-f]{0,4}){2,7}(?![\w:])")
ACCOUNT_ID = re.compile(r"(?<![0-9A-Za-z])\d{12}(?![0-9A-Za-z])")
S3_BUCKET = re.compile(r"\bs3a?://(?P<bucket>[^/\s'\"`]*)")
S3_ARN = re.compile(r"\barn:aws[\w-]*:s3:::(?P<bucket>[^/\s'\"`]*)")
PLACEHOLDER = re.compile(r"^<[^<>\s]+>$")


@dataclass(frozen=True)
class ChangedFile:
    """One line of ``git diff --name-status --no-renames``: A (added), M (modified), D (deleted)."""

    status: str
    path: str


def parse_name_status(text: str) -> list[ChangedFile]:
    files = []
    for line in text.splitlines():
        if not line.strip():
            continue
        status, _, path = line.partition("\t")
        files.append(ChangedFile(status=status[0], path=path))
    return files


def is_misplaced_note(path: str) -> bool:
    """True for a file under ``upgrade-notes/`` that is neither a note nor the folder's README (a subfolder, .YAML)."""
    return path.startswith(f"{NOTES_DIR}/") and path != f"{NOTES_DIR}/README.md" and not is_note_path(path)


def is_breaking(title: str, body: str) -> bool:
    m = TITLE.match(title.strip())
    return bool(m and m["breaking"]) or bool(BREAKING_FOOTER.search(body or ""))


def is_hidden_type(title: str) -> bool:
    """True when the PR, squash-merged, opens no release PR (its note would wait for the next release)."""
    m = TITLE.match(title.strip())
    if not m:
        return False  # pr-title.yml reports a bad title
    return m["type"] not in RELEASED_TYPES and not m["breaking"]


def sensitive_values(text: str) -> list[str]:
    """What a public note must not hold: IPs, URLs of unknown hosts, account numbers, real bucket names.

    Only the host of a URL with a scheme is checked: a bare host name (``db.example:9030``) looks like a file reference
    (``operator.py:142``), so the reviewer of the note must catch it.
    """
    found = []
    for m in URL.finditer(text):
        url = m.group(0)
        scheme, _, rest = url.partition("://")
        if scheme.lower() in ("s3", "s3a"):
            continue  # S3 paths: bucket rule below
        host = rest.split("/", 1)[0].split("@")[-1].split(":")[0].lower()
        if host not in ALLOWED_HOSTS:
            found.append(f"a URL whose host is not on the allowlist: {host or url}")
    for m in IPV4.finditer(text):
        try:
            ipaddress.IPv4Address(m.group(0))
        except ValueError:
            continue
        found.append(f"an IP address: {m.group(0)}")
    for m in IPV6_CANDIDATE.finditer(text):
        candidate = m.group(0)
        if candidate.count(":") < 2 or not re.search(r"[0-9A-Fa-f]", candidate):
            continue
        try:
            ipaddress.IPv6Address(candidate)
        except ValueError:
            continue
        found.append(f"an IP address: {candidate}")
    for m in ACCOUNT_ID.finditer(text):
        found.append(f"a 12-digit cloud account number: {m.group(0)}")
    for m in S3_BUCKET.finditer(text):
        if not PLACEHOLDER.match(m["bucket"]):
            found.append(f"an s3:// path with a real bucket name: {m['bucket'] or '(empty)'} (use s3://<bucket>/...)")
    for m in S3_ARN.finditer(text):
        if not PLACEHOLDER.match(m["bucket"]):
            found.append(f"an S3 ARN with a real bucket name: {m['bucket'] or '(empty)'} (use arn:aws:s3:::<bucket>)")
    return found


def check_pr(title: str, body: str, files: list[ChangedFile], note_texts: dict[str, str]) -> list[str]:
    """The errors of one PR. Empty means the check passes.

    Args:
        title: the PR title.
        body: the PR description.
        files: the files the PR changes, renames split into a deletion and an addition.
        note_texts: the content, at the PR head, of each note the PR adds or changes.
    """
    errors = []
    notes = [f for f in files if is_note_path(f.path)]
    added_or_changed = [f for f in notes if f.status in ("A", "M")]
    added = [f for f in notes if f.status == "A"]

    for f in files:
        if f.status in ("A", "M") and is_misplaced_note(f.path):
            errors.append(
                f"{f.path}: not an upgrade note, so neither the check nor the guide reads it. A note is a .yaml file "
                f"directly in {NOTES_DIR}/ (README.md is the only other file there)."
            )

    for f in notes:
        if f.status == "D":
            errors.append(
                f"{f.path}: the PR deletes an upgrade note. The guide of its release would lose its actions: "
                f"edit the note instead."
            )

    migrations = [f.path for f in files if f.status in ("A", "M") and MIGRATION_PATH.match(f.path)]
    if migrations and not added_or_changed:
        errors.append(
            f"The PR adds or changes a migration ({', '.join(migrations)}) and no upgrade note. "
            f"Add a note in upgrade-notes/ that says when to run it."
        )

    if is_breaking(title, body) and not added:
        errors.append(
            "The PR has a breaking change (`!` in the title or a `BREAKING CHANGE:` footer) and adds no upgrade "
            "note. Add a note in upgrade-notes/ with the actions the operator must do."
        )

    if added and is_hidden_type(title):
        errors.append(
            f"The PR adds an upgrade note but its type opens no release (title: {title.strip()!r}). Its note would "
            f"wait for the next feat or fix. Use feat, fix, perf or revert, or add `!` for a breaking change."
        )

    for f in added_or_changed:
        text = note_texts.get(f.path, "")
        try:
            parse_note(f.path, text)
        except NoteError as e:
            errors.append(str(e))
        for value in sensitive_values(text):
            errors.append(f"{f.path}: the note is public and holds {value}. Use a <placeholder>.")
    return errors
