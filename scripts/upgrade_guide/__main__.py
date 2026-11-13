"""Command line of the upgrade guide. Run from the repo root: ``python -m scripts.upgrade_guide <command>``.

Commands:
    guide            The guide between two versions (``make upgrade-guide FROM=<vA> TO=<vB>``).
    release-section  The "Upgrade guide" section of the GitHub release of one tag.
    check            The rules of the PR check, on the diff between two commits.
    preview          How the notes of a PR will show in the guide, for the job summary of the PR check.
"""

import argparse
import sys
from pathlib import Path

from scripts.upgrade_guide.check import ChangedFile, check_pr, parse_name_status
from scripts.upgrade_guide.guide import (
    Git,
    GuideError,
    build_guide,
    github_repo_url,
    render_guide,
    render_preview,
    render_release_section,
)
from scripts.upgrade_guide.notes import NoteError, is_note_path


def _guide(args: argparse.Namespace) -> int:
    git = Git(args.repo)
    guide = build_guide(git, args.from_version, args.to, notes_ref=args.notes_ref, next_version=args.next_version)
    for warning in guide.warnings:
        print(f"warning: {warning}", file=sys.stderr)
    sys.stdout.write(render_guide(guide, repo_url=github_repo_url()))
    return 0


def _release_section(args: argparse.Namespace) -> int:
    sys.stdout.write(
        render_release_section(Git(args.repo), args.tag, notes_ref=args.notes_ref, repo_url=github_repo_url())
    )
    return 0


def _changed_files(git: Git, base: str, head: str) -> tuple[list[ChangedFile], dict[str, str]]:
    """The files a PR changes, and the text at its head of each note it adds or changes."""
    files = parse_name_status(git.out("diff", "--name-status", "--no-renames", f"{base}...{head}"))
    note_texts = {
        f.path: git.out("show", f"{head}:{f.path}") for f in files if f.status in ("A", "M") and is_note_path(f.path)
    }
    return files, note_texts


def _preview(args: argparse.Namespace) -> int:
    _, note_texts = _changed_files(Git(args.repo), args.base, args.head)
    sys.stdout.write(render_preview(note_texts, pr=args.pr, ref=args.head, repo_url=github_repo_url()))
    return 0


def _check(args: argparse.Namespace) -> int:
    git = Git(args.repo)
    files, note_texts = _changed_files(git, args.base, args.head)
    body = Path(args.body_file).read_text(encoding="utf-8") if args.body_file else ""
    errors = check_pr(args.title, body, files, note_texts)
    for error in errors:
        print(f"error: {error}", file=sys.stderr)
    if not errors:
        print("Upgrade note check passed.", file=sys.stderr)
    return 1 if errors else 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m scripts.upgrade_guide", description=__doc__.split("\n")[0])
    parser.add_argument("--repo", default=".", help="Path of the git repo (default: the current directory).")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("guide", help="Print the guide between two versions, as Markdown.")
    p.add_argument("--from", dest="from_version", required=True, help="The version deployed now (vX.Y.Z tag).")
    p.add_argument(
        "--to", required=True, help="The version to deploy (vX.Y.Z tag), or a branch or commit for the next release."
    )
    p.add_argument("--notes-ref", default="HEAD", help="The ref the note text is read from (default: HEAD).")
    p.add_argument("--next-version", help="Name of the section of the notes not released yet (--to not a tag).")
    p.set_defaults(func=_guide)

    p = sub.add_parser("release-section", help="Print the 'Upgrade guide' section of one release.")
    p.add_argument("--tag", required=True, help="The release tag (vX.Y.Z).")
    p.add_argument("--notes-ref", default="HEAD", help="The ref the note text is read from (default: HEAD).")
    p.set_defaults(func=_release_section)

    p = sub.add_parser("check", help="Check that a PR has the upgrade note it needs.")
    p.add_argument("--base", required=True, help="The base commit of the PR.")
    p.add_argument("--head", required=True, help="The head commit of the PR.")
    p.add_argument("--title", required=True, help="The PR title.")
    p.add_argument("--body-file", help="A file that holds the PR description.")
    p.set_defaults(func=_check)

    p = sub.add_parser("preview", help="Print how the notes a PR adds or changes will show in the guide.")
    p.add_argument("--base", required=True, help="The base commit of the PR.")
    p.add_argument("--head", required=True, help="The head commit of the PR.")
    p.add_argument("--pr", help="The PR number.")
    p.set_defaults(func=_preview)

    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except (GuideError, NoteError) as e:
        print(f"error: {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
