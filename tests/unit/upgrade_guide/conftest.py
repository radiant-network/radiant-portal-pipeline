import os
import subprocess
from pathlib import Path

import pytest

NOTE = """actions:
  - when: {when}
    do: {do}
    check: {check}
"""


class TempRepo:
    """A throwaway git repo to build tags and notes in."""

    def __init__(self, path: Path):
        self.path = path
        path.mkdir(parents=True)
        self.git("init", "-q", "-b", "main")
        self.git("config", "user.email", "tests@example.org")
        self.git("config", "user.name", "tests")
        self.git("config", "commit.gpgsign", "false")
        self.git("config", "tag.gpgsign", "false")

    def git(self, *args: str) -> str:
        env = {**os.environ, "GIT_CONFIG_GLOBAL": os.devnull, "GIT_CONFIG_NOSYSTEM": "1"}
        return subprocess.run(
            ["git", "-C", str(self.path), *args], check=True, capture_output=True, text=True, env=env
        ).stdout

    def write(self, rel: str, text: str) -> None:
        f = self.path / rel
        f.parent.mkdir(parents=True, exist_ok=True)
        f.write_text(text)

    def commit(self, subject: str, files: dict[str, str] | None = None, delete: list[str] | None = None) -> str:
        for rel, text in (files or {}).items():
            self.write(rel, text)
        for rel in delete or []:
            self.git("rm", "-q", rel)
        self.git("add", "-A")
        self.git("commit", "-q", "--allow-empty", "-m", subject)
        return self.git("rev-parse", "HEAD").strip()

    def tag(self, name: str) -> None:
        self.git("tag", name)


def note(when: str = "after", do: str = "Run the migration.", check: str = "Query 0 returns no row.") -> str:
    return NOTE.format(when=when, do=do, check=check)


@pytest.fixture
def repo(tmp_path) -> TempRepo:
    return TempRepo(tmp_path / "repo")
