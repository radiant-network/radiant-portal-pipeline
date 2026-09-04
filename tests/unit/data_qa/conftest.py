"""Loaders for the data_qa helper scripts.

`radiant/data_qa/` is a dbt project, not a Python package — it has no `__init__.py` — so the
scripts are loaded straight off their path. `run_qa` imports `run_results_to_junit` as a
sibling (which is how it resolves when run as `python scripts/run_qa.py`), so the scripts
directory has to be importable while it executes.
"""

import importlib.util
import sys
from pathlib import Path

import pytest

SCRIPTS_DIR = Path(__file__).resolve().parents[3] / "radiant" / "data_qa" / "scripts"


def _load(name: str):
    sys.path.insert(0, str(SCRIPTS_DIR))
    try:
        spec = importlib.util.spec_from_file_location(name, SCRIPTS_DIR / f"{name}.py")
        module = importlib.util.module_from_spec(spec)
        # Registered before exec: `run_qa` uses `from __future__ import annotations`, and
        # @dataclass resolves its field types through sys.modules[cls.__module__].
        sys.modules[name] = module
        spec.loader.exec_module(module)
        return module
    finally:
        sys.path.remove(str(SCRIPTS_DIR))
        sys.modules.pop(name, None)


@pytest.fixture
def run_qa():
    return _load("run_qa")


@pytest.fixture
def junit():
    return _load("run_results_to_junit")
