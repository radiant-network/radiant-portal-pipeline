"""`snv_engine_template`: the run's `snv_engine` param wins, then the Airflow environment, else empty (python)."""

import os
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from jinja2 import Environment
from jinja2.nativetypes import NativeEnvironment

from radiant.dags.operators.utils import snv_engine_template


@pytest.mark.parametrize("env_cls", [Environment, NativeEnvironment], ids=["str", "native"])
@pytest.mark.parametrize(
    ("airflow_env", "conf", "expected"),
    [
        (None, {}, ""),
        (None, {"snv_engine": None}, ""),
        (None, {"snv_engine": "rust"}, "rust"),
        ("rust", {}, "rust"),
        ("rust", {"snv_engine": None}, "rust"),
        ("rust", {"snv_engine": "python"}, "python"),
    ],
    ids=["nothing", "param_empty", "param", "environment", "param_empty_environment", "param_over_environment"],
)
def test_snv_engine_template(env_cls, airflow_env, conf, expected):
    env = {} if airflow_env is None else {"RADIANT_SNV_ENGINE": airflow_env}
    with patch.dict(os.environ, env, clear=True):
        template = snv_engine_template()
    rendered = env_cls().from_string(template).render(dag_run=SimpleNamespace(conf=conf))
    assert rendered == expected
