import os
from unittest.mock import patch

import pytest
from airflow.exceptions import ParamValidationError

from radiant.dags.operators.k8s import NextflowPostprocessing, NextflowSomaticAnnotation

DAG_ID = "radiant-nextflow-somatic-snv-annotation"


def test_dag_is_importable(dag_bag):
    assert dag_bag.get_dag(DAG_ID) is not None
    assert not dag_bag.import_errors


def test_dag_contains_the_driver_and_cleanup_tasks(dag_bag):
    dag = dag_bag.get_dag(DAG_ID)
    assert set(dag.task_ids) == {"run_somatic_annotation", "cleanup_work"}
    assert dag.validate() is None


def test_cleanup_only_runs_after_a_successful_pipeline(dag_bag):
    """The scratch is what `-resume` reads after a failure."""
    cleanup = dag_bag.get_dag(DAG_ID).get_task("cleanup_work")
    assert cleanup.trigger_rule == "all_success"
    assert {t.task_id for t in cleanup.upstream_list} == {"run_somatic_annotation"}


def test_dag_exposes_exactly_input_and_outdir(dag_bag):
    dag = dag_bag.get_dag(DAG_ID)
    assert set(dag.params) == {"input", "outdir"}
    assert dag.params["outdir"] == ""
    with pytest.raises(ParamValidationError):
        dag.params.validate()


def test_retries_make_resume_reachable_and_one_driver_runs_at_a_time(dag_bag):
    dag = dag_bag.get_dag(DAG_ID)
    assert dag.default_args["retries"] >= 1
    assert dag.max_active_runs == 1
    assert dag.schedule is None


def _build(cls, method, env):
    with patch.dict(os.environ, env, clear=True):
        return getattr(cls, method)(input_csv="/ws/in.csv", outdir="/ws/out", run_tag="somatic-run1")


def test_same_pipeline_and_config_as_germline_only_the_params_differ():
    """Same launch script, image and `nextflow-snv-cfg`: same VEP image, cache and flags."""
    env = {"NEXTFLOW_OPERATOR_IMAGE": "registry/launcher:test"}
    somatic = _build(NextflowSomaticAnnotation, "get_run_annotation", env)
    germline = _build(NextflowPostprocessing, "get_run_postprocessing", env)

    assert somatic.arguments == germline.arguments
    assert somatic.image == germline.image == "registry/launcher:test"
    somatic_volumes = {v.name: v for v in somatic.volumes}
    germline_volumes = {v.name: v for v in germline.volumes}
    assert somatic_volumes["nextflow-cfg"].config_map.name == germline_volumes["nextflow-cfg"].config_map.name
    assert somatic_volumes["nextflow-params"].config_map.name == "nextflow-snv-somatic-params"
    assert germline_volumes["nextflow-params"].config_map.name == "nextflow-snv-params"
    assert somatic.name != germline.name

    env_vars = {e.name: e.value for e in somatic.env_vars}
    assert env_vars["RUN_TAG"] == "somatic-run1"
    assert env_vars["NXF_INPUT"] == "/ws/in.csv"


def test_the_somatic_params_configmap_is_overridable():
    env = {"NEXTFLOW_SOMATIC_OPERATOR_PARAMS_CONFIGMAP": "custom-params"}
    op = _build(NextflowSomaticAnnotation, "get_run_annotation", env)
    assert {v.name: v for v in op.volumes}["nextflow-params"].config_map.name == "custom-params"
