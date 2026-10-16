import pytest
from airflow.models.mappedoperator import MappedOperator

DAG_ID = "radiant-nextflow-somatic-snv-annotation-cases"
PIPELINE_DAG_ID = "radiant-nextflow-snv-postprocessing"


def test_dag_is_importable(dag_bag):
    assert dag_bag.get_dag(DAG_ID) is not None
    assert not dag_bag.import_errors


def test_dag_contains_the_expected_stages(dag_bag):
    dag = dag_bag.get_dag(DAG_ID)
    assert set(dag.task_ids) == {
        "discover_scope",
        "select_tasks",
        "generate_inputs",
        "run_pipeline",
        "collect_outputs",
        "list_tenants",
        "register_tasks",
    }
    assert dag.validate() is None


def test_it_finds_its_own_work(dag_bag):
    dag = dag_bag.get_dag(DAG_ID)
    # The case scheduler starts it; a schedule of its own would race it.
    assert dag.schedule_interval is None
    assert dag.get_task("discover_scope").upstream_list == []
    assert set(dag.params) == {"task_ids", "excluded_tenants", "dry_run"}
    assert dag.params["task_ids"] == []
    assert dag.params["dry_run"] is False


def test_discovery_uses_the_somatic_query(dag_bag):
    """The anti-join is on the somatic annotation task type, and the trigger the ssnv VCF."""
    sql = dag_bag.get_dag(DAG_ID).get_task("discover_scope").sql
    assert "radiant_somatic_annotation" in sql
    assert "'ssnv'" in sql
    assert "'gvcf'" not in sql
    assert "{{ mapping.clinical_case }}" in sql


def test_the_germline_launcher_runs_with_the_somatic_params_set(dag_bag):
    dag = dag_bag.get_dag(DAG_ID)
    run_pipeline = dag.get_task("run_pipeline")
    assert run_pipeline.trigger_dag_id == PIPELINE_DAG_ID
    assert run_pipeline.conf["params_set"] == "somatic"
    assert "somatic" in dag_bag.get_dag(PIPELINE_DAG_ID).params.get_param("params_set").schema["enum"]
    # The launcher runs two drivers at once, so this run never queues behind a germline one.
    assert dag_bag.get_dag(PIPELINE_DAG_ID).max_active_runs >= 2
    assert "generate_inputs" in run_pipeline.trigger_run_id
    assert run_pipeline.wait_for_completion is True
    assert run_pipeline.reset_dag_run is True
    assert {t.task_id for t in run_pipeline.upstream_list} == {"generate_inputs"}
    assert "run_pipeline" in {t.task_id for t in dag.get_task("collect_outputs").upstream_list}


def test_its_paths_never_collide_with_the_germline_cases_dag():
    """The case scheduler gives both cases DAGs one run id, and both trigger the same launcher,
    whose launch dir, work dir and cleanup are keyed by the run tag."""
    from radiant.dags import nextflow_snv_postprocessing_cases as germline
    from radiant.dags import nextflow_somatic_snv_annotation_cases as somatic

    assert somatic.RUN_TAG_PREFIX == "somatic-"
    assert somatic.INPUTS_SUBDIR != germline.INPUTS_SUBDIR
    assert somatic.OUTPUTS_SUBDIR != germline.OUTPUTS_SUBDIR


def test_registration_is_last_and_mapped_over_tenants(dag_bag):
    dag = dag_bag.get_dag(DAG_ID)
    register = dag.get_task("register_tasks")
    assert isinstance(register, MappedOperator)
    assert register.downstream_list == []
    assert {t.task_id for t in register.upstream_list} == {"list_tenants", "collect_outputs", "select_tasks"}
    assert register.partial_kwargs.get("max_active_tis_per_dagrun") == 1


def test_only_one_run_at_a_time(dag_bag):
    assert dag_bag.get_dag(DAG_ID).max_active_runs == 1


def test_the_queries_resolve_from_the_sql_search_path(dag_bag):
    dag = dag_bag.get_dag(DAG_ID)
    assert dag.template_searchpath == ["/opt/airflow/dags/radiant/dags/sql"]
    assert dag.render_template_as_native_obj is True


def _run_generate_inputs(dag_bag, monkeypatch, head_error_code):
    """Call the `generate_inputs` callable with S3 and the Airflow context faked."""
    import boto3
    from airflow.operators import python as airflow_python
    from botocore.exceptions import ClientError

    from tests.unit.nextflow.somatic.conftest import BUCKET, calling_task

    class FakeS3:
        def head_object(self, Bucket, Key):
            if head_error_code:
                raise ClientError({"Error": {"Code": head_error_code}}, "HeadObject")

        def get_paginator(self, _):
            return type("P", (), {"paginate": lambda *a, **k: []})()

        def put_object(self, **_):
            pass

    monkeypatch.setenv("NEXTFLOW_INPUTS_ROOT", BUCKET)
    monkeypatch.setenv("NEXTFLOW_OUTPUTS_ROOT", "s3://mock-nextflow-outputs")
    monkeypatch.setattr(boto3, "client", lambda *_: FakeS3())
    monkeypatch.setattr(airflow_python, "get_current_context", lambda: {"run_id": "manual__2026-10-15"})
    callable_ = dag_bag.get_dag(DAG_ID).get_task("generate_inputs").python_callable
    return callable_([calling_task(70).model_dump()])


def test_generate_inputs_writes_under_a_somatic_run_tag(dag_bag, monkeypatch):
    paths = _run_generate_inputs(dag_bag, monkeypatch, None)
    assert paths["run_tag"] == "somatic-manual-2026-10-15"
    assert "/somatic-snv-runs/somatic-manual-2026-10-15" in paths["input_prefix_s3"]


@pytest.mark.parametrize("code", ["404", "NoSuchKey", "NotFound"])
def test_a_missing_raw_vcf_fails_naming_the_task_and_url(dag_bag, monkeypatch, code):
    with pytest.raises(FileNotFoundError, match="calling task 70: s3://mock-nextflow-inputs/"):
        _run_generate_inputs(dag_bag, monkeypatch, code)


@pytest.mark.parametrize("code", ["403", "SlowDown"])
def test_an_s3_error_other_than_not_found_is_not_reported_as_missing(dag_bag, monkeypatch, code):
    from botocore.exceptions import ClientError

    with pytest.raises(ClientError):
        _run_generate_inputs(dag_bag, monkeypatch, code)
