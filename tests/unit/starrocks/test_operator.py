import re
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from airflow.exceptions import TaskDeferred

from radiant.tasks.starrocks.operator import (
    RadiantLoadExomiserOperator,
    RadiantStarRocksBaseOperator,
    RadiantStarRocksOperator,
    RadiantStarRocksPartitionSwapOperator,
    SubmitTaskOptions,
    SwapPartition,
    rows_output_processor,
)
from radiant.tasks.starrocks.trigger import StarRocksTaskCompleteTrigger

# Pin the shared database so mapping assertions don't depend on the process environment.
_SHARED_CONF = {"RADIANT_TABLES_DATABASE": "radiant"}


def _ctx(conf, ti=None):
    return {"dag_run": SimpleNamespace(conf=conf), "ti": ti}


@pytest.mark.parametrize(
    "sql, submit_task_option, expected_sql, expected_task_name",
    [
        (
            "SELECT * FROM table",
            SubmitTaskOptions(3600, 10, False, "auto"),
            "submit /*+set_var(query_timeout=3600, enable_spill=False, spill_mode=auto)*/",
            "Radiant_Operator_Task_",
        ),
        (
            "SELECT * FROM table",
            None,
            "SELECT * FROM table",
            None,
        ),
    ],
)
def test_basic_prepare_sql(
    sql,
    submit_task_option,
    expected_sql,
    expected_task_name,
):
    result_sql, result_task_name = RadiantStarRocksBaseOperator._prepare_sql(
        sql=sql,
        submit_task_options=submit_task_option,
    )
    result_sql = re.sub(r"\s+", "", result_sql.strip())
    expected_sql = re.sub(r"\s+", "", expected_sql.strip())
    assert result_sql.startswith(expected_sql)
    if expected_task_name:
        assert result_task_name.startswith(expected_task_name)
    else:
        assert result_task_name is None


def test_prepare_sql_with_extra_args():
    _result = RadiantStarRocksBaseOperator._prepare_sql(
        sql="SELECT * FROM table",
        submit_task_options=SubmitTaskOptions(max_query_timeout=42, enable_spill=False, extra_args={"foo": "bar"}),
    )
    assert re.sub(r"\s+", "", _result[0].strip()) == re.sub(
        r"\s+",
        "",
        f"""
            submit /*+set_var(
            query_timeout=42, 
            enable_spill=False, 
            spill_mode=auto, 
            foo=bar
            )*/ task {_result[1]} as
            SELECT * FROM table
        """.strip(),
    )


@pytest.mark.parametrize(
    "operator_cls, kwargs",
    [
        (RadiantStarRocksBaseOperator, {"task_id": "test_task"}),
        (RadiantStarRocksOperator, {"task_id": "test_task", "sql": "SELECT 1"}),
        (
            RadiantStarRocksPartitionSwapOperator,
            {"task_id": "test_task", "table": "foo", "insert_partition_sql": "bar"},
        ),
    ],
)
def test_operator_retry_configuration(operator_cls, kwargs):
    op = operator_cls(**kwargs)
    assert op.retries == 3
    assert op.retry_delay.total_seconds() == 15


def test_prepare_context_routes_per_tenant_mapping():
    op = RadiantStarRocksOperator(task_id="t", sql="SELECT 1", tenant_code="chop")
    mapping = op.prepare_template_context(_ctx(_SHARED_CONF))["mapping"]
    assert mapping["starrocks_germline_snv_occurrence"] == "chop_tenant.germline__snv__occurrence"
    assert mapping["starrocks_snv_variant"] == "chop_tenant.snv__variant"
    # Base tables stay in the shared database.
    assert mapping["starrocks_snv_staging_variant"] == "radiant.snv__staging_variant"


def test_prepare_context_without_tenant_uses_base_db():
    op = RadiantStarRocksOperator(task_id="t", sql="SELECT 1")
    ctx = op.prepare_template_context(_ctx(_SHARED_CONF))
    assert ctx["mapping"]["starrocks_germline_snv_occurrence"] == "radiant.germline__snv__occurrence"
    assert "tenants" not in ctx


def _native_env():
    from jinja2.nativetypes import NativeEnvironment

    return NativeEnvironment()


# Regression: mapped operators (.expand / .expand_kwargs) bypass render_template_fields and call
# _do_render_template_fields directly, so the Radiant context (mapping/tenant_code) must be injected
# there too — otherwise '{{ mapping.* }}' is undefined when a mapped task renders.
def test_mapped_render_injects_mapping_and_tenant_code():
    op = RadiantStarRocksOperator(
        task_id="t",
        sql="INSERT INTO {{ mapping.starrocks_germline_snv_occurrence }} "
        "SELECT '{{ tenant_code }}' FROM {{ mapping.starrocks_snv_staging_variant }}",
        tenant_code="chop",
    )
    op._do_render_template_fields(op, op.template_fields, _ctx(_SHARED_CONF), _native_env(), set())
    # Per-tenant table routes to <tenant>_db; base table stays in the shared database; tenant_code resolves.
    assert "chop_tenant.germline__snv__occurrence" in op.sql
    assert "radiant.snv__staging_variant" in op.sql
    assert "'chop'" in op.sql


def _deferred(submit_task_options):
    op = RadiantStarRocksOperator(task_id="t", sql="SELECT 1", submit_task_options=submit_task_options)
    with patch.object(op, "get_db_hook", return_value=MagicMock()), pytest.raises(TaskDeferred) as exc:
        op.submit_query(sql="SELECT 1", method_name="_is_complete")
    return exc.value


# Regression: poll_interval used to be dropped on the floor — submit_query hardcoded sleep_time=30.
def test_submit_query_uses_configured_poll_interval():
    deferred = _deferred(SubmitTaskOptions(max_query_timeout=3600, poll_interval=10))

    assert isinstance(deferred.trigger, StarRocksTaskCompleteTrigger)
    assert deferred.trigger.serialize()[1]["sleep_time"] == 10


# Regression: no defer timeout meant a wedged or restarted triggerer left the task deferred forever.
def test_submit_query_sets_defer_timeout_from_query_timeout():
    deferred = _deferred(SubmitTaskOptions(max_query_timeout=3600, poll_interval=10))

    assert deferred.timeout.total_seconds() == 3600 + SubmitTaskOptions.DEFER_TIMEOUT_MARGIN


def test_submit_query_honours_explicit_defer_timeout():
    deferred = _deferred(SubmitTaskOptions(max_query_timeout=3600, poll_interval=10, defer_timeout=120))

    assert deferred.timeout.total_seconds() == 120


def test_mapped_render_partition_swap_resolves_table_per_tenant():
    op = RadiantStarRocksPartitionSwapOperator(
        task_id="t",
        table="{{ mapping.starrocks_germline_cnv_occurrence }}",
        tenant_code="chusj",
        parameters={"tenant_code": "chusj", "seq_ids": [1]},
        insert_partition_sql="SELECT 1",
        swap_partition=SwapPartition(partition="5", copy_partition_sql="SELECT 1"),
    )
    op._do_render_template_fields(op, op.template_fields, _ctx(_SHARED_CONF), _native_env(), set())
    assert op.table == "chusj_tenant.germline__cnv__occurrence"


def test_rows_output_processor_keys_each_row_by_column_name():
    results = [[(1, "radiant"), (2, "qlin")]]
    descriptions = [[("case_id", 3, None), ("tenant_code", 253, None)]]
    assert rows_output_processor(results, descriptions) == [
        [{"case_id": 1, "tenant_code": "radiant"}, {"case_id": 2, "tenant_code": "qlin"}]
    ]


def test_rows_output_processor_handles_an_empty_result():
    assert rows_output_processor([[]], [[("case_id", 3, None)]]) == [[]]


def _experiment(seq_id, family_role):
    return {
        "seq_id": seq_id,
        "patient_id": seq_id,
        "aliquot": f"SA{seq_id}",
        "tenant_code": "test",
        "family_role": family_role,
        "affected_status": "affected",
        "sex": "female",
        "experimental_strategy": "wgs",
    }


def _annotation_task(task_id, exomiser_filepath, experiments):
    return {
        "task_id": task_id,
        "task_type": "radiant_germline_annotation",
        "part": 5,
        "analysis_type": "germline",
        "experiments": experiments,
        "deleted": False,
        "vcf_filepath": f"s3://test/{task_id}.vcf.gz",
        "exomiser_filepath": exomiser_filepath,
    }


def _run_exomiser_load(tasks):
    op = RadiantLoadExomiserOperator(
        task_id="t",
        table="radiant.raw_exomiser",
        swap_partition=SwapPartition(partition="5", copy_partition_sql="SELECT 1"),
        parameters={"task_ids": [1], "deleted_task_ids": [-1]},
        tasks=tasks,
    )
    hook = MagicMock()
    cursor = hook.get_conn.return_value.cursor.return_value.__enter__.return_value
    with (
        patch.object(op, "get_db_hook", return_value=hook),
        patch.object(op, "render_template", side_effect=lambda content, *_: content),
        patch.object(op, "verify_load_status"),
    ):
        op.run_insert(context={}, partition_exists=False)
    return [call.args[1] for call in cursor.execute.call_args_list]


def test_exomiser_load_reads_the_file_of_the_annotation_task_and_tags_the_proband():
    """RAD-116: the annotation task holds every family member; the Exomiser rows get the proband's
    seq_id and the annotation task id."""
    loads = _run_exomiser_load(
        [
            _annotation_task(
                11,
                "s3://test/CA1.exomiser.variants.tsv",
                [_experiment(102, "mother"), _experiment(101, "proband"), _experiment(103, "father")],
            )
        ]
    )

    assert len(loads) == 1
    assert {k: loads[0][k] for k in ("part", "seq_id", "task_id", "tenant_code", "tsv_filepath")} == {
        "part": "5",
        "seq_id": 101,
        "task_id": 11,
        "tenant_code": "test",
        "tsv_filepath": "s3://test/CA1.exomiser.variants.tsv",
    }


def test_exomiser_load_skips_tasks_without_exomiser_file():
    loads = _run_exomiser_load(
        [
            _annotation_task(11, None, [_experiment(101, "proband")]),
            {
                "task_id": 12,
                "task_type": "alignment_germline_variant_calling",
                "part": 5,
                "analysis_type": "germline",
                "experiments": [_experiment(101, "proband")],
                "deleted": False,
                "cnv_vcf_filepath": "s3://test/12.cnv.vcf.gz",
            },
        ]
    )

    assert loads == []


def test_exomiser_load_skips_deleted_tasks():
    """A deleted task's file may be gone from S3, and the copy step drops its rows anyway."""
    deleted = _annotation_task(11, "s3://test/CA1.exomiser.variants.tsv", [_experiment(101, "proband")])
    deleted["deleted"] = True

    assert _run_exomiser_load([deleted]) == []


def test_exomiser_load_without_proband_raises():
    task = _annotation_task(11, "s3://test/CA1.exomiser.variants.tsv", [_experiment(102, "mother")])

    with pytest.raises(ValueError, match=r"No proband found in task \[11\]"):
        _run_exomiser_load([task])
