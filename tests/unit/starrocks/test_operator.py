import re
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from airflow.exceptions import TaskDeferred

from radiant.tasks.starrocks.operator import (
    RadiantStarRocksBaseOperator,
    RadiantStarRocksOperator,
    RadiantStarRocksPartitionSwapOperator,
    SubmitTaskOptions,
    SwapPartition,
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
