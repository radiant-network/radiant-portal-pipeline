import re
from types import SimpleNamespace

import pytest

from radiant.tasks.starrocks.operator import (
    RadiantStarRocksBaseOperator,
    RadiantStarRocksOperator,
    RadiantStarRocksPartitionSwapOperator,
    SubmitTaskOptions,
)

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
    assert mapping["starrocks_germline_snv_occurrence"] == "chop_db.germline__snv__occurrence"
    # Base tables (incl. the global variant catalog) stay in the shared database.
    assert mapping["starrocks_snv_variant"] == "radiant.snv__variant"


def test_prepare_context_without_tenant_uses_base_db():
    op = RadiantStarRocksOperator(task_id="t", sql="SELECT 1")
    ctx = op.prepare_template_context(_ctx(_SHARED_CONF))
    assert ctx["mapping"]["starrocks_germline_snv_occurrence"] == "radiant.germline__snv__occurrence"
    assert "tenants" not in ctx


def test_prepare_context_exposes_tenants_and_per_tenant_mapping():
    # Upstream pushes a list-of-lists (output_processor convention); it should be flattened.
    ti = SimpleNamespace(xcom_pull=lambda task_ids: [["chop"], ["chusj"]])
    op = RadiantStarRocksOperator(task_id="t", sql="SELECT 1", tenants_task_id="fetch_all_tenants")
    ctx = op.prepare_template_context(_ctx(_SHARED_CONF, ti=ti))
    assert ctx["tenants"] == ["chop", "chusj"]
    chusj = ctx["per_tenant_mapping"]("chusj")
    assert chusj["starrocks_germline_snv_occurrence"] == "chusj_db.germline__snv__occurrence"
