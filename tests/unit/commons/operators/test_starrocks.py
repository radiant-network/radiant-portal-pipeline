import re

import pytest

from dags.commons.operators.starrocks import StarRocksSQLExecuteQueryOperator


@pytest.mark.parametrize(
    "sql, is_submit_task, query_timeout, enable_spill, spill_mode, query_params, expected_sql, expected_task_name",
    [
        (
            "SELECT * FROM table",
            True,
            3600,
            False,
            "auto",
            {"param1": "value1"},
            "submit /*+set_var(query_timeout=3600, enable_spill=False, spill_mode=auto)*/",
            "StarRocksSQLExecuteQueryOperator_Task_",
        ),
        (
            "SELECT * FROM table",
            False,
            3600,
            False,
            "auto",
            {"param1": "value1"},
            "SELECT * FROM table",
            None,
        ),
        (
            "SELECT * FROM table WHERE column = {param1}",
            False,
            3600,
            False,
            "auto",
            {"param1": "value1"},
            "SELECT * FROM table WHERE column = value1",
            None,
        ),
        (
            "SELECT * FROM table WHERE column = {param1}",
            True,
            3600,
            True,
            "manual",
            {"param1": "value1"},
            "submit /*+set_var(query_timeout=3600, enable_spill=True, spill_mode=manual)*/",
            "StarRocksSQLExecuteQueryOperator_Task_",
        ),
        (
            "SELECT * FROM table",
            True,
            3600,
            False,
            "auto",
            None,
            "submit /*+set_var(query_timeout=3600, enable_spill=False, spill_mode=auto)*/",
            "StarRocksSQLExecuteQueryOperator_Task_",
        ),
    ],
)
def test_basic_prepare_sql(
    sql,
    is_submit_task,
    query_timeout,
    enable_spill,
    spill_mode,
    query_params,
    expected_sql,
    expected_task_name,
):
    result_sql, result_task_name = StarRocksSQLExecuteQueryOperator._prepare_sql(
        sql=sql,
        is_submit_task=is_submit_task,
        query_timeout=query_timeout,
        enable_spill=enable_spill,
        spill_mode=spill_mode,
        query_params=query_params,
    )
    result_sql = re.sub(r"\s+", "", result_sql.strip())
    expected_sql = re.sub(r"\s+", "", expected_sql.strip())
    assert result_sql.startswith(expected_sql)
    if expected_task_name:
        assert result_task_name.startswith(expected_task_name)
    else:
        assert result_task_name is None


def test_prepare_sql_with_extra_args():
    _result = StarRocksSQLExecuteQueryOperator._prepare_sql(
        sql="SELECT * FROM table",
        is_submit_task=True,
        query_timeout=42,
        enable_spill=False,
        extra_args={"foo": "bar"},
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
