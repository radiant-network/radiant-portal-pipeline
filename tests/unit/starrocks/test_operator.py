import re

import pytest

from radiant.tasks.starrocks.operator import RadiantStarRocksBaseOperator, SubmitTaskOptions


@pytest.mark.parametrize(
    "sql, submit_task_option, expected_sql, expected_task_name",
    [
        (
                "SELECT * FROM table",
                SubmitTaskOptions(
                    3600,
                    10,
                    False,
                    "auto"),
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
        submit_task_options=SubmitTaskOptions(
        max_query_timeout=42,
        enable_spill=False,
        extra_args={"foo": "bar"}),
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
