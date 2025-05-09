import os

import jinja2

from radiant.dags import SQL_DIR
from radiant.tasks.data.radiant_tables import STARROCKS_OPEN_DATA_MAPPING

_INIT_DIR = os.path.join(SQL_DIR, "open_data", "init")


def test_init_queries(starrocks_session):
    sql_files = [os.path.join(_INIT_DIR, file) for file in os.listdir(_INIT_DIR) if file.endswith(".sql")]
    radiant_mapping = STARROCKS_OPEN_DATA_MAPPING

    with starrocks_session.cursor() as cursor:
        for sql_file in sql_files:
            with open(sql_file) as f:
                rendered_sql = jinja2.Template(f.read()).render({"params": radiant_mapping})

            try:
                cursor.execute(rendered_sql)
            except Exception as e:
                raise Exception(f"Query failed for {sql_file}: {e}") from e

            _result = cursor.fetchall()
            assert _result is not None, f"Query failed for {sql_file}"

        # Validate tables have been created
        for _table in radiant_mapping.values():
            try:
                cursor.execute(f"SHOW CREATE TABLE {_table};")
            except Exception as e:
                raise Exception(f"Create table query failed for {_table}: {e}") from e

            _result = cursor.fetchall()
            assert f"CREATE TABLE `{_table}`" in _result[0][1], f"Create table query failed for {_table}"
