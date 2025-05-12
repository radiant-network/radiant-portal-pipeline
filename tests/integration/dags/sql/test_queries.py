import os

import jinja2

from radiant.dags import SQL_DIR

_RADIANT_INIT_DIR = os.path.join(SQL_DIR, "radiant", "init")
_RADIANT_INSERT_DIR = os.path.join(SQL_DIR, "radiant")
_OPEN_DATA_INIT_DIR = os.path.join(SQL_DIR, "open_data", "init")
_OPEN_DATA_INSERT_DIR = os.path.join(SQL_DIR, "open_data")

_MOCK_PARAMS = {
    "part": 0,
    "case_ids": [0, 1, 2, 3],
    "variant_part": 1,
    "part_lower": 0,
    "part_upper": 10,
}


def _execute_query(cursor, query, args=None):
    try:
        cursor.execute(query, args=args)
        _result = cursor.fetchall()
        assert _result is not None, f"Query failed: {query}"
    except Exception as e:
        raise Exception(f"Query failed: {query}, with exception: {e}") from e


def _validate_init(starrocks_session, sql_dir, tables):
    from radiant.tasks.data.radiant_tables import get_radiant_mapping

    sql_files = [os.path.join(sql_dir, file) for file in os.listdir(sql_dir) if file.endswith(".sql")]

    with starrocks_session.cursor() as cursor:
        for sql_file in sql_files:
            with open(sql_file) as f:
                rendered_sql = jinja2.Template(f.read()).render({"params": get_radiant_mapping()})
            _execute_query(cursor, rendered_sql)

        # Validate tables have been created
        for _table in tables:
            _execute_query(cursor, f"SHOW CREATE TABLE {_table}")


def _explain_insert(starrocks_session, sql_dir):
    from radiant.tasks.data.radiant_tables import get_radiant_mapping

    sql_files = [os.path.join(sql_dir, file) for file in os.listdir(sql_dir) if file.endswith(".sql")]
    with starrocks_session.cursor() as cursor:
        for sql_file in sql_files:
            with open(sql_file) as f:
                rendered_sql = jinja2.Template(f.read()).render({"params": get_radiant_mapping()})
            _execute_query(cursor, f"EXPLAIN {rendered_sql}", args=_MOCK_PARAMS)


def test_queries_are_valid(
    monkeypatch,
    iceberg_client,
    starrocks_session,
    setup_namespace,
    open_data_iceberg_tables,
    starrocks_iceberg_catalog,
):
    monkeypatch.setenv("RADIANT_ICEBERG_CATALOG", starrocks_iceberg_catalog.name)
    monkeypatch.setenv("RADIANT_ICEBERG_DATABASE", setup_namespace)

    from radiant.tasks.data.radiant_tables import (
        SOURCE_SEQUENCING_EXPERIMENT_MAPPING,
        STARROCKS_GERMLINE_SNV_MAPPING,
        STARROCKS_OPEN_DATA_MAPPING,
    )

    # Validate table creation for Open Data & Radiant
    _validate_init(starrocks_session, sql_dir=_OPEN_DATA_INIT_DIR, tables=STARROCKS_OPEN_DATA_MAPPING.values())
    _validate_init(
        starrocks_session,
        sql_dir=_RADIANT_INIT_DIR,
        tables={**STARROCKS_GERMLINE_SNV_MAPPING, **SOURCE_SEQUENCING_EXPERIMENT_MAPPING}.values(),
    )

    # Validate table insertion using SQL `EXPLAIN` for Open Data & Radiant (Requires existing tables)
    _explain_insert(starrocks_session, sql_dir=_OPEN_DATA_INSERT_DIR)
    _explain_insert(starrocks_session, sql_dir=_RADIANT_INSERT_DIR)
