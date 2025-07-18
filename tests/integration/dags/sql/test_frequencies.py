import csv
import os
import jinja2

import pandas as pd

from radiant.dags import DAGS_DIR
from radiant.tasks.data.radiant_tables import get_radiant_mapping

_SQL_DIR = os.path.join(DAGS_DIR, "sql")

def _reset_table(starrocks_session, table_name):
    _radiant_mapping = get_radiant_mapping()
    with open(os.path.join(_SQL_DIR, f"radiant/init/{table_name}_create_table.sql")) as f_in:
        create_table_sql = jinja2.Template(f_in.read()).render({"params": get_radiant_mapping()})

    table_name = _radiant_mapping.get(f"starrocks_{table_name}")

    with starrocks_session.cursor() as cursor:
        cursor.execute(create_table_sql)
        cursor.execute(f"TRUNCATE TABLE {table_name};")


def load_tsv(tsv_path):
    return pd.read_csv(tsv_path, delimiter="\t", quoting=csv.QUOTE_NONE)


def test_staging_variant_frequencies_calculation(starrocks_session, resources_dir):
    """
    Test the frequencies calculation for variants.
    """
    for table_name in ["occurrence", "staging_variant_frequency"]:
        _reset_table(starrocks_session, table_name)

    # Insert some test data into the occurrence table
    table_name = get_radiant_mapping().get("starrocks_occurrence")

    rows = load_tsv(tsv_path=resources_dir / "radiant" / "occurrence.tsv")
    rows = rows.to_dict(orient="records")

    columns = list(rows[0].keys())
    insert_sql = f"""
        INSERT INTO {table_name} ({", ".join(columns)})
        VALUES ({", ".join(["%s"] * len(columns))})
    """

    values = []
    for row in rows:
        value_tuple = [row[col] for col in columns]
        values.append(value_tuple)

    # Insert the data into the occurrence table
    with starrocks_session.cursor() as cursor:
        cursor.executemany(insert_sql, values)

    print("here")
