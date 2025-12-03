import csv
import os

import jinja2
import pandas as pd

from radiant.dags import DAGS_DIR

_SQL_DIR = os.path.join(DAGS_DIR, "sql")


def _reset_table(starrocks_session, table_name, mapping):
    with open(os.path.join(_SQL_DIR, f"radiant/init/{table_name}_create_table.sql")) as f_in:
        create_table_sql = jinja2.Template(f_in.read()).render({"mapping": mapping})

    table_name = mapping.get(f"starrocks_{table_name}")

    with starrocks_session.cursor() as cursor:
        cursor.execute(create_table_sql)
        cursor.execute(f"TRUNCATE TABLE {table_name};")


def load_tsv(starrocks_session, table_name, tsv_path):
    rows = pd.read_csv(tsv_path, delimiter="\t", quoting=csv.QUOTE_NONE)
    rows = rows.replace(to_replace=float("nan"), value=None).to_dict(orient="records")
    columns = list(rows[0].keys())
    insert_sql = f"""
        INSERT INTO {table_name} ({", ".join(columns)})
        VALUES ({", ".join(["%s"] * len(columns))})
    """
    values = []
    for row in rows:
        value_tuple = [row[col] for col in columns]
        values.append(value_tuple)

    with starrocks_session.cursor() as cursor:
        cursor.executemany(insert_sql, values)


def test_staging_variant_frequencies_calculation(starrocks_session, resources_dir, radiant_mapping):
    """
    Test the frequencies calculation for variants.
    """

    for table_name in [
        "germline_snv_occurrence",
        "staging_sequencing_experiment",
        "germline_snv_staging_variant_frequency",
    ]:
        _reset_table(starrocks_session, table_name, radiant_mapping)

    # Insert some test data into the occurrence table
    occurrence_table_name = radiant_mapping.get("starrocks_germline_snv_occurrence")
    seq_exp_table = radiant_mapping.get("starrocks_staging_sequencing_experiment")

    load_tsv(starrocks_session, occurrence_table_name, resources_dir / "radiant/occurrence.tsv")
    load_tsv(starrocks_session, seq_exp_table, resources_dir / "radiant/staging_sequencing_experiment.tsv")

    with open(os.path.join(_SQL_DIR, "radiant/germline_snv_staging_variant_freq_insert.sql")) as f_in:
        variant_freq_insert = jinja2.Template(f_in.read()).render({"mapping": radiant_mapping})

    _select_sql = "SELECT * FROM {{ mapping.starrocks_germline_snv_staging_variant_frequency }}"
    _select_sql = jinja2.Template(_select_sql).render({"mapping": radiant_mapping})

    _params = {"part": 0}

    # Insert the data into the occurrence table
    with starrocks_session.cursor() as cursor:
        cursor.execute(variant_freq_insert, _params)
        cursor.execute(_select_sql)

        results = cursor.fetchall()
        # Values vetted with the content of test resource staging_sequencing_experiment.tsv
        assert results == ((0, -8935141392267608062, 5, 10, 3, 7, 2, 3, 0, 0, 0, 0, 0, 0),)
