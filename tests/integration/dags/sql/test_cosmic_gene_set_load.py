"""End-to-end check of the COSMIC census broker load and the panel rebuild that follows it.

The load does its transforms in the BROKER LOAD `SET` clause, which only a real StarRocks can validate:
flags to booleans, comma lists to trimmed arrays, empty cells to NULL -- the shape the legacy Spark
ETL produced and cosmic_gene_panel_insert.sql relies on.
"""

import json
import os
import time
import uuid

import jinja2

from radiant.dags import DAGS_DIR

_SQL_DIR = os.path.join(DAGS_DIR, "sql")


def _wait_for_load(cursor, label):
    for _ in range(150):
        cursor.execute(
            "SELECT STATE, ERROR_MSG FROM information_schema.loads WHERE LABEL = %(label)s", {"label": label}
        )
        row = cursor.fetchone()
        if not row or row[0] == "FINISHED":
            return
        if row[0] == "CANCELLED":
            raise RuntimeError(f"Load {label} was cancelled: {row[1]}")
        time.sleep(2)
    raise TimeoutError(f"Load for label {label} did not finish in time.")


def test_cosmic_gene_set_load_and_panel_rebuild(
    starrocks_session, radiant_mapping, minio_instance, sample_cosmic_gene_set_tsv
):
    gene_set = radiant_mapping["starrocks_cosmic_gene_set"]
    gene_panel = radiant_mapping["starrocks_cosmic_gene_panel"]
    database_name, table_name = gene_set.split(".")
    label = f"test_cosmic_gene_set_load_{uuid.uuid4().hex}"

    def render(path, **context):
        with open(os.path.join(_SQL_DIR, path)) as f_in:
            return jinja2.Template(f_in.read()).render({"mapping": radiant_mapping, **context})

    load_sql = render(
        "open_data/cosmic_gene_set_load.sql",
        broker_load_timeout=7200,
        database_name=database_name,
        table_name=table_name,
        load_label=label,
        broker_configuration=f"""
            'aws.s3.region' = 'us-east-1',
            'aws.s3.endpoint' = 'http://radiant-minio:9000',
            'aws.s3.enable_path_style_access' = 'true',
            'aws.s3.access_key' = '{minio_instance.access_key}',
            'aws.s3.secret_key' = '{minio_instance.secret_key}'
        """,
    )

    with starrocks_session.cursor() as cursor:
        cursor.execute(render("open_data/init/cosmic_gene_set_create_table.sql"))
        cursor.execute(render("open_data/init/cosmic_gene_panel_create_table.sql"))
        cursor.execute(f"TRUNCATE TABLE {gene_set}")

        # The DAG passes the param list straight through; pymysql renders it as `('s3://...')`.
        cursor.execute(load_sql, {"tsv_filepath": [f"s3://{sample_cosmic_gene_set_tsv}"]})
        cursor.fetchall()
        _wait_for_load(cursor, label)

        cursor.execute(
            f"""
            SELECT symbol, name, chromosome, start, tier, somatic, germline, other_germline_mutation,
                   tumour_types_somatic, tumour_types_germline, tissue_type, translocation_partner, synonyms,
                   cancer_syndrome, molecular_genetics
            FROM {gene_set} ORDER BY symbol
            """
        )
        rows = {r[0]: r for r in cursor.fetchall()}
        assert set(rows) == {"BRCA1", "KRAS", "NOCOORD", "TP53"}

        tp53 = rows["TP53"]
        assert tp53[1] == "tumor protein p53, transformation related", "quoted NAME must keep its comma"
        assert tp53[2:5] == ("17", 7661779, 1)
        assert tp53[5:8] == (1, 1, 0)
        assert json.loads(tp53[8]) == ["breast", "colorectal", "lung"], "elements must be trimmed"
        assert json.loads(tp53[9]) == ["breast", "sarcoma", "adrenocortical carcinoma"]
        assert json.loads(tp53[10]) == ["E", "L", "M"]
        assert tp53[11] is None, "an empty list cell is NULL, not ['']"
        assert json.loads(tp53[12]) == ["ENSG00000141510", "LFS1", "P53"]
        assert tp53[13:15] == ("Li-Fraumeni syndrome", "Dom")

        kras = rows["KRAS"]
        assert kras[5:8] == (1, 0, 0)
        assert kras[9] is None and kras[13] is None

        brca1 = rows["BRCA1"]
        assert brca1[5:8] == (0, 1, 1)
        assert json.loads(brca1[11]) == ["IGH"]

        nocoord = rows["NOCOORD"]
        assert nocoord[2:5] == (None, None, 2)
        assert nocoord[5:8] == (0, 0, 0)
        assert nocoord[8:15] == (None,) * 7

        cursor.execute(render("open_data/cosmic_gene_panel_insert.sql"))
        cursor.execute(f"SELECT symbol, panel FROM {gene_panel} ORDER BY symbol, panel")
        assert cursor.fetchall() == (
            ("BRCA1", "breast"),
            ("BRCA1", "ovarian"),
            ("TP53", "adrenocortical carcinoma"),
            ("TP53", "breast"),
            ("TP53", "sarcoma"),
        )
