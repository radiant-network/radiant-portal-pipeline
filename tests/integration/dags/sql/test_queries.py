import itertools
import os

import jinja2

from radiant.dags import DAGS_DIR
from radiant.tasks.data.radiant_tables import (
    CLINICAL_CATALOG_ENV_KEY,
    CLINICAL_DATABASE_ENV_KEY,
    RADIANT_DATABASE_ENV_KEY,
    RADIANT_ICEBERG_CATALOG_ENV_KEY,
    RADIANT_ICEBERG_DATABASE_ENV_KEY,
)

_SQL_DIR = os.path.join(DAGS_DIR, "sql")

_RADIANT_INIT_DIR = os.path.join(_SQL_DIR, "radiant", "init")
_RADIANT_INSERT_DIR = os.path.join(_SQL_DIR, "radiant")
_OPEN_DATA_INIT_DIR = os.path.join(_SQL_DIR, "open_data", "init")
_OPEN_DATA_INSERT_DIR = os.path.join(_SQL_DIR, "open_data")

_MOCK_PARAMS = {
    "part": 0,
    "case_ids": [0, 1, 2, 3],
    "variant_part": 1,
    "part_lower": 0,
    "part_upper": 10,
    "case_id": 1,
    "seq_id": 1,
    "task_id": 1,
    "analysis_type": "wgs",
    "aliquot": "SA0001",
    "patient_id": 3,
    "experimental_strategy": "wgs",
    "request_id": 1,
    "request_priority": "routine",
    "vcf_filepath": "s3+http://vcf/test.vcf.gz",
    "cnv_vcf_filepath": "s3+http://vcf/test.vcf.gz",
    "exomiser_filepath": ["s3+http://tsv/test.tsv.gz"],
    "sex": "male",
    "family_role": "proband",
    "affected_status": "affected",
    "created_at": "2025-10-01 00:00",
    "updated_at": "2025-10-01 00:00",
    "ingested_at": None,
}


def _execute_query(cursor, query, args=None):
    try:
        cursor.execute(query, args=args)
        _result = cursor.fetchall()
        assert _result is not None, f"Query failed: {query}"
    except Exception as e:
        raise Exception(f"Query failed: {query}, with exception: {e}") from e


def _execute_file(cursor, sql_file, args=None):
    from radiant.tasks.data.radiant_tables import get_radiant_mapping

    with open(sql_file) as f:
        rendered_sql = jinja2.Template(f.read()).render({"params": get_radiant_mapping()})
    return _execute_query(cursor, rendered_sql, args=args)


def _validate_init(starrocks_session, sql_dir, tables=None, views=None, udfs=None):
    with starrocks_session.cursor() as cursor:
        # Create UDFs first because some queries may depend on them
        for udf in udfs or []:
            _execute_file(cursor, os.path.join(sql_dir, udf + "_udf.sql"))

        for filename in itertools.chain(tables or [], views or []):
            _execute_file(cursor, os.path.join(sql_dir, filename + "_create_table.sql"))


def _explain_insert(starrocks_session, sql_dir):
    from radiant.tasks.data.radiant_tables import get_radiant_mapping

    sql_files = [os.path.join(sql_dir, file) for file in os.listdir(sql_dir) if file.endswith(".sql")]
    with starrocks_session.cursor() as cursor:
        for sql_file in sql_files:
            with open(sql_file) as f:
                rendered_sql = jinja2.Template(f.read()).render({"params": get_radiant_mapping()})

            if "staging_exomiser" in sql_file or "load" in sql_file.lower():
                # "EXPLAIN" not supported with "LOAD"
                continue

            _execute_query(cursor, f"EXPLAIN {rendered_sql}", args=_MOCK_PARAMS)


def test_queries_are_valid(
    monkeypatch,
    iceberg_client,
    starrocks_session,
    starrocks_database,
    setup_iceberg_namespace,
    open_data_iceberg_tables,
    starrocks_iceberg_catalog,
    starrocks_jdbc_catalog,
):
    monkeypatch.setenv(RADIANT_ICEBERG_CATALOG_ENV_KEY, starrocks_iceberg_catalog.catalog)
    monkeypatch.setenv(RADIANT_ICEBERG_DATABASE_ENV_KEY, setup_iceberg_namespace)
    monkeypatch.setenv(CLINICAL_CATALOG_ENV_KEY, starrocks_jdbc_catalog.catalog)
    monkeypatch.setenv(CLINICAL_DATABASE_ENV_KEY, starrocks_jdbc_catalog.database)
    monkeypatch.setenv(RADIANT_DATABASE_ENV_KEY, starrocks_database.database)

    # Validate table creation for Open Data & Radiant
    _validate_init(
        starrocks_session,
        sql_dir=_OPEN_DATA_INIT_DIR,
        tables=[
            "1000_genomes",
            "clinvar",
            "dbnsfp",
            "dbsnp",
            "gnomad",
            "spliceai",
            "topmed_bravo",
            "gnomad_constraint",
            "omim_gene_panel",
            "hpo_gene_panel",
            "mondo_term",
            "hpo_term",
            "orphanet_gene_panel",
            "ddd_gene_panel",
            "cosmic_gene_panel",
            "raw_clinvar_rcv_summary",
            "clinvar_rcv_summary",
        ],
    )
    _validate_init(
        starrocks_session,
        sql_dir=_RADIANT_INIT_DIR,
        tables=[
            "consequence",
            "consequence_filter",
            "consequence_filter_partitioned",
            "staging_exomiser",
            "exomiser",
            "occurrence",
            "staging_sequencing_experiment",
            "tmp_variant",
            "staging_variant",
            "variant_lookup",
            "variant",
            "staging_variant_frequency",
            "variant_frequency",
            "variant_partitioned",
        ],
        views=["staging_external_sequencing_experiment", "staging_sequencing_experiment_delta"],
        udfs=["variant_id"],
    )

    # Validate table insertion using SQL `EXPLAIN` for Open Data & Radiant (Requires existing tables)
    _explain_insert(starrocks_session, sql_dir=_OPEN_DATA_INSERT_DIR)
    _explain_insert(starrocks_session, sql_dir=_RADIANT_INSERT_DIR)
