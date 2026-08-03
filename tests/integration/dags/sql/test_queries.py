import itertools
import os

import jinja2

from radiant.dags import DAGS_DIR
from radiant.tasks.data.radiant_tables import RadiantConfigKeys
from tests.integration.conftest import TENANT_CODE

_SQL_DIR = os.path.join(DAGS_DIR, "sql")

_RADIANT_INIT_DIR = os.path.join(_SQL_DIR, "radiant", "init")
_RADIANT_INSERT_DIR = os.path.join(_SQL_DIR, "radiant")
_OPEN_DATA_INIT_DIR = os.path.join(_SQL_DIR, "open_data", "init")
_OPEN_DATA_INSERT_DIR = os.path.join(_SQL_DIR, "open_data")

_MOCK_PARAMS = {
    "part": 0,
    "variant_part": 1,
    "part_lower": 0,
    "part_upper": 10,
    "case_id": 1,
    "seq_id": 1,
    "seq_ids": [1, 2, 3],
    "task_ids": [1, 2, 3],
    "deleted_task_ids": [4, 5, 6],
    "family_id": 1,
    "task_type": "radiant_germline_annotation",
    "task_id": 1,
    "analysis_type": "wgs",
    "aliquot": "SA0001",
    "patient_id": 3,
    "experimental_strategy": "wgs",
    "request_priority": "routine",
    "vcf_filepath": "s3+http://vcf/test.vcf.gz",
    "cnv_vcf_filepath": "s3+http://vcf/test.vcf.gz",
    "exomiser_filepath": ["s3+http://tsv/test.tsv.gz"],
    "sex": "male",
    "family_role": "proband",
    "affected_status": "affected",
    "histology_type": "normal",
    "created_at": "2025-10-01 00:00",
    "updated_at": "2025-10-01 00:00",
    "ingested_at": None,
    "tenants": ["chusj", "radiant"],
    "tenant_code": "chusj",
}


def _execute_query(cursor, query, args=None):
    try:
        cursor.execute(query, args=args)
        _result = cursor.fetchall()
        assert _result is not None, f"Query failed: {query}"
    except Exception as e:
        raise Exception(f"Query failed: {query}, with exception: {e}") from e


def _execute_file(cursor, sql_file, args=None, tenant_code=None):
    from radiant.tasks.data.radiant_tables import get_radiant_mapping

    context = {"mapping": get_radiant_mapping(tenant_code=tenant_code)}
    if "udf" in sql_file:
        context["params"] = {"udf_release_version": "v1.1.0"}

    with open(sql_file) as f:
        rendered_sql = jinja2.Template(f.read()).render(context)

    return _execute_query(cursor, rendered_sql, args=args)


def _validate_init(starrocks_session, sql_dir, tables=None, views=None, udfs=None, tenant_code=None):
    with starrocks_session.cursor() as cursor:
        # Create UDFs first because some queries may depend on them
        for udf in udfs or []:
            _execute_file(cursor, os.path.join(sql_dir, udf + "_udf.sql"), tenant_code=tenant_code)

        for filename in itertools.chain(tables or [], views or []):
            _execute_file(cursor, os.path.join(sql_dir, filename + "_create_table.sql"), tenant_code=tenant_code)


def _explain_insert(starrocks_session, sql_dir):
    from radiant.tasks.data.radiant_tables import get_radiant_mapping

    sql_files = [os.path.join(sql_dir, file) for file in os.listdir(sql_dir) if file.endswith(".sql")]
    with starrocks_session.cursor() as cursor:
        for sql_file in sql_files:
            if (
                "staging_exomiser" in sql_file
                or "load" in sql_file.lower()
                or "cnv_occurrence_insert_partition_delta" in sql_file.lower()
                or "cnv_occurrence_copy_partition" in sql_file.lower()
                or "snv_occurrence_insert_partition_delta" in sql_file.lower()
                or "snv_occurrence_copy_partition" in sql_file.lower()
                or "exomiser_copy_partition" in sql_file.lower()
                or "exomiser_insert_partition_delta" in sql_file.lower()
            ):
                # "EXPLAIN" not supported with "LOAD"
                continue
            with open(sql_file) as f:
                rendered_sql = jinja2.Template(f.read()).render(
                    {
                        "mapping": get_radiant_mapping(),
                        # Tenant-pooling SQL loops `{% for t in tenants %}` and resolves each via
                        # per_tenant_mapping; point both at the same test DB so EXPLAIN can validate the
                        # rendered per-tenant UNION branches against the tables created above.
                        "tenants": _MOCK_PARAMS["tenants"],
                        "per_tenant_mapping": lambda _t: get_radiant_mapping(),
                    }
                )
            _execute_query(cursor, f"EXPLAIN {rendered_sql}", args=_MOCK_PARAMS)


def _explain_tenant_scoped_insert(starrocks_session, sql_files, tenant_code):
    """EXPLAIN the per-tenant variant catalog SQL against a real second database.

    `_explain_insert` renders without a tenant, so every table collapses onto the base test DB and
    cross-database name resolution is never exercised. These files write to `<tenant>_db` while
    still reading `snv__staging_variant` from the base DB, so they need both.
    """
    from radiant.tasks.data.radiant_tables import get_radiant_mapping

    mapping = get_radiant_mapping(tenant_code=tenant_code)
    with starrocks_session.cursor() as cursor:
        for filename in sql_files:
            with open(os.path.join(_RADIANT_INSERT_DIR, filename)) as f:
                rendered_sql = jinja2.Template(f.read()).render({"mapping": mapping})
            _execute_query(cursor, f"EXPLAIN {rendered_sql}", args=_MOCK_PARAMS)


def test_queries_are_valid(
    monkeypatch,
    iceberg_client,
    starrocks_session,
    setup_iceberg_namespace,
    open_data_iceberg_tables,
    mapping_conf,
    starrocks_tenant_database,
):
    for k, v in mapping_conf.items():
        monkeypatch.setenv(k.upper(), v)
    monkeypatch.setenv(RadiantConfigKeys.RADIANT_TENANT_DB_TEMPLATE.env_key, starrocks_tenant_database)
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
            "ensembl_gene",
            "ensembl_exon_by_gene",
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
            "snv_consequence",
            "snv_consequence_filter",
            "snv_consequence_filter_partitioned",
            "staging_exomiser",
            "exomiser",
            "germline_snv_occurrence",
            "staging_sequencing_experiment",
            "snv_tmp_variant",
            "snv_staging_variant",
            "variant_lookup",
            "snv_variant",
            "germline_snv_staging_variant_frequency",
            "germline_snv_variant_frequency",
            "snv_variant_partitioned",
            "somatic_snv_occurrence",
            "somatic_snv_variant_frequency",
            "somatic_snv_staging_variant_frequency",
        ],
        views=["staging_external_sequencing_experiment", "staging_sequencing_experiment_delta"],
        udfs=["variant_id"],
    )

    # Validate table insertion using SQL `EXPLAIN` for Open Data & Radiant (Requires existing tables)
    _explain_insert(starrocks_session, sql_dir=_OPEN_DATA_INSERT_DIR)
    _explain_insert(starrocks_session, sql_dir=_RADIANT_INSERT_DIR)

    # Same again for the variant catalog, but against a real tenant database.
    _validate_init(
        starrocks_session,
        sql_dir=_RADIANT_INIT_DIR,
        tables=[
            "germline_snv_occurrence",
            "germline_snv_variant_frequency",
            "snv_variant",
            "snv_variant_partitioned",
            "somatic_snv_occurrence",
            "somatic_snv_variant_frequency",
        ],
        tenant_code=TENANT_CODE,
    )
    _explain_tenant_scoped_insert(
        starrocks_session,
        sql_files=["snv_variant_insert.sql", "snv_variant_part_insert_part.sql"],
        tenant_code=TENANT_CODE,
    )
