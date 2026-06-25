from airflow import DAG
from airflow.models import Param
from airflow.models.baseoperator import chain

from radiant.dags import DEFAULT_ARGS, NAMESPACE, SQL_DIR
from radiant.tasks.starrocks.operator import RadiantStarRocksOperator

_RADIANT_SQL_INIT_DIR = SQL_DIR / "radiant" / "init"
_OPEN_DATA_SQL_INIT_DIR = SQL_DIR / "open_data" / "init"
_CLINICAL_SQL_INIT_DIR = SQL_DIR / "clinical" / "init"

# Shared tables live once in RADIANT_DATABASE (cross-tenant orchestration, the global variant catalog and
# consequence annotations). Created by the global init DAG below. Mirrors STARROCKS_RADIANT_SHARED_MAPPING.
_SHARED_TABLES = [
    "snv_consequence",
    "snv_consequence_filter",
    "snv_consequence_filter_partitioned",
    "staging_external_sequencing_experiment",
    "staging_sequencing_experiment",
    "staging_sequencing_experiment_delta",
    "snv_tmp_variant",
    "snv_staging_variant",
    "snv_variant",
    "snv_variant_partitioned",
    "variant_lookup",
]

_PER_TENANT_TABLES = [
    "germline_snv_occurrence",
    "germline_cnv_occurrence",
    "germline_snv_staging_variant_frequency",
    "germline_snv_variant_frequency",
    "staging_exomiser",
    "exomiser",
    "somatic_snv_occurrence",
    "somatic_snv_staging_variant_frequency",
    "somatic_snv_variant_frequency",
]


dag_params = {
    "udf_release_version": Param(
        default="v1.2.0",
        description="Release version of the radiant-starrocks-udf JAR (e.g. v1.2.0).",
        type="string",
    ),
}

with DAG(
    dag_id=f"{NAMESPACE}-init-starrocks-tables",
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    params=dag_params,
    tags=["radiant", "starrocks", "manual"],
    dag_display_name="Radiant - Init StarRocks Tables",
) as dag:
    tasks = []

    for table in _SHARED_TABLES:
        tasks.append(
            RadiantStarRocksOperator(
                task_id=f"create_table_{table}",
                sql=str(_RADIANT_SQL_INIT_DIR / f"{table}_create_table.sql"),
            )
        )

    clinical_tables = [
        "patient_access",
        "brim",
    ]
    for table in clinical_tables:
        tasks.append(
            RadiantStarRocksOperator(
                task_id=f"create_table_{table}",
                sql=str(_CLINICAL_SQL_INIT_DIR / f"{table}_create_table.sql"),
            )
        )

    group_ids = [
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
        "ensembl_gene",
        "ensembl_exon_by_gene",
        "cytoband",
        "orphanet_gene_panel",
        "ddd_gene_panel",
        "cosmic_gene_panel",
        "clinvar_rcv_summary",
        "raw_clinvar_rcv_summary",
        "mondo_term",
        "hpo_term",
    ]
    for group in group_ids:
        tasks.append(
            RadiantStarRocksOperator(
                task_id=f"create_{group}",
                sql=str(_OPEN_DATA_SQL_INIT_DIR / f"{group}_create_table.sql"),
            )
        )
    tasks.append(
        RadiantStarRocksOperator(
            task_id="create_variant_id_udf",
            sql=str(_RADIANT_SQL_INIT_DIR / "variant_id_udf.sql"),
        )
    )
    tasks.append(
        RadiantStarRocksOperator(
            task_id="create_cnv_id_udf",
            sql=str(_RADIANT_SQL_INIT_DIR / "cnv_id_udf.sql"),
        )
    )

    chain(*tasks)


tenant_dag_params = {
    "RADIANT_TENANT_CODE": Param(
        default=None,
        description="Tenant code. Per-tenant tables are created in this tenant's database (<tenant>_db), "
        "which must already exist (created externally).",
    ),
}

with DAG(
    dag_id=f"{NAMESPACE}-init-tenant-starrocks-tables",
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    params=tenant_dag_params,
    tags=["radiant", "starrocks", "manual", "tenant"],
    dag_display_name="Radiant - Init Tenant StarRocks Tables",
) as tenant_dag:
    # The DDL files reference {{ mapping.starrocks_* }}; with RADIANT_TENANT_CODE in the run conf the
    # resolver routes these per-tenant tables to <tenant>_db. UDFs are GLOBAL functions and are created
    # once by the global init DAG, so they are not repeated here.
    tenant_tasks = [
        RadiantStarRocksOperator(
            task_id=f"create_table_{table}",
            sql=str(_RADIANT_SQL_INIT_DIR / f"{table}_create_table.sql"),
        )
        for table in _PER_TENANT_TABLES
    ]
    chain(*tenant_tasks)
