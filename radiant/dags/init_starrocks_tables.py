from airflow import DAG
from airflow.utils.helpers import chain

from radiant.dags import DEFAULT_ARGS, NAMESPACE, SQL_DIR
from radiant.tasks.starrocks.operator import RadiantStarRocksOperator

_RADIANT_SQL_INIT_DIR = SQL_DIR / "radiant" / "init"
_OPEN_DATA_SQL_INIT_DIR = SQL_DIR / "open_data" / "init"
_CLINICAL_SQL_INIT_DIR = SQL_DIR / "clinical" / "init"


with DAG(
    dag_id=f"{NAMESPACE}-init-starrocks-tables",
    schedule_interval=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["radiant", "starrocks", "manual"],
    dag_display_name="Radiant - Init StarRocks Tables",
) as dag:
    tasks = []

    tables = [
        "germline_snv_consequence",
        "germline_snv_consequence_filter",
        "germline_snv_consequence_filter_partitioned",
        "germline_snv_occurrence",
        "germline_cnv_occurrence",
        "staging_exomiser",
        "exomiser",
        "staging_external_sequencing_experiment",
        "staging_sequencing_experiment",
        "staging_sequencing_experiment_delta",
        "germline_snv_tmp_variant",
        "germline_snv_staging_variant",
        "variant_lookup",
        "germline_snv_variant",
        "germline_snv_staging_variant_frequency",
        "germline_snv_variant_frequency",
        "germline_snv_variant_partitioned",
    ]
    for table in tables:
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
