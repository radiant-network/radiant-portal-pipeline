from airflow import DAG

from radiant.dags import DEFAULT_ARGS, NAMESPACE, SQL_DIR
from radiant.tasks.starrocks.operator import RadiantStarRocksOperator

_RADIANT_SQL_INIT_DIR = SQL_DIR / "radiant" / "init"
_OPEN_DATA_SQL_INIT_DIR = SQL_DIR / "open_data" / "init"


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
        "consequences",
        "consequences_filter",
        "consequences_filter_partitioned",
        "occurrences",
        "sequencing_experiment",
        "tmp_variants",
        "staging_variants",
        "variant_dict",
        "variants",
        "staging_variants_freq",
        "variants_frequencies",
        "variants_part",
    ]
    for table in tables:
        tasks.append(
            RadiantStarRocksOperator(
                task_id=f"create_table_{table}",
                sql=str(_RADIANT_SQL_INIT_DIR / f"{table}_create_table.sql"),
            )
        )

    group_ids = [
        "1000_genomes",
        "clinvar",
        "dbnsfp",
        "gnomad",
        "spliceai",
        "topmed_bravo",
        "gnomad_constraints",
        "omim_gene_panel",
        "hpo_gene_panel",
        "orphanet_gene_panel",
        "ddd_gene_panel",
        "cosmic_gene_panel",
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
            sql=str(_RADIANT_SQL_INIT_DIR / "variant_id_udf_create.sql"),
        )
    )
