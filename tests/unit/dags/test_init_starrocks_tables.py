from radiant.dags import NAMESPACE

_BASE_DAG_ID = f"{NAMESPACE}-init-starrocks-base-tables"

_BASE_TABLES = [
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


def test_dag_is_importable(dag_bag):
    assert _BASE_DAG_ID in dag_bag.dags
    assert dag_bag.get_dag(_BASE_DAG_ID) is not None


def test_dag_has_correct_number_of_tasks(dag_bag):
    dag = dag_bag.get_dag(_BASE_DAG_ID)
    # 11 base radiant tables + 2 clinical tables + 20 open data tables + 2 UDFs
    assert len(dag.tasks) == 35


def test_dag_has_all_base_tasks(dag_bag):
    dag = dag_bag.get_dag(_BASE_DAG_ID)
    task_ids = [task.task_id for task in dag.tasks]
    for table in _BASE_TABLES:
        assert f"create_table_{table}" in task_ids

    for table in ["table_patient_access", "table_brim"]:
        assert f"create_{table}" in task_ids

    group_ids = [
        "1000_genomes",
        "clinvar",
        "dbnsfp",
        "gnomad",
        "spliceai",
        "topmed_bravo",
        "gnomad_constraint",
        "omim_gene_panel",
        "hpo_gene_panel",
        "orphanet_gene_panel",
        "ddd_gene_panel",
        "cosmic_gene_panel",
        "mondo_term",
        "hpo_term",
    ]
    for group in group_ids:
        assert f"create_{group}" in task_ids
