from radiant.dags import NAMESPACE

_GLOBAL_DAG_ID = f"{NAMESPACE}-init-starrocks-tables"
_TENANT_DAG_ID = f"{NAMESPACE}-init-tenant-starrocks-tables"

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


def test_dag_is_importable(dag_bag):
    assert _GLOBAL_DAG_ID in dag_bag.dags
    assert dag_bag.get_dag(_GLOBAL_DAG_ID) is not None


def test_dag_has_correct_number_of_tasks(dag_bag):
    dag = dag_bag.get_dag(_GLOBAL_DAG_ID)
    # 11 shared radiant tables + 2 clinical tables + 20 open data tables + 2 UDFs
    assert len(dag.tasks) == 35


def test_dag_has_all_shared_tasks(dag_bag):
    dag = dag_bag.get_dag(_GLOBAL_DAG_ID)
    task_ids = [task.task_id for task in dag.tasks]
    for table in _SHARED_TABLES:
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


def test_global_dag_excludes_per_tenant_tables(dag_bag):
    dag = dag_bag.get_dag(_GLOBAL_DAG_ID)
    task_ids = [task.task_id for task in dag.tasks]
    for table in _PER_TENANT_TABLES:
        assert f"create_table_{table}" not in task_ids


def test_tenant_dag_is_importable(dag_bag):
    assert _TENANT_DAG_ID in dag_bag.dags
    assert dag_bag.get_dag(_TENANT_DAG_ID) is not None


def test_tenant_dag_has_all_per_tenant_tables(dag_bag):
    dag = dag_bag.get_dag(_TENANT_DAG_ID)
    task_ids = [task.task_id for task in dag.tasks]
    assert len(dag.tasks) == len(_PER_TENANT_TABLES)
    for table in _PER_TENANT_TABLES:
        assert f"create_table_{table}" in task_ids
