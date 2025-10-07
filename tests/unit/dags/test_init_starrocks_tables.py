from radiant.dags import NAMESPACE


def test_dag_is_importable(dag_bag):
    assert f"{NAMESPACE}-init-starrocks-tables" in dag_bag.dags
    dag = dag_bag.get_dag(f"{NAMESPACE}-init-starrocks-tables")
    assert dag is not None


def test_dag_has_correct_number_of_tasks(dag_bag):
    dag = dag_bag.get_dag(f"{NAMESPACE}-init-starrocks-tables")
    assert len(dag.tasks) == 38  # 17 radiant tables + 19 open data tables + 2 create function


def test_dag_has_all_tasks(dag_bag):
    dag = dag_bag.get_dag(f"{NAMESPACE}-init-starrocks-tables")
    task_ids = [task.task_id for task in dag.tasks]
    tables = [
        "consequence",
        "consequence_filter",
        "consequence_filter_partitioned",
        "germline_cnv_occurrence",
        "occurrence",
        "staging_sequencing_experiment",
        "staging_external_sequencing_experiment",
        "tmp_variant",
        "staging_variant",
        "variant_lookup",
        "variant",
        "staging_variant_frequency",
        "variant_frequency",
        "variant_partitioned",
    ]
    for table in tables:
        assert f"create_table_{table}" in task_ids

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
