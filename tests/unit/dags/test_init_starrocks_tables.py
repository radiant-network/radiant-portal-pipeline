from radiant.dags import NAMESPACE


def test_dag_is_importable(dag_bag):
    assert f"{NAMESPACE}-init-starrocks-tables" in dag_bag.dags
    dag = dag_bag.get_dag(f"{NAMESPACE}-init-starrocks-tables")
    assert dag is not None


def test_dag_has_correct_number_of_tasks(dag_bag):
    dag = dag_bag.get_dag(f"{NAMESPACE}-init-starrocks-tables")
    assert len(dag.tasks) == 25  # 12 radiant tables + 11 open data tables + 1 create function + 1 start task


def test_dag_has_all_tasks(dag_bag):
    dag = dag_bag.get_dag(f"{NAMESPACE}-init-starrocks-tables")
    task_ids = [task.task_id for task in dag.tasks]
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
        assert f"create_table_{table}" in task_ids

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
        assert f"create_{group}" in task_ids
