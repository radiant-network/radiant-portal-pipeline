from radiant.dags import NAMESPACE


def test_dag_is_importable(dag_bag):
    assert f"{NAMESPACE}-import-open-data" in dag_bag.dags
    dag = dag_bag.get_dag(f"{NAMESPACE}-import-open-data")
    assert dag is not None


def test_dag_has_correct_number_of_tasks(dag_bag):
    dag = dag_bag.get_dag(f"{NAMESPACE}-import-open-data")
    variant_group_ids = ["1000_genomes", "clinvar", "dbnsfp", "dbsnp", "gnomad", "spliceai", "topmed_bravo"]
    gene_group_ids = [
        "gnomad_constraint",
        "omim_gene_panel",
        "hpo_gene_panel",
        "orphanet_gene_panel",
        "ddd_gene_panel",
        "cosmic_gene_panel",
        "mondo_term",
        "hpo_term",
    ]
    assert len(dag.tasks) == 3 + len(gene_group_ids) + len(variant_group_ids) * 2


def test_dag_has_all_group_tasks(dag_bag):
    dag = dag_bag.get_dag(f"{NAMESPACE}-import-open-data")
    task_ids = [task.task_id for task in dag.tasks]
    group_ids = ["1000_genomes", "clinvar", "dbnsfp", "dbsnp", "gnomad", "spliceai", "topmed_bravo"]
    for group in group_ids:
        assert f"insert_hashes_{group}" in task_ids
        assert f"insert_{group}" in task_ids

    assert "insert_gnomad_constraint" in task_ids
    assert "insert_omim_gene_panel" in task_ids
