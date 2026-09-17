import pytest

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
        "ensembl_gene",
        "ensembl_exon_by_gene",
        "ddd_gene_panel",
        "cosmic_gene_panel",
        "mondo_term",
        "hpo_term",
    ]
    # start + the metadata-refresh pair + 3 file-driven load/insert tasks + 2 short-circuit gates
    assert len(dag.tasks) == 8 + len(gene_group_ids) + len(variant_group_ids) * 2


def test_metadata_cache_is_refreshed_before_any_source_is_read(dag_bag):
    """`latest` is a tag OpenDataLake moves on each publish, and StarRocks caches external-catalog
    metadata -- so a stale cache makes the refresh re-import the previous release (SJRA-1811 §4, P1)."""
    dag = dag_bag.get_dag(f"{NAMESPACE}-import-open-data")
    refresh = dag.get_task("refresh_iceberg_tables")
    assert refresh.upstream_task_ids == {"get_tables_to_refresh"}
    # Gates the whole chain: the first source load hangs off the refresh, not off `start`.
    assert refresh.downstream_task_ids == {"insert_hashes_1000_genomes"}
    assert dag.get_task("start").downstream_task_ids == {"get_tables_to_refresh"}


def test_file_driven_loads_are_gated_on_their_params(dag_bag):
    dag = dag_bag.get_dag(f"{NAMESPACE}-import-open-data")
    assert dag.get_task("has_raw_rcv_filepaths").downstream_task_ids == {"load_raw_clinvar_rcv_summary"}
    assert dag.get_task("has_cytoband_filepath").downstream_task_ids == {"load_cytoband"}
    # Separate branches: cytoband is not downstream of the RCV chain any more.
    assert "load_cytoband" not in dag.get_task("insert_clinvar_rcv_summary").downstream_task_ids


@pytest.mark.parametrize(
    ("task_id", "param"),
    [("has_raw_rcv_filepaths", "raw_rcv_filepaths"), ("has_cytoband_filepath", "cytoband_filepath")],
)
def test_file_driven_gates_read_params_from_the_run_context(dag_bag, task_id, param):
    gate = dag_bag.get_dag(f"{NAMESPACE}-import-open-data").get_task(task_id)
    assert gate.op_args == (), "params must not be passed positionally"

    def decide(params):
        return gate.python_callable(**gate.determine_kwargs({"params": params}))

    assert decide({param: ["s3://bucket/file.tsv"]}) is True
    assert decide({}) is False
    assert decide({param: None}) is False


def test_dag_has_all_group_tasks(dag_bag):
    dag = dag_bag.get_dag(f"{NAMESPACE}-import-open-data")
    task_ids = [task.task_id for task in dag.tasks]
    group_ids = ["1000_genomes", "clinvar", "dbnsfp", "dbsnp", "gnomad", "spliceai", "topmed_bravo"]
    for group in group_ids:
        assert f"insert_hashes_{group}" in task_ids
        assert f"insert_{group}" in task_ids

    assert "insert_gnomad_constraint" in task_ids
    assert "insert_omim_gene_panel" in task_ids
