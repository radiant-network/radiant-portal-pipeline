import pytest

from radiant.dags import NAMESPACE
from radiant.dags.import_cosmic_mutation_set import normalized_filepath_for

_DAG_ID = f"{NAMESPACE}-import-cosmic-mutation-set"


def test_dag_is_importable(dag_bag):
    assert _DAG_ID in dag_bag.dags
    assert dag_bag.import_errors == {}


def test_dag_runs_normalize_then_the_three_starrocks_steps(dag_bag):
    dag = dag_bag.get_dag(_DAG_ID)
    assert {t.task_id for t in dag.tasks} == {
        "resolve_normalized_filepath",
        "normalize_cosmic_mutation_set_k8s",  # IS_AWS is unset in tests, so the k8s operator is built
        "load_raw_cosmic_mutation_set",
        "insert_cosmic_mutation_set_hashes",
        "insert_cosmic_mutation_set",
    }
    normalize = dag.get_task("normalize_cosmic_mutation_set_k8s")
    assert normalize.upstream_task_ids == {"resolve_normalized_filepath"}
    assert normalize.downstream_task_ids == {"load_raw_cosmic_mutation_set"}
    assert dag.get_task("load_raw_cosmic_mutation_set").downstream_task_ids == {"insert_cosmic_mutation_set_hashes"}
    assert dag.get_task("insert_cosmic_mutation_set_hashes").downstream_task_ids == {"insert_cosmic_mutation_set"}


def test_normalize_reads_its_paths_from_the_params_and_the_resolved_output(dag_bag):
    normalize = dag_bag.get_dag(_DAG_ID).get_task("normalize_cosmic_mutation_set_k8s")
    assert normalize.op_kwargs["input_filepath"] == "{{ params.cosmic_mutation_set_filepath }}"
    assert normalize.op_kwargs["reference_fasta_filepath"] == "{{ params.reference_fasta_filepath }}"
    assert normalize.op_kwargs["output_filepath"].operator.task_id == "resolve_normalized_filepath"


def test_load_reads_the_normalized_file_into_the_staging_table(dag_bag):
    load = dag_bag.get_dag(_DAG_ID).get_task("load_raw_cosmic_mutation_set")
    assert load.table == "{{ mapping.starrocks_raw_cosmic_mutation_set }}"
    assert load.truncate is True
    assert load.parameters == {"tsv_filepath": ["{{ ti.xcom_pull(task_ids='resolve_normalized_filepath') }}"]}
    assert "LOAD LABEL {{ database_name }}.{{ load_label }}" in load.sql
    assert "DATA INFILE %(tsv_filepath)s" in load.sql
    assert "skip_header = 1" in load.sql


def test_insert_dedupes_per_locus_on_sample_mutated(dag_bag):
    insert = dag_bag.get_dag(_DAG_ID).get_task("insert_cosmic_mutation_set")
    assert "INSERT OVERWRITE {{ mapping.starrocks_cosmic_mutation_set }}" in insert.sql
    assert "PARTITION BY t.locus_hash ORDER BY t.sample_mutated DESC" in insert.sql
    hashes = dag_bag.get_dag(_DAG_ID).get_task("insert_cosmic_mutation_set_hashes")
    assert "INSERT INTO {{ mapping.starrocks_variant_lookup }}" in hashes.sql
    assert "FROM {{ mapping.starrocks_raw_cosmic_mutation_set }}" in hashes.sql


def test_params(dag_bag):
    params = dag_bag.get_dag(_DAG_ID).params
    filepath = params.get_param("cosmic_mutation_set_filepath")
    assert filepath.schema["type"] == "string"
    assert not filepath.has_value, "the staging table is truncated first, so the file must be required"
    assert params.get_param("reference_fasta_filepath").schema["type"] == "string"
    assert params["normalized_filepath"] is None


@pytest.mark.parametrize(
    ("input_filepath", "override", "expected"),
    [
        ("s3://b/cosmic/cmc_export.tsv.gz", None, "s3://b/cosmic/cmc_export.normalized.tsv.gz"),
        ("s3://b/cosmic/cmc_export.tsv", None, "s3://b/cosmic/cmc_export.normalized.tsv.gz"),
        ("s3://b/cosmic/cmc_export.tsv.gz", "s3://o/out.tsv.gz", "s3://o/out.tsv.gz"),
        ("s3://b/cosmic/cmc_export.tsv.gz", "", "s3://b/cosmic/cmc_export.normalized.tsv.gz"),
    ],
)
def test_normalized_filepath_for(input_filepath, override, expected):
    assert normalized_filepath_for(input_filepath, override) == expected


def test_resolve_refuses_to_run_without_a_reference(dag_bag):
    resolve = dag_bag.get_dag(_DAG_ID).get_task("resolve_normalized_filepath")
    with pytest.raises(ValueError, match="reference_fasta_filepath"):
        resolve.python_callable(
            params={"cosmic_mutation_set_filepath": "s3://b/x.tsv.gz", "reference_fasta_filepath": ""}
        )
    assert (
        resolve.python_callable(
            params={"cosmic_mutation_set_filepath": "s3://b/x.tsv.gz", "reference_fasta_filepath": "s3://r/ref.fa"}
        )
        == "s3://b/x.normalized.tsv.gz"
    )
