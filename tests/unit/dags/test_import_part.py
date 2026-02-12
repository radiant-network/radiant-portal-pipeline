import pytest

from radiant.dags import NAMESPACE
from radiant.dags.import_part import tasks_output_processor


@pytest.fixture
def mock_results():
    return [
        [
            (
                1,
                "radiant_germline_annotation",
                "file_1.vcf",
                1,
                "germline",
                1,
                "cnv_1.vcf",
                "exomiser_1.tsv",
                1,
                "wgs",
                "routine",
                "sample_1",
                "proband",
                "M",
                "affected",
            ),
            (
                1,
                "radiant_germline_annotation",
                "file_1.vcf",
                1,
                "germline",
                2,
                None,
                None,
                2,
                "wgs",
                "routine",
                "sample_2",
                "role_2",
                "F",
                "not_affected",
            ),
            (
                2,
                "alignment_germline_variant_calling",
                "file_2.vcf",
                2,
                "germline",
                3,
                "cnv_2.vcf",
                None,
                3,
                "wgs",
                "routine",
                "sample_3",
                "role_3",
                "M",
                "affected",
            ),
        ]
    ]


@pytest.fixture
def mock_descriptions():
    return [
        [
            ("task_id",),
            ("task_type",),
            ("vcf_filepath",),
            ("part",),
            ("analysis_type",),
            ("seq_id",),
            ("cnv_vcf_filepath",),
            ("exomiser_filepath",),
            ("patient_id",),
            ("experimental_strategy",),
            ("request_priority",),
            ("aliquot",),
            ("family_role",),
            ("sex",),
            ("affected_status",),
        ]
    ]


def test_tasks_output_processor_returns_correct_tasks(mock_results, mock_descriptions):
    result = tasks_output_processor(mock_results, mock_descriptions)
    assert len(result[0]) == 2
    assert result[0][0]["task_id"] == 1
    assert result[0][1]["task_id"] == 2


def test_tasks_output_processor_handles_empty_results():
    result = tasks_output_processor([[]], [[]])
    assert result == [[]]


def test_dag_is_importable(dag_bag):
    assert f"{NAMESPACE}-import-part" in dag_bag.dags
    dag = dag_bag.get_dag(f"{NAMESPACE}-import-part")
    assert dag is not None


def test_dag_has_no_import_errors(dag_bag):
    assert len(dag_bag.import_errors) == 0


def test_dag_contains_all_tasks(dag_bag):
    dag = dag_bag.get_dag(f"{NAMESPACE}-import-part")
    task_ids = [task.task_id for task in dag.tasks]
    expected_tasks = [
        "start",
        "fetch_sequencing_experiment_delta",
        "get_iceberg_namespace",
        "get_tables_to_refresh",
        "sanity_check_tasks",
        "import_cnv_vcf_k8s",
        "import_germline_snv_vcf",
        "load_exomiser_files",
        "refresh_iceberg_tables",
        "extract_task_ids",
        "extract_seq_ids",
        "insert_variant_hashes",
        "overwrite_germline_snv_tmp_variant",
        "prepare_config",
        "insert_exomiser",
        "germline_cnv_occurrence.sanity_check_cnvs",
        "germline_cnv_occurrence.insert_germline_cnv_occurrences",
        "insert_germline_snv_occurrence",
        "insert_stg_germline_snv_variant_freq",
        "aggregate_germline_snv_variant_freq",
        "germline_snv_variant.insert_germline_snv_staging_variant",
        "germline_snv_variant.insert_germline_snv_variant",
        "germline_snv_variant.compute_parts",
        "germline_snv_variant.insert_germline_snv_variant_part",
        "germline_snv_consequence.import_germline_snv_consequence",
        "germline_snv_consequence.import_germline_snv_consequence_filter",
        "germline_snv_consequence.insert_germline_snv_consequence_filter_part",
        "update_sequencing_experiment",
    ]
    assert set(task_ids) == set(expected_tasks)


def test_dag_task_dependencies_are_valid(dag_bag):
    dag = dag_bag.get_dag(f"{NAMESPACE}-import-part")
    assert dag.validate() is None

    namespace_task = dag.get_task("get_iceberg_namespace")
    import_cnv_vcf_k8s_task = dag.get_task("import_cnv_vcf_k8s")
    assert namespace_task in import_cnv_vcf_k8s_task.get_flat_relatives(upstream=True)