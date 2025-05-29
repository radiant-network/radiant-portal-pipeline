import pytest

from radiant.dags import NAMESPACE
from radiant.dags.import_part import cases_output_processor


@pytest.fixture
def mock_results():
    return [
        [
            (1, "file_1.vcf", 1, "germline", 1, 1, "patient_1", "sample_1", "role_1", "M", True),
            (1, "file_1.vcf", 1, "germline", 2, 2, "patient_2", "sample_2", "role_2", "F", False),
            (2, "file_2.vcf", 2, "germline", 3, 3, "patient_3", "sample_3", "role_3", "M", True),
        ]
    ]


@pytest.fixture
def mock_descriptions():
    return [
        [
            ("case_id",),
            ("vcf_filepath",),
            ("part",),
            ("analysis_type",),
            ("seq_id",),
            ("task_id",),
            ("patient_id",),
            ("sample_id",),
            ("family_role",),
            ("sex",),
            ("affected_status",),
        ]
    ]


def test_cases_output_processor_returns_correct_cases(mock_results, mock_descriptions):
    result = cases_output_processor(mock_results, mock_descriptions)
    assert len(result[0]) == 2
    assert result[0][0]["case_id"] == 1
    assert result[0][1]["case_id"] == 2


def test_cases_output_processor_handles_empty_results():
    result = cases_output_processor([[]], [[]])
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
        "sanity_check_cases",
        "import_vcf",
        "refresh_iceberg_tables",
        "extract_case_ids",
        "insert_variant_hashes",
        "overwrite_tmp_variant",
        "insert_occurrence",
        "insert_stg_variant_freq",
        "aggregate_variant_freq",
        "variant.insert_staging_variant",
        "variant.insert_variant",
        "variant.compute_parts",
        "variant.insert_variant_part",
        "consequence.import_consequence",
        "consequence.import_consequence_filter",
        "consequence.insert_consequence_filter_part",
        "update_sequencing_experiment",
    ]
    assert set(task_ids) == set(expected_tasks)


def test_dag_task_dependencies_are_valid(dag_bag):
    dag = dag_bag.get_dag(f"{NAMESPACE}-import-part")
    assert dag.validate() is None
