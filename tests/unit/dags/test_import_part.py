import pytest

from radiant.dags import NAMESPACE
from radiant.dags.import_part import (
    fan_out_by_tenant,
    fan_out_exomiser_by_tenant,
    tasks_output_processor,
)

_MULTI_TENANT_TASKS = [
    {
        "task_id": 1,
        "deleted": False,
        "experiments": [{"seq_id": 10, "tenant_code": "chop"}, {"seq_id": 11, "tenant_code": "chop"}],
    },
    {"task_id": 2, "deleted": False, "experiments": [{"seq_id": 20, "tenant_code": "chusj"}]},
    {"task_id": 3, "deleted": True, "experiments": [{"seq_id": 30, "tenant_code": "chop"}]},
]


def _by_tenant(rows):
    return {r["tenant_code"]: r for r in rows}


def test_fan_out_by_tenant_groups_ids_per_tenant():
    rows = _by_tenant(fan_out_by_tenant(_MULTI_TENANT_TASKS, part=5))
    assert set(rows) == {"chop", "chusj"}

    chop = rows["chop"]["parameters"]
    assert chop["tenant_code"] == "chop"
    assert chop["part"] == 5
    assert chop["task_ids"] == [1]
    assert chop["deleted_task_ids"] == [3]
    assert chop["seq_ids"] == [10, 11]
    assert chop["deleted_seq_ids"] == [30]

    chusj = rows["chusj"]["parameters"]
    assert chusj["task_ids"] == [2]
    assert chusj["seq_ids"] == [20]
    # Sentinels keep `IN (...)` valid when a tenant has nothing in a bucket.
    assert chusj["deleted_task_ids"] == [-1]
    assert chusj["deleted_seq_ids"] == [-1]


def test_fan_out_exomiser_carries_per_tenant_tasks():
    rows = _by_tenant(fan_out_exomiser_by_tenant(_MULTI_TENANT_TASKS, part=5))
    # chop owns tasks 1 (germline) and 3 (deleted); chusj owns task 2.
    assert {t["task_id"] for t in rows["chop"]["tasks"]} == {1, 3}
    assert {t["task_id"] for t in rows["chusj"]["tasks"]} == {2}
    # still carries the same per-tenant parameters as fan_out_by_tenant
    assert rows["chop"]["parameters"]["tenant_code"] == "chop"


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
                "tenant1",
                "proband",
                "M",
                "affected",
                False,
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
                "tenant1",
                "role_2",
                "F",
                "not_affected",
                False,
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
                "tenant1",
                "role_3",
                "M",
                "affected",
                False,
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
            ("tenant_code",),
            ("family_role",),
            ("sex",),
            ("affected_status",),
            ("deleted",),
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
        "get_iceberg_namespace",
        "get_tables_to_refresh",
        "fetch_sequencing_experiment_delta",
        "fetch_all_tenants",
        "sanity_check_tasks",
        "prepare_config",
        "fan_out_by_tenant",
        "fan_out_exomiser_by_tenant",
        "extract_task_ids",
        "checkpoint_after_setup",
        "import_cnv_vcf_k8s",
        "import_somatic_snv_vcf",
        "import_germline_snv_vcf",
        "checkpoint_after_vcf_imports",
        "load_exomiser_files",
        "refresh_iceberg_tables",
        "germline_cnv_occurrence.sanity_check_cnvs",
        "germline_cnv_occurrence.insert_germline_cnv_occurrences",
        "insert_variant_hashes",
        "overwrite_snv_tmp_variant",
        "insert_exomiser",
        "checkpoint_after_exomiser",
        "germline_snv_occurrence.insert_germline_snv_occurrence",
        "germline_snv_occurrence.insert_stg_germline_snv_variant_freq",
        "germline_snv_occurrence.aggregate_germline_snv_variant_freq",
        "germline_snv_occurrence.sanity_check_delta_germline_snv",
        "somatic_snv_occurrence.insert_somatic_snv_occurrences",
        "somatic_snv_occurrence.insert_stg_somatic_snv_variant_freq",
        "somatic_snv_occurrence.aggregate_somatic_snv_variant_freq",
        "somatic_snv_occurrence.sanity_check_delta_somatic_snv",
        "snv_variant.sanity_check_any_snv",
        "snv_variant.insert_snv_staging_variant",
        "snv_variant.insert_snv_variant",
        "snv_variant.compute_parts",
        "snv_variant.insert_snv_variant_part",
        "snv_consequence.sanity_check_any_snv",
        "snv_consequence.import_snv_consequence",
        "snv_consequence.import_snv_consequence_filter",
        "snv_consequence.insert_snv_consequence_filter_part",
        "checkpoint_after_variants",
        "delete_sequencing_experiments",
        "update_sequencing_experiment",
    ]
    assert set(task_ids) == set(expected_tasks)


def test_dag_task_dependencies_are_valid(dag_bag):
    dag = dag_bag.get_dag(f"{NAMESPACE}-import-part")
    assert dag.validate() is None

    namespace_task = dag.get_task("get_iceberg_namespace")
    import_cnv_vcf_k8s_task = dag.get_task("import_cnv_vcf_k8s")
    assert namespace_task in import_cnv_vcf_k8s_task.get_flat_relatives(upstream=True)
