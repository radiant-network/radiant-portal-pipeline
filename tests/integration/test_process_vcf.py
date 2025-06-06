import sys
from unittest.mock import patch

import pytest

from radiant.tasks.vcf.experiment import Case, Experiment
from radiant.tasks.vcf.process import process_chromosomes


def test_process_chromosomes(
    setup_namespace,
    iceberg_catalog_properties,
    iceberg_client,
    iceberg_container,
    indexed_vcfs,
    s3_fs,
):
    case = Case(
        case_id=1,
        part=1,
        analysis_type="germline",
        experiments=[
            Experiment(
                seq_id=1,
                task_id=1,
                patient_id=1,
                sample_id="SA0001",
                family_role="proband",
                affected_status="affected",
                sex="F",
                experimental_strategy="wgs",
                request_id=1,
                request_priority="routine",
            )
        ],
        vcf_filepath=indexed_vcfs["test.vcf"],
    )
    process_chromosomes(
        ["chr1"],
        case,
        s3_fs,
        catalog_name=iceberg_container.catalog_name,
        namespace=setup_namespace,
        catalog_properties=iceberg_catalog_properties,
    )

    table_names = iceberg_client.list_tables(setup_namespace)
    assert (setup_namespace, "germline_snv_occurrence") in table_names
    occ = iceberg_client.load_table(f"{setup_namespace}.germline_snv_occurrence").scan().to_arrow().to_pandas()
    print(occ)

    assert not occ.empty, "No occurrences were written to the iceberg table"
    assert ((occ["sample_id"] == "SA0001") & (occ["case_id"] == 1)).any(), (
        "Expected sample/case not found in occurrences"
    )
    assert all(occ["chromosome"] == "1"), "Unexpected chromosome values in output"
    assert occ["zygosity"][0] == "HET", "Unexpected zygosity value in output"
    assert occ["zygosity"][1] == "HOM", "Unexpected zygosity value in output"
    # expected_df = pd.DataFrame({
    #     "chromosome": ["chr1"],
    #     "start": [12345],
    #     "reference": ["A"],
    #     "alternate": ["T"],
    #     "case_id": [1],
    #     "sample_id": [1],
    #     # Add other columns as needed
    # })
    #
    # assert_frame_equal(occ.reset_index(drop=True), expected_df, check_like=True)


def fake_error_logging(*args, **kwargs):
    import ctypes

    libc = ctypes.CDLL(None)
    if sys.platform == "darwin":
        # macOS uses __stderrp
        c_stderr = ctypes.c_void_p.in_dll(libc, "__stderrp")
    else:
        # Linux and most other Unix-like OSes use stderr
        c_stderr = ctypes.c_void_p.in_dll(libc, "stderr")
    libc.fprintf(c_stderr, b"[E:: Fake error message\n")


def test_process_chromosomes_error(
    setup_namespace,
    iceberg_catalog_properties,
    iceberg_container,
    indexed_vcfs,
    minio_container,
    s3_fs,
):
    case = Case(
        case_id=1,
        part=1,
        analysis_type="germline",
        experiments=[
            Experiment(
                seq_id=1,
                task_id=1,
                patient_id=1,
                sample_id="SA0001",
                family_role="proband",
                affected_status="affected",
                sex="F",
                experimental_strategy="wgs",
                request_id=1,
                request_priority="routine",
            )
        ],
        vcf_filepath=indexed_vcfs["test.vcf"],
    )
    with (
        pytest.raises(Exception) as exc,
        patch("radiant.tasks.iceberg.table_accumulator.TableAccumulator.write_files", fake_error_logging),
    ):
        process_chromosomes(
            ["chr1"],
            case,
            s3_fs,
            catalog_name=iceberg_container.catalog_name,
            namespace=setup_namespace,
            catalog_properties=iceberg_catalog_properties,
        )

    assert "Detected error: [E::" in str(exc.value)
