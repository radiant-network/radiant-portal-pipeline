import os
import tempfile
from pathlib import Path

import pytest

from radiant.tasks.vcf.cnv.germline.process import process_tasks
from radiant.tasks.vcf.experiment import (
    AlignmentGermlineVariantCallingTask,
    Experiment,
    RadiantGermlineCnvAnnotationTask,
)
from tests.integration.conftest import compress_and_index_vcf


def test_process_task(
    setup_iceberg_namespace,
    iceberg_catalog_properties,
    iceberg_client,
    rest_iceberg_catalog_instance,
    indexed_vcfs,
):
    task = AlignmentGermlineVariantCallingTask(
        task_id=1,
        part=1,
        analysis_type="germline",
        deleted=False,
        experiments=[
            Experiment(
                seq_id=1,
                patient_id=1,
                aliquot="SA0001",
                tenant_code="tenant1",
                family_role="proband",
                affected_status="affected",
                sex="F",
                experimental_strategy="wgs",
                request_priority="routine",
            )
        ],
        cnv_vcf_filepath=indexed_vcfs["test_cnv.vcf"],
    )
    process_tasks(
        [task],
        catalog_name=rest_iceberg_catalog_instance.catalog_name,
        namespace=setup_iceberg_namespace,
        catalog_properties=iceberg_catalog_properties,
    )

    table_names = iceberg_client.list_tables(setup_iceberg_namespace)
    assert (setup_iceberg_namespace, "germline_cnv_occurrence") in table_names
    occ = iceberg_client.load_table(f"{setup_iceberg_namespace}.germline_cnv_occurrence").scan().to_arrow().to_pandas()

    assert not occ.empty, "No occurrences were written to the iceberg table"
    assert (
        (occ["aliquot"] == "SA0001") & (occ["seq_id"] == 1) & (occ["task_id"] == 1) & (occ["tenant_code"] == "tenant1")
    ).any(), "Expected sample/sequencingID/taskID/tenant not found in occurrences"
    assert occ["chromosome"].isin(["1", "X"]).all(), "Some chromosome values are invalid"


_UNIT_RESOURCES_DIR = Path(__file__).parent.parent / "resources"


@pytest.fixture(scope="module")
def slivar_vcf():
    """The trio slivar fixture lives with the unit resources; compressed here as the pipeline publishes it."""
    with tempfile.TemporaryDirectory() as tmpdir:
        dest_path = os.path.join(tmpdir, "test_cnv_slivar.vcf.gz")
        compress_and_index_vcf(_UNIT_RESOURCES_DIR / "test_cnv_slivar.vcf", dest_path)
        yield dest_path


def test_process_slivar_task_writes_the_probands_set(
    setup_iceberg_namespace,
    iceberg_catalog_properties,
    iceberg_client,
    rest_iceberg_catalog_instance,
    slivar_vcf,
):
    """A `radiant_germline_cnv_annotation` task writes the proband's carrier records only, with the parental
    origin, the transmission mode and the parents' FORMAT values, through the real Iceberg schema."""
    members = [
        ("S_KID", 9711, "proband"),
        ("S_MOM", 9712, "mother"),
        ("S_DAD", 9713, "father"),
        ("S_SIB", 9714, "sibling"),
    ]
    task = RadiantGermlineCnvAnnotationTask(
        task_id=9710,
        part=1,
        analysis_type="germline",
        deleted=False,
        experiments=[
            Experiment(
                seq_id=seq_id,
                patient_id=seq_id,
                aliquot=aliquot,
                tenant_code="tenant1",
                family_role=role,
                affected_status="affected" if role in ("proband", "sibling") else "non_affected",
                sex="male",
                experimental_strategy="wgs",
            )
            for aliquot, seq_id, role in members
        ],
        cnv_vcf_filepath=slivar_vcf,
    )
    process_tasks(
        [task],
        catalog_name=rest_iceberg_catalog_instance.catalog_name,
        namespace=setup_iceberg_namespace,
        catalog_properties=iceberg_catalog_properties,
    )

    occ = iceberg_client.load_table(f"{setup_iceberg_namespace}.germline_cnv_occurrence").scan().to_arrow().to_pylist()
    assert {(r["seq_id"], r["aliquot"], r["task_id"]) for r in occ} == {(9711, "S_KID", 9710)}
    assert len(occ) == 6
    maternal = next(r for r in occ if r["name"] == "DRAGEN:LOSS:chr1:10001-20000")
    assert (maternal["parental_origin"], maternal["transmission_mode"]) == ("MOTHER", "inherited_maternal")
    assert (maternal["mother_calls"], maternal["mother_cn"], maternal["mother_name"]) == (
        [0, 1],
        1,
        "DRAGEN:LOSS:chr1:10001-20000",
    )
    assert (maternal["father_calls"], maternal["father_cn"], maternal["father_name"]) == ([0, 0], 2, None)
