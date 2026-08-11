import os
import tempfile
from pathlib import Path

import pytest

from radiant.tasks.vcf.cnv.somatic.process import process_tasks
from radiant.tasks.vcf.experiment import Experiment, TumorOnlyVariantCallingTask
from tests.integration.conftest import compress_and_index_vcf

# The nine ASCN columns are absent altogether on DRAGEN 3.10.8 and declared-but-omitted per record on
# 4.2.4, so the round trip has to keep them nullable rather than reject or coerce a missing value.
ASCN_COLUMNS = ["cn", "cnf", "cnq", "mcn", "mcnf", "mcnq", "maf", "sd", "ascn_as"]

_UNIT_RESOURCES_DIR = Path(__file__).parent.parent / "resources"


@pytest.fixture(scope="module")
def somatic_cnv_vcfs():
    """Compresses and indexes the somatic CNV fixtures, which live with the unit resources.

    The session-wide `indexed_vcfs` fixture only covers `resources/integration`; reading these two from
    where they already are keeps one copy of each file rather than letting a duplicate drift.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        output = {}
        for filename in ("test_somatic_cnv.vcf", "test_somatic_cnv_no_ascn.vcf"):
            dest_path = os.path.join(tmpdir, filename + ".gz")
            compress_and_index_vcf(_UNIT_RESOURCES_DIR / filename, dest_path)
            output[filename] = dest_path

        yield output


def _task(vcf_filepath: str, aliquot: str) -> TumorOnlyVariantCallingTask:
    return TumorOnlyVariantCallingTask(
        task_id=70,
        part=1,
        analysis_type="somatic",
        deleted=False,
        experiments=[
            Experiment(
                seq_id=64,
                patient_id=62,
                aliquot=aliquot,
                tenant_code="tenant1",
                family_role="proband",
                affected_status="affected",
                sex="female",
                experimental_strategy="wgs",
                request_priority="routine",
                histology_type="tumoral",
            )
        ],
        cnv_vcf_filepath=vcf_filepath,
    )


def _process_and_load(task, setup_iceberg_namespace, iceberg_client, catalog, catalog_properties):
    process_tasks(
        [task],
        catalog_name=catalog.catalog_name,
        namespace=setup_iceberg_namespace,
        catalog_properties=catalog_properties,
    )

    table_names = iceberg_client.list_tables(setup_iceberg_namespace)
    assert (setup_iceberg_namespace, "somatic_cnv_occurrence") in table_names

    return iceberg_client.load_table(f"{setup_iceberg_namespace}.somatic_cnv_occurrence").scan().to_arrow().to_pandas()


def test_process_task(
    setup_iceberg_namespace,
    iceberg_catalog_properties,
    iceberg_client,
    rest_iceberg_catalog_instance,
    somatic_cnv_vcfs,
):
    occ = _process_and_load(
        _task(somatic_cnv_vcfs["test_somatic_cnv.vcf"], "TCRBOA6-T"),
        setup_iceberg_namespace,
        iceberg_client,
        rest_iceberg_catalog_instance,
        iceberg_catalog_properties,
    )

    assert not occ.empty, "No occurrences were written to the iceberg table"
    assert (
        (occ["aliquot"] == "TCRBOA6-T")
        & (occ["seq_id"] == 64)
        & (occ["task_id"] == 70)
        & (occ["part"] == 1)
        & (occ["tenant_code"] == "tenant1")
    ).all(), "Expected sample/sequencingID/taskID/part/tenant tagging not found in occurrences"

    # The reference segment (ALT `.`) and the `<CNV>` segment are dropped by the extractor: both would
    # produce a NULL `cnv_id` against a NOT NULL key column and fail the whole StarRocks load.
    assert list(zip(occ["type"], occ["alternate"], strict=True)) == [
        ("LOSS", "<DEL>"),
        ("CNLOH", "<LOH>"),
        ("GAIN", "<DUP>"),
        ("GAINLOH", "<LOH>"),
        ("LOSS", "<DEL>"),
    ]
    assert occ["chromosome"].isin(["1", "2"]).all(), "Some chromosome values are invalid"
    assert (occ["length"] == occ["end"] - occ["start"]).all()

    # `SVLEN` is one value per ALT allele on an LOH row (`SVLEN=-220050,220050`), so a tuple would reach
    # a scalar column; element 0 is stored.
    cnloh = occ[occ["type"] == "CNLOH"].iloc[0]
    assert cnloh["svlen"] == -220050
    assert cnloh["calls"].tolist() == [1, 2], "GT is 1/2 on a legacy multi-allelic LOH row"
    assert cnloh["maf"] == pytest.approx(0.0), "MAF=0 is the direct LOH marker"
    # `CN`/`MCN` are omitted from the LOH record's own FORMAT even though 4.2.4 declares them.
    assert cnloh[["cn", "mcn"]].isna().all()

    # A FORMAT field written `.` for the sample must land as null, not as htslib's missing-value sentinel.
    missing_ascn = occ[occ["start"] == 80000000].iloc[0]
    assert missing_ascn[ASCN_COLUMNS].isna().all()

    gain = occ[occ["type"] == "GAIN"].iloc[0]
    assert gain["cn"] == 3
    assert gain["ascn_as"] == 12
    assert gain["cipos"].tolist() == [-150, 150]
    assert gain["filter"] == "PASS"
    assert occ[occ["type"] == "CNLOH"].iloc[0]["filter"] == "cnvQual;segmentMean", "multi-filter stays joined"


def test_process_task_without_ascn_fields(
    setup_iceberg_namespace,
    iceberg_catalog_properties,
    iceberg_client,
    rest_iceberg_catalog_instance,
    somatic_cnv_vcfs,
):
    """DRAGEN 3.10.8 declares none of the ASCN fields, so `record.format` raises rather than returning
    None -- the whole column set has to come back null instead of failing the write."""
    occ = _process_and_load(
        _task(somatic_cnv_vcfs["test_somatic_cnv_no_ascn.vcf"], "TCRBOA7-T"),
        setup_iceberg_namespace,
        iceberg_client,
        rest_iceberg_catalog_instance,
        iceberg_catalog_properties,
    )

    assert list(occ["type"]) == ["LOSS", "GAIN"]
    assert occ[ASCN_COLUMNS].isna().all().all(), "ASCN columns must be null when DRAGEN never declared them"
    assert occ["cipos"].isna().all(), "CIPOS is not declared by this DRAGEN version"
    assert occ["svtype"].tolist() == ["CNV", "CNV"], "a record with no SVTYPE defaults to CNV"
