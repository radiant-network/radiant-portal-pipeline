from collections import defaultdict

import pytest

from radiant.tasks.vcf.experiment import Experiment, RadiantSomaticAnnotationTask
from radiant.tasks.vcf.snv.somatic.process import commit_partitions, merge_partitions_in_place, process_task


def test_import_somatic_snv_vcf(
    setup_iceberg_namespace,
    iceberg_catalog_properties,
    iceberg_client,
):
    task = RadiantSomaticAnnotationTask(
        task_id=1,
        part=1,
        analysis_type="somatic",
        deleted=False,
        experiments=[
            Experiment(
                seq_id=1,
                patient_id=1,
                aliquot="TCR002361_SRX1091647-T",
                tenant_code="tenant1",
                family_role="proband",
                affected_status="affected",
                sex="female",
                experimental_strategy="wgs",
                request_priority="routine",
                histology_type="tumoral",
            ),
            Experiment(
                seq_id=2,
                patient_id=1,
                aliquot="TCR002361_SRX1091646-N",
                tenant_code="tenant1",
                family_role="proband",
                affected_status="affected",
                sex="female",
                experimental_strategy="wgs",
                request_priority="routine",
                histology_type="normal",
            ),
        ],
        vcf_filepath="tests/resources/test_somatic_snv.vcf",
    )

    table_names = iceberg_client.list_tables(setup_iceberg_namespace)
    assert (setup_iceberg_namespace, "somatic_snv_occurrence") in table_names

    merged_partitions = defaultdict(list)

    partitions = process_task(
        task=task, namespace=setup_iceberg_namespace, catalog_properties=iceberg_catalog_properties
    )
    merge_partitions_in_place(merged_partitions, partitions)
    commit_partitions(merged_partitions, iceberg_catalog_properties=iceberg_catalog_properties)

    # Check that the expected tables were created and contain data
    occ = iceberg_client.load_table(f"{setup_iceberg_namespace}.somatic_snv_occurrence").scan().to_arrow().to_pandas()
    variants = iceberg_client.load_table(f"{setup_iceberg_namespace}.snv_variant").scan().to_arrow().to_pandas()
    consequences = (
        iceberg_client.load_table(f"{setup_iceberg_namespace}.snv_consequence").scan().to_arrow().to_pandas()
    )

    assert len(occ) == 21, "Unexpected number of rows in occurrences table"
    assert occ.chromosome.unique().tolist() == ["1", "4", "12"], "Unexpected chromosome values in occurrences table"

    assert len(variants) == 21, "Unexpected number of rows in variants table"
    assert variants.chromosome.unique().tolist() == ["1", "4", "12"], "Unexpected chromosome values in variants table"

    assert len(consequences) == 236, "Unexpected number of rows in consequences table"
    assert variants.chromosome.unique().tolist() == ["1", "4", "12"], (
        "Unexpected chromosome values in consequences table"
    )

    # FORMAT/SQ is carried per sample — the fixture seeds it on 3 records (normal is VCF column 0)
    assert occ.tumor_sq.dropna().tolist() == pytest.approx([14.7, 9.3, 31.2]), "Unexpected tumor_sq values"
    assert occ.normal_sq.dropna().tolist() == pytest.approx([2.1, 1.0, 0.8]), "Unexpected normal_sq values"

    # INFO/AQ is seeded on 2 records
    assert occ.info_aq.dropna().tolist() == pytest.approx([1.75, 0.5]), "Unexpected info_aq values"

    # info_hotspot resolves from the DRAGEN `hotspot` Flag on one record and from
    # `HotspotAllele=1` on another; every other record leaves it NULL
    assert occ.info_hotspot.dropna().tolist() == [True, True], "Unexpected info_hotspot values"
    assert occ.info_hotspotallele.dropna().tolist() == [1], "Unexpected info_hotspotallele values"
