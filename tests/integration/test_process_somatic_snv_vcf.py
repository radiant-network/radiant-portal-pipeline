from collections import defaultdict

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

    assert not occ.empty, "No occurrences were written to the iceberg table"
    assert not variants.empty, "No variants were written to the iceberg table"
    assert not consequences.empty, "No consequences were written to the iceberg table"
