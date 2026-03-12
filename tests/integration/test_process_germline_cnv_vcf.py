from radiant.tasks.vcf.cnv.germline.process import process_tasks
from radiant.tasks.vcf.experiment import AlignmentGermlineVariantCallingTask, Experiment


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
    assert ((occ["aliquot"] == "SA0001") & (occ["seq_id"] == 1)).any(), (
        "Expected sample/sequencingID not found in occurrences"
    )
    assert occ["chromosome"].isin(["1", "X"]).all(), "Some chromosome values are invalid"
