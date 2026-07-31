import threading

import pytest

from radiant.tasks.iceberg.partition_commit import merge_partition_commits
from radiant.tasks.iceberg.utils import commit_partitions
from radiant.tasks.vcf.experiment import Experiment, RadiantSomaticAnnotationTask
from radiant.tasks.vcf.snv.somatic.process import process_task


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

    partitions = process_task(
        task=task, namespace=setup_iceberg_namespace, catalog_properties=iceberg_catalog_properties
    )
    commit_partitions(merge_partition_commits([partitions]), iceberg_catalog_properties=iceberg_catalog_properties)

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


def _tumor_only_task(task_id: int, vcf_filepath: str) -> RadiantSomaticAnnotationTask:
    """A somatic task with a single tumoral aliquot and no matched normal."""
    return RadiantSomaticAnnotationTask(
        task_id=task_id,
        part=1,
        analysis_type="somatic",
        deleted=False,
        experiments=[
            Experiment(
                seq_id=3,
                patient_id=2,
                aliquot="TCR002361_SRX1091647-T",
                tenant_code="tenant1",
                family_role="proband",
                affected_status="affected",
                sex="female",
                experimental_strategy="wxs",
                request_priority="routine",
                histology_type="tumoral",
            ),
        ],
        vcf_filepath=vcf_filepath,
    )


def test_import_somatic_snv_tumor_only_vcf(
    setup_iceberg_namespace,
    iceberg_catalog_properties,
    iceberg_client,
):
    """A tumor-only task ingests into the same table, leaving every normal_* column NULL.

    The fixture is the tumor column of `test_somatic_snv.vcf` cut out verbatim, so the tumor-side
    values must match the tumor-normal test above exactly — that equality is the point of the test.
    """
    task = _tumor_only_task(task_id=2, vcf_filepath="tests/resources/test_somatic_snv_tumor_only.vcf")

    partitions = process_task(
        task=task, namespace=setup_iceberg_namespace, catalog_properties=iceberg_catalog_properties
    )
    commit_partitions(merge_partition_commits([partitions]), iceberg_catalog_properties=iceberg_catalog_properties)

    def load(table: str):
        df = iceberg_client.load_table(f"{setup_iceberg_namespace}.{table}").scan().to_arrow().to_pandas()
        return df[df.task_id == task.task_id]

    occ = load("somatic_snv_occurrence")
    variants = load("snv_variant")
    consequences = load("snv_consequence")

    # Same counts as the tumor-normal run — the shared catalog path is untouched by tumor-only
    assert len(occ) == 21, "Unexpected number of rows in occurrences table"
    assert occ.chromosome.unique().tolist() == ["1", "4", "12"], "Unexpected chromosome values in occurrences table"
    assert len(variants) == 21, "Unexpected number of rows in variants table"
    assert len(consequences) == 236, "Unexpected number of rows in consequences table"

    # Every normal_* column is NULL: that NULL is what downstream derives tumor-only from
    normal_columns = [column for column in occ.columns if column.startswith("normal_")]
    assert normal_columns, "expected the occurrence table to carry normal_* columns"
    assert occ[normal_columns].isna().all().all(), (
        f"Expected every normal_* column to be NULL, got {occ[normal_columns].notna().any().to_dict()}"
    )

    # Tumor side is identical to the tumor-normal run
    assert occ.tumor_seq_id.unique().tolist() == [3], "Unexpected tumor_seq_id values"
    assert occ.tumor_sq.dropna().tolist() == pytest.approx([14.7, 9.3, 31.2]), "Unexpected tumor_sq values"
    assert occ.info_aq.dropna().tolist() == pytest.approx([1.75, 0.5]), "Unexpected info_aq values"
    assert occ.info_hotspot.dropna().tolist() == [True, True], "Unexpected info_hotspot values"
    assert occ.info_hotspotallele.dropna().tolist() == [1], "Unexpected info_hotspotallele values"
    assert occ.tumor_has_alt.any(), "Expected at least one occurrence carrying the alternate allele"


def test_import_somatic_snv_tumor_only_task_on_tumor_normal_vcf_raises(
    setup_iceberg_namespace,
    iceberg_catalog_properties,
):
    """A single-aliquot task whose VCF holds two samples is a missing normal experiment, not tumor-only.

    Detecting this needs the VCF's pre-subset sample list, which is why `process_task` reads the
    sample names before narrowing the reader.
    """
    task = _tumor_only_task(task_id=3, vcf_filepath="tests/resources/test_somatic_snv.vcf")

    with pytest.raises(ValueError, match="likely a tumor-normal task with a missing or mismatched normal"):
        process_task(task=task, namespace=setup_iceberg_namespace, catalog_properties=iceberg_catalog_properties)


def test_import_somatic_snv_tumor_normal_task_on_tumor_only_vcf_raises(
    setup_iceberg_namespace,
    iceberg_catalog_properties,
):
    """The mirror case: a tumor-normal task pointed at a tumor-only VCF.

    `set_samples` narrows to the intersection and only warns about the rest, so without an explicit
    check the normal experiment is dropped and the task is silently ingested as tumor-only.
    """
    task = RadiantSomaticAnnotationTask(
        task_id=4,
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
        vcf_filepath="tests/resources/test_somatic_snv_tumor_only.vcf",
    )

    with pytest.raises(ValueError, match="the task and the VCF disagree on the analysis"):
        process_task(task=task, namespace=setup_iceberg_namespace, catalog_properties=iceberg_catalog_properties)


def test_concurrent_commits_to_the_shared_variant_table_both_land(
    setup_iceberg_namespace,
    iceberg_catalog_properties,
    iceberg_client,
):
    """Two writers committing disjoint task_id partitions to `snv_variant` must both survive.

    `snv_variant` and `snv_consequence` are written by the germline and somatic flows alike, so
    their commits can collide on Iceberg's snapshot assertion even though the rows are disjoint.
    Absorbing that needs the commit to be re-staged against a refreshed snapshot; re-committing
    the transaction that already failed resends a stale assertion and can never succeed.

    The barrier maximises the chance of a genuine conflict. The assertions hold whether or not
    one occurs, so the test cannot flake -- it just does not always exercise the retry.
    """
    task_ids = (901, 902)
    partitions = [
        process_task(
            task=_tumor_only_task(task_id=task_id, vcf_filepath="tests/resources/test_somatic_snv_tumor_only.vcf"),
            namespace=setup_iceberg_namespace,
            catalog_properties=iceberg_catalog_properties,
        )
        for task_id in task_ids
    ]

    barrier = threading.Barrier(len(partitions))
    errors: list[Exception] = []

    def commit(task_partitions):
        try:
            barrier.wait(timeout=30)
            commit_partitions(
                merge_partition_commits([task_partitions]),
                iceberg_catalog_properties=iceberg_catalog_properties,
            )
        except Exception as e:  # noqa: BLE001 - surfaced by the assertion below
            errors.append(e)

    threads = [threading.Thread(target=commit, args=(p,)) for p in partitions]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert not errors, f"concurrent commits failed: {errors}"

    for table in ("snv_variant", "snv_consequence", "somatic_snv_occurrence"):
        df = iceberg_client.load_table(f"{setup_iceberg_namespace}.{table}").scan().to_arrow().to_pandas()
        # Neither writer may have dropped the other's rows.
        assert set(task_ids) <= set(df.task_id), f"{table} lost rows from a concurrent commit"
