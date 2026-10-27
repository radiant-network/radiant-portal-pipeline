import sys
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from radiant.tasks.iceberg.utils import commit_files
from radiant.tasks.vcf.experiment import Experiment, RadiantGermlineAnnotationTask
from radiant.tasks.vcf.snv.germline.process import process_task

RESOURCES_DIR = Path(__file__).parent.parent / "resources" / "integration"


def test_process_task(
    setup_iceberg_namespace,
    iceberg_catalog_properties,
    iceberg_client,
    rest_iceberg_catalog_instance,
    indexed_vcfs,
):
    task = RadiantGermlineAnnotationTask(
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
        vcf_filepath=indexed_vcfs["test.vcf"],
    )
    partitions_to_commit = process_task(
        task,
        catalog_name=rest_iceberg_catalog_instance.catalog_name,
        namespace=setup_iceberg_namespace,
        catalog_properties=iceberg_catalog_properties,
    )

    # Commit the files to the iceberg tables
    for table_name, partitions in partitions_to_commit.items():
        table = iceberg_client.load_table(table_name)
        commit_files(table, partitions)

    table_names = iceberg_client.list_tables(setup_iceberg_namespace)
    assert (setup_iceberg_namespace, "germline_snv_occurrence") in table_names
    occ = iceberg_client.load_table(f"{setup_iceberg_namespace}.germline_snv_occurrence").scan().to_arrow().to_pandas()

    assert not occ.empty, "No occurrences were written to the iceberg table"
    assert ((occ["aliquot"] == "SA0001") & (occ["task_id"] == 1)).any(), (
        "Expected sample/task not found in occurrences"
    )
    assert all(occ["chromosome"] == "1"), "Unexpected chromosome values in output"
    assert occ["zygosity"][0] == "HET", "Unexpected zygosity value in output"
    assert occ["zygosity"][1] == "HOM", "Unexpected zygosity value in output"
    # expected_df = pd.DataFrame({
    #     "chromosome": ["chr1"],
    #     "start": [12345],
    #     "reference": ["A"],
    #     "alternate": ["T"],
    #     "task_id": [1],
    #     "aliquot": ["SA0001"],
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


def test_process_task_error(
    setup_iceberg_namespace,
    iceberg_catalog_properties,
    rest_iceberg_catalog_instance,
    indexed_vcfs,
):
    task = RadiantGermlineAnnotationTask(
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
        vcf_filepath=indexed_vcfs["test.vcf"],
    )
    with (
        pytest.raises(Exception) as exc,
        patch("radiant.tasks.iceberg.table_accumulator.TableAccumulator.write_files", fake_error_logging),
    ):
        process_task(
            task,
            catalog_name=rest_iceberg_catalog_instance.catalog_name,
            namespace=setup_iceberg_namespace,
            catalog_properties=iceberg_catalog_properties,
        )

    assert "Detected error: [E::" in str(exc.value)


def test_process_task_skips_records_no_sample_carries(
    setup_iceberg_namespace,
    iceberg_catalog_properties,
    iceberg_client,
    rest_iceberg_catalog_instance,
    tmp_path,
):
    """RAD-75: a record is written only when at least one sample of the task carries the alt allele."""
    with open(RESOURCES_DIR / "test.vcf") as f:
        header = [line for line in f if line.startswith("##")]
    records = [
        # proband HET, mother WT, father HET: kept, with the mother's WT row
        "chr1\t1000\t.\tAC\tA\t44.64\tPASS\t.\tGT:AD\t0/1:4,7\t0/0:10,0\t0/1:5,6",
        # nobody carries the alt: skipped
        "chr1\t3000\t.\tG\tT\t44.64\tPASS\t.\tGT:AD\t0/0:10,0\t0/0:12,0\t0/0:9,0",
        # proband HET with a single alt read becomes a no-call (has_alt false), parents WT: skipped
        "chr1\t4000\t.\tG\tT\t44.64\tPASS\t.\tGT:AD\t0/1:10,1\t0/0:12,0\t0/0:8,0",
        # only the mother carries the alt: kept for all three samples
        "chr1\t5000\t.\tG\tT\t44.64\tPASS\t.\tGT:AD\t0/0:10,0\t0/1:6,6\t0/0:8,0",
    ]
    vcf_path = tmp_path / "trio.vcf"
    vcf_path.write_text(
        "".join(header)
        + "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tSA0001\tSA0002\tSA0003\n"
        + "\n".join(records)
        + "\n"
    )

    def experiment(seq_id, aliquot, family_role, sex):
        return Experiment(
            seq_id=seq_id,
            patient_id=seq_id,
            aliquot=aliquot,
            tenant_code="tenant1",
            family_role=family_role,
            affected_status="affected",
            sex=sex,
            experimental_strategy="wgs",
            request_priority="routine",
        )

    task = RadiantGermlineAnnotationTask(
        task_id=75,
        part=1,
        analysis_type="germline",
        deleted=False,
        experiments=[
            experiment(751, "SA0001", "proband", "F"),
            experiment(752, "SA0002", "mother", "F"),
            experiment(753, "SA0003", "father", "M"),
        ],
        vcf_filepath=str(vcf_path),
    )
    partitions_to_commit = process_task(
        task,
        catalog_name=rest_iceberg_catalog_instance.catalog_name,
        namespace=setup_iceberg_namespace,
        catalog_properties=iceberg_catalog_properties,
    )
    for table_name, partitions in partitions_to_commit.items():
        commit_files(iceberg_client.load_table(table_name), partitions)

    def load(table):
        df = iceberg_client.load_table(f"{setup_iceberg_namespace}.{table}").scan().to_arrow().to_pandas()
        return df[df.task_id == task.task_id]

    occ = load("germline_snv_occurrence")
    variants = load("snv_variant")
    consequences = load("snv_consequence")

    assert sorted(variants.start) == [1000, 5000], "Unexpected loci in variants table"
    assert len(occ) == 6, "Expected one occurrence per sample for each kept record"
    assert set(occ.start) == {1000, 5000}, "Unexpected loci in occurrences table"
    assert set(occ[(occ.start == 1000) & (occ.seq_id == 752)].zygosity) == {"WT"}, (
        "The WT row of a kept record is lost"
    )
    proband_1000 = occ[(occ.start == 1000) & (occ.seq_id == 751)].iloc[0]
    assert proband_1000.mother_zygosity == "WT" and proband_1000.father_zygosity == "HET", "Parental fields changed"
    assert not set(consequences.locus_hash) - set(occ.locus_hash), "Consequences written for a skipped record"


def test_process_task_writes_cds_position(
    setup_iceberg_namespace,
    iceberg_catalog_properties,
    iceberg_client,
    rest_iceberg_catalog_instance,
    tmp_path,
):
    """RAD-71: `CDS_position` reaches the Iceberg `snv_consequence` table, split into `cds_start` and `cds_end`.

    The unit tests stop at the extracted dicts, and the StarRocks insert test seeds Iceberg by hand. This is the
    only test that goes through the writer, which drops a field the table schema does not know, with no error.
    """
    with open(RESOURCES_DIR / "test.vcf") as f:
        header = [line for line in f if line.startswith("##")]

    def csq(consequence, impact, transcript, cds_position, pick=""):
        # Allele|Consequence|IMPACT|SYMBOL|Feature_type|Gene|PUBMED|PICK|Feature|EXON|BIOTYPE|INTRON|HGVSc|HGVSp|
        # STRAND|CDS_position|cDNA_position|Protein_position|Amino_acids|Codons|VARIANT_CLASS|HGVSg|CANONICAL|RefSeq
        fields = ["A", consequence, impact, "GENE1", "Transcript", "ENSG0001", "", pick, transcript, ""]
        fields += ["protein_coding", "", "", "", "1", cds_position, "", "", "", "", "", "", "", ""]
        return "|".join(fields)

    records = [
        # SNV: a single position on its coding transcript, NULL on the intronic one
        "chr1\t1000\t.\tG\tA\t44.64\tPASS\tCSQ="
        + csq("missense_variant", "MODERATE", "ENST0001.1", "227", pick="1")
        + ","
        + csq("intron_variant", "MODIFIER", "ENST0002.1", "")
        + "\tGT:AD\t0/1:4,7",
        # deletion: a range, and a range whose start falls outside the CDS
        "chr1\t2000\t.\tGACC\tG\t44.64\tPASS\tCSQ="
        + csq("inframe_deletion", "MODERATE", "ENST0001.1", "621-623", pick="1")
        + ","
        + csq("coding_sequence_variant&5_prime_UTR_variant", "MODIFIER", "ENST0003.1", "?-125")
        + "\tGT:AD\t0/1:4,7",
    ]
    vcf_path = tmp_path / "cds.vcf"
    vcf_path.write_text(
        "".join(header) + "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tSA0001\n" + "\n".join(records) + "\n"
    )

    task = RadiantGermlineAnnotationTask(
        task_id=71,
        part=1,
        analysis_type="germline",
        deleted=False,
        experiments=[
            Experiment(
                seq_id=711,
                patient_id=711,
                aliquot="SA0001",
                tenant_code="tenant1",
                family_role="proband",
                affected_status="affected",
                sex="F",
                experimental_strategy="wgs",
                request_priority="routine",
            )
        ],
        vcf_filepath=str(vcf_path),
    )
    partitions_to_commit = process_task(
        task,
        catalog_name=rest_iceberg_catalog_instance.catalog_name,
        namespace=setup_iceberg_namespace,
        catalog_properties=iceberg_catalog_properties,
    )
    for table_name, partitions in partitions_to_commit.items():
        commit_files(iceberg_client.load_table(table_name), partitions)

    rows = iceberg_client.load_table(f"{setup_iceberg_namespace}.snv_consequence").scan().to_arrow().to_pylist()
    positions = {(r["start"], r["transcript_id"]): (r["cds_start"], r["cds_end"]) for r in rows if r["task_id"] == 71}

    assert positions == {
        (1000, "ENST0001"): (227, 227),
        (1000, "ENST0002"): (None, None),
        (2000, "ENST0001"): (621, 623),
        (2000, "ENST0003"): (None, 125),
    }, "Unexpected (cds_start, cds_end) in the consequences table"


def test_process_task_writes_comphet_pairs(
    setup_iceberg_namespace,
    iceberg_catalog_properties,
    iceberg_client,
    rest_iceberg_catalog_instance,
    tmp_path,
):
    """RAD-39: the slivar_comphet pairs of the progenies land in germline_snv_comphet; a re-import replaces them."""
    with open(RESOURCES_DIR / "test.vcf") as f:
        header = [line for line in f if line.startswith("##")]
    header.append(
        '##INFO=<ID=slivar_comphet,Number=.,Type=String,Description="compound hets called by slivar. '
        'format is sample/gene/id/chrom/pos/ref/alt where id is a unique integer indicating the compound-het pair.">\n'
    )
    columns = "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tSA0001\tSA0002\tSA0003\n"
    tagged = [
        # proband pairs 1000 with 2000 in GENE1; the mother's entry is ignored
        "chr1\t1000\t.\tA\tG\t44.64\tPASS\tslivar_comphet=SA0001/GENE1/1/chr1/2000/C/T,SA0002/GENE1/9/chr1/2000/C/T"
        "\tGT:AD\t0/1:4,7\t0/1:10,6\t0/0:5,0",
        "chr1\t2000\t.\tC\tT\t44.64\tPASS\tslivar_comphet=SA0001/GENE1/1/chr1/1000/A/G\tGT:AD\t0/1:4,7\t0/0:10,0\t0/1:5,6",
        "chr1\t3000\t.\tG\tT\t44.64\tPASS\t.\tGT:AD\t0/1:4,7\t0/0:10,0\t0/0:9,0",
    ]

    def experiment(seq_id, aliquot, family_role, sex):
        return Experiment(
            seq_id=seq_id,
            patient_id=seq_id,
            aliquot=aliquot,
            tenant_code="tenant1",
            family_role=family_role,
            affected_status="affected",
            sex=sex,
            experimental_strategy="wgs",
            request_priority="routine",
        )

    def run(records):
        vcf_path = tmp_path / "trio_comphet.vcf"
        vcf_path.write_text("".join(header) + columns + "\n".join(records) + "\n")
        task = RadiantGermlineAnnotationTask(
            task_id=39,
            part=2,
            analysis_type="germline",
            deleted=False,
            experiments=[
                experiment(391, "SA0001", "proband", "F"),
                experiment(392, "SA0002", "mother", "F"),
                experiment(393, "SA0003", "father", "M"),
            ],
            vcf_filepath=str(vcf_path),
        )
        partitions_to_commit = process_task(
            task,
            catalog_name=rest_iceberg_catalog_instance.catalog_name,
            namespace=setup_iceberg_namespace,
            catalog_properties=iceberg_catalog_properties,
        )
        for table_name, partitions in partitions_to_commit.items():
            commit_files(iceberg_client.load_table(table_name), partitions)

        df = iceberg_client.load_table(f"{setup_iceberg_namespace}.germline_snv_comphet").scan().to_arrow().to_pandas()
        return df[df.task_id == task.task_id]

    def load_occurrences():
        df = (
            iceberg_client.load_table(f"{setup_iceberg_namespace}.germline_snv_occurrence")
            .scan()
            .to_arrow()
            .to_pandas()
        )
        return df[df.task_id == 39]

    comphets = run(tagged)
    hashes = load_occurrences().drop_duplicates("start").set_index("start").locus_hash
    pairs = sorted(
        zip(comphets.seq_id, comphets.locus_hash, comphets.partner_locus_hash, comphets.symbol, strict=True)
    )
    assert pairs == sorted(
        [
            (391, hashes[1000], hashes[2000], "GENE1"),
            (391, hashes[2000], hashes[1000], "GENE1"),
        ]
    ), "Expected the proband's pair once per direction, with the occurrence locus hashes"
    assert set(comphets.part) == {2} and set(comphets.tenant_code) == {"tenant1"}

    untagged = [r.replace(r.split("\t")[7], ".") for r in tagged]
    assert run(untagged).empty, "A re-import without pairs must clear the task's previous pairs"


def test_process_task_reads_slivar_inheritance(
    setup_iceberg_namespace,
    iceberg_catalog_properties,
    iceberg_client,
    rest_iceberg_catalog_instance,
    tmp_path,
):
    """RAD-94: a VCF whose header declares the slivar tags takes both fields from them, with their sources."""
    with open(RESOURCES_DIR / "test.vcf") as f:
        header = [line for line in f if line.startswith("##")]
    for tag in (
        "po_mother",
        "po_denovo",
        "autosomal_dominant_denovo",
        "autosomal_recessive",
        "autosomal_dominant",
        "candidate",
    ):
        header.append(f'##INFO=<ID={tag},Number=.,Type=String,Description="slivar {tag}">\n')
    columns = "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tSA0001\tSA0002\tSA0003\n"
    records = [
        "chr1\t1000\t.\tA\tG\t44.64\tPASS\tautosomal_recessive=SA0001;autosomal_dominant=SA0001;candidate=SA0001;"
        "po_mother=SA0001\tGT:AD\t1/1:0,9\t0/1:10,6\t1/1:0,5",
        "chr1\t2000\t.\tC\tT\t44.64\tPASS\t.\tGT:AD\t0/1:4,7\t0/0:10,0\t0/0:9,0",
    ]
    vcf_path = tmp_path / "trio_inheritance.vcf"
    vcf_path.write_text("".join(header) + columns + "\n".join(records) + "\n")

    def experiment(seq_id, aliquot, family_role, sex):
        return Experiment(
            seq_id=seq_id,
            patient_id=seq_id,
            aliquot=aliquot,
            tenant_code="tenant1",
            family_role=family_role,
            affected_status="affected" if family_role == "proband" else "not_affected",
            sex=sex,
            experimental_strategy="wgs",
            request_priority="routine",
        )

    task = RadiantGermlineAnnotationTask(
        task_id=94,
        part=2,
        analysis_type="germline",
        deleted=False,
        experiments=[
            experiment(941, "SA0001", "proband", "F"),
            experiment(942, "SA0002", "mother", "F"),
            experiment(943, "SA0003", "father", "M"),
        ],
        vcf_filepath=str(vcf_path),
    )
    partitions_to_commit = process_task(
        task,
        catalog_name=rest_iceberg_catalog_instance.catalog_name,
        namespace=setup_iceberg_namespace,
        catalog_properties=iceberg_catalog_properties,
    )
    for table_name, partitions in partitions_to_commit.items():
        commit_files(iceberg_client.load_table(table_name), partitions)

    occ = iceberg_client.load_table(f"{setup_iceberg_namespace}.germline_snv_occurrence").scan().to_arrow().to_pandas()
    occ = occ[occ.task_id == 94].set_index(["seq_id", "start"])

    tagged = occ.loc[(941, 1000)]
    assert tagged.parental_origin == "MOTHER" and tagged.parental_origin_source == "slivar"
    assert tagged.transmission_mode == "autosomal_recessive" and tagged.transmission_mode_source == "slivar"
    assert list(tagged.other_transmission_modes) == ["autosomal_dominant"]
    assert bool(tagged.transmission_mode_candidate) is True

    untagged = occ.loc[(941, 2000)]
    assert pd.isna(untagged.parental_origin) and pd.isna(untagged.transmission_mode), "No tag means no value"
    assert untagged.parental_origin_source == "slivar" and untagged.transmission_mode_source == "slivar"

    mother = occ.loc[(942, 1000)]
    assert pd.isna(mother.parental_origin_source) and pd.isna(mother.transmission_mode_source)
