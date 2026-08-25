from radiant.tasks.vcf.experiment import Experiment, RadiantGermlineAnnotationTask
from radiant.tasks.vcf.snv.common import Common
from radiant.tasks.vcf.snv.consequence import parse_csq_header, process_consequence
from tests.unit.vcf.vcf_test_utils import variant, vcf

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
    vcf_filepath="",
)
common = Common(task.task_id, task.part, "1-1000-AC-A", "hash", "1", 1000, 1000, "AC", "A")


def test_one_sample():
    v = variant("test_consequence_one_sample.vcf")
    with vcf("test_consequence_one_sample.vcf") as vcf_file:
        csq_header = parse_csq_header(vcf_file)

    expected_picked = {
        "aa_change": "p.Lys76Asn",
        "alternate": "A",
        "biotype": "protein_coding",
        "task_id": 1,
        "chromosome": "1",
        "consequences": ["missense_variant"],
        "dna_change": "c.227A>T",
        "end": 1000,
        "exon": {"rank": "2-3", "total": "8"},
        "hgvsc": "c.227A>T",
        "hgvsg": "g.12345G>A",
        "hgvsp": "p.Lys76Asn",
        "impact_score": 3,
        "is_canonical": True,
        "is_mane_plus": False,
        "is_mane_select": False,
        "is_picked": True,
        "locus": "1-1000-AC-A",
        "locus_hash": "hash",
        "mane_select": "ENST00000357654.7",
        "reference": "AC",
        "source": "Ensembl",
        "start": 1000,
        "strand": "1",
        "symbol": "BRCA1",
        "transcript_id": "ENST00000357654",
        "variant_class": "SNV",
        "vep_impact": "MODERATE",
    }

    picked, consequences = process_consequence(v, csq_header, common)
    assert picked == expected_picked
    assert consequences is not None


def test_one_sample_second_consequence_is_refseq():
    v = variant("test_consequence_one_sample.vcf", row_number=2)
    with vcf("test_consequence_one_sample.vcf") as vcf_file:
        csq_header = parse_csq_header(vcf_file)

    _, consequences = process_consequence(v, csq_header, common)

    assert len(consequences) == 1
    assert consequences[0]["source"] == "RefSeq"
    assert consequences[0]["mane_select"] == "ENST00000269305.9"


def test_source_column_absent_from_header_stays_none():
    # test_somatic_snv.vcf declares MANE_SELECT but no SOURCE. An absent column must stay
    # null rather than pick up a neighbouring field's value; a declared-but-empty one is "".
    v = variant("test_somatic_snv.vcf")
    with vcf("test_somatic_snv.vcf") as vcf_file:
        csq_header = parse_csq_header(vcf_file)

    _, consequences = process_consequence(v, csq_header, common)

    assert consequences
    assert all(c["source"] is None for c in consequences)
    assert all(c["mane_select"] == "" for c in consequences)


def test_csq_field_names_are_matched_case_insensitively():
    # VEP spells these columns differently across versions and plugins, so the lookup
    # must not depend on the casing used in the header.
    v = variant("test_consequence_lowercase_csq_no_exon.vcf")
    with vcf("test_consequence_lowercase_csq_no_exon.vcf") as vcf_file:
        csq_header = parse_csq_header(vcf_file)

    picked, _ = process_consequence(v, csq_header, common)

    assert picked["source"] == "Ensembl"
    assert picked["mane_select"] == "NM_007294.3"


def test_missing_exon_field_does_not_raise():
    # EXON is optional: `.split("/")` on the missing value used to raise AttributeError.
    v = variant("test_consequence_lowercase_csq_no_exon.vcf")
    with vcf("test_consequence_lowercase_csq_no_exon.vcf") as vcf_file:
        csq_header = parse_csq_header(vcf_file)

    picked, _ = process_consequence(v, csq_header, common)

    assert picked["exon"] is None
    assert picked["symbol"] == "BRCA1"
