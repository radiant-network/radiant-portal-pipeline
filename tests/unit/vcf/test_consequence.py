from radiant.tasks.vcf.common import Common
from radiant.tasks.vcf.consequence import parse_csq_header, process_consequence
from radiant.tasks.vcf.experiment import Case, Experiment
from tests.unit.vcf.vcf_test_utils import variant, vcf

case = Case(
    case_id=1,
    part=1,
    analysis_type="germline",
    experiments=[
        Experiment(
            seq_id=1,
            patient_id="PA001",
            sample_id="SA0001",
            family_role="proband",
            is_affected=True,
            sex="F",
        )
    ],
    vcf_filepath="",
)
common = Common(case.case_id, case.part, "1-1000-AC-A", "hash", "1", 1000, 1000, "AC", "A")


def test_one_sample():
    v = variant("test_consequence_one_sample.vcf")
    with vcf("test_consequence_one_sample.vcf") as vcf_file:
        csq_header = parse_csq_header(vcf_file)

    expected_picked = {
        "aa_change": "p.Lys76Asn",
        "alternate": "A",
        "biotype": "protein_coding",
        "case_id": 1,
        "chromosome": "1",
        "consequences": ["missense_variant"],
        "dna_change": "c.227A>T",
        "end": 1000,
        "exon": {"rank": 2, "total": 8},
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
        "mane_select": None,
        "reference": "AC",
        "source": None,
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
