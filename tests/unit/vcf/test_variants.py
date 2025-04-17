from tasks.vcf.common import Common
from tasks.vcf.variant import process_variant
from .vcf_test_utils import variant

common = Common(1, "1-1000-AC-A", "hash", "1", 1000, 1000, "AC", "A")


def test_variants_with_id():
    v = variant("test_variants.vcf")
    result = process_variant(v, {}, common)
    expected = {
        "case_id": common.case_id,
        "locus": common.locus,
        "locus_hash": common.locus_hash,
        "chromosome": "1",
        "start": 1000,
        "end": 1000,
        "reference": "AC",
        "alternate": "A",
        "rsnumber": "rs1000",
    }
    assert expected == result


def test_variants_without_id():
    v = variant("test_variants.vcf", 2)
    result = process_variant(v, {}, common)
    expected = {
        "case_id": common.case_id,
        "locus": common.locus,
        "locus_hash": common.locus_hash,
        "chromosome": "1",
        "start": 1000,
        "end": 1000,
        "reference": "AC",
        "alternate": "A",
        "rsnumber": None,
    }
    assert expected == result


def test_variants_with_picked():
    v = variant("test_variants.vcf")
    result = process_variant(v, {"variant_class": "SNV"}, common)
    expected = {
        "case_id": common.case_id,
        "locus": common.locus,
        "locus_hash": common.locus_hash,
        "chromosome": "1",
        "start": 1000,
        "end": 1000,
        "reference": "AC",
        "alternate": "A",
        "rsnumber": "rs1000",
        "variant_class": "SNV",
        "symbol": None,
        "consequences": None,
        "vep_impact": None,
        "impact_score": None,
        "mane_select": None,
        "is_mane_select": None,
        "is_mane_plus": None,
        "is_canonical": None,
        "hgvsg": None,
        "hgvsp": None,
        "hgvsc": None,
        "dna_change": None,
        "aa_change": None,
    }
    assert expected == result
