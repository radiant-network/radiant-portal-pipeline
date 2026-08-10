from types import SimpleNamespace

import pytest

from radiant.tasks.vcf.cnv.somatic.occurrence import process_occurrence, resolve_cnv_type
from tests.unit.vcf.vcf_test_utils import vcf

SOMATIC_CNV_VCF = "test_somatic_cnv.vcf"
SOMATIC_CNV_NO_ASCN_VCF = "test_somatic_cnv_no_ascn.vcf"

ASCN_FIELDS = ("cn", "cnf", "cnq", "mcn", "mcnf", "mcnq", "maf", "sd", "ascn_as")


def records_by_id(vcf_filename: str) -> dict[str, object]:
    """Indexes a fixture's records by their DRAGEN ID, so tests don't depend on row order."""
    with vcf(vcf_filename) as vcf_file:
        return {record.ID: record for record in vcf_file}


def occurrence(record, **overrides) -> dict:
    args = {
        "part": 1,
        "seq_id": 64,
        "tenant_code": "tenant1",
        "task_id": 70,
        "aliquot": "TCRBOA6-T",
        "sample_idx": 0,
        "cnv_type": resolve_cnv_type(record),
    } | overrides
    return process_occurrence(record, **args)


def fake_record(alts: list[str], name: str | None = "DRAGEN:GAIN:chr1:1-2") -> SimpleNamespace:
    """`resolve_cnv_type` only reads ALT, ID and the coordinates it logs, so a stub is enough."""
    return SimpleNamespace(ALT=alts, ID=name, CHROM="chr1", POS=100)


@pytest.fixture(scope="module")
def cnv_records() -> dict:
    return records_by_id(SOMATIC_CNV_VCF)


@pytest.fixture(scope="module")
def no_ascn_records() -> dict:
    return records_by_id(SOMATIC_CNV_NO_ASCN_VCF)


@pytest.mark.parametrize(
    "record_id,expected_type",
    [
        ("DRAGEN:GAIN:chr1:124941719-124975661", "GAIN"),
        ("DRAGEN:LOSS:chr1:450731-7249626", "LOSS"),
        ("DRAGEN:CNLOH:chr1:6667700-6887749", "CNLOH"),
        ("DRAGEN:GAINLOH:chr2:31000001-31500000", "GAINLOH"),
    ],
)
def test_resolve_cnv_type_from_dragen_id(cnv_records, record_id, expected_type):
    """All four types come off the ID; ALT cannot express the two LOH ones at all."""
    assert resolve_cnv_type(cnv_records[record_id]) == expected_type


def test_resolve_cnv_type_skips_reference_segment(cnv_records):
    assert resolve_cnv_type(cnv_records["DRAGEN:REF:chrX:9000001-9100000"]) is None


def test_resolve_cnv_type_skips_unrecognised_alt_even_with_parseable_id(cnv_records):
    """A `<CNV>` ALT would produce a NULL `cnv_id` against a NOT NULL key column and fail the whole
    StarRocks load, so the ALT guard runs ahead of the ID."""
    assert resolve_cnv_type(cnv_records["DRAGEN:GAIN:chrX:154548410-154554958"]) is None


@pytest.mark.parametrize("alts,expected_type", [(["<DUP>"], "GAIN"), (["<DEL>"], "LOSS")])
def test_resolve_cnv_type_falls_back_to_alt(alts, expected_type):
    """Without a parseable ID, ALT still settles the non-LOH cases."""
    assert resolve_cnv_type(fake_record(alts, name=".")) == expected_type
    assert resolve_cnv_type(fake_record(alts, name=None)) == expected_type


@pytest.mark.parametrize("alts", [["<LOH>"], ["<DEL>", "<DUP>"]])
def test_resolve_cnv_type_raises_on_loh_without_parseable_id(alts):
    """Both LOH spellings say heterozygosity was lost, never whether copy number changed with it."""
    with pytest.raises(ValueError, match="CNLOH"):
        resolve_cnv_type(fake_record(alts, name="segment_42"))


@pytest.mark.parametrize(
    "alts,name,expected_type",
    [
        (["<DEL>"], "DRAGEN:GAIN:chr1:1-2", "GAIN"),
        # A caller that splits multi-allelic records would hand us one LOH haplotype at a time.
        (["<DUP>"], "DRAGEN:CNLOH:chr1:1-2", "CNLOH"),
        (["<DEL>", "<DUP>"], "DRAGEN:GAIN:chr1:1-2", "GAIN"),
    ],
)
def test_resolve_cnv_type_trusts_the_id_over_a_disagreeing_alt(caplog, alts, name, expected_type):
    assert resolve_cnv_type(fake_record(alts, name=name)) == expected_type
    assert "trusting the ID" in caplog.text


def test_process_occurrence_gain(cnv_records):
    occ = occurrence(cnv_records["DRAGEN:GAIN:chr1:124941719-124975661"])

    assert occ["part"] == 1
    assert occ["seq_id"] == 64
    assert occ["tenant_code"] == "tenant1"
    assert occ["task_id"] == 70
    assert occ["aliquot"] == "TCRBOA6-T"
    assert occ["chromosome"] == "1"
    assert occ["type"] == "GAIN"
    assert occ["alternate"] == "<DUP>"
    assert occ["start"] == 124941718
    assert occ["end"] == 124975661
    assert occ["length"] == 33943
    assert occ["reflen"] == 33943
    assert occ["svlen"] == 33943
    assert occ["svtype"] == "CNV"
    assert occ["name"] == "DRAGEN:GAIN:chr1:124941719-124975661"
    assert occ["quality"] == pytest.approx(36.0)
    assert occ["filter"] == "PASS"
    assert occ["calls"] == [0, 1]
    assert not occ["phased"]
    assert occ["bc"] == 20
    assert occ["pe"] == [5, 30]
    assert occ["sm"] == pytest.approx(1.43012, rel=1e-5)
    assert tuple(occ["cipos"]) == (-150, 150)
    assert tuple(occ["ciend"]) == (-200, 200)
    # The full DRAGEN 4.2.4 ASCN block, present on this record.
    assert occ["cn"] == 3
    assert occ["cnf"] == pytest.approx(3.12, rel=1e-5)
    assert occ["cnq"] == pytest.approx(180.4, rel=1e-5)
    assert occ["mcn"] == 1
    assert occ["mcnf"] == pytest.approx(1.04, rel=1e-5)
    assert occ["mcnq"] == pytest.approx(95.2, rel=1e-5)
    assert occ["maf"] == pytest.approx(0.34, rel=1e-5)
    assert occ["sd"] == pytest.approx(120.5, rel=1e-5)
    assert occ["ascn_as"] == 12


def test_process_occurrence_keeps_multi_valued_filter(cnv_records):
    """22% of the measured file's rows carry two filters, semicolon-joined."""
    occ = occurrence(cnv_records["DRAGEN:LOSS:chr1:450731-7249626"])

    assert occ["type"] == "LOSS"
    assert occ["alternate"] == "<DEL>"
    assert occ["filter"] == "cnvCopyRatio;LoDFail"
    assert occ["svlen"] == -6798896
    assert occ["length"] == 6798896


def test_process_occurrence_legacy_loh(cnv_records):
    """VCF 4.2 spells an LOH event as multi-allelic `<DEL>,<DUP>`, which brings three traps at once:
    a per-allele `SVLEN`, a `1/2` genotype, and `CN`/`MCN` missing from the record's own FORMAT."""
    occ = occurrence(cnv_records["DRAGEN:CNLOH:chr1:6667700-6887749"])

    assert occ["type"] == "CNLOH"
    assert occ["alternate"] == "<LOH>"
    # Not the `(-220050, 220050)` tuple `INFO.get` returns for a per-allele value.
    assert occ["svlen"] == -220050
    assert occ["length"] == 220050
    assert occ["calls"] == [1, 2]
    assert occ["cn"] is None
    assert occ["mcn"] is None
    assert occ["cnf"] is None
    assert occ["mcnf"] is None
    # `MAF=0` is the direct LOH marker, and must survive as 0.0 rather than being read as missing.
    assert occ["maf"] == pytest.approx(0.0)
    assert occ["ascn_as"] == 1


def test_both_loh_spellings_produce_the_same_row_shape(cnv_records):
    """The point of keying on `type` rather than ALT: an upstream DRAGEN flag changes the spelling of
    an LOH event, and must not change the row we store."""
    legacy = occurrence(cnv_records["DRAGEN:CNLOH:chr1:6667700-6887749"])
    current = occurrence(cnv_records["DRAGEN:GAINLOH:chr2:31000001-31500000"])

    assert legacy["alternate"] == current["alternate"] == "<LOH>"
    assert legacy["type"] == "CNLOH"
    assert current["type"] == "GAINLOH"


def test_process_occurrence_with_dot_valued_ascn_fields(cnv_records):
    """A field written `.` for the sample is a third way to be empty, and the one that does not
    announce itself: htslib returns NaN or INT32_MIN, which would be stored as a measurement."""
    occ = occurrence(cnv_records["DRAGEN:LOSS:chr2:80000001-80400000"])

    assert all(occ[field] is None for field in ASCN_FIELDS)
    assert occ["sm"] == pytest.approx(0.51, rel=1e-5)
    assert occ["bc"] == 48


def test_process_occurrence_without_ascn_fields(no_ascn_records):
    """DRAGEN 3.10.8 declares no ASCN field at all, so reading one raises `KeyError` in cyvcf2 rather
    than returning None -- the whole reason for the defensive read."""
    occ = occurrence(no_ascn_records["DRAGEN:LOSS:chr1:450731-7249626"], aliquot="TCRBOA7-T")

    assert occ["type"] == "LOSS"
    assert all(occ[field] is None for field in ASCN_FIELDS)
    assert occ["bc"] == 1979
    assert occ["pe"] == [25, 7]
    assert occ["sm"] == pytest.approx(1.04404, rel=1e-5)


def test_process_occurrence_defaults_svtype_when_absent(no_ascn_records):
    """VCF 4.4 may omit `SVTYPE`; every record in these files is a CNV segment."""
    occ = occurrence(no_ascn_records["DRAGEN:GAIN:chr1:124941719-124975661"], aliquot="TCRBOA7-T")

    assert occ["svtype"] == "CNV"
    assert occ["svlen"] is None
