"""The COSMIC Mutation Census normalization: COSMIC's anchor-less indels become VCF records against a
reference, bcftools left-aligns them, and the rows come back out keyed on the normalized alleles."""

import gzip
import os
import shutil
from collections import Counter
from unittest.mock import patch

import pytest

from radiant.tasks.open_data import cosmic_mutation_set as cms
from radiant.tasks.open_data.cosmic_mutation_set import (
    BCFTOOLS_EXCLUDED,
    INVALID_ALLELE,
    MALFORMED,
    NO_POSITION,
    OUTPUT_COLUMNS,
    REF_MISMATCH,
    UNMAPPED_CONTIG,
    ContigMap,
    CosmicNormalizationError,
    CosmicNormalizationSummary,
    CosmicRow,
    FaiReader,
    NormalizedRecord,
    RowRejected,
    VcfRecord,
    iter_cosmic_rows,
    iter_normalized,
    merge_back,
    normalize_cosmic_mutation_set,
    parse_bcftools_stderr,
    parse_position,
    run_bcftools_norm,
    to_vcf_record,
    write_output,
    write_vcf,
)
from radiant.tasks.vcf.snv.common import locus_and_hash

# chr1: 130 bp at 60 bp per line so fetches cross line breaks. Positions (1-based):
#   1-10  ACGTACGTAC      11-20 GTACGTACGT
#   21-30 ACGTACGTAC      31-40 GTACGTACGT
#   41-50 ACGTACGTAC      51-60 GTACGTACGT
#   61-70 GAAAAAAAAT   <- homopolymer run of A at 62-69, flanked by G (61) and T (70)
#   71-80 CCCCCCCCCC      81-90 acgtacgtac  (lowercase: must be read as ACGTACGTAC)
#   91-100 GTACGTACGT    101-110 ACGTACGTAC
#   111-120 GTACGTACGT   121-130 ACGTACGTAC
_CHR1 = (
    "ACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGT"
    "GAAAAAAAATCCCCCCCCCCacgtacgtacGTACGTACGTACGTACGTACGTACGTACGT"
    "ACGTACGTAC"
)
_CHRM = "TTTTGGGGCCCCAAAATTTT"
assert len(_CHR1) == 130 and len(_CHRM) == 20

_HEADER = [
    "GENE_NAME",
    "MUTATION_URL",
    "SHARED_AA",
    "GENOMIC_WT_ALLELE_SEQ",
    "GENOMIC_MUT_ALLELE_SEQ",
    "GENOMIC_MUTATION_ID",
    "Mutation genome position GRCh38",
    "COSMIC_SAMPLE_TESTED",
    "COSMIC_SAMPLE_MUTATED",
    "MUTATION_SIGNIFICANCE_TIER",
]


@pytest.fixture
def tiny_ref(tmp_path):
    fasta = tmp_path / "ref.fa"
    lines = [">chr1"]
    lines += [_CHR1[i : i + 60] for i in range(0, len(_CHR1), 60)]
    lines += [">chrM", _CHRM]
    fasta.write_text("\n".join(lines) + "\n")
    chr1_offset = len(">chr1\n")
    chrm_offset = chr1_offset + 60 + 1 + 60 + 1 + 10 + 1 + len(">chrM\n")
    (tmp_path / "ref.fa.fai").write_text(f"chr1\t130\t{chr1_offset}\t60\t61\nchrM\t20\t{chrm_offset}\t20\t21\n")
    return str(fasta)


def _row(position, ref, alt, row_number=1, cosmic_id="COSV1", mutated="5", tested="100", tier="Other"):
    return CosmicRow(
        row_number=row_number,
        position=position,
        ref=ref,
        alt=alt,
        values=("https://cosmic/?id=1", "2", cosmic_id, mutated, tested, tier),
    )


def _write_export(path, rows):
    """Rows as (position, ref, alt, cosmic_id, mutated, tested, tier) in the real header's shape."""
    with gzip.open(path, "wt") as out:
        out.write("\t".join(_HEADER) + "\n")
        for position, ref, alt, cosmic_id, mutated, tested, tier in rows:
            out.write(
                "\t".join(["GENE", "https://cosmic/?id=1", "2", ref, alt, cosmic_id, position, tested, mutated, tier])
                + "\n"
            )


# --- FASTA access ------------------------------------------------------------------------------------------


def test_fai_reader_fetches_across_line_breaks_and_uppercases(tiny_ref):
    with FaiReader(tiny_ref) as fasta:
        assert fasta.fetch("chr1", 1, 4) == "ACGT"
        assert fasta.fetch("chr1", 59, 63) == "GTGAA"  # crosses the first line break
        assert fasta.fetch("chr1", 81, 84) == "ACGT"  # lowercase in the file
        assert fasta.fetch("chr1", 130, 130) == "C"
        assert fasta.fetch("chrM", 1, 20) == _CHRM
        assert fasta.contigs["chr1"].length == 130


@pytest.mark.parametrize(("start", "end"), [(0, 1), (130, 131), (5, 4)])
def test_fai_reader_rejects_out_of_range_fetches(tiny_ref, start, end):
    with FaiReader(tiny_ref) as fasta, pytest.raises(RowRejected) as exc_info:
        fasta.fetch("chr1", start, end)
    assert exc_info.value.reason == MALFORMED


def test_contig_map_prefers_the_fasta_spelling_and_strips_chr_on_the_way_back():
    prefixed = ContigMap.build(["chr1", "chr2", "chrX", "chrM", "chr1_KI270706v1_random"])
    assert prefixed.cosmic_to_fasta == {"1": "chr1", "2": "chr2", "X": "chrX", "MT": "chrM"}
    assert prefixed.to_radiant("chr1") == "1"
    # The pipeline strips `chr` from a VCF CHROM and nothing else, so chrM comes back as M -- the same
    # token the SNV ingestion produces on this reference.
    assert prefixed.to_radiant("chrM") == "M"

    bare = ContigMap.build(["1", "MT", "Y"])
    assert bare.cosmic_to_fasta == {"1": "1", "Y": "Y", "MT": "MT"}
    assert bare.to_radiant("MT") == "MT"


def test_contig_map_needs_at_least_one_cosmic_contig():
    with pytest.raises(CosmicNormalizationError):
        ContigMap.build(["scaffold_1"])


# --- COSMIC row -> VCF record ------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("X:18622959-18622959", ("X", 18622959, 18622959)),
        ("6:93356930-93356929", ("6", 93356930, 93356929)),
        ("", None),
    ],
)
def test_parse_position(text, expected):
    assert parse_position(text) == expected


def test_parse_position_rejects_garbage():
    with pytest.raises(RowRejected) as exc_info:
        parse_position("chr1:12")
    assert exc_info.value.reason == MALFORMED


@pytest.mark.parametrize(
    ("position", "ref", "alt", "expected"),
    [
        # SNV: passed through, ref checked against the FASTA.
        ("1:3-3", "G", "T", VcfRecord("chr1", 3, "G", "T")),
        # MNV / delins: also passed through; bcftools trims any shared bases later.
        ("1:3-4", "GT", "CC", VcfRecord("chr1", 3, "GT", "CC")),
        ("1:61-61", "G", "CA", VcfRecord("chr1", 61, "G", "CA")),
        # Deletion of [S, E]: anchored on the base before (COSMIC gives no anchor).
        ("1:62-64", "AAA", "", VcfRecord("chr1", 61, "GAAA", "G")),
        ("1:70-70", "T", "", VcfRecord("chr1", 69, "AT", "A")),
        # Insertion between two flanking bases S-(S+1): anchored on S.
        ("1:69-70", "", "A", VcfRecord("chr1", 69, "A", "AA")),
        # ...and the reversed span COSMIC once wrote, S-(S-1).
        ("1:70-69", "", "A", VcfRecord("chr1", 69, "A", "AA")),
        # Mitochondrion through the contig map.
        ("MT:5-5", "G", "A", VcfRecord("chrM", 5, "G", "A")),
    ],
)
def test_to_vcf_record_anchors_each_encoding(tiny_ref, position, ref, alt, expected):
    with FaiReader(tiny_ref) as fasta:
        contig_map = ContigMap.build(fasta.contigs)
        assert to_vcf_record(_row(position, ref, alt), contig_map, fasta) == expected


@pytest.mark.parametrize(
    ("position", "ref", "alt", "reason"),
    [
        ("", "G", "T", NO_POSITION),
        ("7:3-3", "G", "T", UNMAPPED_CONTIG),  # chr7 is not in the tiny reference
        ("1:3-3", "A", "T", REF_MISMATCH),  # the FASTA has G at 3
        ("1:62-64", "AAG", "", REF_MISMATCH),  # deleted bases disagree with the FASTA
        ("1:3-4", "G", "T", MALFORMED),  # ref shorter than the span
        ("1:62-62", "AA", "", MALFORMED),  # deleted allele longer than the span
        ("1:1-1", "A", "", MALFORMED),  # deletion at the first base has no left anchor
        ("1:69-71", "", "A", MALFORMED),  # insertion span is not two flanking bases
        ("1:3-3", "", "", MALFORMED),
        ("1:3-3", "G", "N", INVALID_ALLELE),
        ("1:200-200", "G", "T", MALFORMED),  # past the end of the contig
    ],
)
def test_to_vcf_record_rejects_rows_it_cannot_key(tiny_ref, position, ref, alt, reason):
    with FaiReader(tiny_ref) as fasta:
        contig_map = ContigMap.build(fasta.contigs)
        with pytest.raises(RowRejected) as exc_info:
            to_vcf_record(_row(position, ref, alt), contig_map, fasta)
    assert exc_info.value.reason == reason


def test_iter_cosmic_rows_resolves_columns_by_name_and_numbers_rows(tmp_path):
    path = tmp_path / "cmc.tsv.gz"
    _write_export(path, [("1:3-3", "G", "T", "COSV1", "5", "100", "3"), ("", "", "A", "COSV2", "1", "10", "Other")])
    rows = list(iter_cosmic_rows(str(path)))
    assert [r.row_number for r in rows] == [1, 2]
    assert rows[0] == CosmicRow(1, "1:3-3", "G", "T", ("https://cosmic/?id=1", "2", "COSV1", "5", "100", "3"))
    assert rows[1].position == "" and rows[1].ref == "" and rows[1].alt == "A"


def test_iter_cosmic_rows_fails_on_a_missing_column(tmp_path):
    path = tmp_path / "cmc.tsv.gz"
    with gzip.open(path, "wt") as out:
        out.write("GENE_NAME\tMUTATION_URL\n")
    with pytest.raises(CosmicNormalizationError, match="missing columns"):
        list(iter_cosmic_rows(str(path)))


def test_iter_cosmic_rows_fails_on_a_ragged_row(tmp_path):
    path = tmp_path / "cmc.tsv.gz"
    _write_export(path, [("1:3-3", "G", "T", "COSV1", "5", "100", "3")])
    with gzip.open(path, "at") as out:
        out.write("short\trow\n")
    with pytest.raises(CosmicNormalizationError, match="corrupt export"):
        list(iter_cosmic_rows(str(path)))


def test_write_vcf_emits_one_record_per_convertible_row_with_the_row_number_as_id(tiny_ref, tmp_path):
    rows = [
        _row("1:3-3", "G", "T", row_number=1),
        _row("", "G", "T", row_number=2),
        _row("1:69-70", "", "A", row_number=3),
        _row("7:3-3", "G", "T", row_number=4),
        _row("1:3-3", "A", "T", row_number=5),
    ]
    counts = Counter()
    vcf_path = str(tmp_path / "cosmic.vcf")
    with FaiReader(tiny_ref) as fasta:
        written = write_vcf(rows, ContigMap.build(fasta.contigs), fasta, vcf_path, counts)

    assert written == 2
    assert counts["rows_in"] == 5
    assert counts[NO_POSITION] == 1 and counts[UNMAPPED_CONTIG] == 1 and counts[REF_MISMATCH] == 1
    assert counts["contig:7"] == 1
    with open(vcf_path) as handle:
        lines = handle.read().splitlines()
    assert lines[0] == "##fileformat=VCFv4.2"
    assert "##contig=<ID=chr1,length=130>" in lines and "##contig=<ID=chrM,length=20>" in lines
    assert lines[-2:] == ["chr1\t3\t1\tG\tT\t.\t.\t.", "chr1\t69\t3\tA\tAA\t.\t.\t."]


# --- bcftools ----------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "text",
    [
        "REF_MISMATCH\tchr1\t3\tA\nREF_MISMATCH\tchr1\t9\tC\nLines   total/split/realigned/skipped:\t5/0/2/2\n",
        "REF_MISMATCH\tchr1\t3\tA\nREF_MISMATCH\tchr1\t9\tC\n"
        "Lines   total/split/joined/realigned/removed/skipped:\t5/0/0/2/0/2\n",
    ],
    ids=["1.16", "1.21"],
)
def test_parse_bcftools_stderr_handles_both_summary_formats(text):
    stats = parse_bcftools_stderr(text)
    assert stats.ref_mismatches == 2
    assert stats.first_ref_mismatches == ["REF_MISMATCH\tchr1\t3\tA", "REF_MISMATCH\tchr1\t9\tC"]
    assert stats.lines["total"] == 5 and stats.lines["realigned"] == 2
    stats.records_out = 3
    assert stats.excluded == 2


@pytest.mark.skipif(shutil.which("bcftools") is None, reason="bcftools is not on PATH")
def test_run_bcftools_norm_left_aligns_trims_and_sorts_by_id(tiny_ref, tmp_path):
    rows = [
        _row("1:69-70", "", "A", row_number=1),  # insertion at the right end of the A run -> shifts to 61
        _row("1:70-70", "T", "", row_number=2),  # plain deletion, already left-most
        _row("1:3-4", "GT", "GC", row_number=3),  # shared prefix: bcftools trims it to an SNV at 4
        _row("1:62-64", "AAA", "", row_number=4),  # deletion inside the run -> shifts to 61
    ]
    vcf_path = str(tmp_path / "cosmic.vcf")
    with FaiReader(tiny_ref) as fasta:
        contig_map = ContigMap.build(fasta.contigs)
        assert write_vcf(rows, contig_map, fasta, vcf_path, Counter()) == 4
    # A record bcftools must reject: REF disagrees with the FASTA (write_vcf would never emit it).
    with open(vcf_path, "a") as out:
        out.write("chr1\t3\t5\tA\tT\t.\t.\t.\n")

    sorted_path, stats = run_bcftools_norm(vcf_path, tiny_ref, str(tmp_path))

    assert stats.ref_mismatches == 1 and stats.excluded == 1
    assert stats.lines["total"] == 5 and stats.records_out == 4
    assert list(iter_normalized(sorted_path)) == [
        NormalizedRecord(1, "chr1", 61, "G", "GA"),
        NormalizedRecord(2, "chr1", 69, "AT", "A"),
        NormalizedRecord(3, "chr1", 4, "T", "C"),
        NormalizedRecord(4, "chr1", 61, "GAAA", "G"),
    ]


def test_run_bcftools_norm_reports_a_missing_binary(tmp_path):
    with (
        patch("radiant.tasks.open_data.cosmic_mutation_set.shutil.which", return_value=None),
        pytest.raises(CosmicNormalizationError, match="bcftools is not on PATH"),
    ):
        run_bcftools_norm("in.vcf", "ref.fa", str(tmp_path))


# --- merge back and output -------------------------------------------------------------------------------


def test_merge_back_skips_dropped_rows_and_rekeys_on_the_normalized_alleles():
    rows = [_row("1:1-1", "A", "C", row_number=i, cosmic_id=f"COSV{i}") for i in range(1, 6)]
    normalized = [
        NormalizedRecord(1, "chr1", 61, "G", "GA"),
        NormalizedRecord(3, "chrM", 5, "G", "A"),
        NormalizedRecord(5, "chr1", 4, "T", "C"),
    ]
    contig_map = ContigMap.build(["chr1", "chrM"])

    out = list(merge_back(rows, normalized, contig_map))

    assert [o[7] for o in out] == ["COSV1", "COSV3", "COSV5"]
    assert out[0][:5] == ["1", "61", "G", "GA", locus_and_hash("1", 61, "G", "GA")[1]]
    assert out[1][:5] == ["M", "5", "G", "A", locus_and_hash("M", 5, "G", "A")[1]]
    assert out[2][5:] == list(rows[4].values)


@pytest.mark.parametrize(
    "normalized",
    [
        [NormalizedRecord(7, "chr1", 1, "A", "C")],  # past the last source row
        [NormalizedRecord(3, "chr1", 1, "A", "C"), NormalizedRecord(1, "chr1", 1, "A", "C")],  # not sorted
    ],
)
def test_merge_back_refuses_to_lose_track_of_its_rows(normalized):
    rows = [_row("1:1-1", "A", "C", row_number=i) for i in range(1, 6)]
    with pytest.raises(CosmicNormalizationError):
        list(merge_back(rows, normalized, ContigMap.build(["chr1"])))


def test_write_output_writes_a_gzipped_tsv_with_a_header(tmp_path):
    path = str(tmp_path / "out.tsv.gz")
    assert write_output([["1", "61", "G", "GA", "hash", "url", "", "COSV1", "5", "100", "Other"]], path) == 1
    with gzip.open(path, "rt") as handle:
        lines = handle.read().splitlines()
    assert lines[0] == "\t".join(OUTPUT_COLUMNS)
    assert lines[1] == "1\t61\tG\tGA\thash\turl\t\tCOSV1\t5\t100\tOther"


def test_summary_check_enforces_the_row_accounting():
    summary = CosmicNormalizationSummary(rows_in=10, rows_without_position=2, bcftools_excluded=1, rows_out=7)
    summary.check()
    summary.rows_out = 8
    with pytest.raises(CosmicNormalizationError, match="row accounting"):
        summary.check()


@pytest.mark.skipif(shutil.which("bcftools") is None, reason="bcftools is not on PATH")
def test_normalize_cosmic_mutation_set_end_to_end(tiny_ref, tmp_path):
    """The whole task with S3 stubbed: download hands back local copies, upload captures the result."""
    export = tmp_path / "cmc_export.tsv.gz"
    _write_export(
        export,
        [
            ("1:69-70", "", "A", "COSV1", "24", "100", "3"),  # -> 1-61-G-GA
            ("", "", "A", "COSV2", "1", "10", "Other"),  # no GRCh38 position
            ("1:3-3", "G", "T", "COSV3", "2", "50", "Other"),  # SNV, unchanged
            ("7:3-3", "G", "T", "COSV4", "2", "50", "Other"),  # contig absent from the reference
            ("1:3-3", "A", "T", "COSV5", "2", "50", "Other"),  # REF mismatch
        ],
    )
    uploaded = {}

    def fake_download(s3_uri, dest_dir):
        src = {
            "s3://in/cmc_export.tsv.gz": export,
            "s3://ref/ref.fa": tiny_ref,
            "s3://ref/ref.fa.fai": tiny_ref + ".fai",
        }
        local = os.path.join(dest_dir, os.path.basename(s3_uri))
        shutil.copy(src[s3_uri], local)
        return local

    def fake_upload(local_path, s3_uri):
        with gzip.open(local_path, "rt") as handle:
            uploaded[s3_uri] = handle.read().splitlines()
        return s3_uri

    with (
        patch("radiant.tasks.open_data.cosmic_mutation_set.download_s3_file", side_effect=fake_download),
        patch("radiant.tasks.open_data.cosmic_mutation_set.upload_s3_file", side_effect=fake_upload),
    ):
        summary = normalize_cosmic_mutation_set("s3://in/cmc_export.tsv.gz", "s3://ref/ref.fa", "s3://out/n.tsv.gz")

    assert summary["rows_in"] == 5 and summary["rows_out"] == 2
    assert summary[NO_POSITION] == 1 and summary[UNMAPPED_CONTIG] == 1 and summary[REF_MISMATCH] == 1
    assert summary[BCFTOOLS_EXCLUDED] == 0 and summary["dropped_contigs"] == {"7": 1}
    assert summary["output_s3_uri"] == "s3://out/n.tsv.gz"
    lines = uploaded["s3://out/n.tsv.gz"]
    assert lines[0] == "\t".join(OUTPUT_COLUMNS)
    assert lines[1].split("\t")[:4] == ["1", "61", "G", "GA"] and lines[1].split("\t")[7] == "COSV1"
    assert lines[2].split("\t")[:4] == ["1", "3", "G", "T"] and lines[2].split("\t")[7] == "COSV3"
    assert cms.locus_and_hash("1", 61, "G", "GA")[1] == lines[1].split("\t")[4]
