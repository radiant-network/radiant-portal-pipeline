import gzip
import os
import pathlib
import shutil

import pytest

from radiant.tasks.open_data import igv_tracks
from radiant.tasks.open_data.igv_tracks import (
    IGV_TRACK_SOURCES,
    IgvTrackError,
    build_igv_track,
    clingen_gene_to_gff3,
    clingen_region_to_gff3,
    dgv_gold_standard_to_gff3,
    escape_gff3_value,
    track_s3_uri,
    ucsc_chain_self_to_gff3,
    ucsc_genomic_superdups_to_gff3,
    ucsc_repeat_masker_to_gff3,
    ucsc_simple_repeat_to_gff3,
    write_bgzipped,
)

RESOURCES = pathlib.Path(__file__).parents[2] / "resources" / "igv_tracks"
HAS_HTSLIB = bool(shutil.which("bgzip") and shutil.which("tabix"))


def _lines(name: str) -> list[str]:
    return (RESOURCES / name).read_text().splitlines(keepends=True)


def _columns(line: str) -> list[str]:
    columns = line.split("\t")
    assert len(columns) == 9, line
    return columns


def _attributes(line: str) -> dict[str, str]:
    return dict(pair.split("=", 1) for pair in _columns(line)[8].split(";"))


def test_sources_are_the_contract_with_the_portal():
    """Track id -> file name is a contract: the portal backend presigns the same map
    (radiant-portal backend/internal/utils/igv_public_tracks.go, igvPublicTrackFiles), and RAD-30 appendix C is
    the shared reference. A change here needs the same change there."""
    assert {s.track_id: s.file_name for s in IGV_TRACK_SOURCES.values()} == {
        "clinvar": "clinvar.vcf.gz",
        "clinvar_sv": "nstd102.GRCh38.variant_call.vcf.gz",
        "dgv": "DGV_GS_hg38.cleaned.sorted.gff3.gz",
        "clingen_gene": "ClinGen_gene_curation_list_GRCh38.sorted.gff3.gz",
        "clingen_region": "ClinGen_region_curation_list_GRCh38.sorted.gff3.gz",
        "gnomad_sv": "gnomad.v4.1.sv.sites.vcf.gz",
        "gnomad_cnv": "gnomad.v4.1.cnv.all.vcf.gz",
        "segmental_duplications": "hg38.genomicSuperDups.gff3.gz",
        "repeat_masker": "hg38.rmsk.gff3.gz",
        "simple_repeats": "hg38.simpleRepeat.gff3.gz",
        "self_chain": "hg38.chainSelf.gff3.gz",
    }
    for source in IGV_TRACK_SOURCES.values():
        assert source.preset == ("vcf" if source.file_name.endswith(".vcf.gz") else "gff")
        assert (source.convert is None) == (source.preset == "vcf")


def test_escape_gff3_value():
    assert escape_gff3_value("a;b=c&d,e%f\tg") == "a%3Bb%3Dc%26d%2Ce%25f%09g"
    assert escape_gff3_value("Not yet evaluated") == "Not yet evaluated"


@pytest.mark.parametrize(
    ("prefix", "expected"),
    [("s3://b/igv/", "s3://b/igv/clinvar.vcf.gz"), ("s3://b/igv", "s3://b/igv/clinvar.vcf.gz")],
)
def test_track_s3_uri(prefix, expected):
    assert track_s3_uri(prefix, "clinvar.vcf.gz") == expected


def test_clingen_gene_to_gff3():
    lines = list(clingen_gene_to_gff3(_lines("ClinGen_gene_curation_list_GRCh38.tsv")))
    assert len(lines) == 3  # the `#` header lines are skipped
    assert _columns(lines[0])[:8] == ["chr22", "ClinGen", "gene", "42692121", "42721301", ".", ".", "."]
    attrs = _attributes(lines[0])
    assert attrs["ID"] == "A4GALT_53947"
    assert attrs["Name"] == "A4GALT"
    assert attrs["haploinsufficiency_score"] == "30"
    assert attrs["haploinsufficiency_description"] == "Gene_associated_with_autosomal_recessive_phenotype"
    assert attrs["triplosensitivity_description"] == "No_evidence_available"
    assert attrs["date_last_evaluated"] == "2014-12-11"
    # Scores keep their spaces: GFF3 allows them, and the portal reads the value as published.
    assert _attributes(lines[2])["triplosensitivity_score"] == "Not yet evaluated"


def test_clingen_region_to_gff3_skips_tbd_and_sanitizes_the_name():
    lines = list(clingen_region_to_gff3(_lines("ClinGen_region_curation_list_GRCh38.tsv")))
    assert [_attributes(line)["ID"] for line in lines] == ["ISCA-46294", "ISCA-46757"]  # ISCA-46748 is `tbd`
    assert _columns(lines[0])[:5] == ["chr22", "ClinGen", "region", "23489015", "24236853"]
    attrs = _attributes(lines[0])
    assert attrs["Name"] == "22q11.2_recurrent_region_distal_type_III_D-G_D-H_E-H_F-H_or_F-G_includes_SMARCB1"
    assert attrs["haploinsufficiency_description"] == "Sufficient_evidence_for_dosage_pathogenicity"
    # Empty cells give no attribute (the disease ids are empty in the extract).
    assert "haploinsufficiency_disease_id" not in attrs


def test_dgv_gold_standard_encodes_stray_percent_only():
    lines = list(dgv_gold_standard_to_gff3(_lines("DGV.GS.hg38.gff3")))
    assert len(lines) == 2
    assert "Frequency=61.194%25;" in lines[0]
    assert "Frequency=1.5%25;" in lines[1]
    assert list(dgv_gold_standard_to_gff3(["chr1\tx\ty\t1\t2\t.\t.\t.\tName=a%3Bb\n"])) == [
        "chr1\tx\ty\t1\t2\t.\t.\t.\tName=a%3Bb"
    ]


def test_ucsc_genomic_superdups_to_gff3():
    lines = list(ucsc_genomic_superdups_to_gff3(_lines("genomicSuperDups.txt")))
    assert _columns(lines[0])[:8] == [
        "chr1",
        "UCSC_SuperDups",
        "segmental_duplication",
        "10001",
        "87112",
        "0",
        "-",
        ".",
    ]
    attrs = _attributes(lines[0])
    assert attrs["ID"] == "gsd_1"
    assert attrs["Name"] == "chr15:101906152"
    assert attrs["Target"] == "chr15 101906153 101981189"
    assert attrs["frac_match"] == "0.992918"
    assert attrs["align_length"] == "77880"
    assert attrs["match_bases"] == "73743"
    assert attrs["mismatch_bases"] == "526"


def test_ucsc_repeat_masker_to_gff3():
    lines = list(ucsc_repeat_masker_to_gff3(_lines("rmsk.txt")))
    assert _columns(lines[0])[:8] == ["chr1", "RepeatMasker", "Simple_repeat", "10001", "10468", "463", "+", "."]
    attrs = _attributes(lines[0])
    assert attrs == {
        "ID": "rmsk_1",
        "Name": "(TAACCC)n",
        "rep_family": "Simple_repeat",
        "milli_div": "13",
        "milli_del": "6",
        "milli_ins": "17",
    }
    assert _columns(lines[1])[2:7] == ["Satellite", "10469", "11447", "3612", "-"]


def test_ucsc_simple_repeat_to_gff3():
    lines = list(ucsc_simple_repeat_to_gff3(_lines("simpleRepeat.txt")))
    assert _columns(lines[0])[:8] == ["chr1", "trf", "tandem_repeat", "10001", "10468", "789", ".", "."]
    assert _attributes(lines[0]) == {
        "ID": "trf_1",
        "period": "6",
        "copy_num": "77.2",
        "entropy": "1.43",
        "consensus_sequence": "TAACCC",
    }


def test_ucsc_chain_self_to_gff3():
    lines = list(ucsc_chain_self_to_gff3(_lines("chainSelf.txt")))
    columns = _columns(lines[0])
    assert columns[:8] == ["chr1", "UCSC_chainSelf", "self_chain_alignment", "10001", "19844", "897863", ".", "."]
    assert _attributes(lines[0]) == {
        "ID": "chainSelf_23989",
        "score": "897863",
        "normScore": "92.00",
        "Target": "chrY 10436 20679",
        "target_strand": "-",
    }


def test_build_refuses_unknown_track_and_non_s3_prefix():
    with pytest.raises(IgvTrackError, match="Unknown IGV track"):
        build_igv_track("nope", "s3://b/igv/")
    with pytest.raises(IgvTrackError, match="s3://"):
        build_igv_track("clinvar", "/tmp/igv/")


@pytest.mark.skipif(not HAS_HTSLIB, reason="bgzip/tabix not installed (they ship in the radiant-operator image)")
def test_gff3_track_is_sorted_bgzipped_and_indexed(tmp_path):
    source = IGV_TRACK_SOURCES["segmental_duplications"]
    # Two chromosomes, starts out of order within chr1: the UCSC dumps are ordered by bin, not by start.
    raw = tmp_path / "raw.txt.gz"
    rows = [
        "585\tchr2\t500\t600\tn\t0\t+\tchr3\t1\t2\t1\t1\t0\t0\t0\t0\t0\ta\t1\t0\t0\t0\t1\t0\t0\t0\t1\t1\t0\t0",
        "585\tchr1\t900\t950\tn\t0\t+\tchr3\t1\t2\t1\t1\t0\t0\t0\t0\t0\ta\t1\t0\t0\t0\t1\t0\t0\t0\t1\t1\t0\t0",
        "73\tchr1\t100\t200\tn\t0\t-\tchr3\t1\t2\t1\t1\t0\t0\t0\t0\t0\ta\t1\t0\t0\t0\t1\t0\t0\t0\t1\t1\t0\t0",
    ]
    with gzip.open(raw, "wt") as f:
        f.write("\n".join(rows) + "\n")
    out = tmp_path / source.file_name

    assert write_bgzipped(source, str(raw), str(out), str(tmp_path)) == 3
    igv_tracks.tabix_index(str(out), source.preset)

    with gzip.open(out, "rt") as f:
        lines = f.read().splitlines()
    assert lines[0] == "##gff-version 3"
    assert [(line.split("\t")[0], line.split("\t")[3]) for line in lines[1:]] == [
        ("chr1", "101"),
        ("chr1", "901"),
        ("chr2", "501"),
    ]
    assert (tmp_path / f"{source.file_name}.tbi").exists()


@pytest.mark.skipif(not HAS_HTSLIB, reason="bgzip/tabix not installed (they ship in the radiant-operator image)")
def test_vcf_track_is_recompressed_as_is_and_indexed(tmp_path, monkeypatch):
    vcf = (
        "##fileformat=VCFv4.2\n##contig=<ID=1>\n"
        "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n"
        "1\t10\t.\tA\tG\t.\t.\t.\n"
    )
    raw = tmp_path / "upstream.vcf.gz"
    with gzip.open(raw, "wt") as f:  # plain gzip, as some upstreams publish: tabix needs BGZF
        f.write(vcf)

    monkeypatch.setattr(igv_tracks, "download", lambda url, dest: shutil.copyfile(raw, dest) and 0)
    uploads = []
    monkeypatch.setattr("radiant.tasks.utils.upload_s3_file", lambda path, uri: uploads.append(uri) or uri)

    summary = build_igv_track("clinvar", "s3://bucket/igv/", work_dir=str(tmp_path))

    assert uploads == ["s3://bucket/igv/clinvar.vcf.gz", "s3://bucket/igv/clinvar.vcf.gz.tbi"]
    assert summary["file_uri"] == "s3://bucket/igv/clinvar.vcf.gz"
    assert summary["index_uri"] == "s3://bucket/igv/clinvar.vcf.gz.tbi"
    assert summary["features"] == 0


@pytest.fixture
def failing_bgzip(tmp_path, monkeypatch):
    """A bgzip that dies at once, as on a full disk: whoever writes to it next gets a broken pipe."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    fake = bin_dir / "bgzip"
    fake.write_text("#!/bin/sh\necho 'bgzip: No space left on device' >&2\nexit 1\n")
    fake.chmod(0o755)
    monkeypatch.setenv("PATH", f"{bin_dir}{os.pathsep}{os.environ['PATH']}")


@pytest.mark.parametrize("track_id", ["clinvar", "segmental_duplications"])
def test_a_tool_dying_mid_stream_reports_its_own_error_not_a_broken_pipe(tmp_path, failing_bgzip, track_id):
    source = IGV_TRACK_SOURCES[track_id]
    row = "585\tchr1\t100\t200\tn\t0\t+\tchr3\t1\t2\t1\t1\t0\t0\t0\t0\t0\ta\t1\t0\t0\t0\t1\t0\t0\t0\t1\t1\t0\t0\n"
    raw = tmp_path / "raw.gz"
    with gzip.open(raw, "wt") as f:  # well past a pipe buffer, so the writer hits the dead pipe
        f.write(row * 20_000)

    with pytest.raises(IgvTrackError, match="bgzip exited with 1: bgzip: No space left on device"):
        write_bgzipped(source, str(raw), str(tmp_path / "out.gz"), str(tmp_path))


def test_dgv_gets_the_gff3_header_like_the_other_gff3_tracks():
    for source in IGV_TRACK_SOURCES.values():
        if source.preset == "gff":
            assert source.header[0] == "##gff-version 3", source.track_id


@pytest.mark.parametrize(
    ("converter", "table"),
    [
        (ucsc_genomic_superdups_to_gff3, "genomicSuperDups"),
        (ucsc_repeat_masker_to_gff3, "rmsk"),
        (ucsc_simple_repeat_to_gff3, "simpleRepeat"),
        (ucsc_chain_self_to_gff3, "chainSelf"),
    ],
)
def test_a_ucsc_row_that_cannot_be_read_names_its_row(converter, table):
    good = _lines(f"{table}.txt")[0]
    with pytest.raises(IgvTrackError, match=rf"^{table}: cannot convert data row 2 \(3 columns\)"):
        list(converter([good, "585\tchr1\t10\n"]))


def test_is_bgzf(tmp_path):
    bgzf = tmp_path / "a.vcf.gz"
    bgzf.write_bytes(bytes.fromhex("1f8b08040000000000ff06004243") + b"rest")  # clinvar.vcf.gz's first 14 bytes
    plain = tmp_path / "b.vcf.gz"
    with gzip.open(plain, "wb") as f:
        f.write(b"##fileformat=VCFv4.2\n")
    short = tmp_path / "c"
    short.write_bytes(b"\x1f\x8b")
    assert igv_tracks.is_bgzf(str(bgzf)) is True
    assert igv_tracks.is_bgzf(str(plain)) is False
    assert igv_tracks.is_bgzf(str(short)) is False


def test_a_bgzf_vcf_is_copied_without_bgzip(tmp_path, failing_bgzip):
    """No recompression: the failing bgzip on PATH would fail the copy if it were called."""
    raw = tmp_path / "raw"
    content = bytes.fromhex("1f8b08040000000000ff06004243") + b"payload"
    raw.write_bytes(content)
    out = tmp_path / "out.vcf.gz"
    assert write_bgzipped(IGV_TRACK_SOURCES["clinvar"], str(raw), str(out), str(tmp_path)) == 0
    assert out.read_bytes() == content


@pytest.fixture
def cat_as_bgzip(tmp_path, monkeypatch):
    """A bgzip that writes its input unchanged, to read the pipeline's output without htslib."""
    bin_dir = tmp_path / "catbin"
    bin_dir.mkdir()
    fake = bin_dir / "bgzip"
    fake.write_text("#!/bin/sh\nexec cat\n")
    fake.chmod(0o755)
    monkeypatch.setenv("PATH", f"{bin_dir}{os.pathsep}{os.environ['PATH']}")


def test_a_non_utf8_byte_passes_through_unchanged(tmp_path, cat_as_bgzip):
    row = b"585\tchr1\t100\t200\tn\t0\t+\tchr3\t1\t2\t1\t1\t0\t0\t0\t0\t0\ta\t1\t0\t0\t0\t1\t0\t0\t0\t1\t1\t0\t0\n"
    raw = tmp_path / "raw.txt"
    raw.write_bytes(row.replace(b"\tn\t", b"\tn\xe9\t"))  # latin-1 e-acute in the Name column
    out = tmp_path / "out"
    assert write_bgzipped(IGV_TRACK_SOURCES["segmental_duplications"], str(raw), str(out), str(tmp_path)) == 1
    assert b"Name=n\xe9;" in out.read_bytes()
