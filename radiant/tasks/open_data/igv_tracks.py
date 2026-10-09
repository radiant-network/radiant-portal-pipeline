"""Build the public IGV reference tracks the portal shows in its IGV Tracks menu.

A port of Qlin's ``etl_import_igv`` (clin-pipelines-dags): the same 11 upstream files, written under the same
file names, so the portal can map its fixed track ids to them. Each track is downloaded, converted to GFF3 when
the upstream format is a TSV or a UCSC table dump, sorted, bgzipped and tabix-indexed, then written with its
``.tbi`` under one S3 prefix. The portal API presigns those objects; nothing here is a table.

Differences with the Qlin port, all on the output's validity rather than its content (the same list as
radiant/dags/docs/import_igv_tracks.md; keep the two in sync):

- Every GFF3 is sorted on (seqid, start). Qlin skips the sort for the UCSC dumps, which are ordered by
  ``bin`` within a chromosome, not by ``chromStart`` (e.g. ``genomicSuperDups``: chr1:10000 bin 585 comes first).
- VCFs are indexed with the ``vcf`` preset. Qlin indexes them with ``-p gff``, which reads REF as the end.
- VCFs already in BGZF (all four upstreams) are copied as is instead of recompressed: same records.
- Attribute values are percent-encoded for the GFF3 reserved characters (``; = & ,``, tab, newline, ``%``).
  igv.js decodes exactly these (``decodeGFFAttribute`` in js/feature/gff/parseAttributeString.js), so the
  values the portal colors on read as published; today's ClinGen descriptions hold none of them anyway.
- The two UCSC tables that Qlin writes with an empty type column (genomicSuperDups, simpleRepeat) get the
  type ``segmental_duplication`` / ``tandem_repeat``: an empty column 3 is not GFF3.
- A UCSC strand other than ``+``/``-`` is written ``.`` (GFF3 allows ``+ - . ?`` only); hg38 rmsk and
  genomicSuperDups hold only ``+``/``-``, so no row changes today.
- DGV gets the ``##gff-version 3`` line, which upstream lacks, like every other GFF3 track. The ClinGen files
  keep one ``#`` comment naming their source; Qlin's longer comment block is not carried.
- ClinGen is read over HTTPS; ftp.clinicalgenome.org serves both, and Qlin's scheme-less curl falls back to FTP.
- Run settings: Qlin runs at most 4 tasks at a time, so does this DAG; each track here also retries twice.

Runs inside the radiant-operator image (which carries ``bgzip``, ``tabix`` and GNU ``sort``), on Kubernetes or
ECS; one call builds one track, so a failed download retries alone.
"""

from __future__ import annotations

import gzip
import io
import logging
import os
import re
import shutil
import subprocess
import tempfile
import urllib.request
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
from typing import Literal

logger = logging.getLogger(__name__)

GFF3_HEADER = "##gff-version 3"
DOWNLOAD_TIMEOUT_SECONDS = 300
DOWNLOAD_CHUNK_BYTES = 8 * 1024 * 1024
# GNU sort buffer: enough to sort the small tracks in memory, spills the large ones (chainSelf, rmsk) to disk.
SORT_BUFFER = os.getenv("RADIANT_IGV_TRACKS_SORT_BUFFER", "512M")

# GFF3 column 9 reserves these in tag values (https://github.com/The-Sequence-Ontology/Specifications/blob/master/gff3.md).
_GFF3_RESERVED = {"%": "%25", ";": "%3B", "=": "%3D", "&": "%26", ",": "%2C", "\t": "%09", "\n": "%0A", "\r": "%0D"}
_GFF3_RESERVED_RE = re.compile("|".join(re.escape(c) for c in _GFF3_RESERVED))
# A `%` that does not start a percent-encoded byte (DGV writes `Frequency=61.194%`).
_STRAY_PERCENT_RE = re.compile(r"%(?![0-9a-fA-F]{2})")
_LOCATION_RE = re.compile(r"^([^:]+):(\d+)-(\d+)$")


class IgvTrackError(Exception):
    """Raised when a track cannot be built (unknown id, upstream failure, or a tool that failed)."""


def escape_gff3_value(value: str) -> str:
    """Percent-encode the characters GFF3 reserves in an attribute value."""
    return _GFF3_RESERVED_RE.sub(lambda m: _GFF3_RESERVED[m.group(0)], value)


def gff3_attributes(pairs: Iterable[tuple[str, str]]) -> str:
    return ";".join(f"{tag}={escape_gff3_value(value)}" for tag, value in pairs)


def gff3_line(seqid: str, source: str, type_: str, start: int, end: int, score: str, strand: str, attrs: str) -> str:
    return "\t".join([seqid, source, type_, str(start), str(end), score, strand, ".", attrs])


def _data_rows(lines: Iterable[str]) -> Iterator[list[str]]:
    """Tab-split the non-comment, non-empty lines (ClinGen comments its header with ``#``)."""
    for line in lines:
        line = line.rstrip("\r\n")
        if line and not line.startswith("#"):
            yield line.split("\t")


def _parse_location(location: str) -> tuple[str, int, int] | None:
    """``chr22:42692121-42721301`` -> (seqid, start, end); ``tbd`` and malformed locations -> None."""
    match = _LOCATION_RE.match(location.strip())
    if not match:
        return None
    return match.group(1), int(match.group(2)), int(match.group(3))


def _cell(row: list[str], column: int) -> str:
    """1-based column, like the awk scripts this ports; a short row reads as empty."""
    return row[column - 1] if len(row) >= column else ""


def clingen_gene_to_gff3(lines: Iterable[str]) -> Iterator[str]:
    """ClinGen gene dosage curation list (GRCh38 TSV) -> one ``gene`` feature per curated gene.

    Columns: 1 symbol, 2 gene id, 3 cytoband, 4 location, 5/6 haploinsufficiency score/description,
    13/14 triplosensitivity score/description, 21 last evaluated, 22/23 HI/TS disease ids. Locations are
    already 1-based inclusive. Descriptions have spaces replaced by underscores, as in Qlin, since the portal
    colors the track on them.
    """
    for row in _data_rows(lines):
        location = _parse_location(_cell(row, 4))
        if location is None:
            logger.warning("ClinGen gene: skipping row without a location: %s", row[:4])
            continue
        seqid, start, end = location
        symbol, gene_id = _cell(row, 1), _cell(row, 2)
        attrs = gff3_attributes(
            [
                ("ID", f"{symbol}_{gene_id}"),
                ("Name", symbol),
                ("gene_id", gene_id),
                ("cytoband", _cell(row, 3)),
                ("haploinsufficiency_score", _cell(row, 5)),
                ("haploinsufficiency_description", _cell(row, 6).replace(" ", "_")),
                ("triplosensitivity_score", _cell(row, 13)),
                ("triplosensitivity_description", _cell(row, 14).replace(" ", "_")),
                ("date_last_evaluated", _cell(row, 21)),
                ("haploinsufficiency_disease_id", _cell(row, 22)),
                ("triplosensitivity_disease_id", _cell(row, 23)),
            ]
        )
        yield gff3_line(seqid, "ClinGen", "gene", start, end, ".", ".", attrs)


def clingen_region_to_gff3(lines: Iterable[str]) -> Iterator[str]:
    """ClinGen region dosage curation list (GRCh38 TSV) -> one ``region`` feature per curated region.

    Same columns as the gene list, with the ISCA id and region name in 1 and 2. Regions still to be mapped
    have the location ``tbd`` and are skipped. Empty values give no attribute, as in Qlin.
    """
    for row in _data_rows(lines):
        location = _parse_location(_cell(row, 4))
        if location is None:
            logger.warning("ClinGen region: skipping row without a location: %s", row[:4])
            continue
        seqid, start, end = location
        isca_id = _cell(row, 1)
        name = re.sub(r"[();,]", "", _cell(row, 2).replace(" ", "_"))
        optional = [
            ("cytoband", _cell(row, 3)),
            ("haploinsufficiency_score", _cell(row, 5)),
            ("haploinsufficiency_description", _cell(row, 6).replace(" ", "_")),
            ("triplosensitivity_score", _cell(row, 13)),
            ("triplosensitivity_description", _cell(row, 14).replace(" ", "_")),
            ("date_last_evaluated", _cell(row, 21)),
            ("haploinsufficiency_disease_id", _cell(row, 22)),
            ("triplosensitivity_disease_id", _cell(row, 23)),
        ]
        attrs = gff3_attributes(
            [("ID", isca_id), ("Name", name), ("isca_id", isca_id), *[(k, v) for k, v in optional if v]]
        )
        yield gff3_line(seqid, "ClinGen", "region", start, end, ".", ".", attrs)


def dgv_gold_standard_to_gff3(lines: Iterable[str]) -> Iterator[str]:
    """DGV gold standard is already GFF3; only its stray ``%`` signs need encoding."""
    for line in lines:
        line = line.rstrip("\r\n")
        if line and not line.startswith("#"):
            yield _STRAY_PERCENT_RE.sub("%25", line)


def _convert_ucsc_rows(
    lines: Iterable[str], table: str, convert_row: Callable[[int, list[str]], str]
) -> Iterator[str]:
    """Runs ``convert_row`` on each data row (numbered from 1; the UCSC dumps have no header). A row the
    converter cannot read -- too few columns, a non-numeric coordinate -- fails the track with its number and
    width, not with a bare IndexError/ValueError."""
    for n, row in enumerate(_data_rows(lines), start=1):
        try:
            yield convert_row(n, row)
        except (IndexError, ValueError) as e:
            raise IgvTrackError(f"{table}: cannot convert data row {n} ({len(row)} columns): {e!r}") from e


def ucsc_genomic_superdups_to_gff3(lines: Iterable[str]) -> Iterator[str]:
    """UCSC ``genomicSuperDups`` -> one feature per duplicated segment (UCSC starts are 0-based).

    Columns: 2 chrom, 3 chromStart, 4 chromEnd, 5 name, 6 score, 7 strand, 8-10 other chrom/start/end,
    19 alignL, 23 matchB, 24 mismatchB, 27 fracMatch, 28 fracMatchIndel.
    """

    def convert_row(n: int, row: list[str]) -> str:
        attrs = gff3_attributes(
            [
                ("ID", f"gsd_{n}"),
                ("Name", row[4]),
                ("Target", f"{row[7]} {int(row[8]) + 1} {row[9]}"),
                ("frac_match", f"{float(row[26]):.6f}"),
                ("frac_match_indel", f"{float(row[27]):.6f}"),
                ("align_length", row[18]),
                ("match_bases", row[22]),
                ("mismatch_bases", row[23]),
            ]
        )
        strand = row[6] if row[6] in ("+", "-") else "."
        return gff3_line(
            row[1], "UCSC_SuperDups", "segmental_duplication", int(row[2]) + 1, int(row[3]), row[5], strand, attrs
        )

    return _convert_ucsc_rows(lines, "genomicSuperDups", convert_row)


def ucsc_repeat_masker_to_gff3(lines: Iterable[str]) -> Iterator[str]:
    """UCSC ``rmsk`` -> one feature per repeat, typed by its repeat class.

    Columns: 2 swScore, 3-5 milliDiv/Del/Ins, 6 genoName, 7 genoStart, 8 genoEnd, 10 strand, 11 repName,
    12 repClass, 13 repFamily.
    """

    def convert_row(n: int, row: list[str]) -> str:
        attrs = gff3_attributes(
            [
                ("ID", f"rmsk_{n}"),
                ("Name", row[10]),
                ("rep_family", row[12]),
                ("milli_div", row[2]),
                ("milli_del", row[3]),
                ("milli_ins", row[4]),
            ]
        )
        strand = row[9] if row[9] in ("+", "-") else "."
        return gff3_line(row[5], "RepeatMasker", row[11], int(row[6]) + 1, int(row[7]), row[1], strand, attrs)

    return _convert_ucsc_rows(lines, "rmsk", convert_row)


def ucsc_simple_repeat_to_gff3(lines: Iterable[str]) -> Iterator[str]:
    """UCSC ``simpleRepeat`` (Tandem Repeats Finder) -> one ``tandem_repeat`` feature per repeat.

    Columns: 2 chrom, 3 chromStart, 4 chromEnd, 5 name (the tool, ``trf``), 6 period, 7 copyNum, 11 score,
    16 entropy, 17 sequence.
    """

    def convert_row(n: int, row: list[str]) -> str:
        attrs = gff3_attributes(
            [
                ("ID", f"trf_{n}"),
                ("period", row[5]),
                ("copy_num", f"{float(row[6]):.1f}"),
                ("entropy", f"{float(row[15]):.2f}"),
                ("consensus_sequence", row[16]),
            ]
        )
        return gff3_line(row[1], row[4], "tandem_repeat", int(row[2]) + 1, int(row[3]), row[10], ".", attrs)

    return _convert_ucsc_rows(lines, "simpleRepeat", convert_row)


def ucsc_chain_self_to_gff3(lines: Iterable[str]) -> Iterator[str]:
    """UCSC ``chainSelf`` -> one ``self_chain_alignment`` feature per chain, on the target sequence.

    Columns: 2 score, 3 tName, 5 tStart, 6 tEnd, 7 qName, 9 qStrand, 10 qStart, 11 qEnd, 12 id, 13 normScore.
    """

    def convert_row(n: int, row: list[str]) -> str:
        score = float(row[1])
        attrs = gff3_attributes(
            [
                ("ID", f"chainSelf_{row[11]}"),
                ("score", f"{score:.0f}"),
                ("normScore", f"{float(row[12]):.2f}"),
                ("Target", f"{row[6]} {int(row[9]) + 1} {row[10]}"),
                ("target_strand", row[8]),
            ]
        )
        return gff3_line(
            row[2], "UCSC_chainSelf", "self_chain_alignment", int(row[4]) + 1, int(row[5]), row[1], ".", attrs
        )

    return _convert_ucsc_rows(lines, "chainSelf", convert_row)


Converter = Callable[[Iterable[str]], Iterator[str]]


@dataclass(frozen=True)
class IgvTrackSource:
    track_id: str  # the key the portal API returns the file under (public_tracks)
    url: str
    file_name: str  # written under the tracks prefix; the portal backend lists the same names
    preset: Literal["vcf", "gff"]
    convert: Converter | None = None  # None: a VCF, copied as is (recompressed if not BGZF)
    header: tuple[str, ...] = ()  # GFF3 lines written before the sorted features


_UCSC = "https://hgdownload.soe.ucsc.edu/goldenPath/hg38/database"
_CLINGEN = "https://ftp.clinicalgenome.org"

IGV_TRACK_SOURCES: dict[str, IgvTrackSource] = {
    s.track_id: s
    for s in (
        IgvTrackSource(
            "clinvar", "https://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh38/clinvar.vcf.gz", "clinvar.vcf.gz", "vcf"
        ),
        IgvTrackSource(
            "clinvar_sv",
            "https://ftp.ncbi.nlm.nih.gov/pub/dbVar/data/Homo_sapiens/by_study/vcf/nstd102.GRCh38.variant_call.vcf.gz",
            "nstd102.GRCh38.variant_call.vcf.gz",
            "vcf",
        ),
        IgvTrackSource(
            "gnomad_sv",
            "https://storage.googleapis.com/gcp-public-data--gnomad/release/4.1/genome_sv/gnomad.v4.1.sv.sites.vcf.gz",
            "gnomad.v4.1.sv.sites.vcf.gz",
            "vcf",
        ),
        IgvTrackSource(
            "gnomad_cnv",
            "https://storage.googleapis.com/gcp-public-data--gnomad/release/4.1/exome_cnv/gnomad.v4.1.cnv.all.vcf.gz",
            "gnomad.v4.1.cnv.all.vcf.gz",
            "vcf",
        ),
        IgvTrackSource(
            "dgv",
            "https://dgv.tcag.ca/dgv/docs/DGV.GS.hg38.gff3",
            "DGV_GS_hg38.cleaned.sorted.gff3.gz",
            "gff",
            convert=dgv_gold_standard_to_gff3,
            header=(GFF3_HEADER,),  # upstream has none; written like the other GFF3 tracks
        ),
        IgvTrackSource(
            "clingen_gene",
            f"{_CLINGEN}/ClinGen_gene_curation_list_GRCh38.tsv",
            "ClinGen_gene_curation_list_GRCh38.sorted.gff3.gz",
            "gff",
            convert=clingen_gene_to_gff3,
            header=(GFF3_HEADER, "# Converted from ClinGen Gene Curation List (GRCh38)"),
        ),
        IgvTrackSource(
            "clingen_region",
            f"{_CLINGEN}/ClinGen_region_curation_list_GRCh38.tsv",
            "ClinGen_region_curation_list_GRCh38.sorted.gff3.gz",
            "gff",
            convert=clingen_region_to_gff3,
            header=(GFF3_HEADER, "# Converted from ClinGen Region Curation List (GRCh38)"),
        ),
        IgvTrackSource(
            "segmental_duplications",
            f"{_UCSC}/genomicSuperDups.txt.gz",
            "hg38.genomicSuperDups.gff3.gz",
            "gff",
            convert=ucsc_genomic_superdups_to_gff3,
            header=(GFF3_HEADER,),
        ),
        IgvTrackSource(
            "repeat_masker",
            f"{_UCSC}/rmsk.txt.gz",
            "hg38.rmsk.gff3.gz",
            "gff",
            convert=ucsc_repeat_masker_to_gff3,
            header=(GFF3_HEADER,),
        ),
        IgvTrackSource(
            "simple_repeats",
            f"{_UCSC}/simpleRepeat.txt.gz",
            "hg38.simpleRepeat.gff3.gz",
            "gff",
            convert=ucsc_simple_repeat_to_gff3,
            header=(GFF3_HEADER,),
        ),
        IgvTrackSource(
            "self_chain",
            f"{_UCSC}/chainSelf.txt.gz",
            "hg38.chainSelf.gff3.gz",
            "gff",
            convert=ucsc_chain_self_to_gff3,
            header=(GFF3_HEADER,),
        ),
    )
}


def track_s3_uri(prefix: str, file_name: str) -> str:
    return f"{prefix.rstrip('/')}/{file_name}"


def download(url: str, dest_path: str) -> int:
    """Stream ``url`` to ``dest_path``; returns the byte count. Redirects are followed."""
    request = urllib.request.Request(url, headers={"User-Agent": "radiant-portal-pipeline"})
    with urllib.request.urlopen(request, timeout=DOWNLOAD_TIMEOUT_SECONDS) as response, open(dest_path, "wb") as out:
        shutil.copyfileobj(response, out, DOWNLOAD_CHUNK_BYTES)
    return os.path.getsize(dest_path)


def _open_binary(path: str) -> io.BufferedIOBase:
    """Opens the download decompressed, whatever its compression: gzip magic bytes, not the URL, decide."""
    with open(path, "rb") as f:
        magic = f.read(2)
    return gzip.open(path, "rb") if magic == b"\x1f\x8b" else open(path, "rb")


def is_bgzf(path: str) -> bool:
    """BGZF starts with a gzip member whose FEXTRA field holds the ``BC`` subfield (SAM spec, section 4.1)."""
    with open(path, "rb") as f:
        head = f.read(14)
    return len(head) == 14 and head[:4] == b"\x1f\x8b\x08\x04" and head[12:14] == b"BC"


def _check_returncode(process: subprocess.Popen, name: str, stderr_path: str) -> None:
    if process.returncode != 0:
        with open(stderr_path) as err:
            raise IgvTrackError(f"{name} exited with {process.returncode}: {err.read()[-2000:]}")


def write_bgzipped(source: IgvTrackSource, raw_path: str, out_path: str, work_dir: str) -> int:
    """Writes ``out_path`` as BGZF: a VCF recompressed as is, a GFF3 as header + features sorted on
    (seqid, start). Returns the number of features (0 for a VCF, which is not parsed).

    A VCF already in BGZF (all four upstreams do, checked 2026-10-09) is copied as is: recompressing gnomAD SV (1.7 GB)
    on one bgzip thread costs minutes for the same bytes. tabix rejects a file that is not BGZF throughout."""
    if source.convert is None and is_bgzf(raw_path):
        shutil.copyfile(raw_path, out_path)
        return 0

    bgzip_err = os.path.join(work_dir, "bgzip.stderr")
    sort_err = os.path.join(work_dir, "sort.stderr")
    features = 0
    sort: subprocess.Popen | None = None
    # A tool that dies mid-stream (typically on a full disk while sort spills chainSelf) only shows up here as a
    # BrokenPipeError on our next write. Keep it, let both tools exit, and report their stderr instead.
    broken_pipe: BrokenPipeError | None = None
    with open(out_path, "wb") as out, open(bgzip_err, "wb") as bgzip_stderr:
        bgzip = subprocess.Popen(["bgzip", "-c"], stdin=subprocess.PIPE, stdout=out, stderr=bgzip_stderr)
        try:
            with _open_binary(raw_path) as raw:
                if source.convert is None:
                    shutil.copyfileobj(raw, bgzip.stdin, DOWNLOAD_CHUNK_BYTES)
                else:
                    for line in source.header:
                        bgzip.stdin.write(f"{line}\n".encode())
                    bgzip.stdin.flush()
                    # C collation sorts the seqids bytewise: tabix needs each seqid contiguous, not a given order.
                    with open(sort_err, "wb") as sort_stderr:
                        sort = subprocess.Popen(
                            ["sort", "-t", "\t", "-k1,1", "-k4,4n", "-S", SORT_BUFFER, "-T", work_dir],
                            stdin=subprocess.PIPE,
                            stdout=bgzip.stdin,
                            stderr=sort_stderr,
                            env={**os.environ, "LC_ALL": "C"},
                        )
                        try:
                            # surrogateescape on both sides: a non-UTF-8 byte passes through unchanged, as in
                            # Qlin's awk, instead of failing the whole track.
                            with io.TextIOWrapper(
                                sort.stdin, encoding="utf-8", errors="surrogateescape", newline="\n"
                            ) as sort_in:
                                text = io.TextIOWrapper(raw, encoding="utf-8", errors="surrogateescape")
                                for feature in source.convert(text):
                                    sort_in.write(feature)
                                    sort_in.write("\n")
                                    features += 1
                        finally:
                            sort.wait()
        except BrokenPipeError as e:
            broken_pipe = e
        finally:
            try:
                bgzip.stdin.close()
            except BrokenPipeError as e:
                broken_pipe = broken_pipe or e
            bgzip.wait()
    # bgzip first: when it dies, sort fails next on its own broken pipe, which is not the cause.
    _check_returncode(bgzip, "bgzip", bgzip_err)
    if sort is not None:
        _check_returncode(sort, "sort", sort_err)
    if broken_pipe is not None:
        raise IgvTrackError("Pipe to sort/bgzip closed early, but both exited 0") from broken_pipe
    return features


def tabix_index(path: str, preset: str) -> str:
    result = subprocess.run(["tabix", "-f", "-p", preset, path], capture_output=True, text=True)
    if result.returncode != 0:
        raise IgvTrackError(f"tabix -p {preset} {os.path.basename(path)} failed: {result.stderr[-2000:]}")
    return f"{path}.tbi"


def build_igv_track(track_id: str, igv_tracks_prefix: str, work_dir: str | None = None) -> dict:
    """Download, convert, sort, bgzip and index one public track, then write it and its ``.tbi`` under
    ``igv_tracks_prefix``. A rerun replaces both objects with the latest upstream release."""
    if track_id not in IGV_TRACK_SOURCES:
        raise IgvTrackError(f"Unknown IGV track {track_id!r}; expected one of {sorted(IGV_TRACK_SOURCES)}")
    if not igv_tracks_prefix.startswith("s3://"):
        raise IgvTrackError(f"igv_tracks_prefix must be an s3:// URI, got {igv_tracks_prefix!r}")
    source = IGV_TRACK_SOURCES[track_id]

    with tempfile.TemporaryDirectory(prefix=f"igv_{track_id}_", dir=work_dir) as tmp:
        raw_path = os.path.join(tmp, "download")
        logger.info("Downloading %s from %s", track_id, source.url)
        downloaded = download(source.url, raw_path)
        logger.info("Downloaded %d bytes", downloaded)

        out_path = os.path.join(tmp, source.file_name)
        features = write_bgzipped(source, raw_path, out_path, tmp)
        os.remove(raw_path)
        index_path = tabix_index(out_path, source.preset)

        # Imported here: the DAG reads IGV_TRACK_SOURCES at parse time, in an interpreter without wurlitzer.
        from radiant.tasks.utils import upload_s3_file

        file_uri = track_s3_uri(igv_tracks_prefix, source.file_name)
        index_uri = f"{file_uri}.tbi"
        # The data file first, so the window where the two disagree ends with the index upload.
        upload_s3_file(out_path, file_uri)
        upload_s3_file(index_path, index_uri)
        summary = {
            "track_id": track_id,
            "source_url": source.url,
            "downloaded_bytes": downloaded,
            "features": features,
            "file_uri": file_uri,
            "file_bytes": os.path.getsize(out_path),
            "index_uri": index_uri,
        }
    logger.info("IGV track built: %s", summary)
    return summary
