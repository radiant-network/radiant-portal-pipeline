"""Normalize the COSMIC Mutation Census export (``cmc_export.tsv.gz``) for the StarRocks broker load.

COSMIC does not encode variants the way a VCF does: an insertion has an *empty* reference allele and a
position spanning the two flanking bases (``X:18628509-18628510``, alt ``C``), and a deletion has an
*empty* alternate allele over the deleted range (``X:18646011-18646013``, ref ``TGG``). The pipeline keys
variants on VCF-style records -- anchor base included, left-aligned -- so a ``chrom-start-ref-alt`` built
from the COSMIC columns never matches an indel. This module rebuilds every row as a VCF record with the
anchor base read from the reference FASTA, runs ``bcftools norm`` against that same FASTA, and writes the
rows back out keyed on the normalized alleles with the pipeline's own ``locus_hash``.

Runs inside the radiant-operator image (which carries ``bcftools``), on Kubernetes or ECS; it needs the
stdlib, ``boto3`` and a few GB of scratch disk (the GRCh38 FASTA alone is 3 GB).
"""

from __future__ import annotations

import gzip
import logging
import os
import re
import shutil
import subprocess
import tempfile
from collections import Counter
from collections.abc import Iterable, Iterator
from dataclasses import asdict, dataclass, field

from radiant.tasks.utils import download_s3_file, upload_s3_file
from radiant.tasks.vcf.snv.common import locus_and_hash

logger = logging.getLogger(__name__)

POSITION_COLUMN = "Mutation genome position GRCh38"
REF_COLUMN = "GENOMIC_WT_ALLELE_SEQ"
ALT_COLUMN = "GENOMIC_MUT_ALLELE_SEQ"
# Output column -> COSMIC header, copied through verbatim (typing happens in the broker load's SET clause).
PASSTHROUGH_COLUMNS = {
    "mutation_url": "MUTATION_URL",
    "shared_aa": "SHARED_AA",
    "cosmic_id": "GENOMIC_MUTATION_ID",
    "sample_mutated": "COSMIC_SAMPLE_MUTATED",
    "sample_tested": "COSMIC_SAMPLE_TESTED",
    "tier": "MUTATION_SIGNIFICANCE_TIER",
}
OUTPUT_COLUMNS = ["chromosome", "start", "reference", "alternate", "locus_hash", *PASSTHROUGH_COLUMNS]
COSMIC_CONTIGS = [str(i) for i in range(1, 23)] + ["X", "Y", "MT"]

# Rejection reasons; each is a counter in the summary.
NO_POSITION = "rows_without_position"
UNMAPPED_CONTIG = "rows_unmapped_contig"
MALFORMED = "rows_malformed"
INVALID_ALLELE = "rows_invalid_allele"
REF_MISMATCH = "rows_ref_mismatch"
BCFTOOLS_EXCLUDED = "bcftools_excluded"
DROP_REASONS = (NO_POSITION, UNMAPPED_CONTIG, MALFORMED, INVALID_ALLELE, REF_MISMATCH, BCFTOOLS_EXCLUDED)

_ALLELE_RE = re.compile(r"^[ACGT]+$")
_POSITION_RE = re.compile(r"^([^:\s]+):(\d+)-(\d+)$")
_BCFTOOLS_LINES_RE = re.compile(r"Lines\s+([\w/]+):\s*([\d/]+)")
_REF_MISMATCH_PREFIX = "REF_MISMATCH"


class CosmicNormalizationError(Exception):
    """Raised when the normalization cannot produce a trustworthy output (bad input file, bcftools failure,
    or a merge-back that lost track of its rows)."""


class RowRejected(Exception):
    """A row that cannot become a VCF record; ``reason`` is one of the counters above."""

    def __init__(self, reason: str, detail: str = ""):
        super().__init__(f"{reason}: {detail}" if detail else reason)
        self.reason = reason


@dataclass(frozen=True, slots=True)
class CosmicRow:
    row_number: int  # 1-based data-row index, header excluded; the VCF ID that survives bcftools
    position: str  # the raw "Mutation genome position GRCh38" cell
    ref: str
    alt: str
    values: tuple[str, ...]  # PASSTHROUGH_COLUMNS, in order


@dataclass(frozen=True, slots=True)
class VcfRecord:
    contig: str
    pos: int
    ref: str
    alt: str


@dataclass(frozen=True, slots=True)
class NormalizedRecord:
    row_number: int
    contig: str
    pos: int
    ref: str
    alt: str


@dataclass(frozen=True)
class FaiEntry:
    name: str
    length: int
    offset: int
    linebases: int
    linewidth: int


@dataclass
class BcftoolsStats:
    ref_mismatches: int = 0
    first_ref_mismatches: list[str] = field(default_factory=list)
    lines: dict[str, int] = field(default_factory=dict)
    records_out: int = 0

    @property
    def excluded(self) -> int:
        """Records bcftools dropped. Derived from what came back rather than from the summary line: 1.16
        counts ``--check-ref x`` exclusions as ``skipped``, 1.21 as ``removed``."""
        return self.lines.get("total", self.records_out) - self.records_out


@dataclass
class CosmicNormalizationSummary:
    rows_in: int = 0
    rows_without_position: int = 0
    rows_unmapped_contig: int = 0
    rows_malformed: int = 0
    rows_invalid_allele: int = 0
    rows_ref_mismatch: int = 0
    bcftools_excluded: int = 0
    bcftools_lines: dict[str, int] = field(default_factory=dict)
    rows_out: int = 0
    dropped_contigs: dict[str, int] = field(default_factory=dict)
    output_s3_uri: str = ""

    def check(self) -> None:
        dropped = sum(getattr(self, reason) for reason in DROP_REASONS)
        if self.rows_out != self.rows_in - dropped:
            raise CosmicNormalizationError(
                f"row accounting mismatch: {self.rows_in} in, {dropped} dropped, {self.rows_out} out"
            )


# --- reference FASTA -------------------------------------------------------------------------------------


def parse_fai(fai_path: str) -> dict[str, FaiEntry]:
    """Parse a samtools ``.fai`` index (name, length, offset, linebases, linewidth), keeping file order."""
    entries: dict[str, FaiEntry] = {}
    with open(fai_path) as handle:
        for line in handle:
            fields = line.rstrip("\r\n").split("\t")
            if len(fields) < 5:
                raise CosmicNormalizationError(f"malformed .fai line in {fai_path}: {line!r}")
            name, length, offset, linebases, linewidth = fields[:5]
            entries[name] = FaiEntry(name, int(length), int(offset), int(linebases), int(linewidth))
    if not entries:
        raise CosmicNormalizationError(f"empty .fai index: {fai_path}")
    return entries


class FaiReader:
    """Random access into a FASTA through its ``.fai``: ``fetch`` reads ``[start, end]`` (1-based, inclusive)
    with one seek, so the 3 GB genome is never loaded in memory."""

    def __init__(self, fasta_path: str, fai_path: str | None = None):
        self.fasta_path = fasta_path
        self.contigs = parse_fai(fai_path or f"{fasta_path}.fai")
        self._handle = None

    def __enter__(self) -> FaiReader:
        self._handle = open(self.fasta_path, "rb")
        return self

    def __exit__(self, *exc) -> None:
        if self._handle is not None:
            self._handle.close()
            self._handle = None

    def fetch(self, contig: str, start: int, end: int) -> str:
        entry = self.contigs[contig]
        if start < 1 or end < start or end > entry.length:
            raise RowRejected(MALFORMED, f"{contig}:{start}-{end} outside contig length {entry.length}")
        first, last = start - 1, end - 1
        byte = entry.offset + (first // entry.linebases) * entry.linewidth + first % entry.linebases
        nbytes = (end - start + 1) + (last // entry.linebases - first // entry.linebases) * (
            entry.linewidth - entry.linebases
        )
        self._handle.seek(byte)
        seq = self._handle.read(nbytes).replace(b"\r", b"").replace(b"\n", b"").decode("ascii").upper()
        if len(seq) != end - start + 1:
            raise CosmicNormalizationError(f"short read for {contig}:{start}-{end}: {seq!r}")
        return seq


@dataclass(frozen=True)
class ContigMap:
    """COSMIC contig names (``1``..``22``, ``X``, ``Y``, ``MT``) to the FASTA's spelling, and back to the
    pipeline's: ``fasta_name`` with a leading ``chr`` removed, the same transform the SNV extraction applies
    to a VCF CHROM (`radiant/tasks/vcf/snv/common.py`), so the two agree on whatever the reference calls the
    mitochondrion."""

    cosmic_to_fasta: dict[str, str]

    @classmethod
    def build(cls, fasta_names: Iterable[str]) -> ContigMap:
        names = set(fasta_names)
        mapping = {}
        for cosmic in COSMIC_CONTIGS:
            candidates = [cosmic, f"chr{cosmic}"]
            if cosmic == "MT":
                candidates += ["chrM", "M", "chrMT"]
            for candidate in candidates:
                if candidate in names:
                    mapping[cosmic] = candidate
                    break
        if not mapping:
            raise CosmicNormalizationError("no COSMIC contig found in the reference .fai")
        return cls(mapping)

    @staticmethod
    def to_radiant(fasta_name: str) -> str:
        return fasta_name.removeprefix("chr")


# --- COSMIC rows -> VCF -----------------------------------------------------------------------------------


def parse_position(text: str) -> tuple[str, int, int] | None:
    """``"X:18622959-18622959"`` -> ``("X", 18622959, 18622959)``; an empty cell is ``None``."""
    if text == "":
        return None
    match = _POSITION_RE.match(text)
    if not match:
        raise RowRejected(MALFORMED, f"unparsable position {text!r}")
    return match.group(1), int(match.group(2)), int(match.group(3))


def iter_cosmic_rows(path: str) -> Iterator[CosmicRow]:
    """Stream the export, resolving the columns by header name. The file has no quoting, so a plain split is
    both correct and much faster than the csv module over 5.8M rows."""
    with gzip.open(path, "rt", encoding="utf-8", newline="") as handle:
        header = handle.readline().rstrip("\r\n").split("\t")
        index = {name: i for i, name in enumerate(header)}
        needed = [POSITION_COLUMN, REF_COLUMN, ALT_COLUMN, *PASSTHROUGH_COLUMNS.values()]
        missing = [name for name in needed if name not in index]
        if missing:
            raise CosmicNormalizationError(f"COSMIC export is missing columns {missing}; header: {header}")
        position_idx, ref_idx, alt_idx = index[POSITION_COLUMN], index[REF_COLUMN], index[ALT_COLUMN]
        passthrough_idx = [index[name] for name in PASSTHROUGH_COLUMNS.values()]
        width = len(header)
        for row_number, line in enumerate(handle, start=1):
            fields = line.rstrip("\r\n").split("\t")
            if len(fields) != width:
                raise CosmicNormalizationError(
                    f"row {row_number} has {len(fields)} columns, header has {width}: corrupt export"
                )
            yield CosmicRow(
                row_number=row_number,
                position=fields[position_idx].strip(),
                ref=fields[ref_idx].strip().upper(),
                alt=fields[alt_idx].strip().upper(),
                values=tuple(fields[i].strip() for i in passthrough_idx),
            )


def to_vcf_record(row: CosmicRow, contig_map: ContigMap, fasta: FaiReader) -> VcfRecord:
    """Rebuild one COSMIC row as a VCF record, anchoring indels on the reference base (see the module doc).

    Raises ``RowRejected`` for rows that cannot be keyed: no GRCh38 position, a contig the FASTA lacks, a
    span that disagrees with the allele, a non-ACGT allele, or a reference allele the FASTA contradicts.
    """
    parsed = parse_position(row.position)
    if parsed is None:
        raise RowRejected(NO_POSITION)
    chrom, start, end = parsed
    contig = contig_map.cosmic_to_fasta.get(chrom)
    if contig is None:
        raise RowRejected(UNMAPPED_CONTIG, chrom)
    ref, alt = row.ref, row.alt
    if ref and alt:
        # SNV, MNV or delins: already anchored on real bases; bcftools trims any shared prefix/suffix.
        if len(ref) != end - start + 1:
            raise RowRejected(MALFORMED, f"ref {ref!r} does not span {row.position}")
        expected = fasta.fetch(contig, start, end)
        if expected != ref:
            raise RowRejected(REF_MISMATCH, f"{row.position} ref {ref!r}, reference has {expected!r}")
        record = VcfRecord(contig, start, ref, alt)
    elif ref:
        # Deletion of [start, end]: anchor on the base before it.
        if len(ref) != end - start + 1:
            raise RowRejected(MALFORMED, f"deleted {ref!r} does not span {row.position}")
        if start < 2:
            raise RowRejected(MALFORMED, f"deletion at {row.position} has no base to anchor on")
        anchored = fasta.fetch(contig, start - 1, end)
        if anchored[1:] != ref:
            raise RowRejected(REF_MISMATCH, f"{row.position} deleted {ref!r}, reference has {anchored[1:]!r}")
        record = VcfRecord(contig, start - 1, anchored, anchored[0])
    elif alt:
        # Insertion between two flanking bases: COSMIC spans them as S-(S+1) (once seen reversed, S-(S-1)).
        if abs(end - start) != 1:
            raise RowRejected(MALFORMED, f"insertion span {row.position} is not two flanking bases")
        anchor_pos = min(start, end)
        anchor = fasta.fetch(contig, anchor_pos, anchor_pos)
        record = VcfRecord(contig, anchor_pos, anchor, anchor + alt)
    else:
        raise RowRejected(MALFORMED, "both alleles empty")
    if not _ALLELE_RE.match(record.ref) or not _ALLELE_RE.match(record.alt):
        raise RowRejected(INVALID_ALLELE, f"{record.ref}>{record.alt}")
    return record


def write_vcf(
    rows: Iterable[CosmicRow], contig_map: ContigMap, fasta: FaiReader, vcf_path: str, counts: Counter
) -> int:
    """Write every convertible row as a sites-only VCF record whose ID is the input row number. Returns the
    number of records written; rejections are tallied in ``counts`` (and unmapped contigs by name)."""
    mapped = set(contig_map.cosmic_to_fasta.values())
    written = 0
    with open(vcf_path, "w") as out:
        out.write("##fileformat=VCFv4.2\n")
        for name, entry in fasta.contigs.items():
            if name in mapped:
                out.write(f"##contig=<ID={name},length={entry.length}>\n")
        out.write("#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n")
        for row in rows:
            counts["rows_in"] += 1
            try:
                record = to_vcf_record(row, contig_map, fasta)
            except RowRejected as rejected:
                counts[rejected.reason] += 1
                if rejected.reason == UNMAPPED_CONTIG:
                    counts[f"contig:{row.position.split(':', 1)[0]}"] += 1
                elif rejected.reason == REF_MISMATCH and counts[REF_MISMATCH] <= 20:
                    logger.warning("row %d rejected: %s", row.row_number, rejected)
                continue
            out.write(f"{record.contig}\t{record.pos}\t{row.row_number}\t{record.ref}\t{record.alt}\t.\t.\t.\n")
            written += 1
    return written


# --- bcftools ----------------------------------------------------------------------------------------------


def parse_bcftools_stderr(text: str) -> BcftoolsStats:
    """Summarize ``bcftools norm`` stderr: the ``REF_MISMATCH`` lines from ``--check-ref w`` and the closing
    ``Lines total/.../skipped: n/n/.../n`` line, parsed label-driven so 1.16 and 1.21 both work."""
    stats = BcftoolsStats()
    for line in text.splitlines():
        if line.startswith(_REF_MISMATCH_PREFIX):
            stats.ref_mismatches += 1
            if len(stats.first_ref_mismatches) < 20:
                stats.first_ref_mismatches.append(line)
    match = _BCFTOOLS_LINES_RE.search(text)
    if match:
        labels, values = match.group(1).split("/"), match.group(2).split("/")
        stats.lines = dict(zip(labels, map(int, values), strict=False))
    return stats


def _run(argv: list[str], stderr_path: str, env: dict | None = None) -> None:
    """Run a tool with its stderr appended to a file (never captured in memory: a wrong FASTA could make
    bcftools print millions of lines); on failure, re-raise with the tail of that file."""
    with open(stderr_path, "a") as err:
        try:
            subprocess.run(argv, check=True, stdout=subprocess.DEVNULL, stderr=err, env=env)
        except subprocess.CalledProcessError as e:
            with open(stderr_path) as handle:
                tail = handle.read().splitlines()[-50:]
            raise CosmicNormalizationError(f"{argv[0]} failed ({e.returncode}): " + "\n".join(tail)) from e


def run_bcftools_norm(vcf_path: str, fasta_path: str, workdir: str) -> tuple[str, BcftoolsStats]:
    """Left-align and trim the records against the reference, then hand back a TSV sorted by ID.

    ``bcftools norm`` buffers realigned records in a position-sorted window (``--site-win``), so its output
    order is *not* the input order; the ID sort is what makes the merge-back a lockstep walk. ``--check-ref
    wx`` reports and excludes any record whose REF still disagrees with the FASTA (expected 0 after
    `to_vcf_record`'s own check); ``records_out`` counts what survived.
    """
    for tool in ("bcftools", "sort"):
        if shutil.which(tool) is None:
            raise CosmicNormalizationError(f"{tool} is not on PATH")
    bcf_path = os.path.join(workdir, "normalized.bcf")
    tsv_path = os.path.join(workdir, "normalized.tsv")
    sorted_path = os.path.join(workdir, "normalized.sorted.tsv")
    stderr_path = os.path.join(workdir, "bcftools.stderr")
    open(stderr_path, "w").close()
    _run(
        [
            "bcftools",
            "norm",
            "--fasta-ref",
            fasta_path,
            "--check-ref",
            "wx",
            "--no-version",
            "-Ou",
            "-o",
            bcf_path,
            vcf_path,
        ],
        stderr_path=stderr_path,
    )
    with open(stderr_path) as handle:
        stats = parse_bcftools_stderr(handle.read())
    _run(
        ["bcftools", "query", "-f", "%ID\t%CHROM\t%POS\t%REF\t%ALT\n", "-o", tsv_path, bcf_path],
        stderr_path=stderr_path,
    )
    os.remove(bcf_path)
    _run(
        ["sort", "-t", "\t", "-k1,1n", "-S", "512M", "-T", workdir, "-o", sorted_path, tsv_path],
        stderr_path=stderr_path,
        env={**os.environ, "LC_ALL": "C"},
    )
    os.remove(tsv_path)
    with open(sorted_path, "rb") as handle:
        stats.records_out = sum(chunk.count(b"\n") for chunk in iter(lambda: handle.read(1 << 20), b""))
    return sorted_path, stats


def iter_normalized(path: str) -> Iterator[NormalizedRecord]:
    with open(path) as handle:
        for line in handle:
            row_number, contig, pos, ref, alt = line.rstrip("\n").split("\t")
            yield NormalizedRecord(int(row_number), contig, int(pos), ref, alt)


# --- merge back and output -------------------------------------------------------------------------------


def merge_back(
    rows: Iterable[CosmicRow], normalized: Iterable[NormalizedRecord], contig_map: ContigMap
) -> Iterator[list[str]]:
    """Walk the original rows and the ID-sorted normalized records together. A row absent from the
    normalized stream was dropped upstream; a normalized ID that never meets its row means the two streams
    diverged, which is an error rather than something to paper over."""
    pending_iter = iter(normalized)
    pending = next(pending_iter, None)
    for row in rows:
        if pending is None:
            return
        if pending.row_number < row.row_number:
            raise CosmicNormalizationError(f"normalized ID {pending.row_number} has no source row")
        if pending.row_number > row.row_number:
            continue
        chromosome = contig_map.to_radiant(pending.contig)
        _, locus_hash = locus_and_hash(chromosome, pending.pos, pending.ref, pending.alt)
        yield [chromosome, str(pending.pos), pending.ref, pending.alt, locus_hash, *row.values]
        pending = next(pending_iter, None)
    if pending is not None:
        raise CosmicNormalizationError(f"normalized ID {pending.row_number} is past the last source row")


def write_output(out_rows: Iterable[list[str]], path: str) -> int:
    """Gzipped TSV with a header row, the shape ``cosmic_mutation_set_load.sql`` reads by position."""
    written = 0
    with gzip.open(path, "wt", encoding="utf-8", compresslevel=6, newline="") as out:
        out.write("\t".join(OUTPUT_COLUMNS) + "\n")
        for fields in out_rows:
            for value in fields:
                if "\t" in value or "\n" in value:
                    raise CosmicNormalizationError(f"field contains a delimiter: {value!r}")
            out.write("\t".join(fields) + "\n")
            written += 1
    return written


def normalize_cosmic_mutation_set(
    cmc_export_s3_uri: str, reference_fasta_s3_uri: str, output_s3_uri: str, workdir: str | None = None
) -> dict:
    """Download the export and the reference, normalize, upload the result; returns the summary as a dict
    (JSON-serializable, for the task log and XCom)."""
    summary = CosmicNormalizationSummary(output_s3_uri=output_s3_uri)
    with tempfile.TemporaryDirectory(dir=workdir) as tmpdir:
        logger.info("Downloading %s and %s", cmc_export_s3_uri, reference_fasta_s3_uri)
        tsv_path = download_s3_file(cmc_export_s3_uri, tmpdir)
        fasta_path = download_s3_file(reference_fasta_s3_uri, tmpdir)
        download_s3_file(f"{reference_fasta_s3_uri}.fai", tmpdir)

        counts: Counter = Counter()
        vcf_path = os.path.join(tmpdir, "cosmic.vcf")
        with FaiReader(fasta_path) as fasta:
            contig_map = ContigMap.build(fasta.contigs)
            logger.info("Contig mapping: %s", contig_map.cosmic_to_fasta)
            written = write_vcf(iter_cosmic_rows(tsv_path), contig_map, fasta, vcf_path, counts)
        logger.info("Wrote %d VCF records from %d rows (%s)", written, counts["rows_in"], dict(counts))

        sorted_path, stats = run_bcftools_norm(vcf_path, fasta_path, tmpdir)
        os.remove(vcf_path)
        logger.info("bcftools norm: %s, %d records out", stats.lines, stats.records_out)
        for line in stats.first_ref_mismatches:
            logger.warning("bcftools %s", line)

        output_path = os.path.join(tmpdir, "cosmic_mutation_set.normalized.tsv.gz")
        rows_out = write_output(
            merge_back(iter_cosmic_rows(tsv_path), iter_normalized(sorted_path), contig_map), output_path
        )

        summary.rows_in = counts["rows_in"]
        for reason in (NO_POSITION, UNMAPPED_CONTIG, MALFORMED, INVALID_ALLELE, REF_MISMATCH):
            setattr(summary, reason, counts[reason])
        summary.bcftools_excluded = stats.excluded
        summary.bcftools_lines = stats.lines
        summary.rows_out = rows_out
        summary.dropped_contigs = {k.removeprefix("contig:"): v for k, v in counts.items() if k.startswith("contig:")}
        summary.check()

        logger.info("Uploading %d rows to %s", rows_out, output_s3_uri)
        upload_s3_file(output_path, output_s3_uri)
    logger.info("Summary: %s", asdict(summary))
    return asdict(summary)
