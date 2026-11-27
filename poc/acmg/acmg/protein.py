"""ClinVar missense variants by protein position on the MANE Select transcripts, for PS1 and PM5.

`python -m acmg.protein clinvar_protein.parquet` reads ClinVar's hgvs4variation (~540 MB) and the MANE summary
(public, NCBI) and writes the small file to copy under s3://<bucket>/clinvar_protein/.
"""

import gzip
import io
import re
import sys
import urllib.request

import polars as pl

FILE = "clinvar_protein/clinvar_protein.parquet"
SCHEMA = {
    "transcript_id": pl.String, "variation_id": pl.String, "position": pl.Int32, "ref_aa": pl.String,
    "alt_aa": pl.String,
}  # fmt: skip
HGVS = "https://ftp.ncbi.nlm.nih.gov/pub/clinvar/tab_delimited/hgvs4variation.txt.gz"
# The MANE release of Radiant's VEP (Ensembl 114): another one can pick another transcript or protein version.
MANE = "https://ftp.ncbi.nlm.nih.gov/refseq/MANE/MANE_human/release_1.4/MANE.GRCh38.v1.4.summary.txt.gz"
MISSENSE = re.compile(r"p\.([A-Z][a-z]{2})(\d+)([A-Z][a-z]{2})")


def mane_proteins(lines) -> dict[str, str]:
    """MANE Select RefSeq protein (versioned) -> Ensembl transcript without version, as Radiant stores it."""
    out = {}
    for line in lines:
        f = line.rstrip("\n").split("\t")
        if line.startswith("#") or len(f) < 10 or f[9] != "MANE Select":
            continue
        out[f[6]] = f[7].split(".")[0]
    return out


def missense(lines, proteins: dict[str, str]) -> pl.DataFrame:
    """One row per ClinVar variation and MANE Select missense change (same protein: the RefSeq and Ensembl MANE
    transcripts code it identically)."""
    rows = set()
    for line in lines:
        if line.startswith("#"):
            continue
        f = line.rstrip("\n").split("\t")
        if len(f) < 10 or f[8] == "-":
            continue
        accession, _, change = f[8].partition(":")
        m = MISSENSE.fullmatch(change)
        if accession in proteins and m and m.group(1) != m.group(3) and m.group(3) != "Ter":
            rows.add((proteins[accession], f[2], int(m.group(2)), m.group(1), m.group(3)))
    return pl.DataFrame(sorted(rows), schema=SCHEMA, orient="row")


def _lines(url: str):
    request = urllib.request.Request(url, headers={"User-Agent": "acmg-poc"})
    response = urllib.request.urlopen(request, timeout=600)
    return io.TextIOWrapper(gzip.GzipFile(fileobj=response), encoding="utf-8")


if __name__ == "__main__":
    with _lines(MANE) as mane, _lines(HGVS) as hgvs:
        missense(hgvs, mane_proteins(mane)).write_parquet(sys.argv[1])
