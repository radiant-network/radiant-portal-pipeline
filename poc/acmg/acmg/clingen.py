"""ClinGen facts per gene, from the two ClinGen downloads copied under s3://<bucket>/clingen/ (see README)."""

import csv
import io

import polars as pl

DOSAGE = "clingen/ClinGen_gene_curation_list_GRCh38.tsv"
VALIDITY = "clingen/gene-validity.csv"
NO_DISEASE = {"Disputed", "Refuted", "No Known Disease Relationship"}
SCHEMA = {"symbol": pl.String, "hi_score": pl.Int32, "no_disease": pl.Boolean}


def dosage(text: str) -> pl.DataFrame:
    """symbol, hi_score: 0-3, 30 (autosomal recessive), 40 (dosage sensitivity unlikely), null if not evaluated."""
    rows = [line.split("\t") for line in text.splitlines() if line and not line.startswith("#")]
    return pl.DataFrame({"symbol": [r[0] for r in rows], "hi_score": [r[4] for r in rows]}).with_columns(
        pl.col("hi_score").cast(pl.Int32, strict=False)
    )


def validity(text: str) -> pl.DataFrame:
    """symbol, no_disease: true when every gene-disease curation of the gene says there is no disease."""
    rows = list(csv.reader(io.StringIO(text)))
    # A title block, then the header between two "+++" lines.
    start = next(i for i, r in enumerate(rows) if r and r[0] == "GENE SYMBOL") + 2
    data = [(r[0], r[6]) for r in rows[start:] if r]
    return (
        pl.DataFrame(data, schema=["symbol", "classification"], orient="row")
        .group_by("symbol")
        .agg(no_disease=pl.col("classification").is_in(list(NO_DISEASE)).all())
    )


def genes(dosage_text: str, validity_text: str) -> pl.DataFrame:
    return (
        dosage(dosage_text)
        .join(validity(validity_text), on="symbol", how="full", coalesce=True)
        .with_columns(pl.col("no_disease").fill_null(False))
        .unique("symbol")
        .select(list(SCHEMA))
        .cast(SCHEMA)
    )
