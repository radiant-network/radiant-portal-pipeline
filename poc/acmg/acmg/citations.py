"""PubMed articles ClinVar cites for each variation, from var_citations.txt under s3://<bucket>/clinvar_citations/.
They are refs of the criteria that rest on ClinVar and a published flag of the priority: never points.
"""

import io

import polars as pl

FILE = "clinvar_citations/var_citations.txt"
SCHEMA = {"variation_id": pl.String, "pmids": pl.List(pl.Int64)}


def variations(text: str) -> pl.DataFrame:
    """variation_id, pmids: the distinct PubMed ids, ascending. Other sources (PubMed Central, books) are left out."""
    rows = pl.read_csv(
        io.StringIO(text), separator="\t", columns=["VariationID", "citation_source", "citation_id"],
        infer_schema=False, quote_char=None,
    )  # fmt: skip
    return (
        rows.filter(pl.col("citation_source") == "PubMed")
        .group_by(variation_id="VariationID")
        .agg(pmids=pl.col("citation_id").cast(pl.Int64).unique().sort())
        .select(list(SCHEMA))
        .cast(SCHEMA)
    )
