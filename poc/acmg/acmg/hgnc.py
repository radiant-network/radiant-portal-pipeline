"""Current HGNC symbol of each gene symbol, from hgnc_complete_set.txt copied under s3://<bucket>/hgnc/ (see README).

Every source names genes at its own release (Ensembl 114, ClinVar, OMIM, ClinGen, HPO): joins go through the current
symbol so that a renamed gene still matches, whichever side is older.
"""

import io

import polars as pl

FILE = "hgnc/hgnc_complete_set.txt"
SCHEMA = {"symbol": pl.String, "current": pl.String}


def symbols(text: str) -> pl.DataFrame:
    """symbol, current: each current symbol to itself, each previous symbol to its gene's. A previous symbol that is
    some gene's current one, or that several genes had, is left out. Aliases are left out: genes share them."""
    genes = pl.read_csv(io.StringIO(text), separator="\t", columns=["symbol", "prev_symbol"], infer_schema=False)
    current = genes.select("symbol", current="symbol")
    previous = (
        genes.select(pl.col("prev_symbol").str.split("|").alias("old"), current="symbol")
        .explode("old")
        .filter(pl.col("old").is_not_null() & ~pl.col("old").is_in(genes["symbol"].implode()))
        .filter(pl.len().over("old") == 1)
        .select(symbol="old", current="current")
    )
    return pl.concat([current, previous]).unique("symbol").cast(SCHEMA)


def normalize(df: pl.DataFrame, mapping: pl.DataFrame, column: str = "symbol") -> pl.DataFrame:
    """`column` replaced by its current symbol; a symbol HGNC does not know stays as is."""
    m = mapping.rename({"symbol": column})
    return (
        df.join(m, on=column, how="left")
        .with_columns(pl.coalesce("current", column).alias(column))
        .drop("current")
    )
