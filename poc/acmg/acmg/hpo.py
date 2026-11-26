"""Phenotype similarity of a patient to each gene, from the two HPO downloads copied under s3://<bucket>/hpo/ (see
README): Resnik, best-match average from the patient's terms to the gene's (Köhler 2009, Phenomizer)."""

import math

import polars as pl
from acmg.models import Pp4Rule

ONTOLOGY = "hpo/hp.obo"
GENES = "hpo/genes_to_phenotype.txt"
# Only phenotypic abnormalities: the other branches (inheritance, onset, frequency) describe no patient feature.
PHENOTYPIC_ABNORMALITY = "HP:0000118"


def ontology(text: str) -> tuple[pl.DataFrame, pl.DataFrame]:
    """(term, ancestor) of every phenotypic abnormality, itself included; (alt_id, term) of its secondary ids."""
    parents, alt_ids, term, obsolete = {}, [], None, False
    for line in [*text.splitlines(), "[Term]"]:
        if line.startswith("["):
            if term and not obsolete:
                parents.setdefault(term, [])
            term, obsolete = None, False
        elif line.startswith("id: "):
            term = line[4:].strip()
        elif line.startswith("is_a: ") and term:
            parents.setdefault(term, []).append(line[6:].split("!")[0].strip())
        elif line.startswith("alt_id: ") and term:
            alt_ids.append((line[8:].strip(), term))
        elif line.strip() == "is_obsolete: true":
            obsolete = True
            parents.pop(term, None)

    ancestors: dict[str, set[str]] = {}

    def up(t: str) -> set[str]:
        if t not in ancestors:
            ancestors[t] = {t}.union(*(up(p) for p in parents.get(t, []) if p in parents))
        return ancestors[t]

    phenotypic = {t for t in parents if PHENOTYPIC_ABNORMALITY in up(t)}
    pairs = [(t, a) for t in phenotypic for a in up(t) if a in phenotypic]
    closure = pl.DataFrame(pairs, schema=["term", "ancestor"], orient="row")
    alt = pl.DataFrame(alt_ids, schema=["alt_id", "term"], orient="row").filter(pl.col("term").is_in(list(parents)))
    return closure, alt


def annotations(text: str) -> pl.DataFrame:
    """symbol, disease_id, hpo_id: the HPO terms of each disease of a gene."""
    return (
        pl.read_csv(text.encode(), separator="\t", infer_schema=False)
        .select(symbol="gene_symbol", disease_id="disease_id", hpo_id="hpo_id")
        .unique()
    )


def gene_terms(closure: pl.DataFrame, genes: pl.DataFrame) -> pl.DataFrame:
    """symbol, term, ic: each gene's terms with their ancestors, and their information content over the genes."""
    closed = (
        genes.select("symbol", "hpo_id")
        .unique()
        .join(closure, left_on="hpo_id", right_on="term")
        .select("symbol", term="ancestor")
        .unique()
    )
    n = closed["symbol"].n_unique()
    ic = closed.group_by("term").agg(ic=-(pl.len() / n).log())
    return closed.join(ic, on="term")


def normalize(patients: pl.DataFrame, alt: pl.DataFrame) -> pl.DataFrame:
    """Secondary ids to their primary term."""
    return (
        patients.join(alt, left_on="hpo_id", right_on="alt_id", how="left")
        .with_columns(hpo_id=pl.coalesce("term", "hpo_id"))
        .drop("term")
        .unique()
    )


def similarity(patients: pl.DataFrame, closure: pl.DataFrame, terms: pl.DataFrame) -> pl.DataFrame:
    """case, symbol, n_terms, score, top: `patients` (case, hpo_id) against every annotated gene. score = mean over the
    patient's terms of the best IC shared with the gene; top = share of the annotated genes scoring at least as
    high for this case (1 / n for the best gene). Genes sharing nothing informative are left out (top 1)."""
    n_genes = terms["symbol"].n_unique()
    patient = patients.join(closure, left_on="hpo_id", right_on="term")
    n_terms = patient.group_by("case").agg(n_terms=pl.col("hpo_id").n_unique())
    best = (
        patient.join(terms.filter(pl.col("ic") > 0), left_on="ancestor", right_on="term")
        .group_by("case", "hpo_id", "symbol")
        .agg(pl.col("ic").max())
    )
    return (
        best.group_by("case", "symbol")
        .agg(total=pl.col("ic").sum())
        .join(n_terms, on="case")
        .select("case", "symbol", "n_terms", score=pl.col("total") / pl.col("n_terms"))
        .with_columns(top=pl.col("score").rank("max", descending=True).over("case") / n_genes)
    )


def pp4_points(rule: Pp4Rule) -> pl.Expr:
    """PP4 points from `top`, with the table of the case's `n_terms`."""
    expr = pl.lit(0, pl.Int64)
    for table in rule.tables:
        points = pl.lit(0, pl.Int64)
        # Widest bin first: a narrower bin the gene falls in overrides it.
        for bound, p in sorted(table.bins.items(), reverse=True):
            points = pl.when(pl.col("top") <= bound).then(pl.lit(p, pl.Int64)).otherwise(points)
        expr = pl.when(pl.col("n_terms") >= table.min_terms).then(points).otherwise(expr)
    return expr


SUPPORTING_LR = 350 ** (1 / 8)


def points(likelihood_ratio: float) -> int:
    """Bayesian points of a likelihood ratio (Tavtigian 2018: Supporting = 350^(1/8)), rounded down."""
    return math.floor(math.log(likelihood_ratio) / math.log(SUPPORTING_LR)) if likelihood_ratio > 0 else 0
