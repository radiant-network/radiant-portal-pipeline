"""Ranking benchmark of the phenotype score (no clinical truth needed): a ClinVar P/LP variant already classified is
spiked into the classified occurrences of a random proband, with a phenotype drawn from one of its gene's diseases
(some terms made vaguer, some noise terms added). Half the simulations calibrate PP4 points per phenotype bin
(likelihood ratio -> Bayesian points), the other half measure the spike's rank with and without them.

Reads the output of a classification run (--output-db) and the HPO files under s3://<bucket>/hpo/. Writes nothing.
Prints aggregates only: bins, ranks, and how many probands of the tenant have HPO terms.
"""

import argparse
import json
import random
import re

import polars as pl
from acmg import hpo
from acmg.db import HERE, IDENTIFIERS, connect
from acmg.exchange import filesystem, s3_settings
from acmg.models import Ruleset

# `top` bins (share of annotated genes scoring at least as high): upper bounds, best first.
BINS = [0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 1.0]
PLP = "['Pathogenic', 'Likely_pathogenic', 'Pathogenic/Likely_pathogenic']"
NOT_PLP = "['Benign', 'Likely_benign', 'Benign/Likely_benign', 'Uncertain_significance', " \
          "'Conflicting_classifications_of_pathogenicity', 'Conflicting_interpretations_of_pathogenicity']"
FIT_PENALTIES = (2, 4, 8)
# Criteria that read ClinVar at the gene or window level, the spike itself included.
CLINVAR_CRITERIA = ("PM1", "PP2")


def query(sql: str) -> pl.DataFrame:
    with connect() as conn, conn.cursor() as cur:
        cur.execute(sql)
        columns = [d[0] for d in cur.description]
        return pl.DataFrame(cur.fetchall(), schema=columns, orient="row", infer_schema_length=None)


def spikes(args) -> pl.DataFrame:
    """locus_id, symbol, acmg_points, acmg_criteria of the classified ClinVar P/LP variants (no conflict)."""
    return query(f"""
        WITH reviewed AS (
            SELECT locus_id FROM {args.shared_db}.clinvar_rcv_summary
            GROUP BY locus_id HAVING max(review_status_stars) >= {int(args.min_stars)}
        ),
        plp AS (
            SELECT locus_id FROM {args.shared_db}.clinvar
            WHERE arrays_overlap(clin_sig, {PLP}) AND NOT arrays_overlap(clin_sig, {NOT_PLP})
                AND locus_id IN (SELECT locus_id FROM reviewed)
        )
        SELECT c.locus_id, min(k.symbol) AS symbol, max(c.acmg_points) AS acmg_points,
               min(array_join(c.acmg_criteria, ',')) AS acmg_criteria
        FROM {args.output_db}.acmg_classification c
        JOIN {args.shared_db}.snv__consequence k ON k.locus_id = c.locus_id AND k.transcript_id = c.transcript_id
        WHERE c.locus_id IN (SELECT locus_id FROM plp)
        GROUP BY c.locus_id
        ORDER BY c.locus_id""")


def background(args, rng: random.Random) -> pl.DataFrame:
    """seq_id, locus_id, symbol, acmg_points, acmg_criteria (comma-joined), inheritance_penalized of every classified
    occurrence of n random probands (the last from the inheritance fit of the priority step)."""
    seq_ids = query(f"SELECT DISTINCT seq_id FROM {args.output_db}.acmg_classification")["seq_id"].to_list()
    chosen = rng.sample(sorted(seq_ids), min(args.n_background, len(seq_ids)))
    return query(f"""
        SELECT c.seq_id, c.locus_id, k.symbol, c.acmg_points, array_join(c.acmg_criteria, ',') AS acmg_criteria,
               coalesce(p.inheritance_points, 0) < 0 AS inheritance_penalized
        FROM {args.output_db}.acmg_classification c
        JOIN {args.shared_db}.snv__consequence k ON k.locus_id = c.locus_id AND k.transcript_id = c.transcript_id
        LEFT JOIN {args.output_db}.acmg_priority p
            ON p.seq_id = c.seq_id AND p.locus_id = c.locus_id AND p.transcript_id = c.transcript_id
        WHERE c.seq_id IN ({', '.join(str(int(s)) for s in chosen)})""").with_columns(
        pl.col("inheritance_penalized").cast(pl.Boolean)
    )


def hpo_coverage(args, closure: pl.DataFrame, alt: pl.DataFrame) -> dict:
    """Affected probands of the tenant, and how many have observed phenotypic HPO terms (not only, say, the
    inheritance root HP:0000005)."""
    probands = f"""
        SELECT DISTINCT se.case_id, se.patient_id, se.seq_id
        FROM {args.shared_db}.staging_sequencing_experiment se
        WHERE se.analysis_type = 'germline' AND se.tenant_code = '{args.tenant_code}' AND NOT se.deleted
            AND se.family_role = 'proband' AND se.affected_status = 'affected'"""
    total = query(f"SELECT count(DISTINCT seq_id) AS n FROM ({probands}) p")["n"][0]
    terms = query(f"""
        SELECT DISTINCT p.seq_id AS `case`, oc.code_value AS hpo_id
        FROM ({probands}) p
        JOIN {args.clinical_db}.obs_categorical oc ON oc.case_id = p.case_id AND oc.patient_id = p.patient_id
        WHERE oc.observation_code = 'phenotype' AND oc.coding_system = 'HPO' AND oc.interpretation_code = 'positive'
    """)
    per_case = (
        hpo.normalize(terms, alt)
        .join(closure.select("term").unique(), left_on="hpo_id", right_on="term")
        .group_by("case")
        .agg(n=pl.col("hpo_id").n_unique())["n"]
    )
    return {
        "probands": int(total),
        "with_any_hpo": terms["case"].n_unique() if terms.height else 0,
        "with_phenotypic_hpo": per_case.len(),
        "median_terms": float(per_case.median()) if per_case.len() else None,
        "p90_terms": float(per_case.quantile(0.9)) if per_case.len() else None,
    }


def simulated_phenotype(
    rng: random.Random, disease: list[str], ancestors: dict[str, list[str]], pool: list[str], args
) -> list[str]:
    """Some of the disease's terms, each made vaguer (a random ancestor) with probability `imprecision`, and
    `n_noise` random annotated terms."""
    terms = rng.sample(disease, min(args.n_terms, len(disease)))
    terms = [rng.choice(ancestors[t]) if ancestors.get(t) and rng.random() < args.imprecision else t for t in terms]
    return terms + rng.sample(pool, args.n_noise)


def pp4_blocked(criteria: pl.Expr, blocked: list[str]) -> pl.Expr:
    """True when one of the comma-joined codes is a blocked criterion, at any strength (BP4_Moderate is BP4)."""
    codes = criteria.fill_null("").str.split(",")
    return codes.list.eval(pl.element().str.split("_").list.first().is_in(blocked)).list.any()


def spike_rank(values: pl.Series, is_spike: pl.Series) -> float:
    """1-based rank of the spike by descending value, ties counted half."""
    spike = values.filter(is_spike)[0]
    others = values.filter(~is_spike)
    return 1 + (others > spike).sum() + (others == spike).sum() / 2


def simulate(
    cases: list[dict], back: pl.DataFrame, closure: pl.DataFrame, terms: pl.DataFrame, chunk: int = 50
) -> pl.DataFrame:
    """One row per occurrence of each simulated case (sim, symbol, acmg_points, is_spike, top), its spike included.
    `cases`: sim, seq_id, phenotype (terms), locus_id, symbol, acmg_points of the spike."""
    out, per_seq = [], back.partition_by("seq_id", as_dict=True)
    for start in range(0, len(cases), chunk):
        batch = cases[start : start + chunk]
        patients = pl.DataFrame(
            [(c["sim"], t) for c in batch for t in c["phenotype"]], schema=["case", "hpo_id"], orient="row"
        ).unique()
        sim = hpo.similarity(patients, closure, terms).select(sim="case", symbol="symbol", top="top")
        rows = pl.concat(
            [
                per_seq[(c["seq_id"],)]
                .filter(pl.col("locus_id") != c["locus_id"])
                .select(
                    "symbol",
                    pl.col("acmg_points").cast(pl.Int64),
                    "inheritance_penalized",
                    "pp4_blocked",
                    sim=pl.lit(c["sim"], pl.Int64),
                    is_spike=pl.lit(False),
                )
                .vstack(
                    # The spike is causal: its genotype fits the gene's inheritance.
                    pl.DataFrame(
                        {"symbol": [c["symbol"]], "acmg_points": [c["acmg_points"]], "inheritance_penalized": [False],
                         "pp4_blocked": [c["pp4_blocked"]], "sim": [c["sim"]], "is_spike": [True]},
                        schema={"symbol": pl.String, "acmg_points": pl.Int64, "inheritance_penalized": pl.Boolean,
                                "pp4_blocked": pl.Boolean, "sim": pl.Int64, "is_spike": pl.Boolean},
                    )
                )
                for c in batch
            ]
        )
        out.append(rows.join(sim, on=["sim", "symbol"], how="left").with_columns(pl.col("top").fill_null(1.0)))
    return pl.concat(out)


def binned(rows: pl.DataFrame) -> pl.DataFrame:
    return rows.with_columns(bin=pl.col("top").cut(BINS[:-1], labels=[str(b) for b in BINS], left_closed=False))


def calibrate(rows: pl.DataFrame, max_points: int) -> pl.DataFrame:
    """bin, spikes, background, likelihood ratio, PP4 points (clamped to 0..max_points). Counts get +0.5 so an
    empty bin keeps a finite ratio."""
    counts = (
        binned(rows)
        .with_columns(pl.col("bin").cast(pl.String))
        .group_by("bin")
        .agg(spikes=pl.col("is_spike").sum(), background=(~pl.col("is_spike")).sum())
    )
    table = (
        pl.DataFrame({"bin": [str(b) for b in BINS]})
        .join(counts, on="bin", how="left")
        .with_columns(pl.col("spikes", "background").fill_null(0))
    )
    share = lambda c: (pl.col(c) + 0.5) / (pl.col(c).sum() + 0.5 * len(BINS))  # noqa: E731
    return table.with_columns(lr=share("spikes") / share("background")).with_columns(
        pp4_points=pl.col("lr").map_elements(lambda lr: min(max(hpo.points(lr), 0), max_points), pl.Int64)
    )


def evaluate(rows: pl.DataFrame, table: pl.DataFrame) -> dict:
    """Spike ranks (top 1, 5, 10, median) by ACMG points, phenotype, ACMG points + PP4 (not where `pp4_blocked`),
    the latter with each inheritance-fit penalty of FIT_PENALTIES, and with -4 but PP4 on every variant (`_ungated`)."""
    scored = binned(rows).with_columns(pl.col("bin").cast(pl.String)).join(table.select("bin", "pp4_points"), on="bin")
    penalty = pl.col("inheritance_penalized").cast(pl.Float64)
    gated = pl.when(pl.col("pp4_blocked")).then(0).otherwise(pl.col("pp4_points"))
    scored = scored.with_columns(
        acmg=pl.col("acmg_points").cast(pl.Float64),
        phenotype=-pl.col("top"),
        acmg_pp4=(pl.col("acmg_points") + gated).cast(pl.Float64),
        acmg_pp4_fit4_ungated=(pl.col("acmg_points") + pl.col("pp4_points")).cast(pl.Float64) - penalty * 4,
    ).with_columns((pl.col("acmg_pp4") - penalty * p).alias(f"acmg_pp4_fit{p}") for p in FIT_PENALTIES)
    names = ("acmg", "phenotype", "acmg_pp4", *(f"acmg_pp4_fit{p}" for p in FIT_PENALTIES), "acmg_pp4_fit4_ungated")
    ranks = {name: [] for name in names}
    for _, group in scored.group_by("sim"):
        for name in ranks:
            ranks[name].append(spike_rank(group[name], group["is_spike"]))
    report = {}
    for name, values in ranks.items():
        s = pl.Series(values)
        report[name] = {f"top{k}": round(float((s <= k).mean()), 3) for k in (1, 5, 10, 50)} | {
            "median_rank": float(s.median())
        }
    return report


def run(args) -> dict:
    for key in ("shared_db", "output_db", "tenant_code"):
        if not IDENTIFIERS[key].fullmatch(getattr(args, key)):
            raise ValueError(f"invalid {key}: {getattr(args, key)!r}")
    if not re.fullmatch(r"\w+\.\w+", args.clinical_db):
        raise ValueError(f"invalid clinical db: {args.clinical_db!r}")
    rng = random.Random(args.seed)
    s3, bucket = filesystem(), s3_settings()["bucket"]

    def read(key: str) -> str:
        with s3.open_input_stream(f"{bucket}/{key}") as f:
            return f.read().decode()

    closure, alt = hpo.ontology(read(hpo.ONTOLOGY))
    genes = hpo.annotations(read(hpo.GENES))
    terms = hpo.gene_terms(closure, genes)
    # Every list the rng draws from is sorted: group_by and set order change between runs.
    diseases = (
        genes.join(closure.select("term").unique(), left_on="hpo_id", right_on="term")
        .group_by("symbol", "disease_id")
        .agg(pl.col("hpo_id").sort())
        .sort("symbol", "disease_id")
    )
    by_gene = {}
    for r in diseases.iter_rows(named=True):
        by_gene.setdefault(r["symbol"], []).append(r["hpo_id"])
    ancestors = {
        r["term"]: [a for a in r["ancestor"] if a not in (r["term"], hpo.PHENOTYPIC_ABNORMALITY)]
        for r in closure.group_by("term").agg(pl.col("ancestor").sort()).iter_rows(named=True)
    }
    pool = sorted(genes.join(closure.select("term").unique(), left_on="hpo_id", right_on="term")["hpo_id"].unique())

    blocked = Ruleset.load(HERE / "ruleset.yaml").pp4.blocked_by
    candidates = spikes(args).filter(pl.col("symbol").is_in(list(by_gene)))
    candidates = candidates.with_columns(pp4_blocked=pp4_blocked(pl.col("acmg_criteria"), blocked))
    back = background(args, rng).with_columns(pp4_blocked=pp4_blocked(pl.col("acmg_criteria"), blocked))
    seq_ids = sorted(back["seq_id"].unique())
    picked = candidates.sample(min(args.n_sims, candidates.height), seed=args.seed).to_dicts()
    cases = [
        {
            "sim": i,
            "seq_id": rng.choice(seq_ids),
            "phenotype": simulated_phenotype(rng, rng.choice(by_gene[s["symbol"]]), ancestors, pool, args),
            **s,
        }
        for i, s in enumerate(picked)
    ]
    rows = simulate(cases, back, closure, terms)
    calibration, test = rows.filter(pl.col("sim") % 2 == 0), rows.filter(pl.col("sim") % 2 == 1)
    table = calibrate(calibration, args.max_pp4_points)
    no_clinvar = [c["sim"] for c in cases if not any(x in (c["acmg_criteria"] or "") for x in CLINVAR_CRITERIA)]
    return {
        "hpo_coverage": hpo_coverage(args, closure, alt),
        "spike_candidates": candidates.height,
        "simulations": len(cases),
        "background_probands": len(seq_ids),
        "background_occurrences_per_proband": round(back.height / max(len(seq_ids), 1)),
        "pp4_bins": table.with_columns(pl.col(pl.Float64).round(4)).to_dicts(),
        "ranks": evaluate(test, table),
        "ranks_without_clinvar_window_criteria": evaluate(
            test.filter(pl.col("sim").is_in(no_clinvar)), table
        ),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--output-db", default="acmg_poc_out", help="the classification run to read")
    parser.add_argument("--shared-db", default="acmg_poc")
    parser.add_argument("--tenant-code", default="acmg_poc")
    parser.add_argument("--clinical-db", default="radiant_jdbc.public", help="catalog.database of obs_categorical")
    parser.add_argument("--n-sims", type=int, default=1000)
    parser.add_argument("--n-background", type=int, default=100, help="probands drawn as backgrounds")
    parser.add_argument("--n-terms", type=int, default=5, help="disease terms given to the simulated patient")
    parser.add_argument("--n-noise", type=int, default=2, help="random terms added")
    parser.add_argument("--imprecision", type=float, default=0.3, help="chance a term is replaced by an ancestor")
    parser.add_argument("--min-stars", type=int, default=2, help="ClinVar review stars of the spiked variants")
    parser.add_argument("--max-pp4-points", type=int, default=4, help="PP4 cap (Strong)")
    parser.add_argument("--seed", type=int, default=7)
    print(json.dumps(run(parser.parse_args()), indent=2))


if __name__ == "__main__":
    main()
