"""ACMG criteria as Polars expressions over a facts frame (schema: models.FACT_SCHEMA).

Each criterion function returns an Evidence struct expression, null when the criterion is not met.
`evaluate` gives the criteria met at one level (variant or occurrence), `combine` classifies an occurrence
from all its criteria: exclusions and point caps apply there, across both levels.
"""

import polars as pl
from acmg.models import CLASSIFICATION_SCHEMA, EVIDENCE_DTYPE, AcmgClass, Ruleset, Strength, criterion_code

af, an = pl.col("gnomad_af"), pl.col("gnomad_an")
revel, spliceai, loeuf = pl.col("revel_score"), pl.col("spliceai_ds"), pl.col("gnomad_loeuf")


def _evidence(rs: Ruleset, criterion: str, strength: Strength, value: pl.Expr, threshold: float | None) -> pl.Expr:
    points = rs.points[strength] * (-1 if criterion.startswith("B") else 1)
    return pl.struct(
        code=pl.lit(criterion_code(criterion, strength)),
        strength=pl.lit(str(strength)),
        points=pl.lit(points, pl.Int64),
        value=value.cast(pl.Float64),
        threshold=pl.lit(threshold, pl.Float64),
    )


def _graded(rs: Ruleset, criterion: str, grades: list[tuple[pl.Expr, Strength, pl.Expr, float]]) -> pl.Expr:
    """First grade whose condition holds wins: list grades strongest first."""
    (cond, strength, value, threshold), *rest = grades
    expr = pl.when(cond).then(_evidence(rs, criterion, strength, value, threshold))
    for cond, strength, value, threshold in rest:
        expr = expr.when(cond).then(_evidence(rs, criterion, strength, value, threshold))
    return expr.otherwise(pl.lit(None, EVIDENCE_DTYPE))


def _has_any(values: list[str], column: str = "consequences") -> pl.Expr:
    return pl.col(column).list.eval(pl.element().is_in(values)).list.any().fill_null(False)


def _scored(rs: Ruleset, criterion: str, value: pl.Expr, grades: dict[Strength, float]) -> pl.Expr:
    """Strength from a count or a point total, strongest grade first."""
    return _graded(rs, criterion, [(value >= t, s, value, t) for s, t in grades.items()])


def _cohort_af(rs: Ruleset) -> pl.Expr:
    """Allele frequency in the tenant's not affected (WGS or WXS, the higher), null when too few are sequenced.
    The ETL counts carriers: AF = 1 - sqrt(1 - carrier frequency) under Hardy-Weinberg."""
    f = rs.frequency

    def pf(kind: str) -> pl.Expr:
        n = pl.col(f"germline_pn_{kind}_not_affected")
        return pl.when(n >= f.cohort_min_pn).then(pl.col(f"germline_pf_{kind}_not_affected"))

    return 1 - (1 - pl.max_horizontal(pf("wgs"), pf("wxs"))).sqrt()


def _control_frequency(rs: Ruleset) -> pl.Expr:
    """BA1/BS1 frequency: gnomAD AF (enough alleles) or the tenant's not-affected AF, the higher. The tenant's is
    left out for ClinVar P/LP: a founder variant is common in a regional cohort and still pathogenic."""
    cohort = pl.when(~pl.col("clinvar_plp")).then(_cohort_af(rs))
    return pl.max_horizontal(pl.when(an >= rs.frequency.min_an).then(af), cohort)


def ba1(rs: Ruleset) -> pl.Expr:
    freq, t = _control_frequency(rs), rs.frequency.ba1_min_af
    return _graded(rs, "BA1", [(freq > t, Strength.STAND_ALONE, freq, t)])


def bs1(rs: Ruleset) -> pl.Expr:
    freq, t = _control_frequency(rs), rs.frequency.bs1_min_af
    return _graded(rs, "BS1", [(freq > t, Strength.STRONG, freq, t)])


def pm2(rs: Ruleset) -> pl.Expr:
    f = rs.frequency
    rare = af.is_null() | (af <= f.pm2_max_af)
    cond = rare & ~(_cohort_af(rs) > f.pm2_cohort_max_pf).fill_null(False)
    return _graded(rs, "PM2", [(cond, Strength.SUPPORTING, af.fill_null(0.0), f.pm2_max_af)])


def pp3(rs: Ruleset) -> pl.Expr:
    revel_grades = [(revel >= t, s, revel, t) for s, t in rs.revel.pp3.items()]
    splice = (spliceai >= rs.spliceai.pp3_min, Strength.SUPPORTING, spliceai, rs.spliceai.pp3_min)
    return _graded(rs, "PP3", [*revel_grades, splice])


def bp4(rs: Ruleset) -> pl.Expr:
    # REVEL scores missense only; a splice signal above benign_max vetoes the benign call.
    no_splice_signal = spliceai.is_null() | (spliceai <= rs.spliceai.benign_max)
    return _graded(rs, "BP4", [((revel <= t) & no_splice_signal, s, revel, t) for s, t in rs.revel.bp4.items()])


def bp7(rs: Ruleset) -> pl.Expr:
    t = rs.spliceai.benign_max
    cond = _has_any(rs.bp7.consequences) & (spliceai.is_null() | (spliceai <= t))
    return _graded(rs, "BP7", [(cond, Strength.SUPPORTING, spliceai, t)])


def pvs1(rs: Ruleset) -> pl.Expr:
    p, t = rs.pvs1, rs.pvs1.max_loeuf
    # LoF must be a known disease mechanism of the gene: ClinGen HI says so, or ClinVar LoF P/LP says pathogenic
    # (few in a constrained or a recessive-only gene, more in any: LOEUF misses recessive genes). ClinGen "dosage
    # sensitivity unlikely" vetoes all.
    hi, plp_lof = pl.col("clingen_hi_score"), pl.col("gene_plp_lof")
    recessive = _omim_only(p.recessive_inheritance, p.dominant_inheritance)
    clinvar_lof = (
        ((loeuf < t) & (plp_lof >= p.min_gene_plp_lof)).fill_null(False)
        | (plp_lof >= p.min_gene_plp_lof_any_loeuf)
        | (recessive & (plp_lof >= p.min_gene_plp_lof_recessive))
    )
    mechanism = (hi.is_in(p.hi_scores).fill_null(False) | clinvar_lof) & ~hi.is_in(p.not_hi_scores).fill_null(False)
    lof = _has_any(p.consequences) & mechanism
    last_exon = (pl.col("exon_rank") == pl.col("exon_total")).fill_null(False)
    truncating = _has_any(p.nmd_escape_consequences)
    # A stop or a frameshift, or a splice site whose exon skipping shifts the frame, keeps VeryStrong.
    keeps = truncating | pl.col("splice_frameshift").fill_null(False)
    by_type = [(lof & ~keeps & _has_any([c]), s, loeuf, t) for c, s in p.consequence_strength.items()]
    return _graded(
        rs,
        "PVS1",
        [(lof & truncating & last_exon, p.nmd_escape_strength, loeuf, t), *by_type, (lof, Strength.VERY_STRONG, loeuf, t)],
    )


def pm4(rs: Ruleset) -> pl.Expr:
    return _graded(rs, "PM4", [(_has_any(rs.pm4.consequences), Strength.MODERATE, pl.lit(None), None)])


def bs2(rs: Ruleset) -> pl.Expr:
    p = rs.bs2
    gnomad_hom, local_hom = pl.col("gnomad_hom"), pl.col("local_hom_not_affected")
    omim = _has_any(p.inheritance, "omim_inheritance")
    grades = [(omim & (gnomad_hom >= t).fill_null(False), s, gnomad_hom, t) for s, t in p.gnomad_hom.items()]
    local_t = p.min_local_hom_not_affected
    return _graded(rs, "BS2", [*grades, (omim & (local_hom >= local_t), p.local_strength, local_hom, local_t)])


def pm6(rs: Ruleset) -> pl.Expr:
    points = pl.col("denovo_probands") * rs.de_novo.assumed_points
    return _scored(rs, "PM6", points, rs.svi_points)


def pm3(rs: Ruleset) -> pl.Expr:
    w = rs.pm3.points
    points = (
        pl.col("comphet_p_trans") * w.p_trans
        + pl.col("comphet_lp_trans") * w.lp_trans
        + pl.col("comphet_p_unphased") * w.p_unphased
        + pl.col("comphet_lp_unphased") * w.lp_unphased
    )
    return _scored(rs, "PM3", points, rs.svi_points)


def ps4(rs: Ruleset) -> pl.Expr:
    # 2x2 table affected / not affected x carrier / not, +0.5 in each cell (Haldane) so a zero cell stays finite.
    a = pl.col("affected_carriers") + 0.5
    b = pl.col("affected_total") - pl.col("affected_carriers") + 0.5
    c = pl.col("not_affected_carriers") + 0.5
    d = pl.col("not_affected_total") - pl.col("not_affected_carriers") + 0.5
    odds_ratio = (a * d) / (b * c)
    ci_lower = (odds_ratio.log() - 1.96 * (1 / a + 1 / b + 1 / c + 1 / d).sqrt()).exp()
    p = rs.ps4
    cond = pm2(rs).is_not_null() & (odds_ratio >= p.min_odds_ratio) & (ci_lower > p.ci_lower_min)
    return _graded(rs, "PS4", [(cond, Strength.STRONG, odds_ratio, p.min_odds_ratio)])


def pp1(rs: Ruleset) -> pl.Expr:
    return _scored(rs, "PP1", pl.col("affected_carrier_parents"), rs.pp1)


def _omim_only(codes: list[str], excluded: list[str]) -> pl.Expr:
    return _has_any(codes, "omim_inheritance") & ~_has_any(excluded, "omim_inheritance")


def bp2(rs: Ruleset) -> pl.Expr:
    p = rs.bp2
    dominant_only = _omim_only(p.dominant, p.not_recessive)
    carriers = pl.col("comphet_p_trans")
    return _graded(
        rs, "BP2", [(dominant_only & (carriers >= p.min_carriers), Strength.SUPPORTING, carriers, p.min_carriers)]
    )


def bs4(rs: Ruleset) -> pl.Expr:
    # Recessive gene: the causal allele often comes from the unaffected carrier parent, so no segregation test.
    p = rs.bs4
    noncarriers, t = pl.col("affected_noncarrier_parents"), p.min_affected_noncarriers
    cond = _omim_only(p.dominant, p.not_recessive) & (noncarriers >= t)
    return _graded(rs, "BS4", [(cond, Strength.STRONG, noncarriers, t)])


def pm1(rs: Ruleset) -> pl.Expr:
    p = rs.pm1
    plp_missense, blb = pl.col("clinvar_plp_missense_window"), pl.col("clinvar_blb_missense_window")
    cond = _has_any(p.consequences) & (plp_missense >= p.min_plp_missense) & (blb <= p.max_blb_missense)
    return _graded(rs, "PM1", [(cond, Strength.MODERATE, plp_missense, p.min_plp_missense)])


def pp2(rs: Ruleset) -> pl.Expr:
    p = rs.pp2
    plp, plp_missense, blb_missense = pl.col("gene_plp"), pl.col("gene_plp_missense"), pl.col("gene_blb_missense")
    missense_share = plp_missense / plp
    benign_share = blb_missense / (plp_missense + blb_missense)
    cond = (
        _has_any(p.consequences)
        & (plp >= p.min_plp)
        & (missense_share >= p.min_missense_share)
        & (benign_share <= p.max_benign_missense_share)
    )
    return _graded(rs, "PP2", [(cond, Strength.SUPPORTING, missense_share, p.min_missense_share)])


def bp1(rs: Ruleset) -> pl.Expr:
    p = rs.bp1
    lof_share = pl.col("gene_plp_lof") / pl.col("gene_plp")
    cond = _has_any(p.consequences) & (pl.col("gene_plp") >= p.min_plp) & (lof_share >= p.min_lof_share)
    return _graded(rs, "BP1", [(cond, Strength.SUPPORTING, lof_share, p.min_lof_share)])


# Output order of the codes and of the audit.
CRITERIA = {
    "BA1": ba1,
    "BS1": bs1,
    "BS2": bs2,
    "BS4": bs4,
    "PM2": pm2,
    "PM1": pm1,
    "PM3": pm3,
    "PM4": pm4,
    "PM6": pm6,
    "PS4": ps4,
    "PP1": pp1,
    "PP2": pp2,
    "PP3": pp3,
    "BP1": bp1,
    "BP2": bp2,
    "BP4": bp4,
    "BP7": bp7,
    "PVS1": pvs1,
}


VARIANT_CRITERIA = ["BA1", "BS1", "BS2", "PM2", "PM1", "PM4", "PS4", "PP2", "PP3", "BP1", "BP4", "BP7", "PVS1"]
OCCURRENCE_CRITERIA = ["BS4", "PM3", "PM6", "PP1", "BP2"]
assert sorted(VARIANT_CRITERIA + OCCURRENCE_CRITERIA) == sorted(CRITERIA)


def _criterion(name: str, rs: Ruleset) -> pl.Expr:
    """The criterion's evidence, dropped in a gene that ClinGen ties to no disease."""
    expr = CRITERIA[name](rs)
    if name in rs.clingen.no_disease_drops:
        expr = pl.when(~pl.col("clingen_no_disease")).then(expr).otherwise(pl.lit(None, EVIDENCE_DTYPE))
    return expr


def _settle(exprs: dict[str, pl.Expr], rs: Ruleset) -> dict[str, pl.Expr]:
    """Evidence of each criterion after exclusions, then point caps."""
    met = {name: expr.is_not_null() for name, expr in exprs.items()}
    for winner, losers in rs.exclusions.items():
        for loser in losers:
            exprs[loser] = pl.when(~met[winner]).then(exprs[loser]).otherwise(pl.lit(None, EVIDENCE_DTYPE))
    for capped, cap in rs.point_caps.items():
        total = exprs[capped].struct.field("points") + exprs[cap.partner].struct.field("points").fill_null(0)
        exprs[capped] = pl.when(total <= cap.max_points).then(exprs[capped]).otherwise(pl.lit(None, EVIDENCE_DTYPE))
    return exprs


def evaluate(facts: pl.DataFrame | pl.LazyFrame, rs: Ruleset, names: list[str], keys: list[str]) -> pl.LazyFrame:
    """One row per criterion met: keys, criterion, code, strength, points, value, threshold, ruleset_version."""
    wide = facts.lazy().select(*keys, **{name: _criterion(name, rs) for name in names})
    return (
        wide.unpivot(index=keys, on=names, variable_name="criterion", value_name="evidence")
        .filter(pl.col("evidence").is_not_null())
        .unnest("evidence")
        .with_columns(ruleset_version=pl.lit(rs.version))
    )


def to_wide(rows: pl.DataFrame, keys: list[str]) -> pl.DataFrame:
    """From `keys` + a `criteria` list of criterion rows (as evaluate writes them) to one Evidence column per
    criterion, null when not met."""
    met = (
        rows.select(*keys, "criteria")
        .explode("criteria")
        .filter(pl.col("criteria").struct.field("criterion").is_not_null())
        .unnest("criteria")
        .select(*keys, "criterion", evidence=pl.struct(*EVIDENCE_DTYPE.to_schema()).cast(EVIDENCE_DTYPE))
    )
    wide = rows.select(keys)
    if met.height:
        wide = wide.join(met.pivot(on="criterion", index=keys, values="evidence"), on=keys, how="left")
    return wide.with_columns(pl.lit(None, EVIDENCE_DTYPE).alias(c) for c in CRITERIA if c not in wide.columns)


def combine(evidence: pl.DataFrame | pl.LazyFrame, rs: Ruleset, keys: list[str]) -> pl.LazyFrame:
    """Classification from one Evidence column per criterion."""
    # One struct column per criterion: a list of structs would be simpler, but Polars builds it ~15x slower.
    exprs = _settle({name: pl.col(name) for name in CRITERIA}, rs)
    fields = lambda name: [pl.col(c).struct.field(name) for c in exprs]  # noqa: E731
    stand_alone_benign = pl.any_horizontal(
        [(s == Strength.STAND_ALONE) & (p < 0) for s, p in zip(fields("strength"), fields("points"), strict=True)]
    ).fill_null(False)
    points, c = pl.col("acmg_points"), rs.classes
    acmg_class = (
        pl.when(stand_alone_benign).then(pl.lit(AcmgClass.BENIGN))
        .when(points >= c.pathogenic).then(pl.lit(AcmgClass.PATHOGENIC))
        .when(points >= c.likely_pathogenic).then(pl.lit(AcmgClass.LIKELY_PATHOGENIC))
        .when(points >= c.vus).then(pl.lit(AcmgClass.VUS))
        .when(points >= c.likely_benign).then(pl.lit(AcmgClass.LIKELY_BENIGN))
        .otherwise(pl.lit(AcmgClass.BENIGN))
    )  # fmt: skip
    return (
        evidence.lazy()
        .with_columns(**exprs)
        .with_columns(acmg_points=pl.sum_horizontal(fields("points")))
        .select(
            *keys,
            acmg_class=acmg_class.cast(pl.String),
            acmg_points="acmg_points",
            acmg_criteria=pl.concat_list(fields("code")).list.drop_nulls(),
            acmg_audit=pl.struct(ruleset_version=pl.lit(rs.version), criteria=pl.struct(list(exprs))),
            ruleset_version=pl.lit(rs.version),
        )
        .cast(CLASSIFICATION_SCHEMA)
    )


def classify(facts: pl.DataFrame | pl.LazyFrame, rs: Ruleset, keys=("locus_id", "transcript_id")) -> pl.LazyFrame:
    """Both levels on one full facts row: the same result as evaluate, then combine."""
    evidence = facts.lazy().with_columns(**{name: _criterion(name, rs) for name in CRITERIA})
    return combine(evidence, rs, list(keys))
