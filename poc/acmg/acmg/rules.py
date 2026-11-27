"""ACMG criteria as Polars expressions over a facts frame (schema: models.FACT_SCHEMA).

Each criterion function returns an Evidence struct expression, null when the criterion is not met.
`evaluate` gives the criteria met at one level (variant or occurrence), `combine` classifies an occurrence
from all its criteria: exclusions and point caps apply there, across both levels.
"""

import polars as pl
from acmg.models import (
    CANCELLED_DTYPE,
    CLASSIFICATION_SCHEMA,
    DETAILS_DTYPE,
    EVIDENCE_DTYPE,
    AcmgClass,
    Ruleset,
    Strength,
    criterion_code,
)

af, an = pl.col("gnomad_af"), pl.col("gnomad_an")
revel, spliceai, loeuf = pl.col("revel_score"), pl.col("spliceai_ds"), pl.col("gnomad_loeuf")


def _details(values: dict[str, pl.Expr]) -> pl.Expr:
    if not values:
        return pl.lit([], DETAILS_DTYPE)
    return pl.concat_list([pl.struct(name=pl.lit(k), value=v.cast(pl.Float64)) for k, v in values.items()])


def _evidence(
    rs: Ruleset,
    criterion: str,
    strength: Strength,
    value: pl.Expr,
    threshold: float | pl.Expr | None,
    source: str | pl.Expr | None = None,
    details: dict[str, pl.Expr] | None = None,
    refs: pl.Expr | None = None,
) -> pl.Expr:
    points = rs.points[strength] * (-1 if criterion.startswith("B") else 1)
    return pl.struct(
        code=pl.lit(criterion_code(criterion, strength)),
        strength=pl.lit(str(strength)),
        points=pl.lit(points, pl.Int64),
        value=value.cast(pl.Float64),
        threshold=threshold.cast(pl.Float64) if isinstance(threshold, pl.Expr) else pl.lit(threshold, pl.Float64),
        source=source if isinstance(source, pl.Expr) else pl.lit(source, pl.String),
        details=_details(details or {}),
        refs=pl.lit([], pl.List(pl.Int64)) if refs is None else refs.cast(pl.List(pl.Int64)),
    )


def _graded(
    rs: Ruleset,
    criterion: str,
    grades: list[tuple],
    details: dict[str, pl.Expr] | None = None,
    refs: pl.Expr | None = None,
) -> pl.Expr:
    """First grade whose condition holds wins: list grades strongest first. A grade is (condition, strength, value,
    threshold) and an optional source."""
    (cond, *first), *rest = grades
    expr = pl.when(cond).then(_evidence(rs, criterion, *first, details=details, refs=refs))
    for cond, *grade in rest:
        expr = expr.when(cond).then(_evidence(rs, criterion, *grade, details=details, refs=refs))
    return expr.otherwise(pl.lit(None, EVIDENCE_DTYPE))


def _has_any(values: list[str], column: str = "consequences") -> pl.Expr:
    return pl.col(column).list.eval(pl.element().is_in(values)).list.any().fill_null(False)


def _scored(rs: Ruleset, criterion: str, value: pl.Expr, grades: dict[Strength, float], **kwargs) -> pl.Expr:
    """Strength from a count or a point total, strongest grade first."""
    return _graded(rs, criterion, [(value >= t, s, value, t) for s, t in grades.items()], **kwargs)


def _cohort_af(rs: Ruleset) -> pl.Expr:
    """Allele frequency in the tenant's not affected (WGS or WXS, the higher), null when too few are sequenced.
    The ETL counts carriers: AF = 1 - sqrt(1 - carrier frequency) under Hardy-Weinberg."""
    f = rs.frequency

    def pf(kind: str) -> pl.Expr:
        n = pl.col(f"germline_pn_{kind}_not_affected")
        return pl.when(n >= f.cohort_min_pn).then(pl.col(f"germline_pf_{kind}_not_affected"))

    return 1 - (1 - pl.max_horizontal(pf("wgs"), pf("wxs"))).sqrt()


def _control_frequency(rs: Ruleset) -> tuple[pl.Expr, pl.Expr, dict[str, pl.Expr]]:
    """BA1/BS1 frequency: gnomAD AF (enough alleles) or the tenant's not-affected AF, the higher; its source and
    details. The tenant's is left out for ClinVar P/LP: a founder variant is common in a regional cohort and still
    pathogenic."""
    gnomad = pl.when(an >= rs.frequency.min_an).then(af)
    cohort = pl.when(~pl.col("clinvar_plp")).then(_cohort_af(rs))
    source = pl.when(gnomad >= cohort.fill_null(0.0)).then(pl.lit("gnomad")).otherwise(pl.lit("cohort"))
    details = {"gnomad_af": af, "gnomad_an": an, "cohort_af": cohort}
    return pl.max_horizontal(gnomad, cohort), source, details


def ba1(rs: Ruleset) -> pl.Expr:
    (freq, source, details), t = _control_frequency(rs), rs.frequency.ba1_min_af
    return _graded(rs, "BA1", [(freq > t, Strength.STAND_ALONE, freq, t, source)], details)


def _expert_plp(rs: Ruleset) -> pl.Expr:
    # Healthy carriers do not outweigh an expert P/LP: founder, hypomorphic or late-onset recessive variants.
    return pl.col("clinvar_plp_stars") >= rs.clinvar.expert_min_stars


def bs1(rs: Ruleset) -> pl.Expr:
    (freq, source, details), t = _control_frequency(rs), rs.frequency.bs1_min_af
    return _graded(rs, "BS1", [((freq > t) & ~_expert_plp(rs), Strength.STRONG, freq, t, source)], details)


def pm2(rs: Ruleset) -> pl.Expr:
    f = rs.frequency
    # Healthy carriers make a pathogenic allele of a recessive-only gene commoner.
    recessive = _omim_only(rs.pvs1.recessive_inheritance, rs.pvs1.dominant_inheritance)
    max_af = pl.when(recessive).then(f.pm2_max_af_recessive).otherwise(f.pm2_max_af)
    rare = af.is_null() | (af <= max_af)
    cohort = _cohort_af(rs)
    cond = rare & ~(cohort > f.pm2_cohort_max_pf).fill_null(False)
    details = {"gnomad_an": an, "cohort_af": cohort, "cohort_max_af": pl.lit(f.pm2_cohort_max_pf)}
    return _graded(rs, "PM2", [(cond, Strength.SUPPORTING, af.fill_null(0.0), max_af)], details)


def pp3(rs: Ruleset) -> pl.Expr:
    revel_grades = [(revel >= t, s, revel, t, "revel") for s, t in rs.revel.pp3.items()]
    splice = (spliceai >= rs.spliceai.pp3_min, Strength.SUPPORTING, spliceai, rs.spliceai.pp3_min, "spliceai")
    return _graded(rs, "PP3", [*revel_grades, splice], {"revel": revel, "spliceai": spliceai})


def bp4(rs: Ruleset) -> pl.Expr:
    # REVEL scores missense only; a splice signal above benign_max vetoes the benign call.
    no_splice_signal = spliceai.is_null() | (spliceai <= rs.spliceai.benign_max)
    grades = [((revel <= t) & no_splice_signal, s, revel, t, "revel") for s, t in rs.revel.bp4.items()]
    return _graded(rs, "BP4", grades, {"spliceai": spliceai, "spliceai_max": pl.lit(rs.spliceai.benign_max)})


def bp7(rs: Ruleset) -> pl.Expr:
    t = rs.spliceai.benign_max
    cond = _has_any(rs.bp7.consequences) & (spliceai.is_null() | (spliceai <= t))
    return _graded(rs, "BP7", [(cond, Strength.SUPPORTING, spliceai, t, "spliceai")])


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
    clingen = hi.is_in(p.hi_scores).fill_null(False)
    mechanism = (clingen | clinvar_lof) & ~hi.is_in(p.not_hi_scores).fill_null(False)
    lof = _has_any(p.consequences) & mechanism
    last_exon = (pl.col("exon_rank") == pl.col("exon_total")).fill_null(False)
    truncating = _has_any(p.nmd_escape_consequences)
    # A stop or a frameshift, or a splice site whose exon skipping shifts the frame, keeps VeryStrong.
    keeps = truncating | pl.col("splice_frameshift").fill_null(False)
    source = pl.when(clingen).then(pl.lit("clingen_hi")).otherwise(pl.lit("clinvar_lof"))
    by_type = [(lof & ~keeps & _has_any([c]), s, loeuf, t, source) for c, s in p.consequence_strength.items()]
    details = {
        "clingen_hi_score": hi,
        "gene_plp_lof": plp_lof,
        "recessive_only": recessive.cast(pl.Float64),
        "exon_rank": pl.col("exon_rank"),
        "exon_total": pl.col("exon_total"),
        "splice_frameshift": pl.col("splice_frameshift").cast(pl.Float64),
    }
    return _graded(
        rs,
        "PVS1",
        [
            (lof & truncating & last_exon, p.nmd_escape_strength, loeuf, t, source),
            *by_type,
            (lof, Strength.VERY_STRONG, loeuf, t, source),
        ],
        details,
    )


def pm4(rs: Ruleset) -> pl.Expr:
    return _graded(rs, "PM4", [(_has_any(rs.pm4.consequences), Strength.MODERATE, pl.lit(None), None)])


def bs2(rs: Ruleset) -> pl.Expr:
    p = rs.bs2
    gnomad_hom, local_hom = pl.col("gnomad_hom"), pl.col("local_hom_not_affected")
    omim = _has_any(p.inheritance, "omim_inheritance") & ~_expert_plp(rs)
    grades = [(omim & (gnomad_hom >= t).fill_null(False), s, gnomad_hom, t, "gnomad") for s, t in p.gnomad_hom.items()]
    local_t = p.min_local_hom_not_affected
    local = (omim & (local_hom >= local_t), p.local_strength, local_hom, local_t, "cohort")
    return _graded(rs, "BS2", [*grades, local], {"gnomad_hom": gnomad_hom, "cohort_hom": local_hom})


def ps2(rs: Ruleset) -> pl.Expr:
    points = pl.col("denovo_confirmed") * rs.de_novo.confirmed_points
    return _scored(rs, "PS2", points, rs.svi_points, details={"denovo_probands": pl.col("denovo_confirmed")})


def pm6(rs: Ruleset) -> pl.Expr:
    points = pl.col("denovo_assumed") * rs.de_novo.assumed_points
    return _scored(rs, "PM6", points, rs.svi_points, details={"denovo_probands": pl.col("denovo_assumed")})


def pm3(rs: Ruleset) -> pl.Expr:
    w = rs.pm3.points
    points = (
        pl.col("comphet_p_trans") * w.p_trans
        + pl.col("comphet_lp_trans") * w.lp_trans
        + pl.col("comphet_p_unphased") * w.p_unphased
        + pl.col("comphet_lp_unphased") * w.lp_unphased
    )
    details = {c: pl.col(c) for c in ("comphet_p_trans", "comphet_lp_trans", "comphet_p_unphased", "comphet_lp_unphased")}
    return _scored(rs, "PM3", points, rs.svi_points, details=details, refs=_partner())


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
    details = {c: pl.col(c) for c in ("affected_carriers", "affected_total", "not_affected_carriers", "not_affected_total")}
    details |= {"ci_lower": ci_lower, "ci_lower_min": pl.lit(p.ci_lower_min)}
    return _graded(rs, "PS4", [(cond, Strength.STRONG, odds_ratio, p.min_odds_ratio)], details)


def pp1(rs: Ruleset) -> pl.Expr:
    return _scored(rs, "PP1", pl.col("affected_carrier_parents"), rs.pp1)


def _partner() -> pl.Expr:
    return pl.concat_list(pl.col("comphet_partner_locus_id")).list.drop_nulls()


def _omim_only(codes: list[str], excluded: list[str]) -> pl.Expr:
    return _has_any(codes, "omim_inheritance") & ~_has_any(excluded, "omim_inheritance")


def bp2(rs: Ruleset) -> pl.Expr:
    p = rs.bp2
    dominant_only = _omim_only(p.dominant, p.not_recessive)
    carriers = pl.col("comphet_p_trans")
    grade = (dominant_only & (carriers >= p.min_carriers), Strength.SUPPORTING, carriers, p.min_carriers)
    return _graded(rs, "BP2", [grade], refs=_partner())


def bs4(rs: Ruleset) -> pl.Expr:
    # Recessive gene: the causal allele often comes from the unaffected carrier parent, so no segregation test.
    p = rs.bs4
    noncarriers, t = pl.col("affected_noncarrier_parents"), p.min_affected_noncarriers
    # An affected carrier parent makes the affected non-carrier a likelier phenocopy than a failed segregation.
    cond = _omim_only(p.dominant, p.not_recessive) & (noncarriers >= t) & (pl.col("affected_carrier_parents") == 0)
    return _graded(rs, "BS4", [(cond, Strength.STRONG, noncarriers, t)])


def pm1(rs: Ruleset) -> pl.Expr:
    p = rs.pm1
    plp_missense, blb = pl.col("clinvar_plp_missense_window"), pl.col("clinvar_blb_missense_window")
    cond = _has_any(p.consequences) & (plp_missense >= p.min_plp_missense) & (blb <= p.max_blb_missense)
    details = {"blb_missense": blb, "max_blb_missense": pl.lit(p.max_blb_missense)}
    return _graded(rs, "PM1", [(cond, Strength.MODERATE, plp_missense, p.min_plp_missense)], details)


def _same_residue(rs: Ruleset, criterion: str, column: str, strengths) -> pl.Expr:
    reference = pl.col(column)
    ok = _has_any(["missense_variant"]) & (spliceai.fill_null(0.0) <= rs.same_residue.max_spliceai)
    grades = [
        (ok & (reference == 2), strengths.pathogenic, reference, 2),
        (ok & (reference == 1), strengths.likely_pathogenic, reference, 1),
    ]
    return _graded(rs, criterion, grades, {"spliceai_ds": spliceai})


def ps1(rs: Ruleset) -> pl.Expr:
    return _same_residue(rs, "PS1", "clinvar_same_aa", rs.same_residue.ps1)


def pm5(rs: Ruleset) -> pl.Expr:
    return _same_residue(rs, "PM5", "clinvar_same_residue", rs.same_residue.pm5)


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
    details = {
        "gene_plp": plp,
        "benign_missense_share": benign_share,
        "max_benign_missense_share": pl.lit(p.max_benign_missense_share),
    }
    return _graded(rs, "PP2", [(cond, Strength.SUPPORTING, missense_share, p.min_missense_share)], details)


def bp1(rs: Ruleset) -> pl.Expr:
    p = rs.bp1
    lof_share = pl.col("gene_plp_lof") / pl.col("gene_plp")
    cond = _has_any(p.consequences) & (pl.col("gene_plp") >= p.min_plp) & (lof_share >= p.min_lof_share)
    details = {"gene_plp": pl.col("gene_plp"), "gene_plp_lof": pl.col("gene_plp_lof")}
    return _graded(rs, "BP1", [(cond, Strength.SUPPORTING, lof_share, p.min_lof_share)], details)


def _external(rs: Ruleset) -> pl.Expr:
    return pl.lit(None, EVIDENCE_DTYPE)


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
    "PS2": ps2,
    "PS1": ps1,
    "PM5": pm5,
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
# Criteria only a ClinGen VCEP gives (acmg.erepo): the columns exist so that its evidence is combined, never computed.
EXTERNAL_CRITERIA = ["PS3", "PP4", "PP5", "BS3", "BP3", "BP5", "BP6"]
CRITERIA |= dict.fromkeys(EXTERNAL_CRITERIA, _external)


VARIANT_CRITERIA = ["BA1", "BS1", "BS2", "PM2", "PM1", "PM4", "PS1", "PM5", "PS4", "PP2", "PP3", "BP1", "BP4", "BP7", "PVS1"]
OCCURRENCE_CRITERIA = ["BS4", "PM3", "PS2", "PM6", "PP1", "BP2"]
assert sorted(VARIANT_CRITERIA + OCCURRENCE_CRITERIA + EXTERNAL_CRITERIA) == sorted(CRITERIA)


def _criterion(name: str, rs: Ruleset) -> pl.Expr:
    """The criterion's evidence, dropped in a gene that ClinGen ties to no disease."""
    if name in rs.disabled:
        return pl.lit(None, EVIDENCE_DTYPE)
    expr = CRITERIA[name](rs)
    if name in rs.clingen.no_disease_drops:
        expr = pl.when(~pl.col("clingen_no_disease")).then(expr).otherwise(pl.lit(None, EVIDENCE_DTYPE))
    return expr


def _settle(
    exprs: dict[str, pl.Expr], rs: Ruleset, cancelled: list[pl.Expr]
) -> tuple[dict[str, pl.Expr], pl.Expr]:
    """Evidence of each criterion after exclusions, then point caps; and the list of the criteria dropped, these and
    the ones already in `cancelled`."""
    met = {name: expr.is_not_null() for name, expr in exprs.items()}
    cancelled = list(cancelled)
    for winner, losers in rs.exclusions.items():
        for loser in losers:
            cancelled.append(_cancelled(loser, winner, met[winner] & exprs[loser].is_not_null()))
            exprs[loser] = pl.when(~met[winner]).then(exprs[loser]).otherwise(pl.lit(None, EVIDENCE_DTYPE))
    for capped, cap in rs.point_caps.items():
        total = exprs[capped].struct.field("points") + exprs[cap.partner].struct.field("points").fill_null(0)
        cancelled.append(_cancelled(capped, cap.partner, (total > cap.max_points).fill_null(False)))
        exprs[capped] = pl.when(total <= cap.max_points).then(exprs[capped]).otherwise(pl.lit(None, EVIDENCE_DTYPE))
    # A loser of two winners (PS2 under BA1 and BS1) is listed once, under its first.
    unique = pl.concat_list(cancelled).list.drop_nulls().list.eval(
        pl.element().filter(pl.element().struct.field("criterion").is_first_distinct())
    )
    return exprs, unique


def _cancelled(criterion: str, by: str, cond: pl.Expr) -> pl.Expr:
    struct = pl.struct(criterion=pl.lit(criterion), cancelled_by=pl.lit(by))
    return pl.when(cond).then(struct).otherwise(pl.lit(None, CANCELLED_DTYPE))


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
    criterion, null when not met. With a `not_applicable` list column (CSpec), its criteria are dropped and listed in
    `cspec_cancelled`."""
    na = [c for c in ("not_applicable",) if c in rows.columns]
    met = (
        rows.select(*keys, "criteria", *na)
        .explode("criteria")
        .filter(pl.col("criteria").struct.field("criterion").is_not_null())
        .unnest("criteria")
    )
    cancelled = None
    if na:
        # On the long form (~2 criteria per row), not 31 list lookups per wide row: 3x faster at classification.
        hit = pl.col("not_applicable").list.contains(pl.col("criterion")).fill_null(False)
        cancelled = met.filter(hit).group_by(keys).agg(
            cspec_cancelled=pl.struct(criterion="criterion", cancelled_by=pl.lit("CSpec"))
        )
        met = met.filter(~hit).drop("not_applicable")
    # The classification query passes only what combine reads: the explanation fields stay in the criteria tables.
    fields = EVIDENCE_DTYPE.to_schema()
    met = met.with_columns(pl.lit(None, t).alias(f) for f, t in fields.items() if f not in met.columns).select(
        *keys, "criterion", evidence=pl.struct(*fields).cast(EVIDENCE_DTYPE)
    )
    wide = rows.select(keys)
    if met.height:
        wide = wide.join(met.pivot(on="criterion", index=keys, values="evidence"), on=keys, how="left")
    if cancelled is not None:
        wide = wide.join(cancelled, on=keys, how="left").with_columns(
            pl.col("cspec_cancelled").fill_null([]).cast(pl.List(CANCELLED_DTYPE))
        )
    return wide.with_columns(pl.lit(None, EVIDENCE_DTYPE).alias(c) for c in CRITERIA if c not in wide.columns)


def combine(evidence: pl.DataFrame | pl.LazyFrame, rs: Ruleset, keys: list[str]) -> pl.LazyFrame:
    """Classification from one Evidence column per criterion; `cspec_cancelled`, when there, lists the criteria
    to_wide already dropped."""
    # One struct column per criterion: a list of structs would be simpler, but Polars builds it ~15x slower.
    dropped = [pl.col("cspec_cancelled")] if "cspec_cancelled" in evidence.lazy().collect_schema().names() else []
    exprs, cancelled = _settle({name: pl.col(name) for name in CRITERIA}, rs, dropped)
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
        # One with_columns: `cancelled` reads the evidence before exprs overwrite it.
        .with_columns(**exprs, acmg_cancelled=cancelled)
        .with_columns(acmg_points=pl.sum_horizontal(fields("points")))
        .select(
            *keys,
            acmg_class=acmg_class.cast(pl.String),
            acmg_points="acmg_points",
            acmg_criteria=pl.concat_list(fields("code")).list.drop_nulls(),
            acmg_cancelled="acmg_cancelled",
            acmg_audit=pl.struct(ruleset_version=pl.lit(rs.version), criteria=pl.struct(list(exprs))),
            ruleset_version=pl.lit(rs.version),
        )
        .cast(CLASSIFICATION_SCHEMA)
    )


def classify(facts: pl.DataFrame | pl.LazyFrame, rs: Ruleset, keys=("locus_id", "transcript_id")) -> pl.LazyFrame:
    """Both levels on one full facts row: the same result as evaluate, then combine."""
    evidence = facts.lazy().with_columns(**{name: _criterion(name, rs) for name in CRITERIA})
    return combine(evidence, rs, list(keys))
