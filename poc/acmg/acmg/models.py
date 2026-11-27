"""Contracts of the engine: the ruleset it reads, the facts row it takes, the classification row it writes.

Rules run vectorized on whole frames: these models validate the ruleset and give the frame schemas,
they are never instantiated per row on the hot path.
"""

import types
from enum import StrEnum
from pathlib import Path
from typing import Annotated, Union, get_args, get_origin

import polars as pl
import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator


class Strength(StrEnum):
    SUPPORTING = "Supporting"
    MODERATE = "Moderate"
    STRONG = "Strong"
    VERY_STRONG = "VeryStrong"
    STAND_ALONE = "StandAlone"


class AcmgClass(StrEnum):
    PATHOGENIC = "Pathogenic"
    LIKELY_PATHOGENIC = "Likely pathogenic"
    VUS = "VUS"
    LIKELY_BENIGN = "Likely benign"
    BENIGN = "Benign"


# Strength a criterion has in ACMG 2015: its code carries no suffix at that strength (PVS1, PM2_Supporting).
DEFAULT_STRENGTH = {
    "PVS": Strength.VERY_STRONG,
    "PS": Strength.STRONG,
    "PM": Strength.MODERATE,
    "PP": Strength.SUPPORTING,
    "BA": Strength.STAND_ALONE,
    "BS": Strength.STRONG,
    "BP": Strength.SUPPORTING,
}


def criterion_code(criterion: str, strength: Strength) -> str:
    default = DEFAULT_STRENGTH[criterion.rstrip("0123456789")]
    return criterion if strength == default else f"{criterion}_{strength}"


Probability = Annotated[float, Field(ge=0, le=1)]


class Frozen(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class ClassBounds(Frozen):
    pathogenic: int
    likely_pathogenic: int
    vus: int
    likely_benign: int

    @model_validator(mode="after")
    def _decreasing(self):
        bounds = [self.pathogenic, self.likely_pathogenic, self.vus, self.likely_benign]
        if bounds != sorted(bounds, reverse=True) or len(set(bounds)) != 4:
            raise ValueError(f"class bounds must strictly decrease: {bounds}")
        return self


class FrequencyThresholds(Frozen):
    min_an: Annotated[int, Field(ge=0)]
    ba1_min_af: Probability
    bs1_min_af: Probability
    pm2_max_af: Probability
    pm2_cohort_max_pf: Probability
    cohort_min_pn: Annotated[int, Field(ge=0)]

    @model_validator(mode="after")
    def _ordered(self):
        if not self.pm2_max_af < self.bs1_min_af < self.ba1_min_af:
            raise ValueError("expected pm2_max_af < bs1_min_af < ba1_min_af")
        return self


class RevelThresholds(Frozen):
    pp3: dict[Strength, Probability]
    bp4: dict[Strength, Probability]

    @model_validator(mode="after")
    def _ordered(self):
        # Strongest first; the engine takes the first threshold a score meets.
        pp3, bp4 = list(self.pp3.values()), list(self.bp4.values())
        if pp3 != sorted(pp3, reverse=True) or bp4 != sorted(bp4):
            raise ValueError("revel thresholds must be listed strongest first")
        if min(pp3) <= max(bp4):
            raise ValueError("revel pp3 and bp4 ranges overlap")
        return self


class SpliceAiThresholds(Frozen):
    pp3_min: Probability
    benign_max: Probability


class ScopeRule(Frozen):
    # Written into the facts SQL, hence the strict pattern.
    consequences: list[Annotated[str, Field(pattern=r"^[a-z0-9_]+$")]]
    spliceai_min: Probability


class Pvs1Rule(Frozen):
    max_loeuf: Annotated[float, Field(gt=0)]
    min_gene_plp_lof: Annotated[int, Field(ge=0)]
    min_gene_plp_lof_any_loeuf: Annotated[int, Field(gt=0)]
    min_gene_plp_lof_recessive: Annotated[int, Field(gt=0)]
    recessive_inheritance: list[str]
    dominant_inheritance: list[str]
    hi_scores: list[int]
    not_hi_scores: list[int]
    consequences: list[str]
    nmd_escape_consequences: list[str]
    nmd_escape_strength: Strength
    consequence_strength: dict[str, Strength]


class Pm4Rule(Frozen):
    consequences: list[str]


class DeNovoRule(Frozen):
    confirmed_points: Annotated[float, Field(gt=0)]
    assumed_points: Annotated[float, Field(gt=0)]


class Pm3Points(Frozen):
    p_trans: Annotated[float, Field(ge=0)]
    lp_trans: Annotated[float, Field(ge=0)]
    p_unphased: Annotated[float, Field(ge=0)]
    lp_unphased: Annotated[float, Field(ge=0)]


class Pm3Rule(Frozen):
    points: Pm3Points


class Bp2Rule(Frozen):
    dominant: list[str]
    not_recessive: list[str]
    min_carriers: Annotated[int, Field(gt=0)]


class Bs4Rule(Frozen):
    dominant: list[str]
    not_recessive: list[str]
    min_affected_noncarriers: Annotated[int, Field(gt=0)]


# Threshold per strength, strongest first: the engine keeps the first one a value meets.
Grades = dict[Strength, Annotated[float, Field(gt=0)]]


def strongest_first(name: str, grades: dict) -> None:
    values = list(grades.values())
    if not grades or values != sorted(values, reverse=True) or len(set(values)) != len(values):
        raise ValueError(f"{name} thresholds must strictly decrease, strongest first: {grades}")


class Bs2Rule(Frozen):
    inheritance: list[str]
    gnomad_hom: Grades
    min_local_hom_not_affected: Annotated[int, Field(gt=0)]
    local_strength: Strength

    @model_validator(mode="after")
    def _graded_strongest_first(self):
        strongest_first("bs2.gnomad_hom", self.gnomad_hom)
        return self


class Bp7Rule(Frozen):
    consequences: list[str]


class Ps4Rule(Frozen):
    min_odds_ratio: Annotated[float, Field(gt=0)]
    ci_lower_min: Annotated[float, Field(gt=0)]


class ClinvarRule(Frozen):
    min_stars: Annotated[int, Field(ge=0, le=4)]
    expert_min_stars: Annotated[int, Field(ge=0, le=4)]


class ClingenRule(Frozen):
    no_disease_drops: list[str]


class Pm1Rule(Frozen):
    consequences: list[str]
    window_bp: Annotated[int, Field(gt=0)]
    min_plp_missense: Annotated[int, Field(gt=0)]
    max_blb_missense: Annotated[int, Field(ge=0)]


class ResidueStrengths(Frozen):
    pathogenic: Strength
    likely_pathogenic: Strength


class SameResidueRule(Frozen):
    max_spliceai: Probability
    ps1: ResidueStrengths
    pm5: ResidueStrengths


class Pp2Rule(Frozen):
    consequences: list[str]
    min_plp: Annotated[int, Field(gt=0)]
    min_missense_share: Probability
    max_benign_missense_share: Probability


class Bp1Rule(Frozen):
    consequences: list[str]
    min_plp: Annotated[int, Field(gt=0)]
    min_lof_share: Probability


class Pp4Table(Frozen):
    min_terms: Annotated[int, Field(gt=0)]
    bins: dict[Probability, Annotated[int, Field(gt=0)]]

    @model_validator(mode="after")
    def _narrower_scores_more(self):
        points = [p for _, p in sorted(self.bins.items())]
        if not points or points != sorted(points, reverse=True):
            raise ValueError(f"pp4 points must not grow with the bin's top: {self.bins}")
        return self


class Pp4Rule(Frozen):
    # Written into the priority SQL, hence the strict pattern.
    blocked_by: list[Annotated[str, Field(pattern=r"^[A-Z]+[0-9]+$")]]
    tables: list[Pp4Table]

    @model_validator(mode="after")
    def _by_terms(self):
        terms = [t.min_terms for t in self.tables]
        if not terms or terms[0] != 1 or terms != sorted(set(terms)):
            raise ValueError(f"pp4 tables must start at min_terms 1 and increase: {terms}")
        return self


class PriorityRule(Frozen):
    prior: Annotated[float, Field(gt=0, lt=1)]
    very_strong_odds: Annotated[float, Field(gt=1)]


# Written into the priority SQL, hence the strict pattern.
InheritanceCode = Annotated[str, Field(pattern=r"^[A-Z]+$")]


class InheritanceFitRule(Frozen):
    recessive: list[InheritanceCode]
    dominant: list[InheritanceCode]
    single_het_recessive_points: Annotated[int, Field(le=0)]
    healthy_parent_dominant_points: Annotated[int, Field(le=0)]


class PointCap(Frozen):
    partner: str
    max_points: Annotated[int, Field(gt=0)]


class Ruleset(Frozen):
    version: Annotated[str, Field(min_length=1, max_length=30)]
    points: dict[Strength, Annotated[int, Field(gt=0)]]
    classes: ClassBounds
    scope: ScopeRule
    frequency: FrequencyThresholds
    revel: RevelThresholds
    spliceai: SpliceAiThresholds
    pvs1: Pvs1Rule
    bp7: Bp7Rule
    clinvar: ClinvarRule
    clingen: ClingenRule
    pm1: Pm1Rule
    same_residue: SameResidueRule
    pp2: Pp2Rule
    bp1: Bp1Rule
    pm4: Pm4Rule
    bs2: Bs2Rule
    svi_points: Grades
    de_novo: DeNovoRule
    pm3: Pm3Rule
    ps4: Ps4Rule
    pp1: Grades
    bs4: Bs4Rule
    bp2: Bp2Rule
    pp4: Pp4Rule
    priority: PriorityRule
    inheritance_fit: InheritanceFitRule
    disabled: list[str] = []
    exclusions: dict[str, list[str]] = {}
    point_caps: dict[str, PointCap] = {}

    @model_validator(mode="after")
    def _graded_strongest_first(self):
        for name in ("svi_points", "pp1"):
            strongest_first(name, getattr(self, name))
        return self

    @model_validator(mode="after")
    def _all_strengths_scored(self):
        missing = set(Strength) - set(self.points)
        if missing:
            raise ValueError(f"points missing for {sorted(missing)}")
        return self

    @classmethod
    def load(cls, path: Path) -> "Ruleset":
        return cls.model_validate(yaml.safe_load(path.read_text()))


class Fact(BaseModel):
    """Everything the rules read for one occurrence: the variant's facts and the case's (OCCURRENCE_FIELDS)."""

    locus_id: int
    transcript_id: str
    symbol: str | None = None
    consequences: list[str] = []
    revel_score: float | None = None
    spliceai_ds: float | None = None
    gnomad_loeuf: float | None = None
    gnomad_af: float | None = None
    gnomad_an: int | None = None
    germline_pf_wgs_not_affected: float | None = None
    germline_pn_wgs_not_affected: int | None = None
    germline_pf_wxs_not_affected: float | None = None
    germline_pn_wxs_not_affected: int | None = None
    # ClinVar counts computed by variant_facts.sql (never null).
    clinvar_plp: bool = False
    clinvar_plp_stars: int = 0
    clinvar_plp_missense_window: int = 0
    clinvar_blb_missense_window: int = 0
    # ClinVar missense of another variant at the same MANE Select residue: 2 Pathogenic, 1 Likely pathogenic, 0 none.
    clinvar_same_aa: int = 0
    clinvar_same_residue: int = 0
    gene_plp: int = 0
    gene_plp_missense: int = 0
    gene_plp_lof: int = 0
    gene_blb_missense: int = 0
    exon_rank: int | None = None
    exon_total: int | None = None
    # Canonical splice site: skipping its exon shifts the frame (Ensembl start and end phases differ); null if unknown.
    splice_frameshift: bool | None = None
    gnomad_hom: int | None = None
    local_hom_not_affected: int = 0
    omim_inheritance: list[str] = []
    # ClinGen: haploinsufficiency score; every gene-disease validity is Disputed, Refuted or No Known Disease.
    clingen_hi_score: int | None = None
    clingen_no_disease: bool = False
    # Carriers and sequenced patients of the tenant, from the ETL's frequencies (PS4).
    affected_carriers: int = 0
    affected_total: int = 0
    not_affected_carriers: int = 0
    not_affected_total: int = 0
    # The case's own evidence, from occurrence_facts.sql: counts over its affected members (the proband for now).
    denovo_confirmed: int = 0
    denovo_assumed: int = 0
    affected_carrier_parents: int = 0
    affected_noncarrier_parents: int = 0
    comphet_p_trans: int = 0
    comphet_lp_trans: int = 0
    comphet_p_unphased: int = 0
    comphet_lp_unphased: int = 0
    comphet_partner_locus_id: int | None = None


OCCURRENCE_FIELDS = [
    "denovo_confirmed",
    "denovo_assumed",
    "affected_carrier_parents",
    "affected_noncarrier_parents",
    "comphet_p_trans",
    "comphet_lp_trans",
    "comphet_p_unphased",
    "comphet_lp_unphased",
    "comphet_partner_locus_id",
]


class Detail(BaseModel):
    name: str
    value: float | None


class Evidence(BaseModel):
    code: str
    strength: Strength
    points: int
    value: float | None
    threshold: float | None
    # Which branch met the criterion (e.g. PP3 by revel or spliceai), the other values it tested, the loci it cites.
    source: str | None
    details: list[Detail]
    refs: list[int]


class Cancelled(BaseModel):
    """A criterion met, then dropped at classification by an exclusion or a point cap."""

    criterion: str
    cancelled_by: str


class AuditCriteria(BaseModel):
    """Every criterion the engine evaluates; null when not met."""

    BA1: Evidence | None = None
    BS1: Evidence | None = None
    BS2: Evidence | None = None
    BS4: Evidence | None = None
    PM2: Evidence | None = None
    PM1: Evidence | None = None
    PM3: Evidence | None = None
    PM4: Evidence | None = None
    PM6: Evidence | None = None
    PS2: Evidence | None = None
    PS1: Evidence | None = None
    PM5: Evidence | None = None
    PS4: Evidence | None = None
    PP1: Evidence | None = None
    PP2: Evidence | None = None
    PP3: Evidence | None = None
    BP1: Evidence | None = None
    BP2: Evidence | None = None
    BP4: Evidence | None = None
    BP7: Evidence | None = None
    PVS1: Evidence | None = None
    # Only a ClinGen VCEP gives these: nothing to compute them from.
    PS3: Evidence | None = None
    PP4: Evidence | None = None
    PP5: Evidence | None = None
    BS3: Evidence | None = None
    BP3: Evidence | None = None
    BP5: Evidence | None = None
    BP6: Evidence | None = None


class Audit(BaseModel):
    ruleset_version: str
    criteria: AuditCriteria


class Classification(BaseModel):
    """One row of the output table."""

    locus_id: int
    transcript_id: str
    acmg_class: AcmgClass
    acmg_points: int
    acmg_criteria: list[str]
    acmg_cancelled: list[Cancelled]
    acmg_audit: Audit
    ruleset_version: str


_SCALARS = {int: pl.Int64, float: pl.Float64, str: pl.String, bool: pl.Boolean}
_SCALARS |= {Strength: pl.String, AcmgClass: pl.String}


def _dtype(annotation) -> pl.DataType:
    if get_origin(annotation) in (Union, types.UnionType):
        (inner,) = [a for a in get_args(annotation) if a is not type(None)]
        return _dtype(inner)
    if get_origin(annotation) is list:
        return pl.List(_dtype(get_args(annotation)[0]))
    if isinstance(annotation, type) and issubclass(annotation, BaseModel):
        return pl.Struct(polars_schema(annotation))
    return _SCALARS[annotation]


def polars_schema(model: type[BaseModel]) -> dict[str, pl.DataType]:
    return {name: _dtype(field.annotation) for name, field in model.model_fields.items()}


FACT_SCHEMA = polars_schema(Fact)
VARIANT_FACT_SCHEMA = {k: v for k, v in FACT_SCHEMA.items() if k not in OCCURRENCE_FIELDS}
# BP2 reads the gene's inheritance next to the case's comphet partner.
OCCURRENCE_FACT_SCHEMA = {
    "seq_id": pl.Int64,
    "locus_id": pl.Int64,
    "omim_inheritance": FACT_SCHEMA["omim_inheritance"],
} | {k: FACT_SCHEMA[k] for k in OCCURRENCE_FIELDS}
EVIDENCE_DTYPE = pl.Struct(polars_schema(Evidence))
DETAILS_DTYPE = pl.List(pl.Struct(polars_schema(Detail)))
CANCELLED_DTYPE = pl.Struct(polars_schema(Cancelled))
CLASSIFICATION_SCHEMA = polars_schema(Classification)
