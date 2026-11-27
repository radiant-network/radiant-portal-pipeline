"""ClinGen Evidence Repository (VCEP) classifications, from the bulk export copied under s3://<bucket>/erepo/.

A variant a VCEP curated takes the VCEP's criteria in place of the computed ones (sql/vcep_criteria.sql); the class
is then combined as for any other variant. Matched on the ClinVar Variation ID, which is `clinvar.name`.
"""

import io
import re
from collections import Counter

import polars as pl
from acmg.models import DEFAULT_STRENGTH, Ruleset, Strength, criterion_code
from acmg.rules import CRITERIA

FILE = "erepo/erepo.tsv"
CRITERIA_DTYPE = pl.List(
    pl.Struct({"criterion": pl.String, "code": pl.String, "strength": pl.String, "points": pl.Int64})
)
SCHEMA = {
    "variation_id": pl.String,
    "assertion": pl.String,
    "panel": pl.String,
    "disease": pl.String,
    "mondo_id": pl.String,
    "url": pl.String,
    "pmids": pl.List(pl.Int64),
    "criteria": CRITERIA_DTYPE,
}
# The export's strength suffixes (PM3_Very Strong, BA1 as BP4_Stand Alone).
STRENGTHS = {
    "Supporting": Strength.SUPPORTING,
    "Moderate": Strength.MODERATE,
    "Strong": Strength.STRONG,
    "Very Strong": Strength.VERY_STRONG,
    "Stand Alone": Strength.STAND_ALONE,
}
CODE = re.compile(r"(?P<criterion>[A-Z]+\d)(?:_(?P<strength>[A-Za-z ]+))?")


def evidence(met: str | None, rs: Ruleset) -> tuple[list[dict], list[str]]:
    """The criteria of an export's "Applied Evidence Codes (Met)" cell as evidence rows, and the codes not mapped."""
    rows, unmapped = {}, []
    for raw in (met or "").split(","):
        code = raw.strip()
        m = CODE.fullmatch(code)
        if not code:
            continue
        if not m or m["criterion"] not in CRITERIA or (m["strength"] and m["strength"] not in STRENGTHS):
            unmapped.append(code)
            continue
        criterion = m["criterion"]
        strength = STRENGTHS[m["strength"]] if m["strength"] else DEFAULT_STRENGTH[criterion.rstrip("0123456789")]
        points = rs.points[strength] * (-1 if criterion.startswith("B") else 1)
        rows[criterion] = {
            "criterion": criterion,
            "code": criterion_code(criterion, strength),
            "strength": str(strength),
            "points": points,
        }
    return list(rows.values()), unmapped


def classifications(text: str, rs: Ruleset) -> tuple[pl.DataFrame, Counter]:
    """One row per ClinVar variation with at least one criterion met, and the count of each unmapped code.
    The latest approved classification wins when a variant has several (panels, diseases). Not in ClinVar yet ("-"),
    retracted, or no criterion met: left out."""
    raw = pl.read_csv(io.StringIO(text), separator="\t", infer_schema=False, quote_char=None)
    latest = (
        raw.filter((pl.col("Retracted") == "false") & pl.col("ClinVar Variation Id").str.contains(r"^\d+$"))
        .sort("Approval Date", "Uuid", descending=True)
        .unique("ClinVar Variation Id", keep="first", maintain_order=True)
    )
    rows, unmapped = [], Counter()
    for r in latest.to_dicts():
        criteria, bad = evidence(r["Applied Evidence Codes (Met)"], rs)
        unmapped.update(bad)
        if criteria:
            pmids = sorted({int(p) for p in re.findall(r"\d+", r["PubMed Articles"] or "")})
            rows.append(
                {
                    "variation_id": r["ClinVar Variation Id"],
                    "assertion": r["Assertion"],
                    "panel": r["Expert Panel"],
                    "disease": r["Disease"],
                    "mondo_id": r["Mondo Id"],
                    "url": r["Evidence Repo Link"],
                    "pmids": pmids,
                    "criteria": criteria,
                }
            )
    return pl.DataFrame(rows, schema=SCHEMA), unmapped
