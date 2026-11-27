from pathlib import Path

import polars as pl
from acmg import erepo
from acmg.models import Ruleset
from acmg.rules import combine, to_wide

RULESET = Ruleset.load(Path(__file__).parents[1] / "ruleset.yaml")
HEADER = [
    "#Variation", "ClinVar Variation Id", "Allele Registry Id", "HGVS Expressions", "HGNC Gene Symbol", "Disease",
    "Mondo Id", "Mode of Inheritance", "Assertion", "Applied Evidence Codes (Met)", "Applied Evidence Codes (Not Met)",
    "Summary of interpretation", "PubMed Articles", "Expert Panel", "Guideline", "Approval Date", "Published Date",
    "Retracted", "Evidence Repo Link", "Uuid",
]  # fmt: skip


def export(*rows: dict) -> str:
    """A tab-separated export: each row sets the columns it cares about (variation id, met codes, ...)."""
    lines = ["\t".join(HEADER)]
    for row in rows:
        base = {"Retracted": "false", "Approval Date": "2020-01-01", "Uuid": "u", "Assertion": "Pathogenic"}
        lines.append("\t".join(({**base, **row}).get(c, "") for c in HEADER))
    return "\n".join(lines) + "\n"


def test_codes_map_to_criterion_strength_and_points():
    rows, unmapped = erepo.evidence("PM3_Very Strong, PS3_Supporting,PVS1,BS1_Supporting,BA1, PM2_Supporting", RULESET)
    got = {r["criterion"]: (r["code"], r["strength"], r["points"]) for r in rows}
    assert got == {
        "PM3": ("PM3_VeryStrong", "VeryStrong", 8),
        "PS3": ("PS3_Supporting", "Supporting", 1),
        "PVS1": ("PVS1", "VeryStrong", 8),
        "BS1": ("BS1_Supporting", "Supporting", -1),
        "BA1": ("BA1", "StandAlone", -8),
        "PM2": ("PM2_Supporting", "Supporting", 1),
    }
    assert unmapped == []


def test_unknown_codes_are_reported_not_dropped_silently():
    rows, unmapped = erepo.evidence("PP4_Moderate,PVS1_RNA,PM9,BP4_Huge", RULESET)
    assert [r["code"] for r in rows] == ["PP4_Moderate"]
    assert unmapped == ["PVS1_RNA", "PM9", "BP4_Huge"]


def test_latest_non_retracted_classification_wins_and_unusable_rows_are_left_out():
    text = export(
        {"ClinVar Variation Id": "1", "Applied Evidence Codes (Met)": "PS3", "Approval Date": "2020-01-01"},
        {"ClinVar Variation Id": "1", "Applied Evidence Codes (Met)": "PM2", "Approval Date": "2022-05-01",
         "Expert Panel": "B VCEP", "Disease": "d", "Mondo Id": "MONDO:1", "Evidence Repo Link": "https://x/1",
         "PubMed Articles": "30, 7, 30"},
        {"ClinVar Variation Id": "2", "Applied Evidence Codes (Met)": "PS3", "Retracted": "true"},
        {"ClinVar Variation Id": "-", "Applied Evidence Codes (Met)": "PS3"},
        {"ClinVar Variation Id": "3", "Applied Evidence Codes (Met)": ""},
        {"ClinVar Variation Id": "4", "Applied Evidence Codes (Met)": "PS9"},
    )  # fmt: skip
    df, unmapped = erepo.classifications(text, RULESET)
    assert df["variation_id"].to_list() == ["1"]
    row = df.row(0, named=True)
    assert (row["panel"], row["disease"], row["mondo_id"], row["url"]) == ("B VCEP", "d", "MONDO:1", "https://x/1")
    assert row["pmids"] == [7, 30] and [c["code"] for c in row["criteria"]] == ["PM2"]
    assert dict(unmapped) == {"PS9": 1}
    assert df.schema == erepo.SCHEMA


def test_vcep_criteria_are_combined_with_the_existing_logic():
    # PS3_Moderate + PM3_Strong + PM2_Supporting + PP4_Moderate = 2 + 4 + 1 + 2 = 9 points: Likely pathogenic.
    rows, _ = erepo.evidence("PS3_Moderate,PM3_Strong,PM2_Supporting,PP4_Moderate", RULESET)
    keys = ["seq_id", "locus_id", "transcript_id"]
    gathered = pl.DataFrame({"seq_id": [1], "locus_id": [1], "transcript_id": ["T"], "criteria": [rows]})
    (result,) = combine(to_wide(gathered, keys), RULESET, keys).collect().to_dicts()
    assert (result["acmg_class"], result["acmg_points"]) == ("Likely pathogenic", 9)
    assert result["acmg_criteria"] == ["PM2_Supporting", "PM3_Strong", "PS3_Moderate", "PP4_Moderate"]
