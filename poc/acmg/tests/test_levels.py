from argparse import Namespace
from pathlib import Path

import polars as pl
from acmg.exchange import scope
from acmg.models import FACT_SCHEMA, OCCURRENCE_FACT_SCHEMA, VARIANT_FACT_SCHEMA, Fact, Ruleset
from acmg.rules import OCCURRENCE_CRITERIA, VARIANT_CRITERIA, classify, combine, evaluate, to_wide

RULESET = Ruleset.load(Path(__file__).parents[1] / "ruleset.yaml")
KEYS = ["seq_id", "locus_id", "transcript_id"]
FACTS = [
    {"consequences": ["stop_gained"], "gnomad_loeuf": 0.3, "gene_plp_lof": 2, "denovo_probands": 1},  # PVS1, PM2, PM6
    {"consequences": ["missense_variant"], "revel_score": 0.95, "comphet_p_trans": 1, "omim_inheritance": ["AD"]},
    {"gnomad_af": 0.2, "gnomad_an": 150_000, "affected_noncarrier_parents": 1, "omim_inheritance": ["AD"]},  # BA1, BS4
    {"gnomad_af": 0.001, "gnomad_an": 150_000},  # nothing met
]


def three_steps(rows: list[dict]) -> pl.DataFrame:
    """The pipeline's path: criteria per level, gathered per occurrence as the classification query does."""
    full = pl.DataFrame(
        [Fact(locus_id=i, transcript_id=f"T{i}", **r).model_dump() for i, r in enumerate(rows)], schema=FACT_SCHEMA
    ).with_columns(seq_id=pl.lit(7, pl.Int64))
    variant = evaluate(full.select(list(VARIANT_FACT_SCHEMA)), RULESET, VARIANT_CRITERIA, ["locus_id", "transcript_id"])
    occurrence = evaluate(
        full.select(list(OCCURRENCE_FACT_SCHEMA)), RULESET, OCCURRENCE_CRITERIA, ["seq_id", "locus_id"]
    )
    criteria = pl.concat(
        [
            variant.collect().join(full.select("seq_id", "locus_id"), on="locus_id"),
            occurrence.collect().join(full.select("locus_id", "transcript_id"), on="locus_id"),
        ],
        how="diagonal",
    )
    gathered = full.select(KEYS).join(
        criteria.group_by(KEYS).agg(
            criteria=pl.struct("criterion", "code", "strength", "points", "value", "threshold")
        ),
        on=KEYS,
        how="left",
    )
    return combine(to_wide(gathered, KEYS), RULESET, KEYS).collect().sort("locus_id")


def test_three_steps_give_the_one_pass_classification():
    full = pl.DataFrame(
        [Fact(locus_id=i, transcript_id=f"T{i}", **r).model_dump() for i, r in enumerate(FACTS)], schema=FACT_SCHEMA
    )
    expected = classify(full, RULESET).collect().sort("locus_id")
    got = three_steps(FACTS)
    columns = ["locus_id", "acmg_class", "acmg_points", "acmg_criteria"]
    assert got.select(columns).to_dicts() == expected.select(columns).to_dicts()


def test_exclusion_across_levels_applies_at_classification():
    # BP2 (occurrence) cancels PM3 (occurrence), PVS1 (variant) cancels PP3 (variant): settled only when combined.
    rows = three_steps(FACTS).to_dicts()
    assert "PM3" not in rows[1]["acmg_criteria"] and "BP2" in rows[1]["acmg_criteria"]


def test_occurrence_without_criteria_is_vus_with_zero_points():
    last = three_steps(FACTS).row(3, named=True)
    assert (last["acmg_class"], last["acmg_points"], last["acmg_criteria"]) == ("VUS", 0, [])


def test_evaluate_keeps_criteria_met_only():
    facts = pl.DataFrame([Fact(locus_id=1, transcript_id="T1", **FACTS[2]).model_dump()], schema=FACT_SCHEMA)
    rows = evaluate(facts, RULESET, VARIANT_CRITERIA, ["locus_id", "transcript_id"]).collect()
    assert sorted(rows["criterion"]) == ["BA1", "BS1"]  # BS1 too: exclusions wait for the classification


def test_scope_selects_affected_probands_and_mane_select():
    args = Namespace(
        shared_db="s", tenant_db="t", output_db="o", tenant_code="x", clinical_db="c.d", part=3, seq_ids=None,
        all_transcripts=False,
    )
    params = scope(args, RULESET)
    assert "se.family_role = 'proband' AND se.affected_status = 'affected'" in params["cases"]
    # The part narrows the cases too: their old rows are the ones a rerun deletes.
    assert "p.part = 3" in params["cases"]
    assert (
        "c.is_mane_select AND c.transcript_id LIKE 'ENST%'" in params["classified"]
        and "'missense_variant'" in params["classified"]
    )
    assert params["occurrence_part"] == "AND o.part = 3" and params["comphet_part"] == "AND ch.part = 3"
