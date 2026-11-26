import math
import random
from types import SimpleNamespace

import polars as pl
import pytest
from acmg import hpo, simulate
from acmg.models import Pp4Rule, Pp4Table
from pydantic import ValidationError

# HP:0000118 <- A <- A1, A2 ; HP:0000118 <- B ; X is outside the phenotypic abnormalities.
OBO = """format-version: 1.2

[Term]
id: HP:0000001
name: All

[Term]
id: HP:0000118
name: Phenotypic abnormality
is_a: HP:0000001 ! All

[Term]
id: HP:A
alt_id: HP:OLD
is_a: HP:0000118 ! Phenotypic abnormality

[Term]
id: HP:A1
is_a: HP:A ! A

[Term]
id: HP:A2
is_a: HP:A ! A

[Term]
id: HP:B
is_a: HP:0000118 ! Phenotypic abnormality

[Term]
id: HP:X
is_a: HP:0000001 ! All

[Term]
id: HP:GONE
is_obsolete: true

[Typedef]
id: part_of
"""
GENES = (
    "ncbi_gene_id\tgene_symbol\thpo_id\thpo_name\tfrequency\tdisease_id\n"
    "1\tG1\tHP:A1\ta1\t-\tOMIM:1\n"
    "2\tG2\tHP:A2\ta2\t-\tOMIM:2\n"
    "3\tG3\tHP:B\tb\t-\tOMIM:3\n"
    "3\tG3\tHP:X\tx\t-\tOMIM:3\n"
)


def load():
    closure, alt = hpo.ontology(OBO)
    return closure, alt, hpo.gene_terms(closure, hpo.annotations(GENES))


def test_ontology_keeps_phenotypic_abnormalities_with_their_ancestors():
    closure, alt = hpo.ontology(OBO)
    ancestors = {t: sorted(g) for t, g in closure.group_by("term").agg("ancestor").iter_rows()}
    assert ancestors["HP:A1"] == ["HP:0000118", "HP:A", "HP:A1"]
    assert set(ancestors) == {"HP:0000118", "HP:A", "HP:A1", "HP:A2", "HP:B"}
    assert alt.rows() == [("HP:OLD", "HP:A")]


def test_information_content_counts_genes_under_each_term():
    _, _, terms = load()
    ic = dict(terms.select("term", "ic").unique().iter_rows())
    assert math.isclose(ic["HP:0000118"], 0.0, abs_tol=1e-12)
    assert math.isclose(ic["HP:A"], math.log(3 / 2))
    assert math.isclose(ic["HP:A1"], math.log(3))


def test_similarity_ranks_the_exact_gene_first_and_its_sibling_second():
    closure, alt, terms = load()
    patients = hpo.normalize(pl.DataFrame({"case": [1], "hpo_id": ["HP:A1"]}), alt)
    rows = {r["symbol"]: r for r in hpo.similarity(patients, closure, terms).to_dicts()}
    assert math.isclose(rows["G1"]["score"], math.log(3)) and math.isclose(rows["G1"]["top"], 1 / 3)
    assert math.isclose(rows["G2"]["score"], math.log(3 / 2)) and math.isclose(rows["G2"]["top"], 2 / 3)
    assert "G3" not in rows


def test_similarity_does_not_depend_on_row_order():
    closure, _, terms = load()
    patients = pl.DataFrame({"case": [1, 1, 1, 2], "hpo_id": ["HP:A1", "HP:A2", "HP:B", "HP:A"]})
    runs = [
        hpo.similarity(patients.sample(fraction=1.0, shuffle=True, seed=s), closure,
                       terms.sample(fraction=1.0, shuffle=True, seed=s)).sort("case", "symbol")
        for s in range(20)
    ]
    assert all(r.equals(runs[0]) for r in runs)


def test_normalize_maps_secondary_ids():
    _, alt, _ = load()
    assert hpo.normalize(pl.DataFrame({"case": [1], "hpo_id": ["HP:OLD"]}), alt)["hpo_id"].to_list() == ["HP:A"]


def test_points_follow_the_supporting_ratio():
    assert hpo.points(1.0) == 0
    assert hpo.points(hpo.SUPPORTING_LR) == 1
    assert hpo.points(350) == 8
    assert hpo.points(0.5) < 0


def test_pp4_points_take_the_table_of_the_case_terms():
    rule = Pp4Rule(
        blocked_by=["BP4", "BP7"],
        tables=[
            Pp4Table(min_terms=1, bins={0.05: 4, 0.25: 2}),
            Pp4Table(min_terms=2, bins={0.01: 4, 0.05: 3}),
        ]
    )
    genes = pl.DataFrame({"n_terms": [1, 1, 1, 3, 3, 3], "top": [0.01, 0.2, 0.5, 0.005, 0.03, 0.2]})
    assert genes.select(hpo.pp4_points(rule))["literal"].to_list() == [4, 2, 0, 4, 3, 0]


def test_pp4_tables_must_start_at_one_term():
    with pytest.raises(ValidationError):
        Pp4Rule(blocked_by=[], tables=[Pp4Table(min_terms=2, bins={0.01: 4})])


def test_spike_rank_counts_ties_half():
    values, spike = pl.Series([5, 3, 3, 1]), pl.Series([False, True, False, False])
    assert simulate.spike_rank(values, spike) == 2.5


def test_simulated_phenotype_draws_terms_then_noise():
    args = SimpleNamespace(n_terms=2, n_noise=1, imprecision=0.0)
    terms = simulate.simulated_phenotype(random.Random(1), ["HP:A1", "HP:A2", "HP:B"], {}, ["HP:B"], args)
    assert len(terms) == 3 and terms[-1] == "HP:B"


def test_pp4_raises_a_phenotype_matching_spike_above_an_equal_background():
    closure, _, terms = load()
    back = pl.DataFrame(
        {
            "seq_id": [10, 10],
            "locus_id": [1, 2],
            "symbol": ["G2", "G3"],
            "acmg_points": [3, 3],
            "inheritance_penalized": [False, True],
            "pp4_blocked": [False, False],
        }
    )
    cases = [
        {"sim": i, "seq_id": 10, "phenotype": ["HP:A1"], "locus_id": 99, "symbol": "G1", "acmg_points": 3,
         "pp4_blocked": False}
        for i in range(4)
    ]
    rows = simulate.simulate(cases, back, closure, terms)
    assert rows.height == 12 and rows.filter(pl.col("is_spike"))["top"].to_list() == [1 / 3] * 4
    table = simulate.calibrate(rows, max_points=4)
    assert table.filter(pl.col("bin") == "0.25")["pp4_points"][0] == 0
    assert table.filter(pl.col("bin") == "1.0")["pp4_points"][0] == 0
    report = simulate.evaluate(rows, table)
    assert report["acmg"]["median_rank"] == 2.0
    assert report["phenotype"]["top1"] == 1.0
    # G3, a single heterozygous variant of a recessive gene, drops below the spike.
    assert report["acmg_pp4_fit4"]["median_rank"] == 1.5


def test_pp4_blocked_by_criterion_at_any_strength():
    criteria = pl.DataFrame({"c": ["PM2_Supporting,BP4_Moderate", "PM2_Supporting,BP7", "BS2,PVS1", "", None]})
    flags = criteria.select(simulate.pp4_blocked(pl.col("c"), ["BP4", "BP7"]))["c"].to_list()
    assert flags == [True, True, False, False, False]
