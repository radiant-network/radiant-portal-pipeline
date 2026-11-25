from pathlib import Path

import polars as pl
import pytest
from acmg.models import FACT_SCHEMA, AuditCriteria, Classification, Fact, Ruleset
from acmg.rules import CRITERIA, classify
from pydantic import ValidationError

RULESET = Ruleset.load(Path(__file__).parents[1] / "ruleset.yaml")
COMMON = {"gnomad_an": 150_000}


def run(**fields) -> Classification:
    fact = Fact(locus_id=1, transcript_id="T1", **fields)
    (row,) = classify(pl.DataFrame([fact.model_dump()], schema=FACT_SCHEMA), RULESET).collect().to_dicts()
    return Classification.model_validate(row)


def codes(**fields) -> list[str]:
    return run(**fields).acmg_criteria


def test_absent_from_gnomad_is_pm2():
    assert codes() == ["PM2_Supporting"]


def test_pm2_edge_is_inclusive():
    assert codes(gnomad_af=0.0001, **COMMON) == ["PM2_Supporting"]
    assert codes(gnomad_af=0.00011, **COMMON) == []


def test_common_in_not_affected_cohort_is_bs1_not_pm2():
    # Carrier frequencies: 4 % carriers = 2.02 % AF, 11 % carriers = 5.7 % AF.
    assert codes(germline_pf_wgs_not_affected=0.04, germline_pn_wgs_not_affected=50) == ["BS1"]
    assert codes(germline_pf_wxs_not_affected=0.11, germline_pn_wxs_not_affected=50) == ["BA1"]
    assert codes(germline_pf_wxs_not_affected=0.019, germline_pn_wxs_not_affected=50) == ["PM2_Supporting"]  # 0.96 % AF


def test_cohort_frequency_spares_clinvar_plp():
    common = {"germline_pf_wxs_not_affected": 0.11, "germline_pn_wxs_not_affected": 50}
    assert codes(**common, clinvar_plp=True) == []


def test_cohort_frequency_overrides_a_lof():
    lof = {"consequences": ["stop_gained"], "gnomad_loeuf": 0.3, "gene_plp_lof": 2, "gnomad_af": 0.001, **COMMON}
    result = run(**lof, germline_pf_wxs_not_affected=0.05, germline_pn_wxs_not_affected=300)
    assert (result.acmg_criteria, result.acmg_class) == (["BS1", "PVS1"], "VUS")


def test_pm2_kept_when_not_affected_cohort_too_small():
    assert codes(germline_pf_wxs_not_affected=0.5, germline_pn_wxs_not_affected=4) == ["PM2_Supporting"]


def test_ba1_cancels_bs1_and_forces_benign():
    result = run(gnomad_af=0.06, **COMMON)
    assert result.acmg_criteria == ["BA1"]
    assert result.acmg_class == "Benign"


def test_ba1_needs_enough_alleles():
    assert codes(gnomad_af=0.06, gnomad_an=1000) == []


def test_ba1_edge_is_exclusive():
    assert codes(gnomad_af=0.05, **COMMON) == ["BS1"]


def test_bs1():
    result = run(gnomad_af=0.02, **COMMON)
    assert (result.acmg_criteria, result.acmg_points) == (["BS1"], -4)


@pytest.mark.parametrize(
    ("score", "code"),
    [(0.932, "PP3_Strong"), (0.8, "PP3_Moderate"), (0.644, "PP3"), (0.5, None), (0.29, "BP4"), (0.1, "BP4_Moderate"),
     (0.01, "BP4_Strong"), (0.003, "BP4_VeryStrong")],
)  # fmt: skip
def test_revel_grades(score, code):
    assert codes(revel_score=score, gnomad_af=0.001, **COMMON) == ([code] if code else [])


def test_spliceai_gives_pp3_supporting_without_revel():
    assert codes(spliceai_ds=0.2, gnomad_af=0.001, **COMMON) == ["PP3"]


def test_revel_beats_spliceai_when_stronger():
    assert codes(revel_score=0.95, spliceai_ds=0.5, gnomad_af=0.001, **COMMON) == ["PP3_Strong"]


def test_splice_signal_vetoes_bp4():
    assert codes(revel_score=0.1, spliceai_ds=0.15, gnomad_af=0.001, **COMMON) == []


def test_bp7_synonymous_low_spliceai():
    assert codes(consequences=["synonymous_variant"], spliceai_ds=0.05, gnomad_af=0.001, **COMMON) == ["BP7"]
    assert codes(consequences=["synonymous_variant"], spliceai_ds=0.3, gnomad_af=0.001, **COMMON) == ["PP3"]


def test_pvs1_cancels_pp3_and_scores_likely_pathogenic_with_pm2():
    result = run(consequences=["stop_gained"], gnomad_loeuf=0.3, gene_plp_lof=2, spliceai_ds=0.9)
    assert result.acmg_criteria == ["PM2_Supporting", "PVS1"]
    assert (result.acmg_points, result.acmg_class) == (9, "Likely pathogenic")


def test_pvs1_cancels_pm4_and_bp4():
    lof = {"consequences": ["frameshift_variant", "stop_lost"], "gnomad_loeuf": 0.3, "gene_plp_lof": 2, **NOT_RARE}
    assert codes(**lof) == ["PVS1"]
    assert codes(**lof, revel_score=0.01) == ["PVS1"]


def test_pvs1_needs_lof_mechanism_in_clinvar():
    lof = {"consequences": ["stop_gained"], "gnomad_loeuf": 0.3, **NOT_RARE}
    assert codes(**lof, gene_plp_lof=1) == []
    assert codes(**lof, gene_plp_lof=2) == ["PVS1"]


def test_pvs1_needs_constrained_gene():
    assert codes(consequences=["frameshift_variant"], gnomad_loeuf=0.6, gnomad_af=0.001, **COMMON) == []


def test_vus_from_pp3_strong_and_pm2():
    result = run(revel_score=0.95)
    assert (result.acmg_points, result.acmg_class) == (5, "VUS")


def test_likely_benign_then_benign_at_minus_seven():
    assert run(gnomad_af=0.02, revel_score=0.2, **COMMON).acmg_class == "Likely benign"  # -4 -1
    assert run(gnomad_af=0.02, revel_score=0.01, **COMMON).acmg_class == "Benign"  # -4 -4


MISSENSE = {"consequences": ["missense_variant"], "gnomad_af": 0.001, **COMMON}
HOTSPOT = {"clinvar_plp_missense_window": 4}


def test_pm1_in_hotspot():
    assert codes(**MISSENSE, **HOTSPOT) == ["PM1"]


def test_pm1_blocked_by_benign_in_window():
    assert codes(**MISSENSE, **HOTSPOT, clinvar_blb_missense_window=1) == []


def test_pm1_needs_missense():
    assert codes(consequences=["intron_variant"], gnomad_af=0.001, **COMMON, **HOTSPOT) == []


def test_pm1_dropped_when_pm1_and_pp3_exceed_four_points():
    result = run(**MISSENSE, **HOTSPOT, revel_score=0.95)
    assert (result.acmg_criteria, result.acmg_points) == (["PP3_Strong"], 4)


def test_pm1_kept_with_pp3_moderate():
    result = run(**MISSENSE, **HOTSPOT, revel_score=0.8)
    assert (result.acmg_criteria, result.acmg_points) == (["PM1", "PP3_Moderate"], 4)


def test_pp2_in_missense_gene():
    assert codes(**MISSENSE, gene_plp=20, gene_plp_missense=6, gene_blb_missense=1) == ["PP2"]
    assert codes(**MISSENSE, gene_plp=20, gene_plp_missense=5) == []


def test_pp2_blocked_by_benign_missense():
    assert codes(**MISSENSE, gene_plp=20, gene_plp_missense=12, gene_blb_missense=3) == ["PP2"]
    assert codes(**MISSENSE, gene_plp=20, gene_plp_missense=12, gene_blb_missense=4) == []


def test_bp1_in_lof_gene():
    assert codes(**MISSENSE, gene_plp=20, gene_plp_lof=16) == ["BP1"]


def test_gene_criteria_need_enough_plp():
    assert codes(**MISSENSE, gene_plp=9, gene_plp_lof=9) == []
    assert codes(**MISSENSE, gene_plp=9, gene_plp_missense=9) == []


def test_audit_carries_value_threshold_and_version():
    audit = run(gnomad_af=0.02, **COMMON).acmg_audit
    assert audit.ruleset_version == RULESET.version
    assert (audit.criteria.BS1.value, audit.criteria.BS1.threshold, audit.criteria.BS1.points) == (0.02, 0.01, -4)
    assert audit.criteria.PM2 is None


def test_audit_lists_every_criterion():
    assert list(AuditCriteria.model_fields) == list(CRITERIA)


NOT_RARE = {"gnomad_af": 0.001, **COMMON}  # no PM2, no BA1/BS1


def test_pvs1_strong_in_last_exon():
    stop = {"consequences": ["stop_gained"], "gnomad_loeuf": 0.3, "gene_plp_lof": 2, **NOT_RARE}
    assert codes(**stop, exon_rank=5, exon_total=5) == ["PVS1_Strong"]
    assert codes(**stop, exon_rank=4, exon_total=5) == ["PVS1"]


def test_pvs1_splice_keeps_very_strong_in_last_exon():
    assert codes(
        consequences=["splice_donor_variant"], gnomad_loeuf=0.3, gene_plp_lof=2, exon_rank=5, exon_total=5, **NOT_RARE
    ) == ["PVS1"]


def test_pm4_in_frame_indel():
    assert codes(consequences=["inframe_deletion"], **NOT_RARE) == ["PM4"]


def test_bs2_gnomad_or_local_homozygotes_in_omim_gene():
    assert codes(omim_inheritance=["AR"], gnomad_hom=2, **NOT_RARE) == ["BS2"]
    assert codes(omim_inheritance=["AR"], local_hom_not_affected=2, **NOT_RARE) == ["BS2"]
    assert codes(gnomad_hom=5, **NOT_RARE) == []  # no OMIM inheritance


@pytest.mark.parametrize(
    ("probands", "code"), [(1, "PM6_Supporting"), (2, "PM6"), (4, "PM6_Strong"), (8, "PM6_VeryStrong")]
)
def test_pm6_svi_points_from_assumed_de_novo(probands, code):
    assert codes(denovo_probands=probands, **NOT_RARE) == [code]


def test_common_variant_called_de_novo_gets_no_pm6():
    assert codes(denovo_probands=1, gnomad_af=0.02, **COMMON) == ["BS1"]
    assert codes(denovo_probands=1, gnomad_af=0.06, **COMMON) == ["BA1"]


def test_pm3_svi_points_from_comphet_partners():
    assert codes(comphet_p_trans=1, **NOT_RARE) == ["PM3"]  # 1 point
    assert codes(comphet_lp_unphased=1, **NOT_RARE) == []  # 0.25 point
    assert codes(comphet_lp_unphased=2, **NOT_RARE) == ["PM3_Supporting"]  # 0.5 point


COHORT = {"affected_total": 700, "not_affected_total": 1000}


def test_ps4_enriched_in_affected():
    assert codes(affected_carriers=6, **COHORT) == ["PM2_Supporting", "PS4"]
    assert codes(affected_carriers=5, **COHORT) == ["PM2_Supporting"]  # OR 16 but CI reaches 1
    assert codes(affected_carriers=6, **COHORT, **NOT_RARE) == []


def test_ps4_not_for_a_lab_artefact_in_both_groups():
    assert codes(affected_carriers=30, not_affected_carriers=40, **COHORT) == ["PM2_Supporting"]


def test_pp1_meioses():
    assert codes(affected_carrier_parents=2, **NOT_RARE) == []
    assert codes(affected_carrier_parents=3, **NOT_RARE) == ["PP1"]
    assert codes(affected_carrier_parents=5, **NOT_RARE) == ["PP1_Moderate"]


def test_bs4_affected_noncarrier_parent_in_dominant_only_gene():
    result = run(omim_inheritance=["AD"], affected_noncarrier_parents=1, **NOT_RARE)
    assert (result.acmg_criteria, result.acmg_points) == (["BS4"], -4)
    assert codes(omim_inheritance=["AD", "AR"], affected_noncarrier_parents=1, **NOT_RARE) == []
    assert codes(affected_noncarrier_parents=1, **NOT_RARE) == []


def test_bp2_in_dominant_only_gene_cancels_pm3():
    assert codes(omim_inheritance=["AD"], comphet_p_trans=1, **NOT_RARE) == ["BP2"]
    assert codes(omim_inheritance=["AD", "AR"], comphet_p_trans=1, **NOT_RARE) == ["PM3"]


def test_ruleset_rejects_unordered_grades():
    data = RULESET.model_dump(mode="json")
    data["pp1"] = {"Strong": 2, "Moderate": 3, "Supporting": 1}
    with pytest.raises(ValidationError, match="strongest first"):
        Ruleset.model_validate(data)


def test_ruleset_rejects_overlapping_revel_ranges():
    data = RULESET.model_dump(mode="json")
    data["revel"]["bp4"]["Supporting"] = 0.7
    with pytest.raises(ValidationError, match="overlap"):
        Ruleset.model_validate(data)


def test_ruleset_rejects_unordered_frequencies():
    data = RULESET.model_dump(mode="json")
    data["frequency"]["bs1_min_af"] = 0.1
    with pytest.raises(ValidationError, match="pm2_max_af < bs1_min_af < ba1_min_af"):
        Ruleset.model_validate(data)


def test_pvs1_from_clingen_haploinsufficiency():
    stop = {"consequences": ["stop_gained"], **NOT_RARE}
    assert codes(**stop, clingen_hi_score=3) == ["PVS1"]
    assert codes(**stop, clingen_hi_score=30) == ["PVS1"]
    assert codes(**stop, clingen_hi_score=2) == []
    assert codes(**stop, clingen_hi_score=40, gnomad_loeuf=0.3, gene_plp_lof=2) == []


def test_gene_without_disease_drops_pathogenic_gene_criteria():
    assert codes(consequences=["stop_gained"], clingen_hi_score=3, clingen_no_disease=True, **NOT_RARE) == []
    assert codes(**MISSENSE, **HOTSPOT, clingen_no_disease=True) == []
