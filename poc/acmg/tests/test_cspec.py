from acmg.cspec import not_applicable


def code(label: str, genes: list[str], *strengths: tuple[str, str], applicability: str = "Applicable") -> dict:
    descriptors = [{"applicability": a, "status": s} for a, s in strengths]
    return {"entContent": {"label": label, "gene": genes, "applicability": applicability, "strengthDescriptor": descriptors}}


def spec(spec_id: str, *codes: dict) -> dict:
    return {"entId": spec_id, "ld": {"CriteriaCode": list(codes)}}


NA = ("Not applicable", "not approved")
APPLIED = ("Applicable", "approved")


def rows(*specs: dict) -> list[tuple[str, str]]:
    return [tuple(r.values()) for r in not_applicable(list(specs)).to_dicts()]


def test_criterion_every_spec_of_the_gene_says_not_applicable():
    assert rows(spec("GN1", code("PVS1", ["A"], NA), code("PM2", ["A"], APPLIED))) == [("A", "PVS1")]
    assert rows(spec("GN1", code("PP2", ["A"], applicability="Not Applicable for this VCEP"))) == [("A", "PP2")]


def test_one_spec_applying_or_leaving_out_the_criterion_keeps_it():
    assert rows(spec("GN1", code("PVS1", ["A"], NA)), spec("GN2", code("PVS1", ["A"], APPLIED))) == []
    assert rows(spec("GN1", code("PVS1", ["A"], NA)), spec("GN2", code("PM2", ["A"], APPLIED))) == []


def test_any_strength_code_applicable_keeps_the_criterion():
    assert rows(spec("GN1", code("PM2", ["A"], NA), code("PM2_Supporting", ["A"], APPLIED))) == []


def test_strength_status_is_not_read():
    # PAH: PM2_Supporting applicable with its threshold, marked "not approved" in the registry.
    assert rows(spec("GN1", code("PM2", ["A"], NA, ("Applicable", "not approved")))) == []
