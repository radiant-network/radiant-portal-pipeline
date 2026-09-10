"""The 1 + N dbt pass loop and its artifact merge."""

import json

import pytest

TENANTS = '[{"code": "chusj", "schema": "chusj_tenant"}, {"code": "chop", "schema": "chop_tenant"}]'


def _artifact(*unique_ids, elapsed=2.0, metadata=None):
    return {
        "metadata": metadata if metadata is not None else {"project_name": "radiant_data_qa"},
        "elapsed_time": elapsed,
        "results": [{"unique_id": uid, "status": "pass", "execution_time": 0.1} for uid in unique_ids],
    }


# --- pass planning ----------------------------------------------------------------


def test_no_tenants_runs_only_the_shared_pass(run_qa):
    """A plain local run with no TENANTS behaves as a single-schema run of the base tables."""
    (shared,) = run_qa.plan_passes(None, "radiant")
    assert shared.tenant is None
    assert shared.schema == "radiant"
    assert shared.select_args == ["--exclude", "source:tenant_db"]


@pytest.mark.parametrize("empty", ["", "   ", "[]"])
def test_empty_tenants_is_treated_as_none(run_qa, empty):
    assert len(run_qa.plan_passes(empty, "radiant")) == 1


def test_two_tenants_produce_one_shared_plus_two_tenant_passes(run_qa):
    shared, chusj, chop = run_qa.plan_passes(TENANTS, "radiant")

    assert shared.select_args == ["--exclude", "source:tenant_db"]
    # The two selectors are complementary, so every test node runs in exactly one pass.
    assert chusj.select_args == chop.select_args == ["--select", "source:tenant_db"]
    assert (chusj.tenant, chusj.schema) == ("chusj", "chusj_tenant")
    assert (chop.tenant, chop.schema) == ("chop", "chop_tenant")


# --- tiering ----------------------------------------------------------------------


def test_no_declared_reference_keeps_the_full_suite_everywhere(run_qa):
    """Tiering is opt-in: it must not narrow an existing pipeline's coverage silently."""
    _, chusj, chop = run_qa.plan_passes(TENANTS, "radiant")
    assert chusj.select_args == chop.select_args == ["--select", "source:tenant_db"]


def test_reference_tenant_keeps_the_full_suite_and_the_others_get_the_health_tier(run_qa):
    tenants = json.dumps(
        [
            {"code": "chusj", "schema": "chusj_tenant", "reference": True},
            {"code": "chop", "schema": "chop_tenant"},
        ]
    )
    _, chusj, chop = run_qa.plan_passes(tenants, "radiant")

    assert chusj.select_args == ["--select", "source:tenant_db"]

    # The health tier is a union of per-test-type intersections, plus the tag that lets a
    # singular test opt in. Anything else — column sweeps, ranges, dictionaries — is
    # reference-only, because it re-tests SQL that is identical for every tenant.
    assert chop.select_args[0] == "--select"
    assert chop.select_args[1:] == [
        "source:tenant_db,test_name:not_null",
        "source:tenant_db,test_name:unique",
        "source:tenant_db,test_name:unique_combination_of_columns",
        "source:tenant_db,test_name:relationships",
        "source:tenant_db,test_name:should_not_be_empty",
        "source:tenant_db,tag:tenant_health",
    ]
    assert "source:tenant_db,test_name:should_not_contain_same_value" not in chop.select_args


def test_absent_tables_are_excluded_for_that_tenant_only(run_qa):
    """A germline-only cohort has no somatic occurrences — that is its shape, not a defect."""
    tenants = json.dumps(
        [
            {"code": "chusj", "schema": "chusj_tenant", "reference": True},
            {
                "code": "onekg",
                "schema": "onekg_tenant",
                "absent_tables": ["somatic__snv__occurrence", "somatic__cnv__occurrence"],
            },
        ]
    )
    _, chusj, onekg = run_qa.plan_passes(tenants, "radiant")

    assert "--exclude" not in chusj.select_args
    assert onekg.select_args[-3:] == [
        "--exclude",
        "source:tenant_db.somatic__snv__occurrence",
        "source:tenant_db.somatic__cnv__occurrence",
    ]


def test_a_reference_tenant_can_also_declare_absent_tables(run_qa):
    tenants = json.dumps(
        [{"code": "chusj", "schema": "chusj_tenant", "reference": True, "absent_tables": ["exomiser"]}]
    )
    _, chusj = run_qa.plan_passes(tenants, "radiant")
    assert chusj.select_args == ["--select", "source:tenant_db", "--exclude", "source:tenant_db.exomiser"]


def test_malformed_absent_tables_fails_fast(run_qa):
    tenants = '[{"code": "chusj", "schema": "chusj_tenant", "absent_tables": "exomiser"}]'
    with pytest.raises(ValueError, match="absent_tables"):
        run_qa.plan_passes(tenants, "radiant")


def test_schema_is_taken_verbatim_not_rebuilt(run_qa):
    """RADIANT_TENANT_DB_TEMPLATE is configurable, so the container must never rebuild the name."""
    passes = run_qa.plan_passes('[{"code": "chusj", "schema": "tenant_chusj_db"}]', "radiant")
    assert passes[1].schema == "tenant_chusj_db"


@pytest.mark.parametrize("bad", ['[{"code": "chusj"}]', '[{"schema": "chusj_tenant"}]', '["chusj"]'])
def test_malformed_tenant_entry_fails_fast(run_qa, bad):
    with pytest.raises(ValueError, match="code"):
        run_qa.plan_passes(bad, "radiant")


def test_non_list_tenants_fails_fast(run_qa):
    with pytest.raises(ValueError, match="JSON list"):
        run_qa.plan_passes('{"code": "chusj"}', "radiant")


# --- merge ------------------------------------------------------------------------


def test_merge_stamps_the_tenant_and_sums_elapsed(run_qa):
    shared, chusj, chop = run_qa.plan_passes(TENANTS, "radiant")
    merged = run_qa.merge(
        [
            (shared, _artifact("test.radiant_data_qa.shared.a", elapsed=1.0)),
            (chusj, _artifact("test.radiant_data_qa.tenant.b", elapsed=2.0)),
            (chop, _artifact("test.radiant_data_qa.tenant.b", elapsed=4.0)),
        ]
    )

    assert [r["tenant"] for r in merged["results"]] == [None, "chusj", "chop"]
    assert merged["elapsed_time"] == 7.0
    assert merged["metadata"] == {"project_name": "radiant_data_qa"}


def test_a_dead_tenant_pass_becomes_an_error_and_does_not_hide_the_others(run_qa):
    """Tenant A's database being broken must not cost us tenant B's results."""
    shared, chusj, chop = run_qa.plan_passes(TENANTS, "radiant")
    merged = run_qa.merge(
        [
            (shared, _artifact("test.radiant_data_qa.shared.a")),
            (chusj, None),
            (chop, _artifact("test.radiant_data_qa.tenant.b")),
        ]
    )

    by_tenant = {r["tenant"]: r for r in merged["results"]}
    assert by_tenant["chop"]["status"] == "pass"
    assert by_tenant["chusj"]["status"] == "error"
    assert "chusj" in by_tenant["chusj"]["message"]
    # No SJRA tag in the id, so check_results counts it as an unexpected failure.
    assert "SJRA-" not in by_tenant["chusj"]["unique_id"]


# --- main -------------------------------------------------------------------------


def test_main_runs_every_pass_and_writes_one_merged_report(run_qa, tmp_path, monkeypatch):
    seen = []

    def fake_run_pass(qa_pass, env, data_qa_dir):
        seen.append((qa_pass.label, qa_pass.schema, tuple(qa_pass.select_args)))
        return _artifact(f"test.radiant_data_qa.t.{qa_pass.label}")

    monkeypatch.setattr(run_qa, "run_pass", fake_run_pass)
    rc = run_qa.main({"TENANTS": TENANTS, "SR_SCHEMA": "radiant"}, tmp_path)

    assert rc == 0
    assert seen == [
        ("shared", "radiant", ("--exclude", "source:tenant_db")),
        ("chusj", "chusj_tenant", ("--select", "source:tenant_db")),
        ("chop", "chop_tenant", ("--select", "source:tenant_db")),
    ]
    # one run_results.json + one junit.xml, exactly as the DAG expects to upload
    merged = json.loads((tmp_path / "target" / "run_results.json").read_text())
    assert [r["tenant"] for r in merged["results"]] == [None, "chusj", "chop"]
    assert (tmp_path / "reports" / "junit.xml").exists()


def test_main_fails_when_the_shared_pass_cannot_run(run_qa, tmp_path, monkeypatch):
    """No connection means no point trying the tenants — keep run_qa.sh's exit-1 contract."""
    monkeypatch.setattr(run_qa, "run_pass", lambda *a: None)
    assert run_qa.main({"TENANTS": TENANTS, "SR_SCHEMA": "radiant"}, tmp_path) == 1
    assert not (tmp_path / "reports" / "junit.xml").exists()


def test_main_still_reports_when_one_tenant_pass_dies(run_qa, tmp_path, monkeypatch):
    def fake_run_pass(qa_pass, env, data_qa_dir):
        if qa_pass.label == "chusj":
            return None
        return _artifact(f"test.radiant_data_qa.t.{qa_pass.label}")

    monkeypatch.setattr(run_qa, "run_pass", fake_run_pass)
    assert run_qa.main({"TENANTS": TENANTS, "SR_SCHEMA": "radiant"}, tmp_path) == 0

    merged = json.loads((tmp_path / "target" / "run_results.json").read_text())
    statuses = {r["tenant"]: r["status"] for r in merged["results"]}
    assert statuses == {None: "pass", "chusj": "error", "chop": "pass"}


def test_run_pass_clears_a_stale_artifact_before_each_run(run_qa, tmp_path, monkeypatch):
    """A leftover artifact would be misattributed to the pass that failed to produce one."""
    stale = tmp_path / "target" / "run_results.json"
    stale.parent.mkdir(parents=True)
    stale.write_text(json.dumps(_artifact("test.radiant_data_qa.stale.a")))

    monkeypatch.setattr(run_qa.subprocess, "run", lambda *a, **kw: None)
    (qa_pass,) = run_qa.plan_passes(None, "radiant")

    assert run_qa.run_pass(qa_pass, {}, tmp_path) is None
    assert not stale.exists()
