"""Structure checks for the weekly re-annotation DAG (SJRA-1811 §4, §5).

These pin the properties the design argues for and that nothing else would catch: the lock spans the
whole run, the phases are ordered the way §5's diagrams say, and the two phase-3a chains stay
independent of each other.
"""

import pytest

from radiant.dags import NAMESPACE
from radiant.dags.reannotate_open_data import build_cnv_params, build_variant_part_params

_DAG_ID = f"{NAMESPACE}-reannotate-open-data"

# Only P1 is wired up right now; the rest of the DAG is commented out while it is brought online
# one phase at a time. Un-skip each of these together with its phase -- they are the check that the
# phase came back wired the way §5 specifies.
_STAGED = pytest.mark.skip(reason="SJRA-1811 staged rollout: phase disabled in the DAG")


@pytest.fixture
def dag(dag_bag):
    return dag_bag.get_dag(_DAG_ID)


def test_dag_is_importable(dag_bag):
    assert _DAG_ID in dag_bag.dags
    assert dag_bag.get_dag(_DAG_ID) is not None


def test_never_runs_concurrently(dag):
    # Two runs would both try to take the same lock, and the loser fails outright.
    assert dag.max_active_runs == 1


@_STAGED
def test_runs_weekly(dag):
    # Saturday 00:00 (§4). Manual-only until P4 is live: a partial DAG must not fire on its own.
    assert dag.schedule_interval == "0 0 * * 6"


@_STAGED
def test_preflight_runs_before_the_lock_is_taken(dag):
    """A missing table is a setup problem, not a race. Failing ahead of the lock leaves no mutex to
    clear by hand -- which matters while the DAG is staged and this failure is expected."""
    preflight = dag.get_task("preflight_tables_exist")
    assert preflight.upstream_task_ids == set()
    assert preflight.downstream_task_ids == {"acquire_import_lock"}


@_STAGED
def test_lock_is_acquired_first_and_released_last(dag):
    acquire = dag.get_task("acquire_import_lock")
    release = dag.get_task("release_import_lock")
    assert acquire.upstream_task_ids == {"preflight_tables_exist"}
    assert acquire.downstream_task_ids == {"reference_load"}
    # Nothing downstream of the release, and it hangs off the last live task -- so a failure anywhere
    # above leaves the lock held, which §4 requires. Follows the staged rollout: currently the
    # checkpoint, eventually `record_open_data_release`.
    assert release.downstream_task_ids == set()
    assert release.upstream_task_ids == {"sources_loaded"}


def test_preflight_is_the_only_entry_point(dag):
    """With the lock disabled the preflight is the root; when the lock comes back it moves behind it."""
    assert [t.task_id for t in dag.tasks if not t.upstream_task_ids] == ["preflight_tables_exist"]
    assert dag.get_task("preflight_tables_exist").downstream_task_ids == {"reference_load"}


def test_reference_load_precedes_the_checkpoint(dag):
    assert dag.get_task("reference_load").downstream_task_ids == {"sources_loaded"}


@_STAGED
def test_phase_3a_chains_are_independent_of_each_other(dag):
    """§5: top row and bottom row are independent; left-to-right inside a row is not."""
    variant = dag.get_task("snv_variant.insert_snv_variant")
    consequence = dag.get_task("snv_consequence.insert_snv_consequence_filter")

    assert "reannotate_accumulators.reannotate_snv_staging_variant" in variant.upstream_task_ids
    assert "reannotate_accumulators.reannotate_snv_consequence" not in variant.upstream_task_ids

    assert "reannotate_accumulators.reannotate_snv_consequence" in consequence.upstream_task_ids
    assert "reannotate_accumulators.reannotate_snv_staging_variant" not in consequence.upstream_task_ids


@_STAGED
def test_partitioned_copies_come_after_the_tables_they_copy(dag):
    assert "snv_variant.insert_snv_variant" in dag.get_task("snv_variant.insert_snv_variant_part").upstream_task_ids
    assert (
        "snv_consequence.insert_snv_consequence_filter"
        in dag.get_task("snv_consequence.insert_snv_consequence_filter_part").upstream_task_ids
    )


@_STAGED
def test_cnv_rebuild_is_parallel_to_the_snv_rebuild(dag):
    """§5: locus_id/chromosome/start are carried through unchanged, so 3b does not wait on 3a."""
    germline_cnv = dag.get_task("cnv_occurrence.reannotate_germline_cnv_occurrence")
    assert "sources_loaded" in germline_cnv.upstream_task_ids
    for upstream in germline_cnv.upstream_task_ids:
        assert not upstream.startswith("snv_variant."), upstream
        assert not upstream.startswith("snv_consequence."), upstream


@_STAGED
def test_release_is_recorded_only_after_every_rebuild(dag):
    upstream = dag.get_task("record_open_data_release").upstream_task_ids
    assert "snv_variant.insert_snv_variant_part" in upstream
    assert "snv_consequence.insert_snv_consequence_filter_part" in upstream
    assert "cnv_occurrence.reannotate_somatic_cnv_occurrence" in upstream


@_STAGED
def test_an_empty_platform_still_releases_the_lock(dag):
    """Tenant/part discovery short-circuits, so on a platform with no experiments every rebuild skips.

    Under ALL_SUCCESS that would skip P4, and with it the release, stranding the mutex on a run where
    nothing actually went wrong. A real failure still skips P4 and still holds the lock.
    """
    from airflow.utils.trigger_rule import TriggerRule

    assert dag.get_task("record_open_data_release").trigger_rule == TriggerRule.NONE_FAILED
    assert dag.get_task("release_import_lock").trigger_rule == TriggerRule.ALL_SUCCESS


def test_variant_part_params_collapse_ten_parts_into_one():
    rows = [
        {"tenant_code": "CHOP", "part": 0},
        {"tenant_code": "CHOP", "part": 9},
        {"tenant_code": "CHOP", "part": 10},
        {"tenant_code": "SJ", "part": 25},
    ]
    params = build_variant_part_params(rows)
    assert [(p["tenant_code"], p["parameters"]["variant_part"]) for p in params] == [
        ("CHOP", 0),
        ("CHOP", 1),
        ("SJ", 2),
    ]
    assert params[0]["parameters"] == {"variant_part": 0, "part_lower": 0, "part_upper": 10}
    assert params[2]["parameters"] == {"variant_part": 2, "part_lower": 20, "part_upper": 30}


def test_cnv_params_are_one_per_existing_pair_not_the_cross_product():
    rows = [
        {"tenant_code": "SJ", "part": 3},
        {"tenant_code": "CHOP", "part": 1},
        {"tenant_code": "CHOP", "part": 0},
    ]
    assert build_cnv_params(rows) == [
        {"tenant_code": "CHOP", "parameters": {"part": 0}},
        {"tenant_code": "CHOP", "parameters": {"part": 1}},
        {"tenant_code": "SJ", "parameters": {"part": 3}},
    ]
