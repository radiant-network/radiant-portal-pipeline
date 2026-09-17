"""Structure checks for the weekly re-annotation DAG (SJRA-1811 §4, §5).

These pin the properties the design argues for and that nothing else would catch: the lock spans the
whole run, the phases are ordered the way §5's diagrams say, and the two phase-3a chains stay
independent of each other.
"""

import pytest

from radiant.dags import NAMESPACE
from radiant.dags.reannotate_open_data import build_cnv_params, build_variant_part_params

_DAG_ID = f"{NAMESPACE}-reannotate-open-data"


@pytest.fixture
def dag(dag_bag):
    return dag_bag.get_dag(_DAG_ID)


def test_dag_is_importable(dag_bag):
    assert _DAG_ID in dag_bag.dags
    assert dag_bag.get_dag(_DAG_ID) is not None


def test_never_runs_concurrently(dag):
    # Two runs would both try to take the same lock, and the loser fails outright.
    assert dag.max_active_runs == 1


def test_is_manual_until_the_weekly_schedule_is_flipped(dag):
    """§4 asks for Saturday 00:00; the DAG is held at manual trigger until an operator flips it.

    Pinned rather than left unasserted so the flip is a deliberate edit, made together with dropping
    the `manual` tag -- not something that arrives unnoticed with an unrelated change.
    """
    assert dag.schedule_interval is None
    assert "manual" in dag.tags


def test_preflight_runs_before_the_lock_is_taken(dag):
    """A missing table is a setup problem, not a race. Failing ahead of the lock leaves no mutex to
    clear by hand."""
    preflight = dag.get_task("preflight_tables_exist")
    assert preflight.upstream_task_ids == set()
    assert preflight.downstream_task_ids == {"acquire_import_lock"}


def test_lock_is_acquired_first_and_released_last(dag):
    acquire = dag.get_task("acquire_import_lock")
    release = dag.get_task("release_import_lock")
    assert acquire.upstream_task_ids == {"preflight_tables_exist"}
    assert acquire.downstream_task_ids == {"reference_load"}
    # Nothing downstream of the release, and it hangs off the last task in the run -- so a failure
    # anywhere above leaves the lock held, which §4 requires.
    assert release.downstream_task_ids == set()
    assert release.upstream_task_ids == {"record_open_data_release"}


def test_preflight_is_the_only_entry_point(dag):
    """One root: a second would run outside the lock the preflight gates."""
    assert [t.task_id for t in dag.tasks if not t.upstream_task_ids] == ["preflight_tables_exist"]


def test_reference_load_precedes_the_checkpoint(dag):
    assert dag.get_task("reference_load").downstream_task_ids == {"sources_loaded"}


def test_the_ui_lists_the_phases_in_order(dag):
    """The grid sorts each level with `topological_sort`, and a TaskGroup is ordered by the edges
    recorded *on the group* -- which §5's statement-to-statement wiring leaves empty. Both P3a groups
    therefore sorted ahead of `acquire_import_lock` until the checkpoint was wired to them as well.
    """
    order = [node.node_id for node in dag.task_group.topological_sort()]
    landmarks = [
        "preflight_tables_exist",
        "acquire_import_lock",
        "reference_load",
        "sources_loaded",
        "reannotate_accumulators",
        "snv_variant",
        "snv_consequence",
        "cnv_occurrence",
        "rebuilds_complete",
        "record_open_data_release",
        "release_import_lock",
    ]
    positions = [order.index(node_id) for node_id in landmarks]
    assert positions == sorted(positions), dict(zip(landmarks, positions, strict=True))


def test_phase_3a_chains_are_independent_of_each_other(dag):
    """§5: top row and bottom row are independent; left-to-right inside a row is not."""
    variant = dag.get_task("snv_variant.insert_snv_variant")
    consequence = dag.get_task("snv_consequence.insert_snv_consequence_filter")

    assert "reannotate_accumulators.reannotate_snv_staging_variant" in variant.upstream_task_ids
    assert "reannotate_accumulators.reannotate_snv_consequence" not in variant.upstream_task_ids

    assert "reannotate_accumulators.reannotate_snv_consequence" in consequence.upstream_task_ids
    assert "reannotate_accumulators.reannotate_snv_staging_variant" not in consequence.upstream_task_ids


def test_partitioned_copies_come_after_the_tables_they_copy(dag):
    assert "snv_variant.insert_snv_variant" in dag.get_task("snv_variant.insert_snv_variant_part").upstream_task_ids
    assert (
        "snv_consequence.insert_snv_consequence_filter"
        in dag.get_task("snv_consequence.insert_snv_consequence_filter_part").upstream_task_ids
    )


def test_cnv_rebuild_waits_for_the_variant_table_it_counts(dag):
    """Both CNV statements join `snv__variant` for `nb_snv`, and P3a rebuilds that table with INSERT
    OVERWRITE -- so 3b beside 3a counts against whichever copy is current when it runs.

    §5 argued the two were independent because `locus_id`, `chromosome` and `start` survive
    re-annotation unchanged. True of the values, not of the row set the count depends on.
    """
    germline_cnv = dag.get_task("cnv_occurrence.reannotate_germline_cnv_occurrence")
    assert "snv_variant.insert_snv_variant" in germline_cnv.upstream_task_ids

    # The partitioned copy is not what they read, and no CNV statement touches a consequence table --
    # neither is made a predecessor, so the consequence chain still runs beside 3b.
    for upstream in germline_cnv.upstream_task_ids:
        assert upstream != "snv_variant.insert_snv_variant_part", upstream
        assert not upstream.startswith("snv_consequence."), upstream


def test_release_is_recorded_only_after_every_rebuild(dag):
    """All three rebuild groups fan into the final checkpoint, and the release hangs off that."""
    upstream = dag.get_task("rebuilds_complete").upstream_task_ids
    assert "snv_variant.insert_snv_variant_part" in upstream
    assert "snv_consequence.insert_snv_consequence_filter_part" in upstream
    assert "cnv_occurrence.reannotate_somatic_cnv_occurrence" in upstream

    assert "rebuilds_complete" in dag.get_task("record_open_data_release").upstream_task_ids


def test_an_empty_platform_still_releases_the_lock(dag):
    """Tenant/part discovery short-circuits, so on a platform with no experiments every rebuild skips.

    Under ALL_SUCCESS that would skip P4, and with it the release, stranding the mutex on a run where
    nothing actually went wrong. A real failure still skips P4 and still holds the lock.
    """
    from airflow.utils.trigger_rule import TriggerRule

    assert dag.get_task("rebuilds_complete").trigger_rule == TriggerRule.NONE_FAILED
    assert dag.get_task("record_open_data_release").trigger_rule == TriggerRule.NONE_FAILED
    assert dag.get_task("release_import_lock").trigger_rule == TriggerRule.ALL_SUCCESS

    # The trigger rules above are worth nothing on their own: a ShortCircuitOperator defaults to
    # `ignore_downstream_trigger_rules=True`, which skips the whole downstream chain and overrides
    # every rule it meets -- carrying the skip through the release and stranding the mutex.
    for task_id in ("extract_all_tenants", "extract_all_parts", "extract_tenant_parts"):
        assert dag.get_task(task_id).ignore_downstream_trigger_rules is False, task_id


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
