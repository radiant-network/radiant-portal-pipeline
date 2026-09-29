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


def test_each_3a_chain_still_comes_after_the_accumulator_it_reads(dag):
    """§5 had these two chains running in parallel. They are serialised now -- StarRocks I/O, not data --
    so the data dependency is satisfied transitively rather than by a direct edge. Assert the dependency,
    which is the thing that must not break, not the edge, which is free to move.
    """
    variant = dag.get_task("snv_variant.insert_snv_variant")
    consequence = dag.get_task("snv_consequence.insert_snv_consequence_filter")
    staging = dag.get_task("reannotate_accumulators.reannotate_snv_staging_variant")
    cons_acc = dag.get_task("reannotate_accumulators.reannotate_snv_consequence")

    assert _reaches(dag, staging, variant)
    assert _reaches(dag, cons_acc, consequence)


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
    assert _reaches(dag, dag.get_task("snv_variant.insert_snv_variant"), germline_cnv)


def test_release_is_recorded_only_after_every_rebuild(dag):
    """Every rebuild precedes the final checkpoint, and the release hangs off that.

    The groups reach it down the serial spine rather than fanning into it directly, so assert
    reachability -- the invariant -- not the edges, which move whenever the spine is reordered.
    """
    rebuilds_complete = dag.get_task("rebuilds_complete")
    for task_id in (
        "snv_variant.insert_snv_variant_part",
        "snv_consequence.insert_snv_consequence_filter_part",
        "cnv_occurrence.reannotate_somatic_cnv_occurrence",
    ):
        assert _reaches(dag, dag.get_task(task_id), rebuilds_complete), task_id

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


def _starrocks_tasks(dag):
    """Every task that runs a statement on StarRocks, mapped ones included."""
    from airflow.models.mappedoperator import MappedOperator

    from radiant.tasks.starrocks.operator import RadiantStarRocksOperator

    out = []
    for task in dag.tasks:
        cls = task.operator_class if isinstance(task, MappedOperator) else type(task)
        if isinstance(cls, type) and issubclass(cls, RadiantStarRocksOperator):
            out.append(task)
    return out


def _reaches(dag, a, b, seen=None):
    seen = seen if seen is not None else set()
    if a.task_id in seen:
        return False
    seen.add(a.task_id)
    if b.task_id in a.downstream_task_ids:
        return True
    return any(_reaches(dag, dag.get_task(t), b, seen) for t in a.downstream_task_ids)


def test_no_two_starrocks_inserts_can_run_at_the_same_time(dag):
    """Every re-annotation statement is a whole-table scan. Two at once contend for the same disk and
    spill budget, so the DAG must leave no pair of them concurrent.

    Asserted over the reachability closure rather than over the edges written in the Flow section: an
    edge list can look serial while a pair is still concurrent through some other path.
    """
    inserts = _starrocks_tasks(dag)
    assert len(inserts) >= 8, f"expected every StarRocks statement, found {len(inserts)}"

    concurrent = [
        (a.task_id, b.task_id)
        for i, a in enumerate(inserts)
        for b in inserts[i + 1 :]
        if not _reaches(dag, a, b) and not _reaches(dag, b, a)
    ]
    assert concurrent == [], f"these StarRocks inserts can overlap: {concurrent}"


def test_the_accumulators_are_serial_with_each_other(dag):
    """They are independent upserts -- nothing but I/O contention orders them, so the edge is easy to
    drop by accident."""
    consequence = dag.get_task("reannotate_accumulators.reannotate_snv_consequence")
    assert "reannotate_accumulators.reannotate_snv_staging_variant" in consequence.upstream_task_ids


def test_the_mapped_fan_outs_are_serialised_by_the_pool_not_by_a_task_limit(dag):
    """A tenant/part fan-out is N statements, and edges between groups say nothing about them.

    `max_active_tis_per_dagrun=1` looks like the answer and is not: the scheduler counts only
    `EXECUTION_STATES` = {RUNNING, QUEUED}, and these operators SUBMIT TASK then defer -- so every mapped
    instance stops being counted the moment its statement starts running, and the next one is released.
    A pool is the only limit that counts a deferred task, and only with `include_deferred=True`.
    """
    from radiant.tasks.starrocks.operator import STARROCKS_INSERT_POOL

    for task in _starrocks_tasks(dag):
        pool = task.partial_kwargs.get("pool") if hasattr(task, "partial_kwargs") else task.pool
        assert pool == STARROCKS_INSERT_POOL, task.task_id
        limit = (
            task.partial_kwargs.get("max_active_tis_per_dagrun")
            if hasattr(task, "partial_kwargs")
            else task.max_active_tis_per_dagrun
        )
        assert limit is None, f"{task.task_id} relies on a limit that ignores DEFERRED"


def test_a_checkpoint_brackets_every_group_of_starrocks_work(dag):
    """The checkpoints carry no work -- they exist so the graph reads as the serial sequence it is.
    Each group of statements must sit between two of them, or the UI stops showing where one operation
    ends and the next begins."""
    checkpoints = [
        "sources_loaded",
        "accumulators_reannotated",
        "snv_variants_rebuilt",
        "snv_consequences_rebuilt",
        "rebuilds_complete",
    ]
    for task_id in checkpoints:
        task = dag.get_task(task_id)
        assert type(task).__name__ == "EmptyOperator", f"{task_id} should carry no work"
        assert "CHECKPOINT" in task.task_display_name, task_id

    # Consecutive, in this order, down the one spine.
    for earlier, later in zip(checkpoints, checkpoints[1:], strict=False):
        assert _reaches(dag, dag.get_task(earlier), dag.get_task(later)), f"{earlier} -> {later}"
        assert not _reaches(dag, dag.get_task(later), dag.get_task(earlier)), f"{later} -> {earlier}"

    # Every StarRocks statement but the final release sits strictly between two checkpoints.
    for task in _starrocks_tasks(dag):
        if task.task_id == "record_open_data_release":
            continue
        before = [c for c in checkpoints if _reaches(dag, dag.get_task(c), task)]
        after = [c for c in checkpoints if _reaches(dag, task, dag.get_task(c))]
        assert before and after, f"{task.task_id} is not bracketed by checkpoints"


def test_checkpoints_do_not_stall_the_spine_when_a_branch_skips(dag):
    """Tenant/part discovery short-circuits. A checkpoint left on the default ALL_SUCCESS would skip with
    the branch and take the whole spine -- including the lock release -- down with it."""
    from airflow.utils.trigger_rule import TriggerRule

    for task_id in ("accumulators_reannotated", "snv_variants_rebuilt", "snv_consequences_rebuilt"):
        assert dag.get_task(task_id).trigger_rule == TriggerRule.NONE_FAILED, task_id


def test_p1_asks_the_import_to_skip_legacy_sources(dag):
    """A re-annotation picks up what OpenDataLake published; the legacy tables did not move."""
    assert dag.get_task("reference_load").conf == {"skip_legacy_tables": True}


def test_p1_leaves_the_file_driven_loads_unset(dag):
    """`import-open-data` gates its broker loads and the COSMIC hand-off on filepath params. P1 passes none
    of them, so ClinVar RCV summary, cytoband and COSMIC stay a manual, operator-triggered run."""
    conf = dag.get_task("reference_load").conf
    assert "raw_rcv_filepaths" not in conf
    assert "cytoband_filepath" not in conf
    assert "cosmic_gene_set_filepath" not in conf
    assert "cosmic_mutation_set_filepath" not in conf


# --- map index labels ---------------------------------------------------------------------------
#
# Airflow renders `map_index_template` after the task body, with the task context and the DAG's own
# jinja env (`TaskInstance._render_map_index`). A mapped task without one -- or with one that resolves
# to the same string for several map indexes -- shows up in the UI as `0, 1, 2, ...`, or worse as the
# same label repeated, which is indistinguishable from a duplicate.

_MAPPED_TASK_IDS = {
    "snv_variant.insert_snv_variant",
    "snv_variant.insert_snv_variant_part",
    "snv_consequence.insert_snv_consequence_filter_part",
    "cnv_occurrence.reannotate_germline_cnv_occurrence",
    "cnv_occurrence.reannotate_somatic_cnv_occurrence",
}

# One tenant with several parts, one with a single part -- enough for a repeated label to collide.
_TENANT_PARTS = [
    {"tenant_code": "CHOP", "part": 0},
    {"tenant_code": "CHOP", "part": 9},
    {"tenant_code": "CHOP", "part": 10},
    {"tenant_code": "SJ", "part": 3},
]


def _mapped_tasks(dag):
    from airflow.models.mappedoperator import MappedOperator

    return {task.task_id: task for task in dag.tasks if isinstance(task, MappedOperator)}


def test_the_mapped_tasks_are_the_ones_we_think_they_are(dag):
    """Guards the two tests below: a new fan-out added without a label would otherwise go unchecked."""
    assert set(_mapped_tasks(dag)) == _MAPPED_TASK_IDS


def test_every_mapped_task_labels_its_map_indexes(dag):
    """Without a template the UI falls back to the integer map index, which says nothing about which
    tenant or part a run covers -- the first thing you need when one of N fan-out tasks fails."""
    unlabelled = [task_id for task_id, task in _mapped_tasks(dag).items() if not task.map_index_template]
    assert unlabelled == []


def _render(dag, task, expand_kwargs):
    """Render a map index the way `TaskInstance._render_map_index` does, for one expanded task."""
    import types

    env = dag.get_template_env()
    return str(env.from_string(task.map_index_template).render(task=types.SimpleNamespace(**expand_kwargs)))


def test_map_index_labels_are_unique_across_the_fan_out(dag):
    """A label that repeats is worse than the integer default: two rows claim the same work.

    `insert_snv_variant_part` and both CNV fan-outs expand over pairs, so the tenant code alone is not
    a key -- the part has to be in the label too.
    """
    from radiant.dags.reannotate_open_data import build_cnv_params, build_variant_part_params

    tasks = _mapped_tasks(dag)
    cnv_params = build_cnv_params(_TENANT_PARTS)
    expansions = {
        "snv_variant.insert_snv_variant": [{"tenant_code": t} for t in ("CHOP", "SJ")],
        "snv_variant.insert_snv_variant_part": build_variant_part_params(_TENANT_PARTS),
        "snv_consequence.insert_snv_consequence_filter_part": [
            {"parameters": {"part": part}} for part in (0, 3, 9, 10)
        ],
        "cnv_occurrence.reannotate_germline_cnv_occurrence": cnv_params,
        "cnv_occurrence.reannotate_somatic_cnv_occurrence": cnv_params,
    }

    for task_id, expand_kwargs in expansions.items():
        labels = [_render(dag, tasks[task_id], kwargs) for kwargs in expand_kwargs]
        assert len(set(labels)) == len(labels), f"{task_id} renders duplicate map indexes: {labels}"
        # An all-digit label is the integer default wearing a template.
        assert not any(label.isdigit() for label in labels), f"{task_id} renders a bare index: {labels}"


def test_map_index_labels_name_the_tenant_and_the_part(dag):
    """Pins the actual strings -- the point of the label is that a human reads it."""
    from radiant.dags.reannotate_open_data import build_cnv_params, build_variant_part_params

    tasks = _mapped_tasks(dag)

    variant_parts = build_variant_part_params(_TENANT_PARTS)
    assert [_render(dag, tasks["snv_variant.insert_snv_variant_part"], kw) for kw in variant_parts] == [
        "CHOP variant_part=0",
        "CHOP variant_part=1",
        "SJ variant_part=0",
    ]

    cnv_params = build_cnv_params(_TENANT_PARTS)
    assert [_render(dag, tasks["cnv_occurrence.reannotate_germline_cnv_occurrence"], kw) for kw in cnv_params] == [
        "CHOP part=0",
        "CHOP part=9",
        "CHOP part=10",
        "SJ part=3",
    ]

    filter_part = tasks["snv_consequence.insert_snv_consequence_filter_part"]
    assert _render(dag, filter_part, {"parameters": {"part": 7}}) == "part=7"

    assert _render(dag, tasks["snv_variant.insert_snv_variant"], {"tenant_code": "CHOP"}) == "CHOP"
