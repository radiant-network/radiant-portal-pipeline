"""Change detection that gates the re-annotation branches (SJRA-1811 §5).

A re-annotation is hours of whole-table scans, and most weeks only some sources moved. The gate
decides per branch, so a ClinVar publish rebuilds the variant chain and leaves the consequence
chain alone.

The signal is the Iceberg snapshot each ref resolves to, not `dataset_version` -- that column is
NULL on the default `latest` tag, so it can never answer "did this change?". Snapshots come from
`<relation>$refs`, an Iceberg metadata table StarRocks exposes from 3.4.1.
"""

from unittest.mock import patch

import pytest

from radiant.tasks.data import open_data
from radiant.tasks.data.open_data import (
    LEGACY,
    REANNOTATION_SOURCES,
    branches_to_reannotate,
    changed_sources,
)

_CONF = {
    "RADIANT_TABLES_DATABASE": "radiant",
    "RADIANT_ICEBERG_CATALOG": "radiant_iceberg_catalog",
    "RADIANT_ICEBERG_NAMESPACE": "radiant",
    "RADIANT_OPEN_DATA_CATALOG": "odl_catalog",
    "RADIANT_OPEN_DATA_DATABASE": "opendatalake_qa",
    "RADIANT_OPEN_DATA_REF": "latest",
    "RADIANT_OPEN_DATA_USE_LEGACY_TABLES": "",
}

# One source per branch, enough to move a gate on its own.
_VARIANT_SOURCE = "iceberg_clinvar"
_CONSEQUENCE_SOURCE = "iceberg_spliceai"
_CNV_SOURCE = "iceberg_gnomad_sv"


@pytest.fixture
def state():
    """Drive both halves of the comparison: what the refs say now, what the ledger recorded."""
    with (
        patch.object(open_data, "resolve_current_snapshots") as current,
        patch.object(open_data, "last_recorded_snapshots") as recorded,
    ):

        def setup(current_snapshots: dict, recorded_rows: dict):
            current.return_value = current_snapshots
            recorded.return_value = recorded_rows

        yield setup


def _all_contract(snapshot: int = 100) -> dict:
    return {key: snapshot for branch in REANNOTATION_SOURCES.values() for key in branch}


def _recorded_at(snapshot: int = 100, ref: str = "latest") -> dict:
    return {key: (ref, snapshot) for key in _all_contract()}


def test_nothing_moved_means_nothing_to_reannotate(state):
    state(_all_contract(), _recorded_at())

    assert changed_sources(_CONF) == set()
    assert branches_to_reannotate(_CONF) == {
        "snv_variant": False,
        "snv_consequence": False,
        "cnv_occurrence": False,
    }


def test_a_moved_snapshot_gates_only_its_own_branch(state):
    state(_all_contract() | {_CONSEQUENCE_SOURCE: 101}, _recorded_at())

    assert changed_sources(_CONF) == {_CONSEQUENCE_SOURCE}
    gates = branches_to_reannotate(_CONF)
    assert gates["snv_consequence"] is True
    assert gates["snv_variant"] is False
    assert gates["cnv_occurrence"] is False


def test_a_variant_rebuild_drags_the_cnv_rebuild_with_it(state):
    """`nb_snv` counts rows in `snv__variant`, which the variant chain rebuilds with INSERT
    OVERWRITE -- so the CNV counts go stale if the CNV rebuild is skipped beside it."""
    state(_all_contract() | {_VARIANT_SOURCE: 101}, _recorded_at())

    gates = branches_to_reannotate(_CONF)
    assert gates["snv_variant"] is True
    assert gates["cnv_occurrence"] is True, "no CNV source moved, but snv__variant is about to"
    assert gates["snv_consequence"] is False


def test_a_cnv_source_alone_leaves_the_snv_branches_alone(state):
    state(_all_contract() | {_CNV_SOURCE: 101}, _recorded_at())

    gates = branches_to_reannotate(_CONF)
    assert gates["cnv_occurrence"] is True
    assert gates["snv_variant"] is False
    assert gates["snv_consequence"] is False


def test_a_source_never_recorded_counts_as_changed(state):
    """The first run, and any source added to the mapping since the last one."""
    recorded = _recorded_at()
    del recorded[_VARIANT_SOURCE]
    state(_all_contract(), recorded)

    assert changed_sources(_CONF) == {_VARIANT_SOURCE}


def test_an_empty_ledger_reannotates_everything(state):
    state(_all_contract(), {})

    assert branches_to_reannotate(_CONF) == {
        "snv_variant": True,
        "snv_consequence": True,
        "cnv_occurrence": True,
    }


def test_an_unresolvable_snapshot_fails_open(state):
    """A ref that returns no row -- a publish mid-flight, a catalog hiccup -- must re-annotate.

    Treating it as unchanged would skip the rebuild and then stamp a release row saying the
    warehouse is current, with nothing to show the read had failed.
    """
    state(_all_contract() | {_CONSEQUENCE_SOURCE: None}, _recorded_at())

    assert _CONSEQUENCE_SOURCE in changed_sources(_CONF)


def test_a_held_back_source_never_moves(state):
    """A legacy source is read without time travel and nothing refreshes it, so it cannot change.

    Reporting it as changed every run would defeat the gate entirely on an unmigrated environment.
    """
    state(
        _all_contract() | {_CONSEQUENCE_SOURCE: LEGACY},
        _recorded_at() | {_CONSEQUENCE_SOURCE: (LEGACY, None)},
    )

    assert changed_sources(_CONF) == set()


def test_migrating_a_source_counts_as_changed(state):
    """Held back last run, on OpenDataLake now: the values are about to be replaced."""
    state(_all_contract(), _recorded_at() | {_VARIANT_SOURCE: (LEGACY, None)})

    assert changed_sources(_CONF) == {_VARIANT_SOURCE}


def test_rolling_a_source_back_counts_as_changed(state):
    """The mirror image, and the one a gate would most plausibly get wrong."""
    state(
        _all_contract() | {_VARIANT_SOURCE: LEGACY},
        _recorded_at(),
    )

    assert changed_sources(_CONF) == {_VARIANT_SOURCE}


def test_every_branch_watches_at_least_one_source():
    """A branch with an empty source set would gate itself off permanently."""
    for branch, sources in REANNOTATION_SOURCES.items():
        assert sources, branch


def test_watched_sources_are_real_mapping_keys():
    """The map is hand-maintained against the statements; a typo would silently stop gating."""
    from radiant.tasks.data.radiant_tables import ICEBERG_OPEN_DATA_CONTRACT_MAPPING

    watched = {key for sources in REANNOTATION_SOURCES.values() for key in sources}

    assert watched <= set(ICEBERG_OPEN_DATA_CONTRACT_MAPPING), sorted(
        watched - set(ICEBERG_OPEN_DATA_CONTRACT_MAPPING)
    )


def test_every_branch_watches_a_source_that_can_actually_move():
    """Only a source with an OpenDataLake contract has a ref to resolve. One without is read straight
    off the legacy catalog, reports the `LEGACY` sentinel forever, and cannot open a gate -- so a
    branch watching nothing else would be opened by the `snv_variant` coupling or not at all.

    Every watched source has a contract today. The assertion is on the branch, not the source: it
    survives one being held back, and fires if a branch is ever left watching only inert sources.
    """
    from radiant.tasks.data.radiant_tables import ICEBERG_OPEN_DATA_CONTRACT_MAPPING

    for branch, sources in REANNOTATION_SOURCES.items():
        assert sources & set(ICEBERG_OPEN_DATA_CONTRACT_MAPPING), branch


def test_a_refs_read_that_raises_does_not_take_the_run_down():
    """StarRocks older than 3.4.1 has no `$refs` at all, and a catalog hiccup errors on any version.
    Either way the gate is a scheduling optimisation, and failing the DAG over one is the wrong
    trade: unknown reads as changed, and the branch rebuilds."""
    conf = _CONF | {"RADIANT_OPEN_DATA_USE_LEGACY_TABLES": "gnomad_sv"}
    with patch.object(open_data, "_query", side_effect=RuntimeError("Unknown table 'clinvar_v1$refs'")):
        snapshots = open_data.resolve_current_snapshots(conf)

    assert snapshots[_VARIANT_SOURCE] is None
    # A held-back source is never queried, so nothing could have raised for it.
    assert snapshots[_CNV_SOURCE] == LEGACY


def test_a_ledger_without_the_snapshot_column_reannotates_everything():
    """`snapshot_id` arrives with migrations/SJRA-1950_open_data_release_add_snapshot_id.sql. Before
    it is taken, the SELECT errors -- and "nothing recorded" is the honest reading, which is also the
    behaviour the DAG had before the gate existed. P4's INSERT still fails on the missing column, so
    the migration is not silently skippable."""
    with patch.object(open_data, "_query", side_effect=RuntimeError("Unknown column 'snapshot_id'")):
        assert open_data.last_recorded_snapshots(_CONF) == {}


def test_nothing_readable_at_all_gates_nothing_out():
    """Both halves failing is the un-migrated environment. Every branch must run."""
    with patch.object(open_data, "_query", side_effect=RuntimeError("boom")):
        assert branches_to_reannotate(_CONF) == {
            "snv_variant": True,
            "snv_consequence": True,
            "cnv_occurrence": True,
        }
