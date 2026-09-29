import pytest

from radiant.dags import NAMESPACE

# The ClinVar RCV summary task group; its members are addressed as `<group>.<task_id>`.
_RCV = "clinvar_rcv_summary"


def test_dag_is_importable(dag_bag):
    assert f"{NAMESPACE}-import-open-data" in dag_bag.dags
    dag = dag_bag.get_dag(f"{NAMESPACE}-import-open-data")
    assert dag is not None


def test_dag_has_correct_number_of_tasks(dag_bag):
    dag = dag_bag.get_dag(f"{NAMESPACE}-import-open-data")
    variant_group_ids = ["1000_genomes", "clinvar", "dbnsfp", "dbsnp", "gnomad", "spliceai", "topmed_bravo"]
    gene_group_ids = [
        "gnomad_constraint",
        "omim_gene_panel",
        "hpo_gene_panel",
        "orphanet_gene_panel",
        "ensembl_gene",
        "ensembl_exon_by_gene",
        "ddd_gene_panel",
        "mondo_term",
        "hpo_term",
    ]
    # start + the metadata-refresh pair + the snapshot gate + the ledger write's 3 + the RCV
    # group's 4 + cytoband's load + the COSMIC trigger + 3 short-circuit gates
    assert len(dag.tasks) == 15 + len(gene_group_ids) + len(variant_group_ids) * 2


def test_metadata_cache_is_refreshed_before_any_source_is_read(dag_bag):
    """`latest` is a tag OpenDataLake moves on each publish, and StarRocks caches external-catalog
    metadata -- so a stale cache makes the refresh re-import the previous release (SJRA-1811 §4, P1)."""
    dag = dag_bag.get_dag(f"{NAMESPACE}-import-open-data")
    refresh = dag.get_task("refresh_iceberg_tables")
    assert refresh.upstream_task_ids == {"get_tables_to_refresh"}
    # Gates the whole chain: nothing reads a source before the refresh, and the snapshot gate sits
    # between the two so its `$refs` read is not answered from metadata this run has superseded.
    assert refresh.downstream_task_ids == {"compute_import_gates"}
    assert dag.get_task("compute_import_gates").downstream_task_ids == {"insert_hashes_1000_genomes"}
    assert dag.get_task("start").downstream_task_ids == {"get_tables_to_refresh"}


def test_file_driven_loads_are_gated_on_their_params(dag_bag):
    dag = dag_bag.get_dag(f"{NAMESPACE}-import-open-data")
    assert dag.get_task(f"{_RCV}.has_raw_filepaths").downstream_task_ids == {f"{_RCV}.load_raw_from_files"}
    assert dag.get_task("has_cytoband_filepath").downstream_task_ids == {"load_cytoband"}
    assert dag.get_task("cosmic_gene_set.has_cosmic_gene_set_filepath").downstream_task_ids == {
        "cosmic_gene_set.trigger_import_cosmic_gene_set"
    }
    # Separate branches: cytoband is not downstream of the RCV chain any more.
    assert "load_cytoband" not in dag.get_task(f"{_RCV}.insert_summary").downstream_task_ids
    # Every file-driven branch hangs off the end of the source chain, independently of the others.
    last_source = dag.get_task("insert_hpo_term")
    for head in (
        f"{_RCV}.insert_raw_from_open_data",
        "has_cytoband_filepath",
        "cosmic_gene_set.has_cosmic_gene_set_filepath",
        "cosmic_mutation_set.has_cosmic_mutation_set_filepath",
    ):
        assert head in last_source.downstream_task_ids


@pytest.mark.parametrize(
    ("task_id", "param"),
    [
        (f"{_RCV}.has_raw_filepaths", "raw_rcv_filepaths"),
        ("has_cytoband_filepath", "cytoband_filepath"),
        ("cosmic_gene_set.has_cosmic_gene_set_filepath", "cosmic_gene_set_filepath"),
        ("cosmic_mutation_set.has_cosmic_mutation_set_filepath", "cosmic_mutation_set_filepath"),
    ],
)
def test_file_driven_gates_read_params_from_the_run_context(dag_bag, task_id, param):
    gate = dag_bag.get_dag(f"{NAMESPACE}-import-open-data").get_task(task_id)
    assert gate.op_args == (), "params must not be passed positionally"

    def decide(params):
        return gate.python_callable(**gate.determine_kwargs({"params": params}))

    assert decide({param: ["s3://bucket/file.tsv"]}) is True
    assert decide({}) is False
    assert decide({param: None}) is False


def test_cosmic_is_handed_off_to_its_own_dag(dag_bag):
    """COSMIC has no OpenDataLake contract and is no longer an Iceberg source: the census TSV is loaded
    by radiant-import-cosmic-gene-set, which this DAG triggers with the filepath it was given."""
    dag = dag_bag.get_dag(f"{NAMESPACE}-import-open-data")
    assert "insert_cosmic_gene_panel" not in {t.task_id for t in dag.tasks}
    # Self-contained group: the gate and the trigger are the only COSMIC tasks left in this DAG.
    assert {t.task_id for t in dag.task_group.get_child_by_label("cosmic_gene_set")} == {
        "cosmic_gene_set.has_cosmic_gene_set_filepath",
        "cosmic_gene_set.trigger_import_cosmic_gene_set",
    }
    trigger = dag.get_task("cosmic_gene_set.trigger_import_cosmic_gene_set")
    assert trigger.trigger_dag_id == f"{NAMESPACE}-import-cosmic-gene-set"
    assert trigger.conf == {"cosmic_gene_set_filepath": "{{ params.cosmic_gene_set_filepath }}"}
    assert trigger.wait_for_completion is True
    assert dag.params["cosmic_gene_set_filepath"] is None


def test_cosmic_mutation_set_is_handed_off_to_its_own_dag(dag_bag):
    """The Mutation Census needs a bcftools normalization pod before its load, which only
    radiant-import-cosmic-mutation-set knows how to run; this DAG just gates and triggers it."""
    dag = dag_bag.get_dag(f"{NAMESPACE}-import-open-data")
    assert {t.task_id for t in dag.task_group.get_child_by_label("cosmic_mutation_set")} == {
        "cosmic_mutation_set.has_cosmic_mutation_set_filepath",
        "cosmic_mutation_set.cosmic_mutation_set_conf",
        "cosmic_mutation_set.trigger_import_cosmic_mutation_set",
    }
    assert dag.get_task("cosmic_mutation_set.has_cosmic_mutation_set_filepath").downstream_task_ids == {
        "cosmic_mutation_set.cosmic_mutation_set_conf"
    }
    trigger = dag.get_task("cosmic_mutation_set.trigger_import_cosmic_mutation_set")
    assert trigger.trigger_dag_id == f"{NAMESPACE}-import-cosmic-mutation-set"
    assert trigger.conf == "{{ ti.xcom_pull(task_ids='cosmic_mutation_set.cosmic_mutation_set_conf') }}"
    assert trigger.wait_for_completion is True
    assert dag.params["cosmic_mutation_set_filepath"] is None
    assert dag.params["reference_fasta_filepath"] is None

    conf = dag.get_task("cosmic_mutation_set.cosmic_mutation_set_conf").python_callable
    # The FASTA is only forwarded when given, so the triggered DAG's env-var default still applies.
    assert conf(params={"cosmic_mutation_set_filepath": "s3://b/cmc.tsv.gz", "reference_fasta_filepath": None}) == {
        "cosmic_mutation_set_filepath": "s3://b/cmc.tsv.gz"
    }
    assert conf(
        params={"cosmic_mutation_set_filepath": "s3://b/cmc.tsv.gz", "reference_fasta_filepath": "s3://r/f.fa"}
    ) == {
        "cosmic_mutation_set_filepath": "s3://b/cmc.tsv.gz",
        "reference_fasta_filepath": "s3://r/f.fa",
    }


def test_dag_has_all_group_tasks(dag_bag):
    dag = dag_bag.get_dag(f"{NAMESPACE}-import-open-data")
    task_ids = [task.task_id for task in dag.tasks]
    group_ids = ["1000_genomes", "clinvar", "dbnsfp", "dbsnp", "gnomad", "spliceai", "topmed_bravo"]
    for group in group_ids:
        assert f"insert_hashes_{group}" in task_ids
        assert f"insert_{group}" in task_ids

    assert "insert_gnomad_constraint" in task_ids
    assert "insert_omim_gene_panel" in task_ids


def test_the_raw_rcv_table_is_loaded_from_opendatalake_when_the_source_is_not_held_back(dag_bag):
    """`clinvar_rcv` has no pre-contract Iceberg table, so the catalog half of the skip is on the
    contract flag alone -- `skip_legacy`'s `params.skip_legacy_tables and ...` would re-run a source
    that cannot be read on a full import too. The snapshot half is the same as every other group's."""
    from radiant.dags.import_open_data import skip_unchanged

    task = dag_bag.get_dag(f"{NAMESPACE}-import-open-data").get_task(f"{_RCV}.insert_raw_from_open_data")
    assert "params.skip_legacy_tables" not in task.skip_if
    assert skip_unchanged("clinvar_rcv") in task.skip_if

    # Held back: skipped whatever the gates say, because there is nothing to read.
    assert _render_skip_if(task.skip_if, held_back="clinvar_rcv", gates={"iceberg_clinvar_rcv": True}) is True
    # On contract and unchanged: skipped. On contract and moved: imported.
    assert _render_skip_if(task.skip_if, gates={"iceberg_clinvar_rcv": False}) is True
    assert _render_skip_if(task.skip_if, gates={"iceberg_clinvar_rcv": True}) is False


def test_the_rcv_group_is_one_serial_chain(dag_bag):
    dag = dag_bag.get_dag(f"{NAMESPACE}-import-open-data")
    chain = ["insert_raw_from_open_data", "has_raw_filepaths", "load_raw_from_files", "insert_summary"]

    # The group's root hangs off the tail of the source chain, then it is one line to the summary.
    assert dag.get_task(f"{_RCV}.{chain[0]}").upstream_task_ids == {"insert_hpo_term"}
    for upstream, downstream in zip(chain, chain[1:], strict=False):
        assert dag.get_task(f"{_RCV}.{upstream}").downstream_task_ids == {f"{_RCV}.{downstream}"}

    # One arm always skips -- the OpenDataLake insert when the source is held back, the broker load
    # when no filepaths are passed -- so neither may take the rest of the chain down with it.
    assert dag.get_task(f"{_RCV}.has_raw_filepaths").trigger_rule == "none_failed"
    assert dag.get_task(f"{_RCV}.insert_summary").trigger_rule == "none_failed"
    # The load keeps ALL_SUCCESS: that is how the short-circuit gate above skips it.
    assert dag.get_task(f"{_RCV}.load_raw_from_files").trigger_rule == "all_success"


def test_skip_legacy_tables_defaults_to_a_full_import(dag_bag):
    dag = dag_bag.get_dag(f"{NAMESPACE}-import-open-data")
    assert dag.params["skip_legacy_tables"] is False


def test_every_insert_is_gated(dag_bag):
    """The chain is static and serial, so the skip cannot be a branch -- each insert carries its own
    `skip_if`. A group without one re-imports a legacy source on a refresh run."""
    dag = dag_bag.get_dag(f"{NAMESPACE}-import-open-data")
    # The RCV group's own insert carries a skip_if too, but on the contract flag alone -- it is
    # covered by its own test rather than by this chain-wide one.
    gated = {t.task_id for t in dag.tasks if getattr(t, "skip_if", None) and not t.task_id.startswith(f"{_RCV}.")}
    inserts = {t.task_id for t in dag.tasks if t.task_id.startswith("insert_")}
    assert gated == inserts


def test_a_skipped_group_does_not_cascade_down_the_chain(dag_bag):
    """`chain()` wires the inserts head to tail. Under the default ALL_SUCCESS one skipped group would
    skip every source after it."""
    dag = dag_bag.get_dag(f"{NAMESPACE}-import-open-data")
    for task in dag.tasks:
        if getattr(task, "skip_if", None):
            assert task.trigger_rule == "none_failed", task.task_id


def _render_skip_if(template, held_back="", skip_legacy_tables=False, gates=None, force_import=False):
    """Render one `skip_if` the way Airflow will.

    Native types and `StrictUndefined` because that is what this DAG runs under
    (`DAG(template_undefined=jinja2.StrictUndefined)`, plus `render_template_as_native_obj=True`). A
    lenient environment turns a missing mapping key into a falsy Undefined and passes; Airflow raises
    `UndefinedError` -- which is exactly how the then contract-less ensembl sources broke in
    production before they had an `_is_contract` flag. And a string-rendering environment returns
    "False", which is truthy and would skip every gated task.
    """
    import types

    import jinja2
    from jinja2.nativetypes import NativeEnvironment

    from radiant.tasks.data.radiant_tables import get_iceberg_open_data_mapping

    conf = {
        "RADIANT_ICEBERG_CATALOG": "radiant_iceberg_catalog",
        "RADIANT_ICEBERG_NAMESPACE": "radiant",
        "RADIANT_OPEN_DATA_CATALOG": "odl",
        "RADIANT_OPEN_DATA_DATABASE": "odl_db",
        "RADIANT_OPEN_DATA_USE_LEGACY_TABLES": held_back,
    }
    context = {
        "mapping": get_iceberg_open_data_mapping(conf),
        "params": {"skip_legacy_tables": skip_legacy_tables, "force_import": force_import},
        "ti": types.SimpleNamespace(xcom_pull=lambda task_ids: gates),
    }
    return NativeEnvironment(undefined=jinja2.StrictUndefined).from_string(template).render(context)


@pytest.mark.parametrize(
    ("held_back", "flag", "expected"),
    [
        # Param off is a full import, whatever the catalog says.
        ("dbsnp", False, {"dbsnp": False, "clinvar": False, "ensembl_gene": False}),
        # Held back -> reads the Radiant catalog -> skipped. Its neighbours still import.
        ("dbsnp", True, {"dbsnp": True, "clinvar": False, "gnomad": False, "ensembl_gene": False}),
        # ensembl is gated like any other source now that OpenDataLake publishes it.
        ("ensembl_gene", True, {"dbsnp": False, "ensembl_gene": True, "ensembl_exon_by_gene": False}),
        # `*` is the default, so on an unmigrated environment this skips everything.
        ("*", True, {"dbsnp": True, "clinvar": True, "gnomad": True, "ensembl_exon_by_gene": True}),
        # Fully migrated -- nothing stays behind.
        (
            "",
            True,
            {
                "dbsnp": False,
                "clinvar": False,
                "ensembl_gene": False,
                "ensembl_exon_by_gene": False,
            },
        ),
    ],
)
def test_skip_if_resolves_from_the_catalog_the_source_landed_on(held_back, flag, expected):
    """`skip_if` is rendered, not computed, so assert the rendered value -- and that it is a real bool.
    The string "False" is truthy and would skip every gated task.

    Rendered with `StrictUndefined` and native types because that is what Airflow does
    (`DAG(template_undefined=jinja2.StrictUndefined)`, plus `render_template_as_native_obj=True` on this
    DAG). A lenient environment turns a missing mapping key into a falsy Undefined and passes; Airflow
    raises `UndefinedError` -- which is exactly how the then contract-less ensembl sources broke in
    production before they had an `_is_contract` flag.
    """
    from radiant.dags.import_open_data import gated

    # No gate XCom, so the snapshot half is False throughout and what is left is the catalog half.
    for group, skipped in expected.items():
        rendered = _render_skip_if(gated(group), held_back=held_back, skip_legacy_tables=flag)
        assert rendered is skipped, f"{group} rendered {rendered!r}"


def test_every_group_gates_on_the_source_its_sql_reads():
    """`source_keys` is a literal; re-derive it from the SQL so a re-pointed source cannot leave the gate
    watching the wrong table."""
    import re

    from radiant.dags import DAGS_DIR
    from radiant.dags.import_open_data import gene_group_ids, source_keys, variant_group_ids
    from radiant.tasks.data.radiant_tables import IS_CONTRACT_SUFFIX

    for group in variant_group_ids + gene_group_ids:
        sql = (DAGS_DIR / "sql" / "open_data" / f"{group}_insert.sql").read_text()
        keys = {k.removesuffix(IS_CONTRACT_SUFFIX) for k in re.findall(r"mapping\.(iceberg_\w+)", sql)}
        assert len(keys) == 1, f"{group}_insert.sql reads {sorted(keys)}"
        assert source_keys.get(group, f"iceberg_{group}") == keys.pop(), group


# --- The snapshot gate (SJRA-1950) ----------------------------------------------------------------
#
# A full import re-reads every OpenDataLake source, and most weeks only some of them published. The
# gate compares the snapshot each ref resolves to against `open_data_release`, so an unchanged source
# is not read again.

_GATED_SOURCE = "dbsnp"
_GATED_KEY = "iceberg_dbsnp"


def test_force_import_defaults_to_the_gated_run(dag_bag):
    dag = dag_bag.get_dag(f"{NAMESPACE}-import-open-data")
    assert dag.params["force_import"] is False


def test_every_group_carries_the_snapshot_clause_for_its_own_source(dag_bag):
    """`source_keys` already maps a group to the source its SQL reads. A clause keyed to the wrong
    source skips on someone else's publish, or re-reads on every run."""
    from radiant.dags.import_open_data import gene_group_ids, skip_unchanged, variant_group_ids

    dag = dag_bag.get_dag(f"{NAMESPACE}-import-open-data")
    for group in variant_group_ids + gene_group_ids:
        clause = skip_unchanged(group)
        for task_id in (f"insert_{group}", f"insert_hashes_{group}"):
            if task_id in {t.task_id for t in dag.tasks}:
                assert clause in dag.get_task(task_id).skip_if, task_id


@pytest.mark.parametrize(
    ("gates", "force_import", "skipped"),
    [
        # The ordinary run: OpenDataLake published, so the source is re-read.
        ({_GATED_KEY: True}, False, False),
        # Nothing published since the last recorded release.
        ({_GATED_KEY: False}, False, True),
        # The override. It has to beat a closed gate, or there is no way to reload after editing an
        # insert statement or truncating the StarRocks copy -- the gate sees neither.
        ({_GATED_KEY: False}, True, False),
        # Fail open: no XCom at all (the gate task was cleared or skipped), and an XCom that came
        # back without this source in it.
        (None, False, False),
        ({}, False, False),
        ({"iceberg_clinvar": False}, False, False),
    ],
)
def test_the_snapshot_gate_renders_to_a_real_bool(dag_bag, gates, force_import, skipped):
    """`skip_if` is rendered, not computed, and the operator treats any non-empty string as truthy."""
    from radiant.dags.import_open_data import gated

    rendered = _render_skip_if(gated(_GATED_SOURCE), gates=gates, force_import=force_import)
    assert rendered is skipped, f"rendered {rendered!r}"


def test_a_held_back_source_is_never_skipped_by_the_snapshot_half(dag_bag):
    """A held-back source is read off the legacy catalog without time travel: no ref, no snapshot,
    nothing that could ever report it as moved. Gating it on snapshots would import it once and never
    again -- so on a full import it is read whatever the gates say, and only `skip_legacy_tables`
    decides otherwise."""
    from radiant.dags.import_open_data import gated

    template = gated(_GATED_SOURCE)
    for gates in ({_GATED_KEY: False}, {}, None):
        assert _render_skip_if(template, held_back=_GATED_SOURCE, gates=gates) is False, gates

    # The catalog half still governs it.
    assert _render_skip_if(template, held_back=_GATED_SOURCE, skip_legacy_tables=True, gates=None) is True


# --- The release ledger ---------------------------------------------------------------------------
#
# `open_data_release.imported_snapshot_id` is what StarRocks now holds. The import writes it; P4
# promotes it to `reannotated_snapshot_id`. One table, two columns -- a standalone import moves the first
# without the second, which is why one column cannot serve both gates.


def test_the_import_records_what_it_loaded(dag_bag):
    dag = dag_bag.get_dag(f"{NAMESPACE}-import-open-data")
    record = dag.get_task("record_open_data_import")

    assert record.trigger_rule == "none_failed", "a gated-out source skips its insert; the row still stands"
    assert getattr(record, "skip_if", None) is None, "the ledger is what the next run compares against"


def test_the_ledger_is_written_after_every_iceberg_sourced_insert(dag_bag):
    """Including the RCV group's -- `clinvar_rcv` is a contract source and carries a row."""
    dag = dag_bag.get_dag(f"{NAMESPACE}-import-open-data")

    def reaches(a, b, seen=None):
        seen = seen if seen is not None else set()
        if a.task_id in seen:
            return False
        seen.add(a.task_id)
        if b.task_id in a.downstream_task_ids:
            return True
        return any(reaches(dag.get_task(t), b, seen) for t in a.downstream_task_ids)

    record = dag.get_task("record_open_data_import")
    for task in dag.tasks:
        if task.task_id.startswith("insert_") or task.task_id == f"{_RCV}.insert_raw_from_open_data":
            assert reaches(task, record), task.task_id


def test_the_ledger_rows_are_built_downstream_of_the_gate(dag_bag):
    """`build_import_rows` reads the snapshots the gate resolved, over XCom. Resolving them again at
    the end of the run would record whatever `latest` points at by then rather than what the inserts
    read. The XCom is only there to read if the gate is upstream, so that edge is the guarantee."""
    dag = dag_bag.get_dag(f"{NAMESPACE}-import-open-data")
    rows = dag.get_task("build_import_rows")

    def reaches(a, b, seen=None):
        seen = seen if seen is not None else set()
        if a.task_id in seen:
            return False
        seen.add(a.task_id)
        if b.task_id in a.downstream_task_ids:
            return True
        return any(reaches(dag.get_task(t), b, seen) for t in a.downstream_task_ids)

    assert reaches(dag.get_task("compute_import_gates"), rows)
    assert rows.task_id in dag.get_task("record_open_data_import").upstream_task_ids
