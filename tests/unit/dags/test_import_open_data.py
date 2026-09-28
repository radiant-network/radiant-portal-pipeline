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
    # start + the metadata-refresh pair + the RCV group's 4 + cytoband's load + the COSMIC trigger
    # + 3 short-circuit gates
    assert len(dag.tasks) == 11 + len(gene_group_ids) + len(variant_group_ids) * 2


def test_metadata_cache_is_refreshed_before_any_source_is_read(dag_bag):
    """`latest` is a tag OpenDataLake moves on each publish, and StarRocks caches external-catalog
    metadata -- so a stale cache makes the refresh re-import the previous release (SJRA-1811 §4, P1)."""
    dag = dag_bag.get_dag(f"{NAMESPACE}-import-open-data")
    refresh = dag.get_task("refresh_iceberg_tables")
    assert refresh.upstream_task_ids == {"get_tables_to_refresh"}
    # Gates the whole chain: the first source load hangs off the refresh, not off `start`.
    assert refresh.downstream_task_ids == {"insert_hashes_1000_genomes"}
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
    ):
        assert head in last_source.downstream_task_ids


@pytest.mark.parametrize(
    ("task_id", "param"),
    [
        (f"{_RCV}.has_raw_filepaths", "raw_rcv_filepaths"),
        ("has_cytoband_filepath", "cytoband_filepath"),
        ("cosmic_gene_set.has_cosmic_gene_set_filepath", "cosmic_gene_set_filepath"),
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
    """`clinvar_rcv` has no pre-contract Iceberg table, so the skip is on the contract flag alone --
    `skip_legacy`'s `params.skip_legacy_tables and ...` would re-run a source that cannot be read on a
    full import too."""
    task = dag_bag.get_dag(f"{NAMESPACE}-import-open-data").get_task(f"{_RCV}.insert_raw_from_open_data")
    assert task.skip_if == "{{ not mapping.get('iceberg_clinvar_rcv_is_contract') }}"
    assert "params.skip_legacy_tables" not in task.skip_if


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
    import jinja2
    from jinja2.nativetypes import NativeEnvironment

    from radiant.dags.import_open_data import skip_legacy
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
        "params": {"skip_legacy_tables": flag},
    }
    env = NativeEnvironment(undefined=jinja2.StrictUndefined)
    for group, skipped in expected.items():
        rendered = env.from_string(skip_legacy(group)).render(context)
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
