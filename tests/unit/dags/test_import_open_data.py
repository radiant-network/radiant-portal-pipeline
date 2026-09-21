import pytest

from radiant.dags import NAMESPACE


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
        "cosmic_gene_panel",
        "mondo_term",
        "hpo_term",
    ]
    # start + the metadata-refresh pair + 3 file-driven load/insert tasks + 2 short-circuit gates
    assert len(dag.tasks) == 8 + len(gene_group_ids) + len(variant_group_ids) * 2


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
    assert dag.get_task("has_raw_rcv_filepaths").downstream_task_ids == {"load_raw_clinvar_rcv_summary"}
    assert dag.get_task("has_cytoband_filepath").downstream_task_ids == {"load_cytoband"}
    # Separate branches: cytoband is not downstream of the RCV chain any more.
    assert "load_cytoband" not in dag.get_task("insert_clinvar_rcv_summary").downstream_task_ids


@pytest.mark.parametrize(
    ("task_id", "param"),
    [("has_raw_rcv_filepaths", "raw_rcv_filepaths"), ("has_cytoband_filepath", "cytoband_filepath")],
)
def test_file_driven_gates_read_params_from_the_run_context(dag_bag, task_id, param):
    gate = dag_bag.get_dag(f"{NAMESPACE}-import-open-data").get_task(task_id)
    assert gate.op_args == (), "params must not be passed positionally"

    def decide(params):
        return gate.python_callable(**gate.determine_kwargs({"params": params}))

    assert decide({param: ["s3://bucket/file.tsv"]}) is True
    assert decide({}) is False
    assert decide({param: None}) is False


def test_dag_has_all_group_tasks(dag_bag):
    dag = dag_bag.get_dag(f"{NAMESPACE}-import-open-data")
    task_ids = [task.task_id for task in dag.tasks]
    group_ids = ["1000_genomes", "clinvar", "dbnsfp", "dbsnp", "gnomad", "spliceai", "topmed_bravo"]
    for group in group_ids:
        assert f"insert_hashes_{group}" in task_ids
        assert f"insert_{group}" in task_ids

    assert "insert_gnomad_constraint" in task_ids
    assert "insert_omim_gene_panel" in task_ids


def test_skip_legacy_tables_defaults_to_a_full_import(dag_bag):
    dag = dag_bag.get_dag(f"{NAMESPACE}-import-open-data")
    assert dag.params["skip_legacy_tables"] is False


def test_every_insert_is_gated(dag_bag):
    """The chain is static and serial, so the skip cannot be a branch -- each insert carries its own
    `skip_if`. A group without one re-imports a legacy source on a refresh run."""
    dag = dag_bag.get_dag(f"{NAMESPACE}-import-open-data")
    gated = {t.task_id for t in dag.tasks if getattr(t, "skip_if", None)}
    inserts = {t.task_id for t in dag.tasks if t.task_id.startswith("insert_") and "rcv" not in t.task_id}
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
        ("dbsnp", True, {"dbsnp": True, "clinvar": False, "gnomad": False, "ensembl_gene": True}),
        # `*` is the default, so on an unmigrated environment this skips everything.
        ("*", True, {"dbsnp": True, "clinvar": True, "gnomad": True, "cosmic_gene_panel": True}),
        # Fully migrated -- only the three with no upstream contract stay behind. These have no
        # `_is_contract` key at all, so they are also the StrictUndefined regression case.
        (
            "",
            True,
            {
                "dbsnp": False,
                "clinvar": False,
                "ensembl_gene": True,
                "ensembl_exon_by_gene": True,
                "cosmic_gene_panel": True,
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
    raises `UndefinedError` -- which is exactly how the three contract-less sources broke in production.
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
