from radiant.dags import NAMESPACE

_DAG_ID = f"{NAMESPACE}-import-cosmic-gene-set"


def test_dag_is_importable(dag_bag):
    assert not dag_bag.import_errors
    assert dag_bag.get_dag(_DAG_ID) is not None


def test_dag_loads_the_census_then_rebuilds_the_panel(dag_bag):
    dag = dag_bag.get_dag(_DAG_ID)
    assert {t.task_id for t in dag.tasks} == {"load_cosmic_gene_set", "insert_cosmic_gene_panel"}
    assert dag.get_task("load_cosmic_gene_set").downstream_task_ids == {"insert_cosmic_gene_panel"}


def test_load_replaces_cosmic_gene_set_from_the_filepath_param(dag_bag):
    load = dag_bag.get_dag(_DAG_ID).get_task("load_cosmic_gene_set")
    assert load.table == "{{ mapping.starrocks_cosmic_gene_set }}"
    assert load.truncate is True
    assert load.parameters == {"tsv_filepath": "{{ params.cosmic_gene_set_filepath }}"}
    # The DagBag resolves `template_ext` files at parse time, so `sql` is already the statement.
    assert load.sql.lstrip("-").lstrip().startswith("Loads the COSMIC Cancer Gene Census")
    assert "LOAD LABEL {{ database_name }}.{{ load_label }}" in load.sql
    assert "DATA INFILE %(tsv_filepath)s" in load.sql


def test_panel_insert_reads_the_starrocks_table_not_iceberg(dag_bag):
    from radiant.dags import DAGS_DIR

    sql = (DAGS_DIR / "sql" / "open_data" / "cosmic_gene_panel_insert.sql").read_text()
    assert "mapping.starrocks_cosmic_gene_set" in sql
    assert "mapping.iceberg_" not in sql


def test_filepath_param_is_required(dag_bag):
    """A run without a file is a mistake: the load truncates first, so it must fail at trigger time
    rather than empty the table."""
    dag = dag_bag.get_dag(_DAG_ID)
    param = dag.params.get_param("cosmic_gene_set_filepath")
    assert param.schema["type"] == "array"
    assert not param.has_value
