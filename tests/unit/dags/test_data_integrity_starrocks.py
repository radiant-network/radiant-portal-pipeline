from radiant.dags.data_integrity_starrocks import _resolve_tenant_schemas

DAG_ID = "radiant-data-integrity-starrocks"


def test_data_integrity_dag_loads(dag_bag):
    dag = dag_bag.get_dag(DAG_ID)
    assert dag is not None
    assert not dag_bag.import_errors


def test_data_integrity_dag_has_expected_tasks(dag_bag):
    dag = dag_bag.get_dag(DAG_ID)
    assert set(dag.task_ids) == {"resolve_tenants", "run_dbt", "upload_to_testquality", "check_qa_results"}


def test_resolve_tenants_runs_before_dbt(dag_bag):
    """The tenant list reaches the container as an XCom, so it must be resolved first."""
    dag = dag_bag.get_dag(DAG_ID)
    assert "run_dbt" in dag.get_task("resolve_tenants").downstream_task_ids


def test_data_integrity_dag_params(dag_bag):
    """`tenants` defaults to empty, which means "discover every tenant".

    The scheduled import overrides it with just the tenants in its batch. Asserted as an
    exact set to catch param creep.
    """
    dag = dag_bag.get_dag(DAG_ID)
    assert set(dag.params) == {"skip_testquality_push", "tenants"}
    assert dag.params["tenants"] == []
    assert dag.params["skip_testquality_push"] is False


def test_resolve_tenant_schemas_uses_the_default_template():
    assert _resolve_tenant_schemas({}, ["chop", "chusj"]) == [
        {"code": "chop", "schema": "chop_tenant"},
        {"code": "chusj", "schema": "chusj_tenant"},
    ]


def test_resolve_tenant_schemas_honours_a_custom_template():
    """The container is handed resolved database names, so it never rebuilds them itself."""
    conf = {"RADIANT_TENANT_DB_TEMPLATE": "tenant_{tenant}_db"}
    assert _resolve_tenant_schemas(conf, ["chusj"]) == [{"code": "chusj", "schema": "tenant_chusj_db"}]
