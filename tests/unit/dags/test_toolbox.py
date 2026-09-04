from radiant.dags.toolbox import _generate_user_password, _resolve_env_vars

DAG_ID = "radiant-toolbox"


def test_toolbox_dag_loads(dag_bag):
    dag = dag_bag.get_dag(DAG_ID)
    assert dag is not None
    assert not dag_bag.import_errors


def test_toolbox_dag_has_expected_tasks(dag_bag):
    dag = dag_bag.get_dag(DAG_ID)
    assert set(dag.task_ids) == {"generate_user_password", "resolve_env_vars", "run_toolbox_command"}


def test_toolbox_dag_params(dag_bag):
    dag = dag_bag.get_dag(DAG_ID)
    assert set(dag.params) == {"command", "args", "env_vars"}
    assert dag.params["command"] == "create-tenant"
    assert dag.params["args"] == []
    assert dag.params["env_vars"] == []


def test_resolve_env_vars():
    resolved = _resolve_env_vars([{"name": "RANGER_URL", "value": "http://ranger:6080"}])
    assert resolved == {"RANGER_URL": "http://ranger:6080"}


def test_resolve_env_vars_empty():
    assert _resolve_env_vars([]) == {}


def test_generate_user_password_only_for_create_user():
    assert _generate_user_password("create-tenant") == ""
    assert _generate_user_password("refresh-tenants") == ""


def test_generate_user_password_for_create_user():
    password = _generate_user_password("create-user", token_urlsafe=lambda n: "x" * n)
    assert password == "x" * 18
