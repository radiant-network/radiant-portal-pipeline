from radiant.dags.toolbox import _delete_if_expired, _generate_user_password, _resolve_env_vars

DAG_ID = "radiant-toolbox"


def test_toolbox_dag_loads(dag_bag):
    dag = dag_bag.get_dag(DAG_ID)
    assert dag is not None
    assert not dag_bag.import_errors


def test_toolbox_dag_has_expected_tasks(dag_bag):
    dag = dag_bag.get_dag(DAG_ID)
    assert set(dag.task_ids) == {
        "generate_user_password",
        "select_execution_path",
        "check_import_lock",
        "resolve_env_vars",
        "run_toolbox_command",
    }


def test_toolbox_dag_params(dag_bag):
    dag = dag_bag.get_dag(DAG_ID)
    assert set(dag.params) == {"command", "args", "env_vars"}
    assert dag.params["command"] == "create-tenant"
    assert dag.params["args"] == []
    assert dag.params["env_vars"] == []
    assert "check-lock" in dag.params.get_param("command").schema["enum"]


def test_toolbox_dag_branch_routes_check_lock_to_its_own_task(dag_bag):
    dag = dag_bag.get_dag(DAG_ID)
    branch = dag.get_task("select_execution_path")
    # Both possible destinations must be direct downstream tasks of the branch, or Airflow
    # won't skip the one not chosen at runtime.
    assert "check_import_lock" in branch.downstream_task_ids
    assert "resolve_env_vars" in branch.downstream_task_ids


def test_toolbox_dag_check_lock_never_reaches_the_external_binary(dag_bag):
    dag = dag_bag.get_dag(DAG_ID)
    check_lock_task = dag.get_task("check_import_lock")
    downstream_ids = {t.task_id for t in check_lock_task.get_flat_relatives(upstream=False)}
    assert "run_toolbox_command" not in downstream_ids


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


def test_delete_if_expired_true_when_flag_present():
    assert _delete_if_expired(["-delete-if-expired"]) is True


def test_delete_if_expired_false_when_flag_absent():
    assert _delete_if_expired([]) is False
    assert _delete_if_expired(["-code", "demo"]) is False
