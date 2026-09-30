DAG_ID = "radiant-notify-cases"


def test_dag_is_importable(dag_bag):
    assert dag_bag.get_dag(DAG_ID) is not None
    assert not dag_bag.import_errors


def test_it_is_manual_and_serial(dag_bag):
    """Every run emails the laboratories, so nothing may trigger it on its own, and two runs
    on the same group at once would double the mail."""
    dag = dag_bag.get_dag(DAG_ID)
    assert dag.schedule_interval is None
    assert dag.max_active_runs == 1
    assert "manual" in dag.tags


def test_the_group_is_settled_before_the_notification(dag_bag):
    dag = dag_bag.get_dag(DAG_ID)
    assert set(dag.task_ids) == {"create_case_group", "notify_labs"}
    assert {t.task_id for t in dag.get_task("notify_labs").upstream_list} == {"create_case_group"}
    assert dag.validate() is None


def test_params_are_tenant_group_and_optional_cases(dag_bag):
    dag = dag_bag.get_dag(DAG_ID)
    assert set(dag.params) == {"tenant", "case_group_name", "case_ids"}
    assert dag.params["case_ids"] == []
