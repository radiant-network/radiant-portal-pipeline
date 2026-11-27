from airflow.sdk.definitions.taskgroup import MappedTaskGroup

DAG_ID = "radiant-case-evaluation"


def test_dag_is_importable(dag_bag):
    assert dag_bag.get_dag(DAG_ID) is not None
    assert not dag_bag.import_errors


def test_dag_contains_the_expected_stages(dag_bag):
    assert set(dag_bag.get_dag(DAG_ID).task_ids) == {
        "has_cases",
        "fetch_case_statuses",
        "fetch_pipeline_state",
        "select_ready_cases",
        "evaluate_cases.set_in_progress",
        "evaluate_cases.create_case_group",
        "evaluate_cases.notify_labs",
    }


def test_it_reads_the_named_cases_only(dag_bag):
    dag = dag_bag.get_dag(DAG_ID)
    for task_id in ("fetch_case_statuses", "fetch_pipeline_state"):
        task = dag.get_task(task_id)
        assert task.parameters == {"case_ids": "{{ params.case_ids }}"}
        assert task.upstream_task_ids == {"has_cases"}
    assert dag.get_task("select_ready_cases").upstream_task_ids == {"fetch_case_statuses", "fetch_pipeline_state"}


def test_each_tenant_is_evaluated_on_its_own(dag_bag):
    group = dag_bag.get_dag(DAG_ID).task_group_dict["evaluate_cases"]
    assert isinstance(group, MappedTaskGroup)


def test_not_scheduled_and_one_run_at_a_time(dag_bag):
    dag = dag_bag.get_dag(DAG_ID)
    assert dag.schedule is None
    assert dag.max_active_runs == 1
