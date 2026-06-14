def test_data_integrity_dag_loads(dag_bag):
    dag = dag_bag.get_dag("radiant-data-integrity-starrocks")
    assert dag is not None
    assert not dag_bag.import_errors


def test_data_integrity_dag_has_expected_tasks(dag_bag):
    dag = dag_bag.get_dag("radiant-data-integrity-starrocks")
    assert set(dag.task_ids) == {"run_dbt", "upload_to_testquality", "check_qa_results"}
