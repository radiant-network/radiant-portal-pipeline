from radiant.dags import NAMESPACE


def test_dag_is_importable(dag_bag):
    assert f"{NAMESPACE}-init-iceberg-tables" in dag_bag.dags
    dag = dag_bag.get_dag(f"{NAMESPACE}-init-iceberg-tables")
    assert dag is not None


def test_dag_has_correct_number_of_tasks(dag_bag):
    dag = dag_bag.get_dag(f"{NAMESPACE}-init-iceberg-tables")
    assert len(dag.tasks) == 6  # 1 init + 1 get namespace + 4 germline tables


def test_dag_has_correct_tasks(dag_bag):
    dag = dag_bag.get_dag(f"{NAMESPACE}-init-iceberg-tables")
    task_ids = [task.task_id for task in dag.tasks]
    assert "init_database" in task_ids
    assert "create_germline_snv_occurrence_table" in task_ids
    assert "create_germline_variant_table" in task_ids
    assert "create_germline_consequence_table" in task_ids
    assert "create_germline_cnv_occurrence_table" in task_ids
