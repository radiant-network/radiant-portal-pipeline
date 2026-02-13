from radiant.dags import NAMESPACE


def test_dag_is_importable(dag_bag):
    assert f"{NAMESPACE}-init-iceberg-tables" in dag_bag.dags
    dag = dag_bag.get_dag(f"{NAMESPACE}-init-iceberg-tables")
    assert dag is not None


def test_dag_has_correct_number_of_tasks(dag_bag):
    dag = dag_bag.get_dag(f"{NAMESPACE}-init-iceberg-tables")
    assert len(dag.tasks) == 6  # 1 get namespace + 1 init + 4 germline tables


def test_dag_has_correct_tasks(dag_bag):
    dag = dag_bag.get_dag(f"{NAMESPACE}-init-iceberg-tables")
    task_ids = [task.task_id for task in dag.tasks]
    assert "get_iceberg_namespace" in task_ids
    assert "init_database_k8s" in task_ids
    assert "create_germline_snv_occurrence_table_k8s" in task_ids
    assert "create_germline_variant_table_k8s" in task_ids
    assert "create_germline_consequence_table_k8s" in task_ids
    assert "create_germline_cnv_occurrence_table_k8s" in task_ids


def test_dag_has_correct_task_dependencies(dag_bag):
    dag = dag_bag.get_dag(f"{NAMESPACE}-init-iceberg-tables")
    assert "get_iceberg_namespace" in dag.get_task("init_database_k8s").upstream_task_ids
    assert "init_database_k8s" in dag.get_task("create_germline_snv_occurrence_table_k8s").upstream_task_ids
    assert "create_germline_snv_occurrence_table_k8s" in dag.get_task("create_germline_variant_table_k8s").upstream_task_ids
    assert "create_germline_variant_table_k8s" in dag.get_task("create_germline_consequence_table_k8s").upstream_task_ids
    assert "create_germline_consequence_table_k8s" in dag.get_task("create_germline_cnv_occurrence_table_k8s").upstream_task_ids
