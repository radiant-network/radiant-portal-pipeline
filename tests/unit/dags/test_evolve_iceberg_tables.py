from radiant.dags import NAMESPACE
from radiant.dags.evolve_iceberg_tables import ICEBERG_TABLES

DAG_ID = f"{NAMESPACE}-evolve-iceberg-tables"


def test_dag_is_importable(dag_bag):
    assert DAG_ID in dag_bag.dags
    assert dag_bag.get_dag(DAG_ID) is not None


def test_dag_has_correct_number_of_tasks(dag_bag):
    dag = dag_bag.get_dag(DAG_ID)
    assert len(dag.tasks) == 1 + len(ICEBERG_TABLES)  # 1 get namespace + 1 per table


def test_dag_has_one_task_per_iceberg_table(dag_bag):
    dag = dag_bag.get_dag(DAG_ID)
    task_ids = [task.task_id for task in dag.tasks]
    assert "get_iceberg_namespace" in task_ids
    for table in ICEBERG_TABLES:
        assert f"evolve_{table}_table_k8s" in task_ids


def test_every_table_task_depends_only_on_the_namespace(dag_bag):
    dag = dag_bag.get_dag(DAG_ID)
    for table in ICEBERG_TABLES:
        assert dag.get_task(f"evolve_{table}_table_k8s").upstream_task_ids == {"get_iceberg_namespace"}


def test_tables_match_the_init_dag(dag_bag):
    """The evolve DAG must cover every table the init DAG creates, or a schema change is missed."""
    init_dag = dag_bag.get_dag(f"{NAMESPACE}-init-iceberg-tables")
    created = {
        task.task_id.removeprefix("create_").removesuffix("_table_k8s")
        for task in init_dag.tasks
        if task.task_id.startswith("create_")
    }
    # the init DAG names two tasks after the schema rather than the table
    created = {"snv_variant" if name == "variant" else name for name in created}
    created = {"snv_consequence" if name == "consequence" else name for name in created}
    assert created == set(ICEBERG_TABLES)
