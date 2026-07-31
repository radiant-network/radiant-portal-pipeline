from airflow.utils.trigger_rule import TriggerRule

from radiant.dags import NAMESPACE

DAG_ID = f"{NAMESPACE}-import-snv-vcf"


def test_dag_is_importable(dag_bag):
    assert DAG_ID in dag_bag.dags
    dag = dag_bag.get_dag(DAG_ID)
    assert dag is not None


def test_dag_has_correct_number_of_tasks(dag_bag):
    dag = dag_bag.get_dag(DAG_ID)
    assert len(dag.tasks) == 7


def test_dag_has_correct_tasks(dag_bag):
    dag = dag_bag.get_dag(DAG_ID)
    task_ids = {task.task_id for task in dag.tasks}
    assert task_ids == {
        "get_iceberg_namespace",
        "get_germline_tasks",
        "get_somatic_tasks",
        "create_germline_parquet_files_k8s",
        "create_somatic_parquet_files_k8s",
        "merge_commits",
        "commit_partitions_k8s",
    }


def test_dag_task_dependencies_are_correct(dag_bag):
    dag = dag_bag.get_dag(DAG_ID)
    namespace_task = dag.get_task("get_iceberg_namespace")
    merge_commits_task = dag.get_task("merge_commits")
    commit_partitions = dag.get_task("commit_partitions_k8s")

    assert namespace_task in merge_commits_task.upstream_list
    assert commit_partitions in merge_commits_task.downstream_list

    # Both flows fan out per task and fan into the SAME commit task: `snv_variant` and
    # `snv_consequence` are written by each, so a single committer per part is what keeps
    # their Iceberg commits from racing each other.
    for get_task_id, extract_task_id in (
        ("get_germline_tasks", "create_germline_parquet_files_k8s"),
        ("get_somatic_tasks", "create_somatic_parquet_files_k8s"),
    ):
        get_tasks_task = dag.get_task(get_task_id)
        extract_task = dag.get_task(extract_task_id)
        assert extract_task in get_tasks_task.downstream_list
        assert merge_commits_task in extract_task.downstream_list


def test_merge_commits_survives_a_flow_with_no_tasks(dag_bag):
    """A part with no somatic tasks (or no germline ones) must still commit the other flow.

    That flow's writer expands to zero mapped instances, which Airflow marks SKIPPED. Under the
    default ALL_SUCCESS the skip would cascade and nothing would commit at all.
    """
    dag = dag_bag.get_dag(DAG_ID)
    assert dag.get_task("merge_commits").trigger_rule == TriggerRule.NONE_FAILED


def test_extraction_tasks_share_the_import_vcf_pool(dag_bag):
    """Germline and somatic writers draw from one pool so total node pressure stays bounded."""
    dag = dag_bag.get_dag(DAG_ID)
    for task_id in ("create_germline_parquet_files_k8s", "create_somatic_parquet_files_k8s"):
        assert dag.get_task(task_id).pool == "import_vcf"
