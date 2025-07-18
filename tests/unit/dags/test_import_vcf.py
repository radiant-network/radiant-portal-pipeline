from radiant.dags import NAMESPACE


def test_dag_is_importable(dag_bag):
    assert f"{NAMESPACE}-import-vcf" in dag_bag.dags
    dag = dag_bag.get_dag(f"{NAMESPACE}-import-vcf")
    assert dag is not None


def test_dag_has_correct_number_of_tasks(dag_bag):
    dag = dag_bag.get_dag(f"{NAMESPACE}-import-vcf")
    assert len(dag.tasks) == 4  # get_cases and import_vcf


def test_dag_has_correct_tasks(dag_bag):
    dag = dag_bag.get_dag(f"{NAMESPACE}-import-vcf")
    task_ids = [task.task_id for task in dag.tasks]
    assert task_ids[0] == "get_cases"
    assert task_ids[1] == "create_parquet_files"
    assert task_ids[2] == "merge_commits"
    assert task_ids[3] == "commit_partitions"


def test_dag_task_dependencies_are_correct(dag_bag):
    dag = dag_bag.get_dag(f"{NAMESPACE}-import-vcf")
    get_cases_task = dag.get_task("get_cases")
    import_vcf_task = dag.get_task("create_parquet_files")
    merge_commits_task = dag.get_task("merge_commits")
    commit_partitions = dag.get_task("commit_partitions")

    assert import_vcf_task in get_cases_task.downstream_list
    assert merge_commits_task in import_vcf_task.downstream_list
    assert commit_partitions in merge_commits_task.downstream_list
