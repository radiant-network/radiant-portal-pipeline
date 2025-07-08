from radiant.dags.import_radiant import pre_process_exomiser_filepaths


def test_dag_loads_without_errors(dag_bag):
    dag = dag_bag.get_dag("radiant-import")
    assert dag is not None
    assert not dag_bag.import_errors


def test_dag_has_expected_tasks(dag_bag):
    dag = dag_bag.get_dag("radiant-import")
    expected_tasks = {
        "start",
        "partitioner_group.fetch_sequencing_experiment_delta",
        "partitioner_group.assign_partitions",
        "partitioner_group.insert_sequencing_experiment",
        "fetch_sequencing_experiment",
        "assign_priority",
        "import_part",
    }
    assert set(dag.task_ids) == expected_tasks


def test_dag_task_dependencies_are_correct(dag_bag):
    dag = dag_bag.get_dag("radiant-import")
    start = dag.get_task("start")
    fetch_delta = dag.get_task("partitioner_group.fetch_sequencing_experiment_delta")
    assign_partitions = dag.get_task("partitioner_group.assign_partitions")
    insert_exp = dag.get_task("partitioner_group.insert_sequencing_experiment")
    fetch_exp = dag.get_task("fetch_sequencing_experiment")
    assign_priority = dag.get_task("assign_priority")
    import_part = dag.get_task("import_part")

    # Check direct downstream dependencies
    assert fetch_delta in start.get_direct_relatives(upstream=False)
    assert assign_partitions in fetch_delta.get_direct_relatives(upstream=False)
    assert insert_exp in assign_partitions.get_direct_relatives(upstream=False)
    assert fetch_exp in insert_exp.get_direct_relatives(upstream=False) or fetch_exp in start.get_direct_relatives(
        upstream=False
    )
    assert assign_priority in fetch_exp.get_direct_relatives(upstream=False)
    assert import_part in assign_priority.get_direct_relatives(upstream=False)


def test_processes_valid_json_string():
    input_data = [{"exomiser_filepaths": '["file1", "file2"]'}]
    expected_output = [{"exomiser_filepaths": ["file1", "file2"]}]
    assert pre_process_exomiser_filepaths(input_data) == expected_output


def test_handles_empty_json_string():
    input_data = [{"exomiser_filepaths": "[]"}]
    expected_output = [{"exomiser_filepaths": []}]
    assert pre_process_exomiser_filepaths(input_data) == expected_output


def test_handles_invalid_json_string():
    input_data = [{"exomiser_filepaths": "[invalid_json"}]
    expected_output = [{"exomiser_filepaths": []}]
    assert pre_process_exomiser_filepaths(input_data) == expected_output


def test_handles_non_string_values():
    input_data = [{"exomiser_filepaths": None}]
    expected_output = [{"exomiser_filepaths": None}]
    assert pre_process_exomiser_filepaths(input_data) == expected_output


def test_processes_mixed_input():
    input_data = [
        {"exomiser_filepaths": '["file1"]'},
        {"exomiser_filepaths": "[]"},
        {"exomiser_filepaths": "[invalid_json"},
        {"exomiser_filepaths": None},
    ]
    expected_output = [
        {"exomiser_filepaths": ["file1"]},
        {"exomiser_filepaths": []},
        {"exomiser_filepaths": []},
        {"exomiser_filepaths": None},
    ]
    assert pre_process_exomiser_filepaths(input_data) == expected_output
