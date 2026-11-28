def test_dag_loads_without_errors(dag_bag):
    dag = dag_bag.get_dag("radiant-import")
    assert dag is not None
    assert not dag_bag.import_errors


def test_dag_has_expected_tasks(dag_bag):
    dag = dag_bag.get_dag("radiant-import")
    expected_tasks = {
        "start",
        "fetch_import_facts",
        "decide_import",
        "journal_import",
        "list_processing",
        "set_processing",
        "partitioner_group.assign_partitions",
        "partitioner_group.insert_sequencing_experiment",
        "flag_deleted_sequencing_experiment",
        "prepare_tenants_tables",
        "assign_priority",
        "import_part",
        "data_integrity_checks",
        "journal_import_outcomes",
        "has_cases_to_release",
        "trigger_case_release",
    }
    assert set(dag.task_ids) == expected_tasks


def test_dag_task_dependencies_are_correct(dag_bag):
    dag = dag_bag.get_dag("radiant-import")

    def downstream(task_id):
        return dag.get_task(task_id).downstream_task_ids

    assert "fetch_import_facts" in downstream("start")
    assert "decide_import" in downstream("fetch_import_facts")
    assert {"journal_import", "partitioner_group.assign_partitions"} <= downstream("decide_import")
    assert "partitioner_group.insert_sequencing_experiment" in downstream("partitioner_group.assign_partitions")
    assert "flag_deleted_sequencing_experiment" in downstream("partitioner_group.insert_sequencing_experiment")
    # The staging rows are inserted and flagged before any part reads them.
    assert {"assign_priority", "prepare_tenants_tables"} <= downstream("flag_deleted_sequencing_experiment")
    assert "import_part" in downstream("assign_priority")
    assert "data_integrity_checks" in downstream("import_part")


def test_outcomes_are_journaled_whatever_the_parts_did(dag_bag):
    """All done: a failed part still journals its outcomes, and the cases of the other parts are released."""
    outcomes = dag_bag.get_dag("radiant-import").get_task("journal_import_outcomes")
    assert outcomes.trigger_rule == "all_done"
    assert "import_part" in outcomes.upstream_task_ids


def test_the_cases_with_new_data_are_handed_to_the_release(dag_bag):
    dag = dag_bag.get_dag("radiant-import")
    trigger = dag.get_task("trigger_case_release")
    assert trigger.trigger_dag_id == "radiant-case-release"
    # The statuses and emails are that DAG's: a failure there does not fail the import.
    assert trigger.wait_for_completion is False
    assert trigger.conf == {
        "case_ids": "{{ ti.xcom_pull(task_ids='journal_import_outcomes') }}",
        "excluded_tenants": "{{ params.excluded_tenants }}",
        "notify": "{{ params.notify }}",
    }
    assert dag.get_task("has_cases_to_release").upstream_task_ids == {"journal_import_outcomes"}


def test_data_integrity_checks_scoped_to_this_batch_tenants(dag_bag):
    """QA only tests the tenants this run touched, not every tenant on the platform.

    prepare_tenants_tables returns sorted(tenants); the DAG renders templates as native
    objects, so this reaches the QA DAG as a real list rather than a repr.
    """
    dag = dag_bag.get_dag("radiant-import")
    qa = dag.get_task("data_integrity_checks")
    assert qa.conf == {"tenants": "{{ ti.xcom_pull(task_ids='prepare_tenants_tables') }}"}
    assert dag.render_template_as_native_obj is True


def test_data_integrity_failure_does_not_fail_the_import(dag_bag):
    """The checks run after the data is loaded: a failed integrity run is accepted, not raised."""
    dag = dag_bag.get_dag("radiant-import")
    qa = dag.get_task("data_integrity_checks")
    assert qa.wait_for_completion is True
    assert set(qa.allowed_states) == {"success", "failed"}
    assert qa.failed_states == []


def test_partition_triggers_are_serialised_and_bounded(dag_bag):
    """The partition fan-out waits rather than races, and no wait is unbounded.

    `import_part` is `max_active_runs=1`, so a trigger whose partition is not next in line sits on a
    QUEUED run. Queued runs are promoted in `execution_date` order -- the instant each trigger ran --
    so the 1-slot pool is what keeps `assign_priority`'s order; and the wait needs a deadline, or one
    wedged partition holds every sibling trigger open forever.
    """
    from radiant.tasks.locking import STALE_LOCK_MAX_AGE

    trigger = dag_bag.get_dag("radiant-import").get_task("import_part")
    assert trigger.pool == "import_part"
    assert trigger.execution_timeout == STALE_LOCK_MAX_AGE
    # Bounded by the same constant the stale-lock reclaim uses, so the two cannot drift apart.
    assert dag_bag.get_dag("radiant-import-part").max_active_runs == 1


def test_the_cases_to_load_move_to_processing_beside_the_load(dag_bag):
    """Not upstream of the parts: a failed status move does not hold the data back."""
    dag = dag_bag.get_dag("radiant-import")
    assert {t.task_id for t in dag.get_task("set_processing").upstream_list} == {"list_processing"}
    assert {t.task_id for t in dag.get_task("list_processing").upstream_list} == {"decide_import"}
    assert "set_processing" not in dag.get_task("import_part").upstream_task_ids
