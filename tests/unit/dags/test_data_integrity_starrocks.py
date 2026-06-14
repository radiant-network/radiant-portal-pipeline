import xml.etree.ElementTree as ET


def test_to_junit_xml_contract():
    """Lock the contract the DAG relies on (upload_to_testquality task): given a
    run_results-shaped dict, to_junit_xml returns a parsable JUnit XML string.

    If a QA change to data_qa/scripts/run_results_to_junit.py breaks this test, the
    data-integrity-starrocks DAG is broken too. The DAG only needs the function to
    exist, accept a dict, and return valid XML — internal XML structure is QA's concern.
    """
    from radiant.data_qa.scripts.run_results_to_junit import to_junit_xml

    run_results = {
        "metadata": {"project_name": "radiant_data_qa"},
        "elapsed_time": 1.0,
        "results": [
            {"unique_id": "test.radiant.should_be_unique_x.abc", "status": "pass", "execution_time": 0.1},
            {
                "unique_id": "test.radiant.should_not_contain_null_y.def",
                "status": "fail",
                "execution_time": 0.2,
                "failures": 3,
                "message": "3 rows",
            },
        ],
    }

    xml = to_junit_xml(run_results)  # 1. import exists + 2. signature accepts a dict

    assert isinstance(xml, str)  # 3. returns a string
    root = ET.fromstring(xml)  # 3. parsable XML
    assert root.get("tests") == "2"  # DAG expects a coherent JUnit report
    assert root.get("failures") == "1"


def test_data_integrity_dag_loads(dag_bag):
    dag = dag_bag.get_dag("radiant-data-integrity-starrocks")
    assert dag is not None
    assert not dag_bag.import_errors


def test_data_integrity_dag_has_expected_tasks(dag_bag):
    dag = dag_bag.get_dag("radiant-data-integrity-starrocks")
    assert set(dag.task_ids) == {"run_dbt", "upload_to_testquality", "check_qa_results"}
