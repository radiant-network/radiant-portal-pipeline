import logging

import pytest
from airflow.exceptions import AirflowSkipException

from radiant.tasks.nextflow import case_status
from radiant.tasks.nextflow.portal import PortalError
from tests.unit.nextflow.cnv.conftest import member_row as cnv_member_row
from tests.unit.nextflow.conftest import member_row as snv_member_row
from tests.unit.nextflow.qc.conftest import document_rows as qc_document_rows


def status_row(case_id, status_code="submitted", has_variants=False, tenant_code="radiant"):
    return {"case_id": case_id, "tenant_code": tenant_code, "status_code": status_code, "has_variants": has_variants}


def keep_all(cases):
    return cases


def test_work_waiting_is_the_union_of_every_pipeline_and_the_import_delta():
    waiting = case_status.cases_with_work(
        snv_rows=[snv_member_row(case_id=1)],
        cnv_rows=[cnv_member_row(case_id=2)],
        qc_rows=qc_document_rows(case_id=3),
        import_rows=[{"case_id": 4}, {"case_id": 4}],
        locate_qc=keep_all,
    )
    assert waiting == {1, 2, 3, 4}


@pytest.mark.parametrize("reason", ["pending_sequencing", "no_gvcf", "tenant_not_granted"])
def test_a_case_the_pipeline_would_exclude_is_not_work_waiting(reason):
    """Moved to `processing`, it would wait there with nothing running for it."""
    waiting = case_status.cases_with_work(
        snv_rows=[snv_member_row(case_id=1, exclusion_reason=reason)],
        cnv_rows=[cnv_member_row(case_id=2, exclusion_reason=reason)],
        qc_rows=qc_document_rows(case_id=3, exclusion_reason=reason),
        import_rows=[],
        locate_qc=keep_all,
    )
    assert waiting == set()


def test_a_qc_case_whose_dragen_metrics_cannot_be_found_is_not_work_waiting():
    """The QC DAG excludes it at `locate_metrics`, so it would hold the case in processing for ever."""
    probed = []

    def locate_none(cases):
        probed.extend(c.case_id for c in cases)
        return []

    waiting = case_status.cases_with_work([], [], qc_document_rows(case_id=3), [], locate_qc=locate_none)
    assert probed == [3]
    assert waiting == set()


def test_the_qc_probe_is_skipped_when_there_is_no_qc_candidate():
    """It needs S3 and the workspace root; a run with no QC work must not depend on either."""

    def fail(_):
        raise AssertionError("probed with no candidate")

    assert case_status.cases_with_work([snv_member_row(case_id=1)], [], [], [], locate_qc=fail) == {1}


def test_the_s3_locator_runs_the_qc_dag_s_probe(monkeypatch):
    from radiant.tasks.nextflow.qc import metrics

    calls = []

    def locate_metrics(cases, list_dir, list_tree, inputs_root):
        calls.append(inputs_root)
        return cases[:1], []

    monkeypatch.setattr(metrics, "locate_metrics", locate_metrics)
    monkeypatch.setattr(metrics, "S3Lister", lambda: type("L", (), {"list_dir": None, "list_tree": None})())
    assert case_status.s3_qc_locator("s3://inputs")(["a", "b"]) == ["a"]
    assert calls == ["s3://inputs"]


def test_nothing_discovered_is_no_work():
    assert case_status.cases_with_work([], [], [], [], locate_qc=keep_all) == set()


def test_only_submitted_cases_with_work_or_variants_move_to_processing():
    rows = [
        status_row(1),  # work waiting
        status_row(2, has_variants=True),  # work done before the control DAG saw it
        status_row(3),  # nothing to do yet
        status_row(4, status_code="processing"),  # already there
        status_row(5, status_code="processing", has_variants=True),
    ]
    assert case_status.plan_processing(rows, waiting={1, 4}, tenants=[]) == [{"tenant": "radiant", "case_ids": [1, 2]}]


def test_every_done_processing_case_with_variants_moves_to_in_progress_not_only_this_runs():
    rows = [
        status_row(1, status_code="processing", has_variants=True),
        status_row(2, status_code="processing"),  # no variants imported yet: stays
        status_row(3, status_code="submitted", has_variants=True),  # its tenant refused processing
    ]
    assert case_status.plan_in_progress(rows, waiting=set(), tenants=[]) == [{"tenant": "radiant", "case_ids": [1]}]


def test_a_case_with_a_pipeline_still_pending_stays_processing_even_with_variants(caplog):
    """SNV post-processing failed, but the alignment's CNV VCF was imported: the case has
    variants, and must still not be opened for analysis before its SNVs are in."""
    rows = [
        status_row(1, status_code="processing", has_variants=True),
        status_row(2, status_code="processing", has_variants=True),
    ]
    with caplog.at_level(logging.INFO):
        batches = case_status.plan_in_progress(rows, waiting={2}, tenants=[])
    assert batches == [{"tenant": "radiant", "case_ids": [1]}]
    assert "kept in processing, a pipeline still has work for them: [2]" in caplog.text


def test_a_done_case_with_no_variants_stays_processing_and_is_reported(caplog):
    rows = [status_row(1, status_code="processing", has_variants=False)]
    with caplog.at_level(logging.WARNING):
        assert case_status.plan_in_progress(rows, waiting=set(), tenants=[]) == []
    assert "no pipeline left to run but no variant data: [1]" in caplog.text


def test_has_variants_is_read_from_the_query_s_integer():
    """StarRocks returns the boolean expression as 0/1."""
    rows = [
        status_row(1, status_code="processing", has_variants=1),
        status_row(2, status_code="processing", has_variants=0),
    ]
    assert case_status.plan_in_progress(rows, waiting=set(), tenants=[]) == [{"tenant": "radiant", "case_ids": [1]}]


def test_changes_are_batched_once_per_tenant():
    rows = [
        status_row(3, tenant_code="radiant"),
        status_row(1, tenant_code="qlin"),
        status_row(2, tenant_code="radiant"),
    ]
    assert case_status.plan_processing(rows, waiting={1, 2, 3}, tenants=[]) == [
        {"tenant": "qlin", "case_ids": [1]},
        {"tenant": "radiant", "case_ids": [2, 3]},
    ]


def test_a_tenant_outside_the_allow_list_is_skipped_and_logged(caplog):
    rows = [status_row(1, tenant_code="radiant"), status_row(2, tenant_code="qlin")]
    with caplog.at_level(logging.WARNING):
        batches = case_status.plan_processing(rows, waiting={1, 2}, tenants=["radiant"])
    assert batches == [{"tenant": "radiant", "case_ids": [1]}]
    assert "tenant 'qlin' is not in the allow-list" in caplog.text


def test_tenants_from_env_takes_the_first_set_variable(monkeypatch):
    monkeypatch.setenv("NEXTFLOW_CNV_TENANTS", "")
    monkeypatch.setenv("NEXTFLOW_POSTPROCESSING_TENANTS", " radiant , qlin,")
    assert case_status.tenants_from_env(*case_status.CNV_TENANTS_ENVS) == ["radiant", "qlin"]
    monkeypatch.setenv("NEXTFLOW_CNV_TENANTS", "cnv-only")
    assert case_status.tenants_from_env(*case_status.CNV_TENANTS_ENVS) == ["cnv-only"]


def test_tenants_from_env_is_empty_when_nothing_is_set(monkeypatch):
    monkeypatch.delenv("NEXTFLOW_POSTPROCESSING_TENANTS", raising=False)
    assert case_status.tenants_from_env(*case_status.SNV_TENANTS_ENVS) == []


def test_change_tenant_status_sends_the_batch(monkeypatch):
    calls = []

    def update(tenant, case_ids, status_code):
        calls.append((tenant, case_ids, status_code))
        return [{"case_id": 1, "updated": True, "current_status_code": status_code}]

    monkeypatch.setattr("radiant.tasks.nextflow.register.update_case_system_status", update)
    result = case_status.change_tenant_status({"tenant": "radiant", "case_ids": [1]}, case_status.PROCESSING)
    assert calls == [("radiant", [1], "processing")]
    assert result == [{"case_id": 1, "updated": True, "current_status_code": "processing"}]


def test_a_tenant_the_service_account_is_not_granted_on_is_skipped(monkeypatch, caplog):
    def update(*_):
        raise PortalError("PATCH .../cases/status returned 403", status=403)

    monkeypatch.setattr("radiant.tasks.nextflow.register.update_case_system_status", update)
    with caplog.at_level(logging.WARNING), pytest.raises(AirflowSkipException, match="qlin"):
        case_status.change_tenant_status({"tenant": "qlin", "case_ids": [1]}, case_status.IN_PROGRESS)
    assert "tenant 'qlin' skipped" in caplog.text


@pytest.mark.parametrize("status", [400, 500, None])
def test_any_other_portal_error_fails_the_task(monkeypatch, status):
    def update(*_):
        raise PortalError("boom", status=status)

    monkeypatch.setattr("radiant.tasks.nextflow.register.update_case_system_status", update)
    with pytest.raises(PortalError):
        case_status.change_tenant_status({"tenant": "radiant", "case_ids": [1]}, case_status.PROCESSING)


def test_the_cases_to_notify_include_those_an_earlier_attempt_moved():
    """A retried `set_in_progress` gets `updated: false` for what its first attempt moved; those
    cases still reached in_progress in this run and must be in the email."""
    results = [
        {"case_id": 2, "updated": True, "current_status_code": "in_progress"},
        {"case_id": 1, "updated": False, "current_status_code": "in_progress"},
        {"case_id": 3, "updated": False, "current_status_code": "revoked"},
    ]
    assert case_status.cases_now_in(results, case_status.IN_PROGRESS) == [1, 2]
