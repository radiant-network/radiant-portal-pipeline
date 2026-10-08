"""Which cases the case status control DAG moves, and to what.

The pipeline only ever makes two status changes: `submitted` -> `processing` when a case has
work waiting, and `processing` -> `in_progress` once every pipeline is done with it and it has
variant data. Everything here
decides *which* cases; `portal.set_case_system_status` sends the change and the portal checks
the expected status under a row lock, so a case a geneticist moved in the meantime is left
alone.

Airflow is imported inside `change_tenant_status` only, like `register.py`: the rules stay
importable from plain unit tests.
"""

import logging
import os
from collections.abc import Callable

from pydantic import BaseModel

from radiant.tasks.nextflow.cnv.resolve import select_cases as select_cnv_cases
from radiant.tasks.nextflow.qc.model import QcCase
from radiant.tasks.nextflow.qc.resolve import resolve_cases as resolve_qc_cases
from radiant.tasks.nextflow.qc.resolve import select_cases as select_qc_cases
from radiant.tasks.nextflow.resolve import select_cases as select_snv_cases

LOGGER = logging.getLogger(__name__)

SUBMITTED = "submitted"
PROCESSING = "processing"
IN_PROGRESS = "in_progress"

# The tenant allow-lists of the three "from Cases" DAGs, in their order of precedence. Discovery
# filters with the same lists, so a case the DAG would exclude as `tenant_not_granted` is not
# counted as work waiting.
SNV_TENANTS_ENVS = ("NEXTFLOW_POSTPROCESSING_TENANTS",)
CNV_TENANTS_ENVS = ("NEXTFLOW_CNV_TENANTS", "NEXTFLOW_POSTPROCESSING_TENANTS")
QC_TENANTS_ENVS = ("NEXTFLOW_QC_TENANTS", "NEXTFLOW_POSTPROCESSING_TENANTS")


def tenants_from_env(*env_names: str) -> list[str]:
    """The first of `env_names` that is set, split on commas. Empty means "do not filter"."""
    raw = next((os.getenv(name) for name in env_names if os.getenv(name)), "")
    return [tenant.strip() for tenant in raw.split(",") if tenant.strip()]


class CaseStatus(BaseModel):
    case_id: int
    tenant_code: str
    status_code: str
    has_variants: bool


QcLocator = Callable[[list[QcCase]], list[QcCase]]


def s3_qc_locator(inputs_root: str) -> QcLocator:
    """The QC DAG's `locate_metrics` probe over S3, keeping the cases whose metrics it finds."""
    from radiant.tasks.nextflow.qc.metrics import S3Lister, locate_metrics

    def locate(cases: list[QcCase]) -> list[QcCase]:
        lister = S3Lister()
        kept, _ = locate_metrics(cases, lister.list_dir, lister.list_tree, inputs_root)
        return kept

    return locate


def cases_with_work(
    snv_rows: list[dict],
    cnv_rows: list[dict],
    qc_rows: list[dict],
    import_rows: list[dict],
    locate_qc: QcLocator,
) -> set[int]:
    """The cases at least one pipeline will run on: the union of what each "from Cases" DAG
    would select, and of the cases in the import delta.

    The Nextflow candidates go through the DAGs' own `select_cases`, lenient as on a scheduled
    run, and the QC ones through its DRAGEN metrics probe too (`locate_qc`). A case they exclude
    (sequencing pending, no gVCF, tenant not granted, metrics not found...) is not counted:
    it would sit in `processing` with nothing running for it, and never reach `in_progress`.
    """
    qc = select_qc_cases(qc_rows, strict=False)
    qc_cases = locate_qc(resolve_qc_cases([m.model_dump() for m in qc.members])) if qc.case_ids else []
    selected = {
        "snv post-processing": select_snv_cases(snv_rows, strict=False).case_ids,
        "cnv post-processing": select_cnv_cases(cnv_rows, strict=False).case_ids,
        "quality control": sorted(c.case_id for c in qc_cases),
        "import": sorted({row["case_id"] for row in import_rows}),
    }
    for pipeline, case_ids in selected.items():
        LOGGER.info("%s: %d case(s) with work waiting %s", pipeline, len(case_ids), case_ids)
    return {case_id for case_ids in selected.values() for case_id in case_ids}


def plan_processing(status_rows: list[dict], waiting: set[int], tenants: list[str]) -> list[dict]:
    """The `submitted` cases to move to `processing`, one `{tenant, case_ids}` per tenant.

    The cases with work waiting, and also the ones that already have variants: work done before
    this DAG saw the case (a manual rerun, data imported before the statuses existed) would
    otherwise leave it `submitted` for ever. The same run's `evaluate_cases` moves those on.
    """
    cases = [CaseStatus(**row) for row in status_rows]
    return _by_tenant(
        [c for c in cases if c.status_code == SUBMITTED and (c.case_id in waiting or c.has_variants)],
        tenants,
        PROCESSING,
    )


def plan_in_progress(status_rows: list[dict], waiting: set[int], tenants: list[str]) -> list[dict]:
    """Every `processing` case whose pipelines are all done and that has variant data.

    `waiting` is discovery run again after the pipelines: a case still in it has a pipeline
    left to run (one failed, or the import has not picked up its new files), so it stays
    `processing` even with variants in -- CNVs imported while its SNV annotation failed must not
    open it for analysis. Every `processing` case is looked at, not only this run's, so the
    later run that finishes it moves it on.
    """
    cases = [CaseStatus(**row) for row in status_rows if row["status_code"] == PROCESSING]
    held = sorted(c.case_id for c in cases if c.case_id in waiting)
    if held:
        LOGGER.info(
            "%d processing case(s) kept in processing, a pipeline still has work for them: %s", len(held), held
        )
    no_variants = sorted(c.case_id for c in cases if c.case_id not in waiting and not c.has_variants)
    if no_variants:
        LOGGER.warning(
            "%d processing case(s) with no pipeline left to run but no variant data: %s", len(no_variants), no_variants
        )
    return _by_tenant([c for c in cases if c.case_id not in waiting and c.has_variants], tenants, IN_PROGRESS)


def _by_tenant(cases: list[CaseStatus], tenants: list[str], target: str) -> list[dict]:
    by_tenant: dict[str, list[int]] = {}
    for case in cases:
        by_tenant.setdefault(case.tenant_code, []).append(case.case_id)

    batches = []
    for tenant in sorted(by_tenant):
        case_ids = sorted(set(by_tenant[tenant]))
        if tenants and tenant not in tenants:
            LOGGER.warning(
                "tenant '%s' is not in the allow-list %s: %d case(s) left out of %s: %s",
                tenant,
                tenants,
                len(case_ids),
                target,
                case_ids,
            )
            continue
        LOGGER.info("tenant '%s': %d case(s) to move to %s: %s", tenant, len(case_ids), target, case_ids)
        batches.append({"tenant": tenant, "case_ids": case_ids})
    return batches


def change_tenant_status(batch: dict, status_code: str) -> list[dict]:
    """Send one tenant's status changes; a tenant the service account is not granted on is
    logged and skipped rather than failed.

    The portal refuses the whole batch with a 403 when the service account is missing
    `can_ingest_data` at every lab of the tenant, and nothing is changed. Failing would turn
    the control DAG red every day for a grant nobody asked for; a skip with the reason in the
    log says the same thing without the noise. Any other error fails the task.
    """
    from airflow.exceptions import AirflowSkipException

    from radiant.tasks.nextflow.portal import PortalError
    from radiant.tasks.nextflow.register import update_case_system_status

    tenant = batch["tenant"]
    try:
        return update_case_system_status(tenant, batch["case_ids"], status_code)
    except PortalError as error:
        if error.status != 403:
            raise
        LOGGER.warning("tenant '%s' skipped: %s", tenant, error)
        raise AirflowSkipException(f"tenant '{tenant}' is not granted to the service account") from None
