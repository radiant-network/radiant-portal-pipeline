"""Row fixtures shaped like what `sql/clinical/pending_somatic_annotation_select.sql` returns:
one row per (calling task, case, sequencing experiment), tumor first."""

from radiant.tasks.nextflow.somatic.model import CallingTask, SomaticCase

BUCKET = "s3://mock-nextflow-inputs"
OUTDIR = "s3://mock-nextflow-outputs/somatic-snv/somatic-manual-2026-10-15T00-00-00-00-00"


def vcf_url(task_id: int) -> str:
    return f"{BUCKET}/dragen/run-42/{task_id}/calls.snv.vcf.gz"


def row(
    task_id=70,
    case_id=23,
    task_type="tumor_only_variant_calling",
    seq_id=64,
    aliquot="TUMOR-1",
    histology="tumoral",
    **overrides,
):
    base = {
        "case_id": case_id,
        "submitter_case_id": f"CASE-{case_id}",
        "tenant_code": "radiant",
        "project_code": "N1",
        "calling_task_id": task_id,
        "calling_task_type": task_type,
        "seq_id": seq_id,
        "aliquot": aliquot,
        "strategy": "wgs",
        "histology": histology,
        "ssnv_url": vcf_url(task_id),
        "ssnv_matches": 1,
        "exclusion_reason": None,
    }
    base.update(overrides)
    return base


def calling_task_rows(task_id=70, case_id=23, normal=False, **overrides) -> list[dict]:
    """A tumor-only call, or a tumor-normal one with `normal=True`."""
    task_type = "somatic_variant_calling" if normal else overrides.pop("task_type", "tumor_only_variant_calling")
    rows = [row(task_id=task_id, case_id=case_id, task_type=task_type, **overrides)]
    if normal:
        rows.append(
            row(
                task_id=task_id,
                case_id=case_id,
                task_type=task_type,
                seq_id=63,
                aliquot="NORMAL-1",
                histology="normal",
                **overrides,
            )
        )
    return rows


def calling_task(task_id=70, normal_aliquot=None, cases=None) -> CallingTask:
    return CallingTask(
        task_id=task_id,
        task_type="somatic_variant_calling" if normal_aliquot else "tumor_only_variant_calling",
        sequencing_type="WGS",
        vcf_url=vcf_url(task_id),
        tumor_aliquot="TUMOR-1",
        normal_aliquot=normal_aliquot,
        cases=cases
        or [SomaticCase(case_id=23, submitter_case_id="CASE-23", tenant_code="radiant", project_code="N1")],
    )
