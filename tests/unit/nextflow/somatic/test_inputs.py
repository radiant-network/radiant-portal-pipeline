import pytest

from radiant.tasks.nextflow.somatic.inputs import SAMPLESHEET_COLUMNS, build_inputs
from tests.unit.nextflow.somatic.conftest import BUCKET, calling_task


def test_the_samplesheet_is_one_row_per_calling_task_on_the_mount():
    files = build_inputs([calling_task(70), calling_task(75, "NORMAL-1")], BUCKET, "/workspace/inputs")
    assert set(files) == {"samplesheet.csv"}
    assert files["samplesheet.csv"].splitlines() == [
        ",".join(SAMPLESHEET_COLUMNS),
        "70,WGS,/workspace/inputs/dragen/run-42/70/calls.snv.vcf.gz",
        "75,WGS,/workspace/inputs/dragen/run-42/75/calls.snv.vcf.gz",
    ]


def test_the_columns_are_the_normalize_step_s():
    """The second `oneOf` branch of the pipeline's schema: no `gvcf`, no `sample`."""
    assert SAMPLESHEET_COLUMNS == ["familyId", "sequencingType", "vcf"]


def test_a_vcf_off_the_mount_fails_naming_every_task_and_url():
    off = calling_task(71).model_copy(update={"vcf_url": "s3://elsewhere/a.vcf.gz"})
    off2 = calling_task(72).model_copy(update={"vcf_url": "s3://elsewhere/b.vcf.gz"})
    with pytest.raises(ValueError) as error:
        build_inputs([calling_task(70), off, off2], BUCKET, "/workspace/inputs")
    assert "calling task 71" in str(error.value) and "s3://elsewhere/a.vcf.gz" in str(error.value)
    assert "calling task 72" in str(error.value)
