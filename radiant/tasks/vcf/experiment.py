from abc import ABC, abstractmethod

from pydantic import BaseModel, ConfigDict

RADIANT_GERMLINE_ANNOTATION_TASK = "radiant_germline_annotation"
ALIGNMENT_GERMLINE_VARIANT_CALLING_TASK = "alignment_germline_variant_calling"
EXOMISER_TASK = "exomiser"


class Experiment(BaseModel):
    model_config = ConfigDict(frozen=True)

    seq_id: int
    patient_id: int
    aliquot: str
    family_role: str
    affected_status: str
    sex: str
    experimental_strategy: str
    request_priority: str | None = None


class BaseTask(BaseModel, ABC):
    model_config = ConfigDict(frozen=True)

    task_id: int
    part: int
    analysis_type: str
    experiments: list[Experiment]

    @staticmethod
    @abstractmethod
    def gather_additional_args(rows: list[dict]) -> dict:
        pass


class RadiantGermlineAnnotationTask(BaseTask):
    task_type: str = RADIANT_GERMLINE_ANNOTATION_TASK
    vcf_filepath: str
    index_vcf_filepath: str | None = None

    @staticmethod
    def gather_additional_args(rows: list[dict]) -> dict:
        for r in rows:
            if r["family_role"] == "proband":
                return {
                    "vcf_filepath": r["vcf_filepath"],
                    "index_vcf_filepath": r.get("index_vcf_filepath"),
                }
        raise ValueError("No proband found in rows for `radiant_germline_annotation` task.")


class AlignmentGermlineVariantCallingTask(BaseTask):
    task_type: str = ALIGNMENT_GERMLINE_VARIANT_CALLING_TASK
    cnv_vcf_filepath: str

    @staticmethod
    def gather_additional_args(rows: list[dict]) -> dict:
        if len(rows) > 1:
            raise ValueError("`alignment_germline_variant_calling` task expects a single row per task.")
        return {
            "cnv_vcf_filepath": rows[0]["cnv_vcf_filepath"],
        }


class ExomiserTask(BaseTask):
    task_type: str = EXOMISER_TASK
    exomiser_filepath: str

    @staticmethod
    def gather_additional_args(rows: list[dict]) -> dict:
        if len(rows) > 1:
            raise ValueError("`exomiser` task expects a single row per task.")
        return {
            "exomiser_filepath": rows[0]["exomiser_filepath"],
        }


_TASK_TYPES = {
    RADIANT_GERMLINE_ANNOTATION_TASK: RadiantGermlineAnnotationTask,
    ALIGNMENT_GERMLINE_VARIANT_CALLING_TASK: AlignmentGermlineVariantCallingTask,
    EXOMISER_TASK: ExomiserTask,
}


def _build_experiments(rows: list[dict]) -> list[Experiment]:
    return [
        Experiment(
            seq_id=row["seq_id"],
            patient_id=row["patient_id"],
            aliquot=row["aliquot"],
            family_role=row["family_role"],
            affected_status=row["affected_status"],
            sex=row["sex"],
            experimental_strategy=row["experimental_strategy"],
            request_priority=row.get("request_priority"),
        )
        for row in rows
    ]


def _get_task_type(rows: list[dict]) -> type[BaseTask]:
    task_type = set([r["task_type"] for r in rows])
    if len(task_type) != 1:
        raise ValueError(f"Inconsistent task types in rows, found: {task_type}")

    task_type = task_type.pop()
    if task_type not in _TASK_TYPES:
        raise ValueError(f"Unknown task type: {task_type}")

    return _TASK_TYPES[task_type]


def build_task_from_rows(rows: list[dict]) -> BaseTask:
    if not rows:
        raise ValueError("No rows provided")

    task_cls = _get_task_type(rows)
    experiments = _build_experiments(rows)
    base_args = {
        "task_id": rows[0]["task_id"],
        "part": rows[0]["part"],
        "task_type": rows[0]["task_type"],
        "analysis_type": rows[0]["analysis_type"],
        "experiments": experiments,
    }
    extra_args = task_cls.gather_additional_args(rows)
    return task_cls(**base_args, **extra_args)


def build_task_from_dict(task_dict: dict) -> BaseTask:
    task_type = task_dict.pop("task_type")
    if task_type not in _TASK_TYPES:
        raise ValueError(f"Unknown task type: {task_type}")
    return _TASK_TYPES[task_type].model_validate(task_dict)
