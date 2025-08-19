from pydantic import BaseModel


class Experiment(BaseModel):
    seq_id: int
    task_id: int
    patient_id: int
    aliquot: str
    family_role: str
    affected_status: str
    sex: str
    experimental_strategy: str
    request_id: int | None = None
    request_priority: str | None = None
    exomiser_filepaths: list[str] | None = None
    cnv_vcf_filepath: str | None = None


class Case(BaseModel):
    case_id: int
    part: int
    vcf_filepath: str
    analysis_type: str
    experiments: list[Experiment]
    index_vcf_filepath: str | None = None
