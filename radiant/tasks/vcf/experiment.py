from pydantic import BaseModel


class Experiment(BaseModel):
    seq_id: int
    task_id: int
    patient_id: str
    sample_id: str
    family_role: str
    affected_status: str
    sex: str
    experimental_strategy: str


class Case(BaseModel):
    case_id: int
    part: int
    vcf_filepath: str
    analysis_type: str
    experiments: list[Experiment]
