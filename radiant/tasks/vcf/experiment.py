from pydantic import BaseModel


class Experiment(BaseModel):
    seq_id: int
    patient_id: str
    sample_id: str
    family_role: str
    is_affected: bool
    sex: str


class Case(BaseModel):
    case_id: int
    part: int
    vcf_filepath: str
    experiments: list[Experiment]
