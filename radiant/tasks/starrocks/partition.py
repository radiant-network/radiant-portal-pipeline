from dataclasses import dataclass
from datetime import datetime
from enum import Enum

from pydantic import BaseModel

DT_EPOCH = datetime(year=1970, month=1, day=1)


class SequencingDeltaCommon(BaseModel):
    case_id: int
    seq_id: int
    task_id: int
    analysis_type: str
    aliquot: str
    patient_id: int
    experimental_strategy: str
    request_id: int
    request_priority: str
    vcf_filepath: str | None = None
    exomiser_filepath: str | None = None
    sex: str
    family_role: str
    affected_status: str
    created_at: datetime
    updated_at: datetime


class SequencingDeltaInput(SequencingDeltaCommon):
    patient_part: int | None = None
    case_part: int | None = None
    max_part: int | None = None
    max_count: int | None = None


class SequencingDeltaOutput(SequencingDeltaCommon):
    part: int
    ingested_at: datetime


@dataclass
class PartitionCounter:
    id: int
    count: int


@dataclass
class SequencingType:
    mask: int
    limit: int


class PriorityLevel(Enum):
    STAT: int = 100
    ASAP: int = 200
    URGENT: int = 300
    ROUTINE: int = 400

    @classmethod
    def from_string(cls, value: str) -> "PriorityLevel":
        match value.lower():
            case "stat":
                return cls.STAT
            case "asap":
                return cls.ASAP
            case "urgent":
                return cls.URGENT
            case _:
                return cls.ROUTINE

    def __gt__(self, other: "PriorityLevel") -> bool:
        return self.value > other.value

    def __lt__(self, other: "PriorityLevel") -> bool:
        return self.value < other.value


class SequencingExperimentPartitionAssigner:
    """
    Helper class to assign partitions for sequencing experiments delta objects.
    """

    SEQUENCING_TYPES = {
        "wgs": SequencingType(mask=0x00000000, limit=100),
        "wxs": SequencingType(mask=0x00010000, limit=1000),
    }

    def __init__(self):
        self.state = {k: PartitionCounter(id=self.SEQUENCING_TYPES[k].mask, count=0) for k in self.SEQUENCING_TYPES}
        self.case_patient_mapping = {k: {} for k in self.SEQUENCING_TYPES}

    def _compute_partition(
        self,
        experimental_strategy: str,
        case_id: int,
        patient_id: int,
        case_part: int | None = None,
        patient_part: int | None = None,
    ) -> int:
        """
        Computes the partition based on case and patient parts.
        """
        _case = case_part or self.case_patient_mapping[experimental_strategy].get(case_id)
        _patient = patient_part or self.case_patient_mapping[experimental_strategy].get(patient_id)
        if _case is not None:
            assigned_part = _case
        elif _patient is not None:
            assigned_part = _patient
        else:
            assigned_part = self.state[experimental_strategy].id

        if assigned_part == self.state[experimental_strategy].id:
            self.state[experimental_strategy].count += 1

        if self.state[experimental_strategy].count >= self.SEQUENCING_TYPES[experimental_strategy].limit:
            self.state[experimental_strategy].id += 1
            self.state[experimental_strategy].count = 0

        return assigned_part

    def assign_partitions(self, delta: list[dict]) -> list[SequencingDeltaOutput]:
        """
        Assigns a partition based on the partition key.
        This is a placeholder for the actual partitioning logic.
        """
        partitioned_items = []
        for d in delta:
            _item = SequencingDeltaInput.model_validate(d)
            _item.experimental_strategy = _item.experimental_strategy.lower()

            if (
                _item.max_part
                and _item.max_count
                and _item.max_part > self.state[_item.experimental_strategy].id
                and _item.max_count > self.state[_item.experimental_strategy].count
            ):
                self.state[_item.experimental_strategy].id = _item.max_part
                self.state[_item.experimental_strategy].count = _item.max_count

            _output = SequencingDeltaOutput(
                **vars(_item)
                | {
                    "part": self._compute_partition(
                        experimental_strategy=_item.experimental_strategy,
                        case_id=_item.case_id,
                        patient_id=_item.patient_id,
                        case_part=_item.case_part,
                        patient_part=_item.patient_part,
                    ),
                    "ingested_at": DT_EPOCH,
                }
            )
            partitioned_items.append(_output)

            self.case_patient_mapping[_output.experimental_strategy][_output.case_id] = _output.part
            self.case_patient_mapping[_output.experimental_strategy][_output.patient_id] = _output.part

        return partitioned_items


class SequencingExperimentPriorityAssigner:
    """
    Helper class to assign priorities for sequencing experiments partitions.
    """

    @staticmethod
    def assign_priorities(sequencing_experiments: list[dict]) -> list[int]:
        """
        Assigns a priority based on the request priority.
        This is a placeholder for the actual priority assignment logic.
        """
        partitions = {}
        for seq_exp in sequencing_experiments:
            model = SequencingDeltaOutput.model_validate(seq_exp)
            priority_level = PriorityLevel.from_string(model.request_priority)
            try:
                partitions[seq_exp["part"]].append(priority_level)
            except KeyError:
                partitions[seq_exp["part"]] = [priority_level]

        _grouped = {k: min(v) for k, v in partitions.items()}
        _sorted = sorted(_grouped.items(), key=lambda item: item[1])
        return [part for part, _ in _sorted]
