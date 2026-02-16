from dataclasses import dataclass
from datetime import datetime
from enum import Enum

from pydantic import BaseModel

DT_EPOCH = datetime(year=1970, month=1, day=1)


class SequencingDeltaCommon(BaseModel):
    case_id: int
    task_id: int
    seq_id: int
    task_type: str
    analysis_type: str
    aliquot: str
    patient_id: int
    experimental_strategy: str
    request_priority: str | None = None
    vcf_filepath: str | None = None
    cnv_vcf_filepath: str | None = None
    exomiser_filepath: str | None = None
    sex: str
    family_id: int
    family_role: str
    affected_status: str
    created_at: datetime
    updated_at: datetime


class SequencingDeltaInput(SequencingDeltaCommon):
    patient_part: int | None = None
    seq_part: int | None = None
    case_part: int | None = None
    family_part: int | None = None
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
    STAT = 100
    ASAP = 200
    URGENT = 300
    ROUTINE = 400

    @classmethod
    def from_string(cls, value: str | None) -> "PriorityLevel":
        if value is None:
            return cls.ROUTINE
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
        self.task_patient_mapping = {
            k: {"patient_id": {}, "seq_id": {}, "case_id": {}, "family_id": {}} for k in self.SEQUENCING_TYPES
        }

    def _compute_partition(
        self,
        experimental_strategy: str,
        patient_id: int,
        seq_id: int,
        case_id: int,
        family_id: int,
        patient_part: int | None = None,
        seq_part: int | None = None,
        case_part: int | None = None,
        family_part: int | None = None,
    ) -> int:
        """
        Computes the partition based on task and patient parts.
        """

        # Creates a set of all provided partition information to check for consistency
        # We should have only one unique partition if any of the partition information is provided
        parts = {
            patient_part,
            seq_part,
            case_part,
            family_part,
            self.task_patient_mapping[experimental_strategy]["patient_id"].get(patient_id),
            self.task_patient_mapping[experimental_strategy]["seq_id"].get(seq_id),
            self.task_patient_mapping[experimental_strategy]["case_id"].get(case_id),
            self.task_patient_mapping[experimental_strategy]["family_id"].get(family_id),
        }
        parts.discard(None)

        if parts:
            if len(parts) > 1:
                raise ValueError(
                    "Inconsistent partitioning information provided for "
                    f"patient_id/part [{patient_id}/{patient_part}], "
                    f"seq_id/part [{seq_id}/{seq_part}], "
                    f"case_id/part [{case_id}/{case_part}], "
                    f"family_id/part [{family_id}/{family_part}]"
                )
            _p = parts.pop()

            # If were still within the same partition, we need to increment the count to reflect the new assignment
            if _p == self.state[experimental_strategy].id:
                self.state[experimental_strategy].count += 1

            return _p

        # Check if limit was reached before assigning the partition
        if self.state[experimental_strategy].count >= self.SEQUENCING_TYPES[experimental_strategy].limit:
            self.state[experimental_strategy].id += 1
            self.state[experimental_strategy].count = 0

        assigned_part = self.state[experimental_strategy].id
        self.state[experimental_strategy].count += 1

        self.task_patient_mapping[experimental_strategy]["patient_id"][patient_id] = assigned_part
        self.task_patient_mapping[experimental_strategy]["seq_id"][seq_id] = assigned_part
        self.task_patient_mapping[experimental_strategy]["case_id"][case_id] = assigned_part
        self.task_patient_mapping[experimental_strategy]["family_id"][family_id] = assigned_part

        return assigned_part

    def _bootstrap_state(self, inputs: list[SequencingDeltaInput]) -> None:
        """
        This bootstraps the state of the partition assigner only if it moves the state forward.

        Verify all the inputs to get the highest `max_part` and `max_count` combination
        for a specific `experimental_strategy`.

        This is used as the initial partition start for adding new sequencing experiments.
        """
        for strategy in self.SEQUENCING_TYPES:
            bootstrap_candidates = [
                (i.max_part, i.max_count)
                for i in inputs
                if i.experimental_strategy == strategy and i.max_part is not None and i.max_count is not None
            ]

            if not bootstrap_candidates:
                continue

            target_part, target_count = max(bootstrap_candidates)
            current_state = self.state[strategy]

            if target_part > current_state.id:
                current_state.id = target_part
                current_state.count = target_count
            elif target_part == current_state.id:
                current_state.count = max(current_state.count, target_count)

    def assign_partitions(self, delta: list[dict]) -> list[SequencingDeltaOutput]:
        """
        Assigns a partition based on the partition key.
        This is a placeholder for the actual partitioning logic.
        """
        inputs: list[SequencingDeltaInput] = []
        for d in delta:
            _item = SequencingDeltaInput.model_validate(d)
            _item.experimental_strategy = _item.experimental_strategy.lower()
            inputs.append(_item)

        self._bootstrap_state(inputs)

        partitioned_items = []
        for _item in inputs:
            _item.experimental_strategy = _item.experimental_strategy.lower()

            _output = SequencingDeltaOutput(
                **vars(_item)
                | {
                    "part": self._compute_partition(
                        experimental_strategy=_item.experimental_strategy,
                        patient_id=_item.patient_id,
                        seq_id=_item.seq_id,
                        case_id=_item.case_id,
                        family_id=_item.family_id,
                        patient_part=_item.patient_part,
                        seq_part=_item.seq_part,
                        case_part=_item.case_part,
                        family_part=_item.family_part,
                    ),
                    "ingested_at": DT_EPOCH,
                }
            )
            partitioned_items.append(_output)

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
