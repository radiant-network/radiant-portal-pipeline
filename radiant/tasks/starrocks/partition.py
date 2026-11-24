import logging
from dataclasses import dataclass
from datetime import datetime
from enum import Enum

from pydantic import BaseModel

DT_EPOCH = datetime(year=1970, month=1, day=1)

logger = logging.getLogger("airflow.task")


class SequencingDeltaCommon(BaseModel):
    case_id: int
    task_id: int
    seq_id: int
    task_type: str
    analysis_type: str
    aliquot: str
    patient_id: int
    experimental_strategy: str
    tenant_code: str
    request_priority: str | None = None
    vcf_filepath: str | None = None
    cnv_vcf_filepath: str | None = None
    exomiser_filepath: str | None = None
    sex: str
    family_id: int | None = None
    family_role: str
    affected_status: str
    histology_type: str | None = None
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

    @staticmethod
    def _linked_groups(inputs: list[SequencingDeltaInput]) -> list[list[int]]:
        """
        Groups the indexes of the rows that share a patient, seq, case or family id of the same experimental strategy,
        directly or through other rows. The groups keep the order of their first row.
        """
        parent = list(range(len(inputs)))

        def find(i: int) -> int:
            while parent[i] != i:
                parent[i] = parent[parent[i]]
                i = parent[i]
            return i

        first_row_of_id = {}
        for i, item in enumerate(inputs):
            ids = [("patient_id", item.patient_id), ("seq_id", item.seq_id), ("case_id", item.case_id)]
            if item.family_id is not None:
                ids.append(("family_id", item.family_id))
            for id_ in ids:
                j = first_row_of_id.setdefault((item.experimental_strategy, *id_), i)
                parent[find(i)] = find(j)

        groups: dict[int, list[int]] = {}
        for i in range(len(inputs)):
            groups.setdefault(find(i), []).append(i)
        return list(groups.values())

    def _compute_partition(self, group: list[SequencingDeltaInput]) -> int:
        """
        Computes the partition of a group of linked rows: the highest part they already have in staging, else the
        current part, rolled over before the group so that a group is never split.
        """
        strategy = group[0].experimental_strategy
        state = self.state[strategy]

        parts = {p for i in group for p in (i.patient_part, i.seq_part, i.case_part, i.family_part) if p is not None}
        if len(parts) > 1:
            # A patient then has experiments in two parts: the germline SNV frequencies count them twice.
            logger.warning(
                f"Linked rows are already in parts {sorted(parts)}, the group goes to part {max(parts)}, for "
                f"patient_ids {sorted({i.patient_id for i in group})}, "
                f"seq_ids {sorted({i.seq_id for i in group})}, "
                f"case_ids {sorted({i.case_id for i in group})}, "
                f"family_ids {sorted({i.family_id for i in group if i.family_id is not None})}"
            )

        if parts:
            part = max(parts)
            if part == state.id:
                state.count += len(group)
            return part

        if state.count >= self.SEQUENCING_TYPES[strategy].limit:
            state.id += 1
            state.count = 0
        state.count += len(group)
        return state.id

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

        parts: list[int | None] = [None] * len(inputs)
        for group in self._linked_groups(inputs):
            part = self._compute_partition([inputs[i] for i in group])
            for i in group:
                parts[i] = part

        return [
            SequencingDeltaOutput(**vars(_item) | {"part": part, "ingested_at": DT_EPOCH})
            for _item, part in zip(inputs, parts, strict=True)
        ]


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
