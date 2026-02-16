from collections import Counter
from datetime import datetime

import numpy as np
import pandas as pd
import pytest

from radiant.tasks.starrocks.partition import (
    PriorityLevel,
    SequencingDeltaInput,
    SequencingDeltaOutput,
    SequencingExperimentPartitionAssigner,
    SequencingExperimentPriorityAssigner,
)


def create_sequencing_delta_input_from_base(**kwargs) -> SequencingDeltaInput:
    defaults = {
        "case_id": 1,
        "seq_id": 1,
        "task_id": 1,
        "task_type": "radiant_germline_annotation",
        "analysis_type": "germline",
        "aliquot": "1",
        "patient_id": 1,
        "experimental_strategy": "wgs",
        "request_priority": "stat",
        "vcf_filepath": "s3://bucket/path/file.vcf.gz",
        "sex": "male",
        "family_id": 1,
        "family_role": "proband",
        "affected_status": "affected",
        "created_at": datetime(2025, 1, 1),
        "updated_at": datetime(2025, 1, 1),
        "max_part": None,
        "max_count": None,
        "patient_part": None,
        "seq_part": None,
        "case_part": None,
        "family_part": None,
    }

    # Update defaults with any arguments passed by the user
    defaults.update(kwargs)

    return SequencingDeltaInput(**defaults)


def test_partition_assigner_bootstrap_uses_max():
    assigner = SequencingExperimentPartitionAssigner()
    _delta = [
        create_sequencing_delta_input_from_base(
            case_id=1,
            seq_id=1,
            task_id=1,
            patient_id=1,
            family_id=1,
            max_part=1,
            max_count=50,
        ).model_dump(),
        create_sequencing_delta_input_from_base(
            case_id=2,
            seq_id=2,
            task_id=2,
            patient_id=2,
            family_id=2,
            max_part=2,
            max_count=0,
        ).model_dump(),
    ]

    partitioned = assigner.assign_partitions(_delta)
    assert partitioned[0].part == 2
    assert assigner.state["wgs"].id == 2
    assert assigner.state["wgs"].count == 2


def test_partition_assigner_bootstrap_rollover():
    assigner = SequencingExperimentPartitionAssigner()
    _delta = [
        create_sequencing_delta_input_from_base(
            max_part=1,
            max_count=1,
        ).model_dump()
    ]

    partitioned = assigner.assign_partitions(_delta)
    assert partitioned[0].part == 1
    assert assigner.state["wgs"].id == 1
    assert assigner.state["wgs"].count == 2


def test_partition_assigner_bootstrap_rollover_same_part_higher_count():
    assigner = SequencingExperimentPartitionAssigner()

    _delta = [
        create_sequencing_delta_input_from_base(
            max_part=0,
            max_count=50,
        ).model_dump()
    ]

    partitioned = assigner.assign_partitions(_delta)

    assert partitioned[0].part == 0
    assert assigner.state["wgs"].id == 0
    assert assigner.state["wgs"].count == 51


@pytest.mark.parametrize(
    "experimental_strategy, expected_part",
    [
        ("wgs", 0x00000000),
        ("wxs", 0x00010000),
    ],
)
def test__partition_assigner__from_empty_single(experimental_strategy, expected_part):
    _delta = [
        create_sequencing_delta_input_from_base(
            experimental_strategy=experimental_strategy,
        ).model_dump()
    ]

    partitioned = SequencingExperimentPartitionAssigner().assign_partitions(_delta)
    assert partitioned[0].part == expected_part


@pytest.mark.parametrize(
    "experimental_strategy, limit, expected_part",
    [
        ("wgs", 100, 0x00000000),
        ("wxs", 1000, 0x00010000),
    ],
)
def test__partition_assigner__from_empty_multiples_single_partition(experimental_strategy, limit, expected_part):
    _delta = [
        create_sequencing_delta_input_from_base(
            experimental_strategy=experimental_strategy,
        ).model_dump()
    ]

    partitioned = SequencingExperimentPartitionAssigner().assign_partitions(_delta)
    assert set([p.part for p in partitioned]) == {expected_part}


@pytest.mark.parametrize(
    "experimental_strategy, limit, expected_parts",
    [
        ("wgs", 101, {0x00000000: 100, 0x00000001: 1}),
        ("wgs", 201, {0x00000000: 100, 0x00000001: 100, 0x00000002: 1}),
        ("wxs", 1001, {0x00010000: 1000, 0x00010001: 1}),
        ("wxs", 2001, {0x00010000: 1000, 0x00010001: 1000, 0x00010002: 1}),
    ],
)
def test__partition_assigner__from_empty_multiples_many_partitions(experimental_strategy, limit, expected_parts):
    _delta = [
        create_sequencing_delta_input_from_base(
            case_id=i,
            seq_id=i,
            task_id=i,
            aliquot=str(i),
            patient_id=i,
            experimental_strategy=experimental_strategy,
            family_id=i,
        ).model_dump()
        for i in range(limit)
    ]

    partitioned = SequencingExperimentPartitionAssigner().assign_partitions(_delta)
    parts = Counter([p.part for p in partitioned])
    assert parts == expected_parts


@pytest.mark.parametrize(
    "seq_part, patient_part, case_part, family_part, max_part, max_count, expected_parts",
    [
        (None, None, None, None, None, None, {0: 1}),  # Empty task
        (None, 42, None, None, None, None, {42: 1}),  # patient matching
        (None, None, 42, None, None, None, {42: 1}),  # case matching
        (None, None, None, 42, None, None, {42: 1}),  # family matching
        (None, None, None, None, 42, 50, {42: 1}),  # max_part not full
        (None, 24, None, None, 42, 50, {24: 1}),  # patient matching with max_part
    ],
)
def test__partition_assigner__with_matching_patient(
    seq_part, patient_part, case_part, family_part, max_part, max_count, expected_parts
):
    _delta = [
        create_sequencing_delta_input_from_base(
            patient_part=patient_part,
            seq_part=seq_part,
            case_part=case_part,
            family_part=family_part,
            max_part=max_part,
            max_count=max_count,
        ).model_dump()
    ]

    partitioned = SequencingExperimentPartitionAssigner().assign_partitions(_delta)
    parts = Counter([p.part for p in partitioned])
    assert parts == expected_parts


def test__partition_assigner_same_cases():
    _delta = [
        create_sequencing_delta_input_from_base(
            case_id=1,
            seq_id=i,
            task_id=i,
            patient_id=i,
            family_id=i,
        ).model_dump() for i in range(2)
    ]
    partitioned = SequencingExperimentPartitionAssigner().assign_partitions(_delta)
    assert len(partitioned) == 2
    assert partitioned[0].part == partitioned[1].part


def test__partition_assigner_same_seq():
    _delta = [
        create_sequencing_delta_input_from_base(
            case_id=i,
            seq_id=1,
            task_id=i,
            patient_id=i,
            family_id=i,
        ).model_dump() for i in range(2)
    ]
    partitioned = SequencingExperimentPartitionAssigner().assign_partitions(_delta)
    assert len(partitioned) == 2
    assert partitioned[0].part == partitioned[1].part


def test__partition_assigner_same_patients():
    _delta = [
        create_sequencing_delta_input_from_base(
            case_id=i,
            seq_id=i,
            task_id=i,
            patient_id=1,
            family_id=i,
        ).model_dump() for i in range(2)
    ]
    partitioned = SequencingExperimentPartitionAssigner().assign_partitions(_delta)
    assert len(partitioned) == 2
    assert partitioned[0].part == partitioned[1].part


def test__partition_assigner_same_family():
    _delta = [
        create_sequencing_delta_input_from_base(
            case_id=i,
            seq_id=i,
            task_id=i,
            patient_id=i,
            family_id=1,
        ).model_dump() for i in range(2)
    ]
    partitioned = SequencingExperimentPartitionAssigner().assign_partitions(_delta)
    assert len(partitioned) == 2
    assert partitioned[0].part == partitioned[1].part


def test__partition_assigner_wgs_same_values_with_limits():
    df = pd.read_csv("tests/resources/test_partitions_wgs.tsv", sep="\t")
    df = df.replace({np.nan: None})
    _delta = [SequencingDeltaInput(**row.to_dict()).model_dump() for _, row in df.iterrows()]
    partitioned = SequencingExperimentPartitionAssigner().assign_partitions(_delta)
    parts = Counter([p.part for p in partitioned])
    assert parts == Counter({0: 104, 1: 100})


def test__partition_assigner_match_increases_current_part_count():
    _delta = [
        create_sequencing_delta_input_from_base(
            case_id=1,  # Same case
            seq_id=i,
            task_id=i,
            aliquot=str(i),
            patient_id=i,
            family_id=i,
        ).model_dump()
        for i in range(100)
    ]

    _delta.append(
        create_sequencing_delta_input_from_base(
            case_id=2,
            seq_id=100,
            task_id=100,
            aliquot="100",
            patient_id=100,
            family_id=100,
        ).model_dump()
    )

    _delta.extend(
        [
            create_sequencing_delta_input_from_base(
                case_id=1,  # Same case as first 100 items
                seq_id=i,
                task_id=i,
                aliquot=str(i),
                patient_id=i,
                family_id=i,
            ).model_dump()
            for i in range(101, 151)
        ]
    )

    _delta.append(
        create_sequencing_delta_input_from_base(
            case_id=2,
            seq_id=200,
            task_id=200,
            aliquot="200",
            patient_id=200,
            family_id=200,
        ).model_dump()
    )

    partitioned = SequencingExperimentPartitionAssigner().assign_partitions(_delta)
    parts = Counter([p.part for p in partitioned])
    assert parts == Counter({0: 150, 1: 2})
    assert partitioned[0].part == 0
    assert partitioned[-2].part == 0
    assert partitioned[100].part == 1
    assert partitioned[-1].part == 1


def test__partition_assigner_wgs_limits():
    _delta = [
        create_sequencing_delta_input_from_base(
            case_id=i,
            seq_id=i,
            task_id=i,
            aliquot=str(i),
            patient_id=i,
            family_id=i,
        ).model_dump()
        for i in range(500)
    ]
    partitioned = SequencingExperimentPartitionAssigner().assign_partitions(_delta)
    parts = Counter([p.part for p in partitioned])
    assert parts == Counter({0: 100, 1: 100, 2: 100, 3: 100, 4: 100})


def test__partition_assigner_wxs_limits():
    _delta = [
        create_sequencing_delta_input_from_base(
            case_id=i,
            seq_id=i,
            task_id=i,
            experimental_strategy="wxs",
            aliquot=str(i),
            patient_id=i,
            family_id=i,
        ).model_dump()
        for i in range(5000)
    ]
    partitioned = SequencingExperimentPartitionAssigner().assign_partitions(_delta)
    parts = Counter([p.part for p in partitioned])
    assert parts == Counter({65536: 1000, 65537: 1000, 65538: 1000, 65539: 1000, 65540: 1000})


def test_priority_level_enum():
    assert PriorityLevel.from_string("stat") == PriorityLevel.STAT
    assert PriorityLevel.from_string("asap") == PriorityLevel.ASAP
    assert PriorityLevel.from_string("urgent") == PriorityLevel.URGENT
    assert PriorityLevel.from_string("routine") == PriorityLevel.ROUTINE
    assert PriorityLevel.from_string(None) == PriorityLevel.ROUTINE
    assert PriorityLevel.from_string("unknown") == PriorityLevel.ROUTINE
    assert PriorityLevel.STAT < PriorityLevel.ASAP
    assert PriorityLevel.ASAP < PriorityLevel.URGENT
    assert PriorityLevel.URGENT < PriorityLevel.ROUTINE
    assert min(PriorityLevel) == PriorityLevel.STAT


@pytest.mark.parametrize(
    "parts, priorities, expected_priority",
    [
        ([0, 1, 2, 3], ["stat", "asap", "urgent", "routine"], [0, 1, 2, 3]),
        ([0, 1, 2, 3], ["routine", "urgent", "asap", "stat"], [3, 2, 1, 0]),
        ([0, 1, 2, 3], ["stat", "urgent", "stat", "routine"], [0, 2, 1, 3]),
        ([0, 1, 2, 3], ["stat", "stat", "stat", "stat"], [0, 1, 2, 3]),
        ([0, 1], ["unknown", "low"], [0, 1]),
        ([0, 0, 1, 1], ["stat", "asap", "urgent", "routine"], [0, 1]),
    ],
)
def test__priority_assigner__assign_priorities(parts, priorities, expected_priority):
    sequencing_experiments = [
        SequencingDeltaOutput(
            case_id=0,
            seq_id=0,
            task_id=0,
            task_type="radiant_germline_annotation",
            part=part,
            analysis_type="germline",
            aliquot=str(0),
            patient_id=0,
            experimental_strategy="wgs",
            request_priority=priority,
            vcf_filepath="s3://bucket/path/file.vcf.gz",
            sex="male",
            family_id=0,
            family_role="proband",
            affected_status="affected",
            created_at=datetime(year=2025, month=1, day=1),
            updated_at=datetime(year=2025, month=1, day=1),
            ingested_at=datetime(year=2025, month=1, day=1),
        ).model_dump()
        for (part, priority) in zip(parts, priorities, strict=False)
    ]
    assert SequencingExperimentPriorityAssigner.assign_priorities(sequencing_experiments) == expected_priority
