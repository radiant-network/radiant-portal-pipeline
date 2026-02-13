from collections import Counter
from datetime import datetime

import pytest

from radiant.tasks.starrocks.partition import (
    PriorityLevel,
    SequencingDeltaInput,
    SequencingDeltaOutput,
    SequencingExperimentPartitionAssigner,
    SequencingExperimentPriorityAssigner,
)


@pytest.mark.parametrize(
    "experimental_strategy, expected_part",
    [
        ("wgs", 0x00000000),
        ("wxs", 0x00010000),
    ],
)
def test__partition_assigner__from_empty_single(experimental_strategy, expected_part):
    _delta = [
        SequencingDeltaInput(
            case_id=1,
            seq_id=1,
            task_id=1,
            task_type="radiant_germline_annotation",
            analysis_type="germline",
            aliquot=str(1),
            patient_id=1,
            experimental_strategy=experimental_strategy,
            request_priority="stat",
            vcf_filepath="s3://bucket/path/file.vcf.gz",
            sex="male",
            family_id=1,
            family_role="proband",
            affected_status="affected",
            created_at=datetime(year=2025, month=1, day=1),
            updated_at=datetime(year=2025, month=1, day=1),
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
        SequencingDeltaInput(
            case_id=i,
            seq_id=i,
            task_id=i,
            task_type="radiant_germline_annotation",
            analysis_type="germline",
            aliquot=str(i),
            patient_id=i,
            experimental_strategy=experimental_strategy,
            request_priority="stat",
            vcf_filepath="s3://bucket/path/file.vcf.gz",
            sex="male",
            family_id=i,
            family_role="proband",
            affected_status="affected",
            created_at=datetime(year=2025, month=1, day=1),
            updated_at=datetime(year=2025, month=1, day=1),
        ).model_dump()
        for i in range(limit)
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
        SequencingDeltaInput(
            case_id=i,
            seq_id=i,
            task_id=i,
            task_type="radiant_germline_annotation",
            analysis_type="germline",
            aliquot=str(i),
            patient_id=i,
            experimental_strategy=experimental_strategy,
            request_priority="stat",
            vcf_filepath="s3://bucket/path/file.vcf.gz",
            sex="male",
            family_id=i,
            family_role="proband",
            affected_status="affected",
            created_at=datetime(year=2025, month=1, day=1),
            updated_at=datetime(year=2025, month=1, day=1),
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
        SequencingDeltaInput(
            case_id=0,
            seq_id=0,
            task_id=0,
            task_type="radiant_germline_annotation",
            analysis_type="germline",
            aliquot=str(0),
            patient_id=0,
            experimental_strategy="wgs",
            request_priority="stat",
            vcf_filepath="s3://bucket/path/file.vcf.gz",
            sex="male",
            family_id=0,
            family_role="proband",
            affected_status="affected",
            created_at=datetime(year=2025, month=1, day=1),
            updated_at=datetime(year=2025, month=1, day=1),
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


def test__partition_assigner_with_related_experiments():
    _delta = [
        SequencingDeltaInput(
            case_id=1,
            seq_id=1,
            task_id=1,
            patient_id=1,
            family_id=1,
            experimental_strategy="wgs",
            # dummy values for other fields
            task_type="",
            analysis_type="",
            aliquot="",
            request_priority="",
            sex="",
            family_role="",
            affected_status="",
            created_at=datetime.now(),
            updated_at=datetime.now(),
        ).model_dump(),
        SequencingDeltaInput(
            case_id=2,
            seq_id=2,
            task_id=2,
            patient_id=1,  # Same patient
            family_id=2,
            experimental_strategy="wgs",
            # dummy values for other fields
            task_type="",
            analysis_type="",
            aliquot="",
            request_priority="",
            sex="",
            family_role="",
            affected_status="",
            created_at=datetime.now(),
            updated_at=datetime.now(),
        ).model_dump(),
    ]
    partitioned = SequencingExperimentPartitionAssigner().assign_partitions(_delta)
    assert len(partitioned) == 2
    assert partitioned[0].part == partitioned[1].part


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
