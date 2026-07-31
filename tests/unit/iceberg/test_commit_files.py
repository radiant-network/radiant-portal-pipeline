from unittest.mock import MagicMock, call

import pytest
from pyiceberg.exceptions import CommitFailedException
from pyiceberg.expressions import And, EqualTo

from radiant.tasks.iceberg.partition_commit import PartitionCommit, merge_partition_commits
from radiant.tasks.iceberg.utils import _partition_filter_expr, commit_files


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    """The retry path backs off exponentially; tests must not actually wait."""
    monkeypatch.setattr("radiant.tasks.iceberg.utils.time.sleep", lambda _seconds: None)


def make_table(commit_side_effects: list):
    """A table whose successive `commit_transaction` calls raise/return the given effects.

    Each `table.transaction()` hands back a fresh mock, so the test can assert that a retry
    re-stages its updates instead of re-committing the transaction it already built.
    """
    table = MagicMock()
    table.name.return_value = "radiant.snv_variant"
    transactions = []

    def new_transaction():
        tx = MagicMock()
        effect = commit_side_effects[len(transactions)]
        if isinstance(effect, Exception):
            tx.commit_transaction.side_effect = effect
        transactions.append(tx)
        return tx

    table.transaction.side_effect = new_transaction
    return table, transactions


PARTITIONS = [PartitionCommit(parquet_files=["s3://bucket/a.parquet"], partition_filter={"task_id": 7})]


def test_conflicting_commit_is_retried_against_a_fresh_snapshot():
    """A concurrent commit must be absorbed by re-staging, not by re-committing.

    Staging captures an `AssertRefSnapshotId` against the snapshot current at that moment, so
    re-committing the same transaction resends a stale assertion and fails identically forever.
    """
    table, transactions = make_table([CommitFailedException("conflict"), None])

    commit_files(table, PARTITIONS)

    # Two distinct transactions, each preceded by its own refresh.
    assert len(transactions) == 2
    assert table.refresh.call_count == 2
    # The retry re-stages the same idempotent delete + add_files.
    for tx in transactions:
        tx.delete.assert_called_once_with(EqualTo("task_id", 7))
        tx.add_files.assert_called_once_with(["s3://bucket/a.parquet"])
        tx.commit_transaction.assert_called_once()


def test_commit_raises_after_exhausting_retries():
    table, transactions = make_table([CommitFailedException("conflict")] * 3)

    with pytest.raises(CommitFailedException):
        commit_files(table, PARTITIONS, max_retries=3)

    assert len(transactions) == 3
    assert table.refresh.call_count == 3


def test_successful_commit_does_not_retry():
    table, transactions = make_table([None])

    commit_files(table, PARTITIONS)

    assert len(transactions) == 1
    assert table.refresh.call_count == 1


def test_no_partitions_is_a_noop():
    table, transactions = make_table([])

    commit_files(table, [])

    assert transactions == []
    table.refresh.assert_not_called()


def test_files_are_not_added_when_a_partition_has_none():
    """An empty partition still needs its delete, so a re-run drops the previous rows."""
    table, transactions = make_table([None])

    commit_files(table, [PartitionCommit(parquet_files=[], partition_filter={"task_id": 7})])

    transactions[0].delete.assert_called_once_with(EqualTo("task_id", 7))
    transactions[0].add_files.assert_not_called()


def test_multi_column_partition_filter_is_anded():
    expr = _partition_filter_expr(PartitionCommit(parquet_files=[], partition_filter={"part": 3, "task_id": 7}))
    assert expr == And(EqualTo("part", 3), EqualTo("task_id", 7))


def test_empty_partition_filter_is_rejected():
    with pytest.raises(ValueError, match="at least one key-value pair"):
        _partition_filter_expr(PartitionCommit(parquet_files=["s3://bucket/a.parquet"], partition_filter={}))


def test_all_partitions_of_a_table_share_one_transaction():
    """Every task's partition lands in the same commit, so a part is one snapshot per table."""
    table, transactions = make_table([None, None])

    commit_files(
        table,
        [
            PartitionCommit(parquet_files=["s3://bucket/a.parquet"], partition_filter={"task_id": 7}),
            PartitionCommit(parquet_files=["s3://bucket/b.parquet"], partition_filter={"task_id": 8}),
        ],
    )

    # Both partitions of the same table share one transaction.
    assert len(transactions) == 1
    assert transactions[0].delete.call_args_list == [call(EqualTo("task_id", 7)), call(EqualTo("task_id", 8))]


def test_merge_partition_commits_concatenates_and_skips_empties():
    """A part with no somatic tasks contributes an empty result that must not break the merge."""
    merged = merge_partition_commits(
        [
            {"radiant.snv_variant": [{"id": "g1"}], "radiant.germline_snv_occurrence": [{"id": "go"}]},
            {},
            None,
            {"radiant.snv_variant": [{"id": "s1"}], "radiant.somatic_snv_occurrence": [{"id": "so"}]},
        ]
    )

    assert merged == {
        "radiant.snv_variant": [{"id": "g1"}, {"id": "s1"}],
        "radiant.germline_snv_occurrence": [{"id": "go"}],
        "radiant.somatic_snv_occurrence": [{"id": "so"}],
    }
