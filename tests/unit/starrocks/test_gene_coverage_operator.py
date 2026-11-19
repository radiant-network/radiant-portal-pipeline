from unittest.mock import MagicMock, patch

import pymysql
import pytest

from radiant.tasks.starrocks.operator import (
    GENE_COVERAGE_FORMATS,
    GeneCoverageFileError,
    GeneCoverageLoadError,
    RadiantLoadGeneCoverageOperator,
    SwapPartition,
    parse_gene_coverage_header,
    raise_gene_coverage_errors,
)

OLD_HEADER = (
    "#gene,size,average_coverage,coverage5,coverage15,coverage30,coverage50,coverage100,coverage200,"
    "coverage300,coverage400,coverage500,coverage1000"
)
NEW_HEADER = (
    "#gene,type,size,average_coverage,coverage5,coverage15,coverage30,coverage50,coverage100,coverage200,"
    "coverage300,coverage400,coverage500,coverage1000"
)


@pytest.mark.parametrize(
    "header, expected",
    [
        (OLD_HEADER, "old"),
        (NEW_HEADER, "new"),
        (OLD_HEADER.lstrip("#"), "old"),
        (f"﻿{NEW_HEADER}\r", "new"),
        (NEW_HEADER.replace(",", ", "), "new"),
    ],
)
def test_parse_gene_coverage_header_tells_the_two_formats_apart(header, expected):
    assert parse_gene_coverage_header(header) == expected


@pytest.mark.parametrize(
    "header",
    [
        "",
        "A1BG,1469,538.5",
        OLD_HEADER.replace(",coverage1000", ""),
        NEW_HEADER.replace("type,size", "size,type"),
        OLD_HEADER.replace(",", "\t"),
    ],
)
def test_parse_gene_coverage_header_rejects_any_other_header(header):
    with pytest.raises(ValueError, match="Unexpected coverage_by_gene header"):
        parse_gene_coverage_header(header)


def test_gene_coverage_formats_load_the_new_format_type_column_and_set_cds_on_the_old_one():
    old_columns, old_type = GENE_COVERAGE_FORMATS["old"]
    new_columns, new_type = GENE_COVERAGE_FORMATS["new"]
    assert old_columns.split(", ")[:4] == ["gene", "size", "average_coverage", "coverage_5x"]
    assert new_columns.split(", ")[:5] == ["gene", "type", "size", "average_coverage", "coverage_5x"]
    assert len(old_columns.split(", ")) == 13
    assert len(new_columns.split(", ")) == 14
    assert old_columns.split(", ")[-1] == "coverage_1000x"
    assert (old_type, new_type) == ("'CDS'", "type")


def _header_operator(rows=None, error=None):
    op = RadiantLoadGeneCoverageOperator(
        task_id="t",
        table="radiant.raw_gene_coverage",
        swap_partition=SwapPartition(partition="5", copy_partition_sql="SELECT 1"),
        files=[],
    )
    op.broker_configuration = "'aws.s3.region' = 'mock-region'"
    cursor = MagicMock()
    cursor.execute.side_effect = error
    cursor.fetchall.return_value = rows or []
    return op, cursor


def test_read_format_reads_the_header_line_with_starrocks():
    op, cursor = _header_operator(rows=[(NEW_HEADER,)])

    assert op.read_format(cursor, "s3://mock-bucket/tests/a.csv") == "new"
    sql, params = cursor.execute.call_args.args
    assert "FROM FILES(" in sql
    assert "'aws.s3.region' = 'mock-region'" in sql
    assert "WHERE $1 LIKE '%%average_coverage%%'" in sql
    assert params == {"filepath": "s3://mock-bucket/tests/a.csv"}


@pytest.mark.parametrize(
    "rows, message",
    [
        ([], "found 0"),
        ([(OLD_HEADER,), (OLD_HEADER,)], "found 2"),
        ([("gene;size;average_coverage",)], "Unexpected coverage_by_gene header"),
    ],
)
def test_read_format_makes_a_missing_or_unknown_header_a_file_error(rows, message):
    op, cursor = _header_operator(rows=rows)
    with pytest.raises(GeneCoverageFileError, match=message):
        op.read_format(cursor, "s3://mock-bucket/tests/a.csv")


def test_read_format_makes_a_starrocks_error_a_file_error():
    op, cursor = _header_operator(error=pymysql.err.OperationalError(5600, "No files were found"))
    with pytest.raises(GeneCoverageFileError, match="No files were found"):
        op.read_format(cursor, "s3://mock-bucket/tests/missing.csv")


@pytest.mark.parametrize("error", [pymysql.err.OperationalError(2013, "Lost connection"), RuntimeError("boom")])
def test_read_format_lets_a_client_error_through(error):
    op, cursor = _header_operator(error=error)
    with pytest.raises(type(error)):
        op.read_format(cursor, "s3://mock-bucket/tests/a.csv")


def _file(task_id, seq_id, filepath, tenant_code="test"):
    return {
        "seq_id": seq_id,
        "task_id": task_id,
        "task_type": "alignment_germline_variant_calling",
        "tenant_code": tenant_code,
        "filepath": filepath,
    }


def _run_load(files, headers, *, partition_exists=True, cancelled=(), failing=()):
    """Run the load with the given header per file; a load whose path is in `cancelled` ends CANCELLED, and the
    statement of a load whose path is in `failing` raises a database error."""
    op = RadiantLoadGeneCoverageOperator(
        task_id="t",
        table="radiant.raw_gene_coverage",
        swap_partition=SwapPartition(partition="5", copy_partition_sql="SELECT 1"),
        parameters={"seq_ids": [1], "deleted_seq_ids": [-1]},
        files=files,
        sql="LOAD {{ file_columns }} SET type = {{ type_value }} {{ temporary_partition_clause }}",
    )
    hook = MagicMock()
    cursor = hook.get_conn.return_value.cursor.return_value.__enter__.return_value
    loads = []

    def execute(sql, params=None):
        if "FROM FILES(" in sql:
            header = headers[params["filepath"]]
            if isinstance(header, Exception):
                raise header
            cursor.fetchall.return_value = [(header,)]
        elif "information_schema.loads" in sql:
            path = loads[-1][1]["filepath"]
            cursor.fetchone.return_value = ("CANCELLED", "bad value") if path in cancelled else ("FINISHED", None)
        else:
            if params and params.get("filepath") in failing:
                raise pymysql.err.OperationalError(2013, "Lost connection to StarRocks")
            loads.append((sql, params))

    cursor.execute.side_effect = execute

    import jinja2

    with (
        patch.object(op, "get_db_hook", return_value=hook),
        patch.object(op, "render_template", side_effect=lambda content, ctx, *_: jinja2.Template(content).render(ctx)),
        patch.object(op, "swap_partitions") as swap,
    ):
        op.run_insert(context={}, partition_exists=partition_exists)
    _run_load.hook = hook
    return loads, op.errors, swap


def test_gene_coverage_load_picks_the_column_list_of_each_file_format():
    loads, errors, swap = _run_load(
        [_file(1, 10, "s3://mock/old.csv"), _file(2, 20, "s3://mock/new.csv", tenant_code="other")],
        {"s3://mock/old.csv": OLD_HEADER, "s3://mock/new.csv": NEW_HEADER},
    )

    assert errors == []
    assert [sql for sql, _ in loads] == [
        f"LOAD {GENE_COVERAGE_FORMATS['old'][0]} SET type = 'CDS' TEMPORARY PARTITION (tp5)",
        f"LOAD {GENE_COVERAGE_FORMATS['new'][0]} SET type = type TEMPORARY PARTITION (tp5)",
    ]
    assert [params for _, params in loads] == [
        {"part": "5", "seq_id": 10, "task_id": 1, "tenant_code": "test", "filepath": "s3://mock/old.csv"},
        {"part": "5", "seq_id": 20, "task_id": 2, "tenant_code": "other", "filepath": "s3://mock/new.csv"},
    ]
    swap.assert_called_once()


def test_gene_coverage_load_goes_on_after_a_bad_file_and_keeps_its_error():
    """A broken header and a cancelled load are both kept, with the S3 path; the good file loads, the part swaps."""
    loads, errors, swap = _run_load(
        [
            _file(1, 10, "s3://mock/broken.csv"),
            _file(2, 20, "s3://mock/cancelled.csv"),
            _file(3, 30, "s3://mock/good.csv"),
        ],
        {
            "s3://mock/broken.csv": "A1BG,1469",
            "s3://mock/cancelled.csv": OLD_HEADER,
            "s3://mock/good.csv": NEW_HEADER,
        },
        cancelled=("s3://mock/cancelled.csv",),
    )

    assert [params["filepath"] for _, params in loads] == ["s3://mock/cancelled.csv", "s3://mock/good.csv"]
    assert [(e["filepath"], e["task_id"]) for e in errors] == [
        ("s3://mock/broken.csv", 1),
        ("s3://mock/cancelled.csv", 2),
    ]
    assert "Unexpected coverage_by_gene header" in errors[0]["error"]
    assert "bad value" in errors[1]["error"]
    swap.assert_called_once()


def test_gene_coverage_load_keeps_an_unreadable_file_error():
    loads, errors, _ = _run_load(
        [_file(1, 10, "s3://mock/missing.csv")],
        {"s3://mock/missing.csv": pymysql.err.OperationalError(5600, "No files were found")},
    )
    assert loads == []
    assert [(e["filepath"], e["task_id"]) for e in errors] == [("s3://mock/missing.csv", 1)]
    assert "No files were found" in errors[0]["error"]


def test_gene_coverage_load_without_files_still_swaps_the_copied_partition():
    """The copy step dropped the rows of the delta's experiments: a re-alignment without file must lose them."""
    loads, errors, swap = _run_load([], {})
    assert (loads, errors) == ([], [])
    swap.assert_called_once()


def test_gene_coverage_load_into_a_new_partition_does_not_swap():
    loads, _, swap = _run_load(
        [_file(1, 10, "s3://mock/old.csv")], {"s3://mock/old.csv": OLD_HEADER}, partition_exists=False
    )
    assert loads[0][0].endswith("SET type = 'CDS' ")
    swap.assert_not_called()


def test_gene_coverage_load_returns_its_errors_as_xcom():
    op = RadiantLoadGeneCoverageOperator(
        task_id="t",
        table="radiant.raw_gene_coverage",
        swap_partition=SwapPartition(partition="5", copy_partition_sql="SELECT 1"),
        files=[],
    )

    def prepare_partition(context):
        op.errors.append({"filepath": "s3://mock/a.csv", "task_id": 1, "error": "boom"})

    with patch.object(op, "prepare_partition", side_effect=prepare_partition):
        assert op.execute(context={}) == [{"filepath": "s3://mock/a.csv", "task_id": 1, "error": "boom"}]


def test_raise_gene_coverage_errors_names_each_bad_file():
    raise_gene_coverage_errors([])
    raise_gene_coverage_errors(None)
    with pytest.raises(GeneCoverageLoadError) as e:
        raise_gene_coverage_errors(
            [
                {"filepath": "s3://mock/a.csv", "task_id": 1, "error": "boom"},
                {"filepath": "s3://mock/b.csv", "task_id": 2, "error": "bang"},
            ]
        )
    assert "2 coverage_by_gene file(s)" in str(e.value)
    assert "s3://mock/a.csv (task 1): boom" in str(e.value)
    assert "s3://mock/b.csv (task 2): bang" in str(e.value)


def test_gene_coverage_load_keeps_the_loaded_rows_of_a_bad_file_experiment():
    """The copy step dropped the experiments of the delta: the bad file's experiment gets its loaded rows back, except
    those of a task this run loaded, then the part swaps."""
    _, errors, swap = _run_load(
        [_file(1, 10, "s3://mock/broken.csv"), _file(3, 30, "s3://mock/good.csv")],
        {"s3://mock/broken.csv": "A1BG,1469", "s3://mock/good.csv": NEW_HEADER},
    )

    assert [e["task_id"] for e in errors] == [1]
    restore = _run_load.hook.run.call_args
    assert "INSERT INTO radiant.raw_gene_coverage TEMPORARY PARTITION (tp5)" in restore.kwargs["sql"]
    assert "FROM radiant.raw_gene_coverage PARTITION (p5)" in restore.kwargs["sql"]
    assert restore.kwargs["parameters"] == {"seq_ids": [10], "task_ids": [3]}
    swap.assert_called_once()


def test_gene_coverage_load_restores_nothing_without_a_bad_file():
    _run_load([_file(1, 10, "s3://mock/old.csv")], {"s3://mock/old.csv": OLD_HEADER})
    _run_load.hook.run.assert_not_called()


def test_gene_coverage_load_fails_on_a_database_error_without_swapping():
    """A lost connection is not a bad file: the task fails before the swap, Airflow retries, the loaded rows stay."""
    with pytest.raises(pymysql.err.OperationalError):
        _run_load(
            [_file(1, 10, "s3://mock/old.csv")],
            {"s3://mock/old.csv": OLD_HEADER},
            failing=("s3://mock/old.csv",),
        )


def _waiting_operator(rows):
    op = RadiantLoadGeneCoverageOperator(
        task_id="t",
        table="radiant.raw_gene_coverage",
        swap_partition=SwapPartition(partition="5", copy_partition_sql="SELECT 1"),
        files=[],
        broker_load_timeout=3,
    )
    cursor = MagicMock()
    cursor.fetchone.side_effect = rows
    return op, cursor


@patch("radiant.tasks.starrocks.operator.time.sleep")
def test_wait_for_load_cancels_a_load_that_does_not_finish_in_time(_sleep):
    op, cursor = _waiting_operator([("LOADING", None)] * 3)
    with pytest.raises(GeneCoverageFileError, match="did not finish in time"):
        op.wait_for_load(cursor, "label_1")
    cursor.execute.assert_called_with("CANCEL LOAD FROM radiant WHERE LABEL = %(label)s", {"label": "label_1"})


@patch("radiant.tasks.starrocks.operator.time.sleep")
def test_wait_for_load_does_not_count_a_missing_load_as_finished(_sleep):
    op, cursor = _waiting_operator([None, ("FINISHED", None)])
    op.wait_for_load(cursor, "label_1")
    assert cursor.fetchone.call_count == 2


@patch("radiant.tasks.starrocks.operator.time.sleep")
def test_wait_for_load_fails_when_the_load_never_shows_up(_sleep):
    op, cursor = _waiting_operator([None] * 100)
    op.broker_load_timeout = 100
    with pytest.raises(RuntimeError, match="not in information_schema.loads"):
        op.wait_for_load(cursor, "label_1")
