import io
from unittest.mock import MagicMock, patch

import pymysql
import pytest
from botocore.exceptions import ClientError, EndpointConnectionError

from radiant.tasks.starrocks.operator import (
    GENE_COVERAGE_FORMATS,
    GENE_COVERAGE_HEADER_BYTES,
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


def _client_error(code, operation="GetObject"):
    return ClientError({"Error": {"Code": code, "Message": f"mock {code}"}}, operation)


class _FakeS3:
    """`get_object` of boto3's S3 client: each path's content (bytes or str), or the error it raises."""

    def __init__(self, contents):
        self.contents = contents
        self.calls = []

    def get_object(self, Bucket, Key, Range):  # noqa: N803 (boto3 argument names)
        self.calls.append((Bucket, Key, Range))
        content = self.contents[f"s3://{Bucket}/{Key}"]
        if isinstance(content, Exception):
            raise content
        data = content.encode() if isinstance(content, str) else content
        first, last = (int(b) for b in Range.removeprefix("bytes=").split("-"))
        return {"Body": io.BytesIO(data[first : last + 1])}


def _with_row(header):
    return f"{header}\nBRCA1,5589,136.5,1.0,1.0,0.99,0.98,0.9,0.5,0.2,0.1,0.05,0.0\n"


def _operator():
    return RadiantLoadGeneCoverageOperator(
        task_id="t",
        table="radiant.raw_gene_coverage",
        swap_partition=SwapPartition(partition="5", copy_partition_sql="SELECT 1"),
        files=[],
    )


def test_read_format_reads_the_first_bytes_of_the_file_only():
    s3 = _FakeS3({"s3://mock-bucket/tests/a.csv": _with_row(NEW_HEADER) * 1000})

    assert _operator().read_format(s3, "s3://mock-bucket/tests/a.csv") == "new"
    assert s3.calls == [("mock-bucket", "tests/a.csv", f"bytes=0-{GENE_COVERAGE_HEADER_BYTES - 1}")]


@pytest.mark.parametrize(
    "content, expected",
    [
        (_with_row(OLD_HEADER), "old"),
        # A file of its header alone, with or without the end of line.
        (OLD_HEADER, "old"),
        (f"{NEW_HEADER}\r\n", "new"),
        # A UTF-8 byte order mark.
        (b"\xef\xbb\xbf" + _with_row(NEW_HEADER).encode(), "new"),
    ],
)
def test_read_format_takes_the_first_line_as_the_header(content, expected):
    s3 = _FakeS3({"s3://mock-bucket/a.csv": content})
    assert _operator().read_format(s3, "s3://mock-bucket/a.csv") == expected


@pytest.mark.parametrize(
    "content, message",
    [
        # The header must be the first line: the broker load skips the first line only.
        (f"BRCA1,5589,136.5\n{OLD_HEADER}\n", "Unexpected coverage_by_gene header"),
        ("gene;size;average_coverage\n", "Unexpected coverage_by_gene header"),
        ("x" * (GENE_COVERAGE_HEADER_BYTES + 10), "No header line in the first"),
        (_client_error("InvalidRange"), "InvalidRange"),
    ],
)
def test_read_format_makes_a_missing_or_unknown_header_a_file_error(content, message):
    s3 = _FakeS3({"s3://mock-bucket/a.csv": content})
    with pytest.raises(GeneCoverageFileError, match=message):
        _operator().read_format(s3, "s3://mock-bucket/a.csv")


@pytest.mark.parametrize("code", ["NoSuchKey", "NoSuchBucket", "AccessDenied", "404", "403"])
def test_read_format_makes_a_file_s3_cannot_read_a_file_error(code):
    s3 = _FakeS3({"s3://mock-bucket/missing.csv": _client_error(code)})
    with pytest.raises(GeneCoverageFileError, match=f"S3 cannot read the file \\({code}\\)"):
        _operator().read_format(s3, "s3://mock-bucket/missing.csv")


@pytest.mark.parametrize("path", ["/mock/a.csv", "s3://mock-bucket", "s3://mock-bucket/", "gs://mock-bucket/a.csv"])
def test_read_format_makes_a_path_that_is_no_s3_object_a_file_error(path):
    with pytest.raises(GeneCoverageFileError, match="Not an S3 object path"):
        _operator().read_format(_FakeS3({}), path)


@pytest.mark.parametrize(
    "error",
    [
        # Throttling and server errors are temporary: never a bad file.
        _client_error("SlowDown"),
        _client_error("InternalError"),
        _client_error("503"),
        EndpointConnectionError(endpoint_url="https://mock-endpoint"),
        RuntimeError("boom"),
    ],
)
def test_read_format_raises_any_other_error_as_is(error):
    s3 = _FakeS3({"s3://mock-bucket/a.csv": error})
    with pytest.raises(type(error)):
        _operator().read_format(s3, "s3://mock-bucket/a.csv")


def _file(task_id, seq_id, filepath, tenant_code="test"):
    return {
        "seq_id": seq_id,
        "task_id": task_id,
        "task_type": "alignment_germline_variant_calling",
        "tenant_code": tenant_code,
        "filepath": filepath,
    }


def _paths(params):
    """The S3 paths of one load job, in data_desc order."""
    return [params[f"filepath_{i}"] for i in range(len([k for k in params if k.startswith("filepath_")]))]


def _run_load(files, headers, *, partition_exists=True, cancelled=(), failing=(), files_per_load=100):
    """Run the load with the given header per file (or the S3 error of its read); a load job holding a path of
    `cancelled` ends CANCELLED (and writes nothing), and the statement of a job holding a path of `failing` raises a
    database error."""
    op = RadiantLoadGeneCoverageOperator(
        task_id="t",
        table="radiant.raw_gene_coverage",
        swap_partition=SwapPartition(partition="5", copy_partition_sql="SELECT 1"),
        parameters={"seq_ids": [1], "deleted_seq_ids": [-1]},
        files=files,
        sql="LOAD {{ file_count }} {{ table_name }} {{ file_columns }} SET type = {{ type_value }} "
        "{{ temporary_partition_clause }}",
        files_per_load=files_per_load,
    )
    hook = MagicMock()
    cursor = hook.get_conn.return_value.cursor.return_value.__enter__.return_value
    loads = []

    s3 = _FakeS3(
        {path: header if isinstance(header, Exception) else _with_row(header) for path, header in headers.items()}
    )

    def execute(sql, params=None):
        if "information_schema.loads" in sql:
            bad = set(_paths(loads[-1][1])) & set(cancelled)
            cursor.fetchone.return_value = ("CANCELLED", f"bad value in {sorted(bad)}") if bad else ("FINISHED", None)
        else:
            if params and set(_paths(params)) & set(failing):
                raise pymysql.err.OperationalError(2013, "Lost connection to StarRocks")
            loads.append((sql, params))

    cursor.execute.side_effect = execute

    import jinja2

    with (
        patch.object(op, "get_db_hook", return_value=hook),
        patch.object(op, "get_s3_client", return_value=s3),
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
        f"LOAD 1 raw_gene_coverage {GENE_COVERAGE_FORMATS['old'][0]} SET type = 'CDS' TEMPORARY PARTITION (tp5)",
        f"LOAD 1 raw_gene_coverage {GENE_COVERAGE_FORMATS['new'][0]} SET type = type TEMPORARY PARTITION (tp5)",
    ]
    assert [params for _, params in loads] == [
        {"part": "5", "seq_id_0": 10, "task_id_0": 1, "tenant_code_0": "test", "filepath_0": "s3://mock/old.csv"},
        {"part": "5", "seq_id_0": 20, "task_id_0": 2, "tenant_code_0": "other", "filepath_0": "s3://mock/new.csv"},
    ]
    swap.assert_called_once()


def test_gene_coverage_load_puts_the_files_of_one_format_in_one_load_job():
    """One job per format, each file with its own experiment, task and tenant."""
    loads, errors, _ = _run_load(
        [
            _file(1, 10, "s3://mock/old-1.csv"),
            _file(2, 20, "s3://mock/new-1.csv"),
            _file(3, 30, "s3://mock/old-2.csv", tenant_code="other"),
            _file(4, 40, "s3://mock/new-2.csv"),
        ],
        {
            "s3://mock/old-1.csv": OLD_HEADER,
            "s3://mock/new-1.csv": NEW_HEADER,
            "s3://mock/old-2.csv": OLD_HEADER,
            "s3://mock/new-2.csv": NEW_HEADER,
        },
    )

    assert errors == []
    assert [sql.split()[1] for sql, _ in loads] == ["2", "2"]
    assert [_paths(params) for _, params in loads] == [
        ["s3://mock/old-1.csv", "s3://mock/old-2.csv"],
        ["s3://mock/new-1.csv", "s3://mock/new-2.csv"],
    ]
    old = loads[0][1]
    assert (old["seq_id_1"], old["task_id_1"], old["tenant_code_1"]) == (30, 3, "other")


def test_gene_coverage_load_splits_a_format_into_jobs_of_files_per_load_files():
    files = [_file(i, i * 10, f"s3://mock/old-{i}.csv") for i in range(1, 6)]
    loads, errors, _ = _run_load(files, dict.fromkeys((f["filepath"] for f in files), OLD_HEADER), files_per_load=2)

    assert errors == []
    assert [len(_paths(params)) for _, params in loads] == [2, 2, 1]


def test_gene_coverage_load_reloads_the_files_of_a_cancelled_job_one_by_one():
    """The cancelled job wrote nothing: each of its files loads alone, so that only the bad one is lost."""
    loads, errors, swap = _run_load(
        [
            _file(1, 10, "s3://mock/good-1.csv"),
            _file(2, 20, "s3://mock/bad.csv"),
            _file(3, 30, "s3://mock/good-2.csv"),
        ],
        {"s3://mock/good-1.csv": OLD_HEADER, "s3://mock/bad.csv": OLD_HEADER, "s3://mock/good-2.csv": OLD_HEADER},
        cancelled=("s3://mock/bad.csv",),
    )

    assert [_paths(params) for _, params in loads] == [
        ["s3://mock/good-1.csv", "s3://mock/bad.csv", "s3://mock/good-2.csv"],
        ["s3://mock/good-1.csv"],
        ["s3://mock/bad.csv"],
        ["s3://mock/good-2.csv"],
    ]
    assert [(e["filepath"], e["task_id"]) for e in errors] == [("s3://mock/bad.csv", 2)]
    assert "bad value" in errors[0]["error"]
    # The bad file's experiment gets its loaded rows back; the two good tasks are not copied back.
    restore = _run_load.hook.run.call_args
    assert restore.kwargs["parameters"] == {"seq_ids": [20], "task_ids": [1, 3]}
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

    assert [_paths(params) for _, params in loads] == [["s3://mock/cancelled.csv"], ["s3://mock/good.csv"]]
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
        {"s3://mock/missing.csv": _client_error("NoSuchKey")},
    )
    assert loads == []
    assert [(e["filepath"], e["task_id"]) for e in errors] == [("s3://mock/missing.csv", 1)]
    assert "S3 cannot read the file (NoSuchKey)" in errors[0]["error"]


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


def test_gene_coverage_load_sql_has_one_data_desc_per_file_after_the_second_render():
    """The data_desc loop is a `raw` block: the first render (task fields) keeps it, the second (per job) runs it."""
    import os

    import jinja2

    from radiant.dags import DAGS_DIR

    with open(os.path.join(DAGS_DIR, "sql", "radiant", "staging_gene_coverage_load.sql")) as f_in:
        first = jinja2.Template(f_in.read()).render(
            database_name="radiant",
            load_label="{{load_label}}",
            broker_configuration="'aws.s3.region' = 'mock-region'",
            broker_load_timeout=600,
        )
    assert "{% for i in range(file_count) %}" in first

    second = jinja2.Template(first).render(
        load_label="mock_label",
        file_count=2,
        table_name="raw_gene_coverage",
        temporary_partition_clause="TEMPORARY PARTITION (tp5)",
        file_columns=GENE_COVERAGE_FORMATS["old"][0],
        type_value=GENE_COVERAGE_FORMATS["old"][1],
    )
    assert "LOAD LABEL radiant.mock_label" in second
    assert second.count("INTO TABLE raw_gene_coverage TEMPORARY PARTITION (tp5)") == 2
    for i in (0, 1):
        assert f"DATA INFILE(%(filepath_{i})s)" in second
        assert f"seq_id = %(seq_id_{i})s" in second
        assert f"tenant_code = %(tenant_code_{i})s" in second
    # A comma between the two data_descs, none after the last.
    assert second.count("WHERE type = 'CDS',") == 1
    assert second.count("WHERE type = 'CDS'") == 2
    assert "'timeout' = '600'" in second


def test_gene_coverage_load_needs_at_least_one_file_per_load():
    with pytest.raises(ValueError, match="files_per_load"):
        RadiantLoadGeneCoverageOperator(task_id="t", table="radiant.raw_gene_coverage", files=[], files_per_load=0)
