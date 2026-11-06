"""RAD-181 -- the CNV Exomiser result on the germline CNV occurrences.

The CNV Exomiser report of a `radiant_germline_cnv_annotation` task is broker-loaded into `raw_exomiser_cnv`
(`staging_exomiser_cnv_load.sql`, as `load_exomiser_cnv_files` runs it), then the germline CNV occurrence insert copies
the best row of each record onto the task's occurrences. cnv-post-processing runs Exomiser on one of two VCFs, and the
report's `VCF_ID` follows it:

- the proband VCF (JOIN_PROBAND_CNV): the proband's own call ID, the occurrence's `name` (fixture
  `mock_cnv_proband.exomiser.variants.tsv`, task `_PROBAND_ROUTE_TASK`);
- the family VCF: the slivar record's ID, the occurrence's `vcf_id` (fixture `mock_cnv_family.exomiser.variants.tsv`,
  task `_FAMILY_ROUTE_TASK`).
"""

import os
import time
import uuid

import jinja2
import pyarrow as pa
import pytest
from pyiceberg.expressions import In

from tests.integration.dags.sql.test_cnv_nb_snv import _SQL_DIR, _create_cnv_id_udf, _create_table

# Own parts, tasks and sequencings, apart from the other CNV tests, so the rows are addressable for the teardown.
_PART = 1810
_TENANT = "test"
_SEQ_ID = 1810001
_OTHER_SEQ_ID = 1810002
_PROBAND_ROUTE_TASK = 18100
_ALIGNMENT_TASK = 18101
_FAMILY_ROUTE_TASK = 18102
_TASKS = [_PROBAND_ROUTE_TASK, _ALIGNMENT_TASK, _FAMILY_ROUTE_TASK]

# Slivar records of the proband-route task. `name` is the proband's own call (FORMAT/OID), `vcf_id` the record's ID.
_TWO_GENES = "DRAGEN:GAIN:chr1:1001-5000"  # name = vcf_id; two Exomiser rows, two genes
_KEPT_PARENT_CALL = "DRAGEN:LOSS:chr1:10101-19900"  # name; the record's ID is the mother's call, below
_MOTHER_CALL = "DRAGEN:LOSS:chr1:10001-20000"
_NOT_CONTRIBUTING = "DRAGEN:LOSS:chr1:30001-40000"  # its only Exomiser row has CONTRIBUTING_VARIANT = 0
_UNRANKED = "DRAGEN:GAIN:chr1:50001-60000"  # no Exomiser row
# Slivar record of the family-route task: the report names the record's ID (the mother's call), not `name`.
_FAMILY_NAME = "DRAGEN:LOSS:chr2:30101-39900"
_FAMILY_VCF_ID = "DRAGEN:LOSS:chr2:30001-40000"

_EXOMISER_COLUMNS = (
    "exomiser_rank",
    "exomiser_variant_score",
    "exomiser_gene_combined_score",
    "exomiser_moi",
    "exomiser_acmg_classification",
    "exomiser_acmg_evidence",
)
_EMPTY = dict.fromkeys(_EXOMISER_COLUMNS)


def _cnv_row(*, task_id, seq_id, name, vcf_id, start, end, cnv_type="LOSS", chromosome="1"):
    return {
        "part": _PART,
        "seq_id": seq_id,
        "tenant_code": _TENANT,
        "task_id": task_id,
        "aliquot": f"SA{seq_id}",
        "chromosome": chromosome,
        "alternate": "<DEL>" if cnv_type == "LOSS" else "<DUP>",
        "start": start,
        "end": end,
        "type": cnv_type,
        "length": end - start,
        "name": name,
        "vcf_id": vcf_id,
    }


_CNV_ROWS = [
    _cnv_row(
        task_id=_PROBAND_ROUTE_TASK,
        seq_id=_SEQ_ID,
        name=_TWO_GENES,
        vcf_id=_TWO_GENES,
        start=1000,
        end=5000,
        cnv_type="GAIN",
    ),
    _cnv_row(
        task_id=_PROBAND_ROUTE_TASK,
        seq_id=_SEQ_ID,
        name=_KEPT_PARENT_CALL,
        vcf_id=_MOTHER_CALL,
        start=10000,
        end=20000,
    ),
    _cnv_row(
        task_id=_PROBAND_ROUTE_TASK,
        seq_id=_SEQ_ID,
        name=_NOT_CONTRIBUTING,
        vcf_id=_NOT_CONTRIBUTING,
        start=30000,
        end=40000,
    ),
    _cnv_row(
        task_id=_PROBAND_ROUTE_TASK,
        seq_id=_SEQ_ID,
        name=_UNRANKED,
        vcf_id=_UNRANKED,
        start=50000,
        end=60000,
        cnv_type="GAIN",
    ),
    # The proband's DRAGEN set: the same call as the slivar record above, under the alignment task.
    _cnv_row(
        task_id=_ALIGNMENT_TASK,
        seq_id=_SEQ_ID,
        name=_KEPT_PARENT_CALL,
        vcf_id=_KEPT_PARENT_CALL,
        start=10100,
        end=19900,
    ),
    _cnv_row(
        task_id=_FAMILY_ROUTE_TASK,
        seq_id=_OTHER_SEQ_ID,
        name=_FAMILY_NAME,
        vcf_id=_FAMILY_VCF_ID,
        start=30000,
        end=40000,
        chromosome="2",
    ),
]


def _render(path, mapping, **context):
    with open(os.path.join(_SQL_DIR, "radiant", path)) as f_in:
        return jinja2.Template(f_in.read()).render({"mapping": mapping, "partition": _PART, **context})


def _broker_load(cursor, mapping, minio_instance, *, seq_id, task_id, s3_key):
    """The load `RadiantLoadExomiserOperator` runs for a CNV annotation task, with its parameters."""
    database_name, table_name = mapping["starrocks_staging_exomiser_cnv"].split(".")
    label = f"test_raw_exomiser_cnv_load_{uuid.uuid4().hex}"
    sql = _render(
        "staging_exomiser_cnv_load.sql",
        mapping,
        broker_load_timeout=7200,
        database_name=database_name,
        table_name=table_name,
        load_label=label,
        broker_configuration=f"""
            'aws.s3.region' = 'us-east-1',
            'aws.s3.endpoint' = 'http://radiant-minio:9000',
            'aws.s3.enable_path_style_access' = 'true',
            'aws.s3.access_key' = '{minio_instance.access_key}',
            'aws.s3.secret_key' = '{minio_instance.secret_key}'
        """,
        temporary_partition_clause="",
    )
    cursor.execute(
        sql,
        {
            "part": _PART,
            "seq_id": seq_id,
            "task_id": task_id,
            "tenant_code": _TENANT,
            "tsv_filepath": f"s3://{s3_key}",
        },
    )
    for _ in range(150):
        cursor.execute("SELECT STATE FROM information_schema.loads WHERE LABEL = %(label)s", {"label": label})
        state = cursor.fetchone()
        if not state or state[0] == "FINISHED":
            return
        if state[0] == "CANCELLED":
            raise RuntimeError(f"Load for label {label} was cancelled.")
        time.sleep(2)
    raise TimeoutError(f"Load for label {label} did not finish in time.")


@pytest.fixture
def loaded_reports(
    starrocks_session,
    iceberg_client,
    setup_iceberg_namespace,
    # For `gnomad_sv_v1`, which the CNV insert LEFT JOINs and which exists only in Iceberg.
    open_data_iceberg_tables,
    radiant_mapping,
    minio_instance,
    sample_cnv_exomiser_tsvs,
):
    for table in ("germline_cnv_occurrence", "germline_snv_occurrence", "snv_variant", "staging_exomiser_cnv"):
        _create_table(starrocks_session, "radiant", table, radiant_mapping, truncate=False)
    for table in ("cytoband", "ensembl_gene"):
        _create_table(starrocks_session, "open_data", table, radiant_mapping, truncate=False)
    _create_cnv_id_udf(starrocks_session, radiant_mapping)

    raw = radiant_mapping["starrocks_staging_exomiser_cnv"]
    target = radiant_mapping["starrocks_germline_cnv_occurrence"]
    with starrocks_session.cursor() as cursor:
        for table in (raw, target):
            cursor.execute(f"DELETE FROM {table} WHERE part = %(part)s", {"part": _PART})
        _broker_load(
            cursor,
            radiant_mapping,
            minio_instance,
            seq_id=_SEQ_ID,
            task_id=_PROBAND_ROUTE_TASK,
            s3_key=sample_cnv_exomiser_tsvs["proband"],
        )
        _broker_load(
            cursor,
            radiant_mapping,
            minio_instance,
            seq_id=_OTHER_SEQ_ID,
            task_id=_FAMILY_ROUTE_TASK,
            s3_key=sample_cnv_exomiser_tsvs["family"],
        )

    iceberg_table = iceberg_client.load_table(f"{setup_iceberg_namespace}.germline_cnv_occurrence")
    # The table's own schema: Iceberg renumbers field ids on create, and the append is validated by id.
    iceberg_table.append(pa.Table.from_pylist(_CNV_ROWS, schema=iceberg_table.schema().as_arrow()))
    with starrocks_session.cursor() as cursor:
        cursor.execute(f"REFRESH EXTERNAL TABLE {radiant_mapping['iceberg_germline_cnv_occurrence']}")

    yield

    # The Iceberg CNV table is shared with the VCF tests, which assert unfiltered row counts on it.
    iceberg_table.delete(In("task_id", _TASKS))
    with starrocks_session.cursor() as cursor:
        for table in (raw, target):
            cursor.execute(f"DELETE FROM {table} WHERE part = %(part)s", {"part": _PART})


def _insert(cursor, mapping):
    """The positional insert of `insert_germline_cnv_occurrences`, one tenant: fails if the column order drifts."""
    sql = _render("germline_cnv_occurrence_insert_partition_delta.sql", mapping).rstrip().rstrip(";")
    cursor.execute(
        f"INSERT INTO {mapping['starrocks_germline_cnv_occurrence']} {sql}",
        {"seq_ids": [_SEQ_ID, _OTHER_SEQ_ID], "task_ids": _TASKS, "tenant_code": _TENANT},
    )


def _exomiser_by_task_and_name(cursor, mapping):
    cursor.execute(
        f"SELECT task_id, name, {', '.join(_EXOMISER_COLUMNS)} "
        f"FROM {mapping['starrocks_germline_cnv_occurrence']} WHERE part = %(part)s",
        {"part": _PART},
    )
    return {(row[0], row[1]): dict(zip(_EXOMISER_COLUMNS, row[2:], strict=True)) for row in cursor.fetchall()}


def _assert_exomiser(exomiser):
    assert set(exomiser) == {
        (_PROBAND_ROUTE_TASK, _TWO_GENES),
        (_PROBAND_ROUTE_TASK, _KEPT_PARENT_CALL),
        (_PROBAND_ROUTE_TASK, _NOT_CONTRIBUTING),
        (_PROBAND_ROUTE_TASK, _UNRANKED),
        (_ALIGNMENT_TASK, _KEPT_PARENT_CALL),
        (_FAMILY_ROUTE_TASK, _FAMILY_NAME),
    }

    # Two genes for one record: the row with the highest gene combined score (0.9 over 0.4) wins, whole.
    two_genes = exomiser[(_PROBAND_ROUTE_TASK, _TWO_GENES)]
    assert two_genes["exomiser_variant_score"] == pytest.approx(0.8)
    assert two_genes["exomiser_gene_combined_score"] == pytest.approx(0.9)
    assert {k: two_genes[k] for k in ("exomiser_rank", "exomiser_moi", "exomiser_acmg_classification")} == {
        "exomiser_rank": 1,
        "exomiser_moi": "AD",
        "exomiser_acmg_classification": "pathogenic",
    }
    assert two_genes["exomiser_acmg_evidence"] == '["PVS1","PM2"]'

    # Proband route: matched on `name`, although the record's ID is the mother's call. Empty evidence is NULL.
    kept_parent_call = exomiser[(_PROBAND_ROUTE_TASK, _KEPT_PARENT_CALL)]
    assert kept_parent_call["exomiser_gene_combined_score"] == pytest.approx(0.7)
    assert (
        kept_parent_call["exomiser_rank"],
        kept_parent_call["exomiser_acmg_classification"],
        kept_parent_call["exomiser_acmg_evidence"],
    ) == (2, "likely_pathogenic", None)

    # Family route: matched on the record's ID (`vcf_id`), as the report names it.
    family = exomiser[(_FAMILY_ROUTE_TASK, _FAMILY_NAME)]
    assert family["exomiser_gene_combined_score"] == pytest.approx(0.6)
    assert (family["exomiser_rank"], family["exomiser_acmg_classification"], family["exomiser_acmg_evidence"]) == (
        1,
        "likely_benign",
        '["BP1"]',
    )

    # A record Exomiser did not rank, a non-contributing row, and the DRAGEN set of the same call: empty.
    assert exomiser[(_PROBAND_ROUTE_TASK, _UNRANKED)] == _EMPTY
    assert exomiser[(_PROBAND_ROUTE_TASK, _NOT_CONTRIBUTING)] == _EMPTY
    assert exomiser[(_ALIGNMENT_TASK, _KEPT_PARENT_CALL)] == _EMPTY


def test_load_keeps_the_contributing_rows_with_their_record_id(loaded_reports, starrocks_session, radiant_mapping):
    with starrocks_session.cursor() as cursor:
        cursor.execute(
            f"SELECT task_id, seq_id, vcf_id, rank, acmg_classification "
            f"FROM {radiant_mapping['starrocks_staging_exomiser_cnv']} WHERE part = %(part)s ORDER BY task_id, rank",
            {"part": _PART},
        )
        assert cursor.fetchall() == (
            (_PROBAND_ROUTE_TASK, _SEQ_ID, _TWO_GENES, 1, "pathogenic"),
            (_PROBAND_ROUTE_TASK, _SEQ_ID, _KEPT_PARENT_CALL, 2, "likely_pathogenic"),
            (_PROBAND_ROUTE_TASK, _SEQ_ID, _TWO_GENES, 3, "uncertain_significance"),
            (_FAMILY_ROUTE_TASK, _OTHER_SEQ_ID, _FAMILY_VCF_ID, 1, "likely_benign"),
        )


def test_ingest_copies_the_best_exomiser_row_of_each_record(loaded_reports, starrocks_session, radiant_mapping):
    """AC1 (values, best row), AC3 (empty values), and both Exomiser routes."""
    with starrocks_session.cursor() as cursor:
        _insert(cursor, radiant_mapping)
        _assert_exomiser(_exomiser_by_task_and_name(cursor, radiant_mapping))


def test_reannotation_keeps_the_exomiser_values(loaded_reports, starrocks_session, radiant_mapping):
    """The re-annotation reads the StarRocks table, which has no `vcf_id`: the values are carried through."""
    sql = _render("germline_cnv_occurrence_reannotate_partition.sql", radiant_mapping).rstrip().rstrip(";")
    with starrocks_session.cursor() as cursor:
        _insert(cursor, radiant_mapping)
        cursor.execute(sql, {"part": _PART})
        _assert_exomiser(_exomiser_by_task_and_name(cursor, radiant_mapping))


def test_copy_step_keeps_the_reports_of_the_tasks_not_in_the_run(loaded_reports, starrocks_session, radiant_mapping):
    """A later run of the family-route task alone: its copy step keeps the proband-route task's rows."""
    raw = radiant_mapping["starrocks_staging_exomiser_cnv"]
    sql = _render("staging_exomiser_cnv_insert_partition_delta.sql", radiant_mapping, table=raw)
    with starrocks_session.cursor() as cursor:
        cursor.execute(sql.rstrip().rstrip(";"), {"task_ids": [_FAMILY_ROUTE_TASK], "deleted_task_ids": [-1]})
        assert {row[2] for row in cursor.fetchall()} == {_PROBAND_ROUTE_TASK}
