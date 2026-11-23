"""The Exomiser load of `import_part` (`load_exomiser_files`): the real `RadiantLoadExomiserOperator`, on StarRocks and
the S3 store, with a thin hook on the test session.

The reports of a batch load in one broker load job, one `data_desc` per report, each tagged in its SET with its task,
the proband's seq_id and the tenant. The rows are the ones the per-task broker load wrote (same values, column by
column, generated `locus` / `locus_hash` included).
"""

import os

import jinja2
import pytest

from radiant.dags import DAGS_DIR
from radiant.tasks.starrocks.operator import RadiantLoadExomiserOperator, SwapPartition
from tests.integration.dags.sql.test_gene_coverage import _SessionHook

_SQL_DIR = os.path.join(DAGS_DIR, "sql")
_PART = 3780
_TENANT = "test"

# The rows of `sample.variants.tsv` (all contributing), as the broker load wrote them: rank, id, symbol,
# entrez_gene_id, moi, variant_score, gene_combined_score, contributing_variant, chromosome, start, end, reference,
# alternate, acmg_classification, acmg_evidence, locus, locus_hash.
_SAMPLE_ROWS = [
    (
        1,
        "101",
        "BRCA1",
        "672",
        "AD",
        0.87,
        0.89,
        1,
        "17",
        43044295,
        43125482,
        "A",
        "G",
        "pathogenic",
        '["PM2","PP3","PS4"]',
        "17-43044295-A-G",
        "27ec059fb43f3839aa62081ccedb699c44c404671419f3b7f029c85859e66e4f",
    ),
    (
        2,
        "102",
        "TP53",
        "7157",
        "AR",
        0.72,
        0.75,
        1,
        "17",
        7571720,
        7590868,
        "G",
        "A",
        "likely_pathogenic",
        '["PS1","PM1"]',
        "17-7571720-G-A",
        "c93adac27aecf4945756301ceda2d91a5c94cf3aaeb3ebde7918de2078238bc2",
    ),
    (
        3,
        "103",
        "CFTR",
        "1080",
        "AD",
        0.62,
        0.65,
        1,
        "7",
        117120016,
        117308718,
        "T",
        "C",
        "uncertain_significance",
        '["PP5"]',
        "7-117120016-T-C",
        "c71ffdbd33d4725647c7154c63e97f25e6938999f5c4f04343a86de0e285d4a3",
    ),
    # No ACMG evidence: NULL, not an empty array.
    (
        4,
        "103",
        "CFTR",
        "1080",
        "AD",
        0.62,
        0.65,
        1,
        "7",
        117120016,
        117308718,
        "T",
        "C",
        "uncertain_significance",
        None,
        "7-117120016-T-C",
        "c71ffdbd33d4725647c7154c63e97f25e6938999f5c4f04343a86de0e285d4a3",
    ),
]
_COLUMNS = (
    "seq_id, task_id, tenant_code, rank, id, symbol, entrez_gene_id, moi, variant_score, gene_combined_score, "
    "contributing_variant, chromosome, start, end, reference, alternate, acmg_classification, acmg_evidence, locus, "
    "locus_hash"
)


def _experiment(seq_id):
    return {
        "seq_id": seq_id,
        "patient_id": seq_id,
        "aliquot": f"SA{seq_id}",
        "tenant_code": _TENANT,
        "family_role": "proband",
        "affected_status": "affected",
        "sex": "female",
        "experimental_strategy": "wgs",
    }


def _task(task_id, seq_id, exomiser_filepath, *, deleted=False):
    return {
        "task_id": task_id,
        "task_type": "radiant_germline_annotation",
        "part": _PART,
        "analysis_type": "germline",
        "experiments": [_experiment(seq_id)],
        "deleted": deleted,
        "vcf_filepath": f"s3://test/{task_id}.vcf.gz",
        "exomiser_filepath": exomiser_filepath,
    }


def broker_configuration(minio_instance):
    return f"""
        'aws.s3.region' = 'us-east-1',
        'aws.s3.endpoint' = 'http://radiant-minio:9000',
        'aws.s3.enable_path_style_access' = 'true',
        'aws.s3.access_key' = '{minio_instance.access_key}',
        'aws.s3.secret_key' = '{minio_instance.secret_key}'
    """


def run_exomiser_load(session, mapping, minio_instance, *, table_key, sql, copy_sql, part, tasks, **kwargs):
    """`load_exomiser_files` / `load_exomiser_cnv_files` of one part: copy step, batched inserts, swap."""
    table = mapping[table_key]
    database_name, table_name = table.split(".")

    def render(path, **context):
        with open(os.path.join(_SQL_DIR, "radiant", path)) as f_in:
            return jinja2.Template(f_in.read()).render(context)

    task_ids = [t["task_id"] for t in tasks]
    deleted_task_ids = [t["task_id"] for t in tasks if t["deleted"]] or [-1]
    op = RadiantLoadExomiserOperator(
        task_id="load_exomiser_files",
        table=table,
        swap_partition=SwapPartition(
            partition=str(part), copy_partition_sql=render(copy_sql, table=table, partition=part).rstrip().rstrip(";")
        ),
        parameters={"task_ids": task_ids, "deleted_task_ids": deleted_task_ids},
        tasks=tasks,
        **kwargs,
    )
    op.sql = render(
        sql,
        database_name=database_name,
        table_name=table_name,
        broker_load_timeout=600,
        load_label="{{load_label}}",
        broker_configuration=broker_configuration(minio_instance),
    )
    op.get_db_hook = lambda: _SessionHook(session)
    op.execute(context={})


@pytest.fixture
def raw_exomiser(starrocks_session, radiant_mapping):
    table = radiant_mapping["starrocks_staging_exomiser"]
    with open(os.path.join(_SQL_DIR, "radiant/init/staging_exomiser_create_table.sql")) as f_in:
        create_table_sql = jinja2.Template(f_in.read()).render({"mapping": radiant_mapping})
    with starrocks_session.cursor() as cursor:
        cursor.execute(create_table_sql)
        cursor.execute(f"DELETE FROM {table} WHERE part = %(part)s", {"part": _PART})
    yield table
    with starrocks_session.cursor() as cursor:
        cursor.execute(f"DELETE FROM {table} WHERE part = %(part)s", {"part": _PART})


def _load(starrocks_session, radiant_mapping, minio_instance, tasks, **kwargs):
    run_exomiser_load(
        starrocks_session,
        radiant_mapping,
        minio_instance,
        table_key="starrocks_staging_exomiser",
        sql="staging_exomiser_load.sql",
        copy_sql="staging_exomiser_insert_partition_delta.sql",
        part=_PART,
        tasks=tasks,
        **kwargs,
    )


def _rows(starrocks_session, table):
    with starrocks_session.cursor() as cursor:
        cursor.execute(f"SELECT {_COLUMNS} FROM {table} WHERE part = %(part)s ORDER BY task_id, rank", {"part": _PART})
        return cursor.fetchall()


def _expected(seq_id, task_id):
    return [(seq_id, task_id, _TENANT, *row) for row in _SAMPLE_ROWS]


def test_exomiser_load_of_several_reports_into_a_new_partition(
    starrocks_session, radiant_mapping, minio_instance, sample_exomiser_tsv, raw_exomiser
):
    """Three reports, two in one batch: each one's rows tagged with its own task and proband."""
    path = f"s3://{sample_exomiser_tsv}"
    tasks = [_task(37801, 3780101, path), _task(37802, 3780201, path), _task(37803, 3780301, path)]

    _load(starrocks_session, radiant_mapping, minio_instance, tasks, files_per_load=2)

    assert _rows(starrocks_session, raw_exomiser) == (
        *_expected(3780101, 37801),
        *_expected(3780201, 37802),
        *_expected(3780301, 37803),
    )


def test_exomiser_load_into_an_existing_partition_replaces_the_tasks_of_the_run(
    starrocks_session, radiant_mapping, minio_instance, sample_exomiser_tsv, raw_exomiser
):
    """The second run reloads one task, adds one and deletes one, in one batch: the task not in the run stays."""
    path = f"s3://{sample_exomiser_tsv}"
    _load(
        starrocks_session,
        radiant_mapping,
        minio_instance,
        [_task(37801, 3780101, path), _task(37802, 3780201, path), _task(37803, 3780301, path)],
    )

    _load(
        starrocks_session,
        radiant_mapping,
        minio_instance,
        [
            _task(37802, 3780201, path),
            _task(37803, 3780301, path, deleted=True),
            _task(37804, 3780401, path),
        ],
    )

    assert _rows(starrocks_session, raw_exomiser) == (
        *_expected(3780101, 37801),
        *_expected(3780201, 37802),
        *_expected(3780401, 37804),
    )


def test_exomiser_load_without_reports_still_swaps_the_partition(
    starrocks_session, radiant_mapping, minio_instance, sample_exomiser_tsv, raw_exomiser
):
    """A task re-imported without its report loses its rows; the other tasks keep theirs."""
    path = f"s3://{sample_exomiser_tsv}"
    tasks = [_task(37801, 3780101, path), _task(37802, 3780201, path)]
    _load(starrocks_session, radiant_mapping, minio_instance, tasks)

    _load(starrocks_session, radiant_mapping, minio_instance, [_task(37802, 3780201, None)])

    assert _rows(starrocks_session, raw_exomiser) == tuple(_expected(3780101, 37801))
