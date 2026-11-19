"""RAD-315: the gene coverage of the current alignments, end to end on StarRocks, MinIO and the clinical seeds.

The import is driven the way import_part drives it: the discovery query (gene_coverage_file_select.sql), then the real
RadiantLoadGeneCoverageOperator (header read by StarRocks FILES(), one broker load per file, partition swap), the real
RadiantStarRocksPartitionSwapOperator with the per-tenant insert. A thin hook runs their SQL on the test session.

Seeds: the proband of case 1 (experiment 1, alignment task 1) has an old-format file, the tumor-only alignment of
case 23 (experiment 64, task 76) a new-format one. The other shapes are test-local, on ids 99xx, removed after each
test: the normal of the tumor-normal case 22 (experiment 63) with an alignment, a re-alignment of experiment 1, a
broken file on experiment 3 or on the alignment of experiment 1, a `covgene` on the tumor-normal calling task 73.
"""

import os

import jinja2
import pytest

from radiant.dags import DAGS_DIR
from radiant.tasks.starrocks.operator import (
    RadiantLoadGeneCoverageOperator,
    RadiantStarRocksPartitionSwapOperator,
    SwapPartition,
)
from tests.integration.conftest import RESOURCES_DIR
from tests.integration.fixtures_common import execute_clinical

pytestmark = pytest.mark.usefixtures("pipeline_views")

_SQL_DIR = os.path.join(DAGS_DIR, "sql")
_PART = 315
_TENANT = "radiant"

OLD_FORMAT_URL = "s3://test-vcf/gene_coverage/old_format.csv"
NEW_FORMAT_URL = "s3://test-vcf/gene_coverage/new_format.csv"
NORMAL_URL = "s3://test-vcf/gene_coverage/normal.coverage_by_gene.csv"
REALIGNED_URL = "s3://test-vcf/gene_coverage/realigned.coverage_by_gene.csv"
BROKEN_URL = "s3://test-vcf/gene_coverage/broken_header.coverage_by_gene.csv"
TUMOR_CALLING_URL = "s3://test-vcf/gene_coverage/tumor_normal_calling.coverage_by_gene.csv"

_FILES = {
    OLD_FORMAT_URL: "old_format.coverage_by_gene.csv",
    NEW_FORMAT_URL: "new_format.coverage_by_gene.csv",
    NORMAL_URL: "old_format.coverage_by_gene.csv",
    REALIGNED_URL: "realigned.coverage_by_gene.csv",
    BROKEN_URL: "broken_header.coverage_by_gene.csv",
    TUMOR_CALLING_URL: "new_format.coverage_by_gene.csv",
}

# gene_id, chromosome, name. Mock rows, removed after the module.
_ENSEMBL_GENES = [
    ("ENSG00000012048", "17", "BRCA1"),
    ("ENSG00000141510", "17", "TP53"),
    ("ENSG00000185960", "X", "SHOX"),
    ("ENSG00000100101", "22", "MOCKSYM1"),
    # A name on two chromosomes: the first by chromosome is kept ('11' < '2').
    ("ENSGMOCK0000001", "2", "MOCKDUP"),
    ("ENSGMOCK0000002", "11", "MOCKDUP"),
    # Ensembl says MT; the table says M, as the variant tables.
    ("ENSGMOCKMT00001", "MT", "MT-CO1"),
]

_COLUMNS = ("seq_id", "task_id", "symbol", "chromosome", "gene", "ensembl_gene_id", "size", "average_coverage")


class _SessionHook:
    """The subset of the DB-API hook the operators call, on the test's pymysql session."""

    def __init__(self, session):
        self.session = session

    def get_conn(self):
        return self.session

    def run(self, sql, autocommit=True, parameters=None):
        with self.session.cursor() as cursor:
            cursor.execute(sql, parameters)

    def get_first(self, sql):
        with self.session.cursor() as cursor:
            cursor.execute(sql)
            return cursor.fetchone()


def _render(path, **context):
    with open(os.path.join(_SQL_DIR, path)) as f_in:
        return jinja2.Template(f_in.read()).render(context)


@pytest.fixture(scope="module")
def gene_coverage_tables(starrocks_session, radiant_mapping, s3_fs):
    for url, name in _FILES.items():
        s3_fs.put_file(str(RESOURCES_DIR / "gene_coverage" / name), url.removeprefix("s3://"))

    ensembl_gene = radiant_mapping["starrocks_ensembl_gene"]
    gene_ids = ", ".join(f"'{gene_id}'" for gene_id, _, _ in _ENSEMBL_GENES)
    with starrocks_session.cursor() as cursor:
        cursor.execute(_render("radiant/init/staging_gene_coverage_create_table.sql", mapping=radiant_mapping))
        cursor.execute(_render("radiant/init/gene_coverage_create_table.sql", mapping=radiant_mapping))
        cursor.execute(_render("open_data/init/ensembl_gene_create_table.sql", mapping=radiant_mapping))
        cursor.execute(f"DELETE FROM {ensembl_gene} WHERE gene_id IN ({gene_ids})")
        cursor.execute(
            f"INSERT INTO {ensembl_gene} (gene_id, chromosome, name) VALUES "
            + ", ".join(f"('{g}', '{c}', '{n}')" for g, c, n in _ENSEMBL_GENES)
        )
    yield
    with starrocks_session.cursor() as cursor:
        cursor.execute(f"DELETE FROM {ensembl_gene} WHERE gene_id IN ({gene_ids})")


@pytest.fixture
def empty_part(starrocks_session, radiant_mapping, gene_coverage_tables):
    with starrocks_session.cursor() as cursor:
        for key in ("starrocks_staging_gene_coverage", "starrocks_gene_coverage"):
            cursor.execute(f"DELETE FROM {radiant_mapping[key]} WHERE part = %(part)s", {"part": _PART})


def _discover(starrocks_session, radiant_mapping, seq_ids, deleted_seq_ids=(-1,)):
    with starrocks_session.cursor() as cursor:
        cursor.execute(
            _render("radiant/gene_coverage_file_select.sql", mapping=radiant_mapping),
            {"seq_ids": list(seq_ids), "deleted_seq_ids": list(deleted_seq_ids)},
        )
        columns = [d[0] for d in cursor.description]
        return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]


def _import(starrocks_session, radiant_mapping, minio_instance, seq_ids, deleted_seq_ids=(-1,)):
    """Discovery, shared load and per-tenant insert of one part. Returns the load's errors."""
    params = {"seq_ids": list(seq_ids), "deleted_seq_ids": list(deleted_seq_ids)}
    files = _discover(starrocks_session, radiant_mapping, seq_ids, deleted_seq_ids)
    hook = _SessionHook(starrocks_session)

    raw = radiant_mapping["starrocks_staging_gene_coverage"]
    database_name, table_name = raw.split(".")
    broker_configuration = f"""
        'aws.s3.region' = 'us-east-1',
        'aws.s3.endpoint' = 'http://radiant-minio:9000',
        'aws.s3.enable_path_style_access' = 'true',
        'aws.s3.access_key' = '{minio_instance.access_key}',
        'aws.s3.secret_key' = '{minio_instance.secret_key}'
    """
    load = RadiantLoadGeneCoverageOperator(
        task_id="load_gene_coverage_files",
        table=raw,
        swap_partition=SwapPartition(
            partition=str(_PART),
            copy_partition_sql=_render("radiant/staging_gene_coverage_copy_partition.sql", table=raw, partition=_PART),
        ),
        parameters=params,
        files=files,
        sql=_render(
            "radiant/staging_gene_coverage_load.sql",
            database_name=database_name,
            table_name=table_name,
            broker_load_timeout=600,
            broker_configuration=broker_configuration,
            load_label="{{load_label}}",
            temporary_partition_clause="{{temporary_partition_clause}}",
            file_columns="{{file_columns}}",
            type_value="{{type_value}}",
        ),
    )
    load.get_db_hook = lambda: hook
    # The header query reads the file with the same credentials as the broker load.
    load.broker_configuration = broker_configuration
    errors = load.execute(context={})

    target = radiant_mapping["starrocks_gene_coverage"]
    insert = RadiantStarRocksPartitionSwapOperator(
        task_id="insert_gene_coverage",
        table=target,
        swap_partition=SwapPartition(
            partition=str(_PART),
            copy_partition_sql=_render("radiant/gene_coverage_copy_partition.sql", table=target, partition=_PART),
        ),
        insert_partition_sql=_render(
            "radiant/gene_coverage_insert_partition_delta.sql", mapping=radiant_mapping, partition=_PART
        ),
        parameters={**params, "tenant_code": _TENANT},
    )
    insert.get_db_hook = lambda: hook
    insert.execute(context={})
    return errors


def _rows(starrocks_session, radiant_mapping, seq_id=None):
    with starrocks_session.cursor() as cursor:
        cursor.execute(
            f"SELECT {', '.join(_COLUMNS)} FROM {radiant_mapping['starrocks_gene_coverage']} "
            f"WHERE part = %(part)s ORDER BY seq_id, task_id, symbol, chromosome",
            {"part": _PART},
        )
        rows = [dict(zip(_COLUMNS, r, strict=True)) for r in cursor.fetchall()]
    return [r for r in rows if seq_id is None or r["seq_id"] == seq_id]


def _by_gene(rows):
    return {r["gene"]: r for r in rows}


def _add_clinical(postgres_instance, tasks=(), contexts=(), documents=(), links=(), cases=()):
    sql = []
    if tasks:
        values = ", ".join(
            f"({i}, '{t}', 'Dragen', '4.4.4', 'GRch38', '{created}', 'radiant')" for i, t, created in tasks
        )
        sql.append(
            "INSERT INTO task (id, task_type_code, pipeline_name, pipeline_version, genome_build, created_on, "
            f"tenant_code) VALUES {values};"
        )
    if contexts:
        values = ", ".join(f"({t}, null, {s})" for t, s in contexts)
        sql.append(f"INSERT INTO task_context (task_id, case_id, sequencing_experiment_id) VALUES {values};")
    if documents:
        values = ", ".join(
            f"({i}, 'cov-{i}.csv', 'genomic', 'covgene', 'csv', 1, '{url}', 'hash{i}', 'radiant')"
            for i, url in documents
        )
        sql.append(
            "INSERT INTO document (id, name, data_category_code, data_type_code, format_code, size, url, hash, "
            f"tenant_code) VALUES {values};"
        )
    if links:
        values = ", ".join(f"({t}, {d}, 'output')" for t, d in links)
        sql.append(f"INSERT INTO task_has_document (task_id, document_id, type) VALUES {values};")
    if cases:
        values = ", ".join(f"({c}, {s})" for c, s in cases)
        sql.append(f"INSERT INTO case_has_sequencing_experiment (case_id, sequencing_experiment_id) VALUES {values};")
    execute_clinical(postgres_instance, "\n".join(sql))


def _remove_clinical(postgres_instance, task_ids=(), document_ids=(), cases=()):
    sql = []
    if document_ids:
        ids = ", ".join(str(i) for i in document_ids)
        sql += [
            f"DELETE FROM task_has_document WHERE document_id IN ({ids});",
            f"DELETE FROM document WHERE id IN ({ids});",
        ]
    if task_ids:
        ids = ", ".join(str(i) for i in task_ids)
        sql += [f"DELETE FROM task_context WHERE task_id IN ({ids});", f"DELETE FROM task WHERE id IN ({ids});"]
    for c, s in cases:
        sql.append(
            f"DELETE FROM case_has_sequencing_experiment WHERE case_id = {c} AND sequencing_experiment_id = {s};"
        )
    execute_clinical(postgres_instance, "\n".join(sql))


@pytest.fixture
def tumor_normal_coverage(postgres_clinical_seeds, postgres_instance):
    """Case 22: the normal (experiment 63) gets an alignment with a `covgene`; the tumor-normal calling task 73
    (experiments 62 and 63) gets one too, which is not an alignment and must never load."""
    _add_clinical(
        postgres_instance,
        tasks=[(9901, "alignment_germline_variant_calling", "2021-10-12 13:08:00")],
        contexts=[(9901, 63)],
        documents=[(9901, NORMAL_URL), (9902, TUMOR_CALLING_URL)],
        links=[(9901, 9901), (73, 9902)],
    )
    yield
    _remove_clinical(postgres_instance, task_ids=[9901], document_ids=[9901, 9902])


@pytest.fixture
def realignment(postgres_clinical_seeds, postgres_instance):
    """Adds, when called, a newer alignment of experiment 1 (case 1's proband), with its own `covgene`."""

    def add():
        _add_clinical(
            postgres_instance,
            tasks=[(9911, "alignment_germline_variant_calling", "2022-01-01 00:00:00")],
            contexts=[(9911, 1)],
            documents=[(9911, REALIGNED_URL)],
            links=[(9911, 9911)],
        )

    yield add
    _remove_clinical(postgres_instance, task_ids=[9911], document_ids=[9911])


@pytest.fixture
def broken_file(postgres_clinical_seeds, postgres_instance):
    """Experiment 3 (case 1's father): a `covgene` whose header is no DRAGEN header."""
    _add_clinical(postgres_instance, documents=[(9921, BROKEN_URL)], links=[(3, 9921)])
    yield
    _remove_clinical(postgres_instance, document_ids=[9921])


@pytest.fixture
def broken_new_file(postgres_clinical_seeds, postgres_instance):
    """Adds, when called, a newer `covgene` to the alignment of experiment 1 (task 1) whose header is broken."""

    def add():
        _add_clinical(postgres_instance, documents=[(9931, BROKEN_URL)], links=[(1, 9931)])

    yield add
    _remove_clinical(postgres_instance, document_ids=[9931])


@pytest.fixture
def shared_experiment(postgres_clinical_seeds, postgres_instance):
    """Experiment 1 in a second case (case 16), so its alignment is shared by two cases."""
    _add_clinical(postgres_instance, cases=[(16, 1)])
    yield
    _remove_clinical(postgres_instance, cases=[(16, 1)])


def test_old_format_germline_alignment_loads_one_row_per_gene_with_the_gene_name_rule(
    starrocks_session, radiant_mapping, minio_instance, empty_part
):
    """AC1, AC4, AC5, AC6. The proband of case 1: the staging view hides its alignment task behind the slivar VCF,
    the coverage still loads."""
    errors = _import(starrocks_session, radiant_mapping, minio_instance, seq_ids=[1])

    assert errors == []
    rows = _by_gene(_rows(starrocks_session, radiant_mapping))
    assert set(rows) == {
        "BRCA1",
        "SHOX_chrX",
        "SHOX_chrY",
        "ENSG00000100101",
        "ENSG00000999999",
        "MOCKDUP",
        "NOTINENSEMBL",
        "MT-CO1",
        "ENSG00000141510.7",
    }
    assert {r["task_id"] for r in rows.values()} == {1}

    def key(gene):
        r = rows[gene]
        return r["symbol"], r["chromosome"], r["ensembl_gene_id"]

    assert key("BRCA1") == ("BRCA1", "17", "ENSG00000012048")
    # PAR genes: two rows SHOX, X and Y from the suffix.
    assert key("SHOX_chrX") == ("SHOX", "X", "ENSG00000185960")
    assert key("SHOX_chrY") == ("SHOX", "Y", None)
    # Ensembl ids: the symbol of the id, else the id itself.
    assert key("ENSG00000100101") == ("MOCKSYM1", "22", "ENSG00000100101")
    assert key("ENSG00000999999") == ("ENSG00000999999", None, "ENSG00000999999")
    # A versioned id resolves on the id without its version.
    assert key("ENSG00000141510.7") == ("TP53", "17", "ENSG00000141510")
    assert key("MT-CO1") == ("MT-CO1", "M", "ENSGMOCKMT00001")
    # A name on two chromosomes keeps one row, the first by chromosome.
    assert key("MOCKDUP") == ("MOCKDUP", "11", "ENSGMOCK0000002")
    assert key("NOTINENSEMBL") == ("NOTINENSEMBL", None, None)

    brca1 = rows["BRCA1"]
    assert (brca1["seq_id"], brca1["size"], brca1["average_coverage"]) == (1, 5589, 136.5)
    with starrocks_session.cursor() as cursor:
        cursor.execute(
            f"SELECT coverage_5x, coverage_30x, coverage_1000x FROM {radiant_mapping['starrocks_gene_coverage']} "
            "WHERE part = %(part)s AND gene = 'BRCA1'",
            {"part": _PART},
        )
        assert cursor.fetchall() == ((1.0, pytest.approx(0.99), 0.0),)


def test_new_format_tumor_only_alignment_loads_its_cds_rows_only(
    starrocks_session, radiant_mapping, minio_instance, empty_part
):
    """AC2, AC4: the UTR, clinvar and lncRNA rows of the new format are not loaded."""
    errors = _import(starrocks_session, radiant_mapping, minio_instance, seq_ids=[64])

    assert errors == []
    rows = _rows(starrocks_session, radiant_mapping)
    assert [(r["seq_id"], r["task_id"], r["symbol"], r["chromosome"], r["gene"]) for r in rows] == [
        (64, 76, "BRCA1", "17", "BRCA1"),
        (64, 76, "MOCKSYM1", "22", "ENSG00000100101"),
        (64, 76, "SHOX", "X", "SHOX_chrX"),
        (64, 76, "SHOX", "Y", "SHOX_chrY"),
        (64, 76, "TP53", "17", "TP53"),
    ]
    assert _by_gene(rows)["BRCA1"]["average_coverage"] == 140.25
    # The load keeps the CDS rows only: the staging table never holds the others.
    with starrocks_session.cursor() as cursor:
        cursor.execute(
            f"SELECT DISTINCT type FROM {radiant_mapping['starrocks_staging_gene_coverage']} WHERE part = %(part)s",
            {"part": _PART},
        )
        assert cursor.fetchall() == (("CDS",),)


def test_tumor_normal_loads_the_normal_only(
    starrocks_session, radiant_mapping, minio_instance, empty_part, tumor_normal_coverage
):
    """AC3: the tumor (62) has no alignment of the two types; the calling task's `covgene` is never read."""
    files = _discover(starrocks_session, radiant_mapping, seq_ids=[62, 63])
    assert [(f["seq_id"], f["task_id"], f["filepath"]) for f in files] == [(63, 9901, NORMAL_URL)]

    errors = _import(starrocks_session, radiant_mapping, minio_instance, seq_ids=[62, 63])

    assert errors == []
    assert {(r["seq_id"], r["task_id"]) for r in _rows(starrocks_session, radiant_mapping)} == {(63, 9901)}


def test_an_alignment_shared_by_two_cases_or_loaded_twice_is_not_duplicated(
    starrocks_session, radiant_mapping, minio_instance, empty_part, shared_experiment
):
    """AC7."""
    assert len(_discover(starrocks_session, radiant_mapping, seq_ids=[1])) == 1

    _import(starrocks_session, radiant_mapping, minio_instance, seq_ids=[1, 64])
    first = _rows(starrocks_session, radiant_mapping)
    _import(starrocks_session, radiant_mapping, minio_instance, seq_ids=[1, 64])
    second = _rows(starrocks_session, radiant_mapping)

    assert len(first) == 14
    assert second == first


def test_an_import_keeps_the_experiments_it_does_not_reload(
    starrocks_session, radiant_mapping, minio_instance, empty_part
):
    _import(starrocks_session, radiant_mapping, minio_instance, seq_ids=[1, 64])
    _import(starrocks_session, radiant_mapping, minio_instance, seq_ids=[64])

    assert {r["seq_id"] for r in _rows(starrocks_session, radiant_mapping)} == {1, 64}
    assert len(_rows(starrocks_session, radiant_mapping, seq_id=1)) == 9


def test_a_realignment_replaces_the_coverage_of_the_old_alignment(
    starrocks_session, radiant_mapping, minio_instance, empty_part, realignment
):
    _import(starrocks_session, radiant_mapping, minio_instance, seq_ids=[1])
    assert {r["task_id"] for r in _rows(starrocks_session, radiant_mapping)} == {1}

    realignment()
    _import(starrocks_session, radiant_mapping, minio_instance, seq_ids=[1])

    rows = _rows(starrocks_session, radiant_mapping)
    assert [(r["seq_id"], r["task_id"], r["symbol"], r["average_coverage"]) for r in rows] == [(1, 9911, "TP53", 60.0)]


def test_an_alignment_without_covgene_gives_no_row_and_no_error(
    starrocks_session, radiant_mapping, minio_instance, empty_part
):
    """AC8. Experiment 2 (case 1's mother) has an alignment and no `covgene`."""
    assert _discover(starrocks_session, radiant_mapping, seq_ids=[2]) == []

    errors = _import(starrocks_session, radiant_mapping, minio_instance, seq_ids=[2])

    assert errors == []
    assert _rows(starrocks_session, radiant_mapping) == []


def test_a_broken_file_is_reported_and_the_good_files_still_load(
    starrocks_session, radiant_mapping, minio_instance, empty_part, broken_file
):
    """AC8. The good file of the part loads; the bad one is reported with its S3 path, and its alignment, never
    loaded, has no rows."""
    errors = _import(starrocks_session, radiant_mapping, minio_instance, seq_ids=[1, 3])

    assert [(e["filepath"], e["task_id"]) for e in errors] == [(BROKEN_URL, 3)]
    assert "Unexpected coverage_by_gene header" in errors[0]["error"]
    assert {r["seq_id"] for r in _rows(starrocks_session, radiant_mapping)} == {1}


def test_a_bad_file_keeps_the_coverage_an_earlier_import_loaded(
    starrocks_session, radiant_mapping, minio_instance, empty_part, broken_new_file
):
    """The copy steps drop the experiment; the load copies its loaded rows back, so the swap does not delete them."""
    _import(starrocks_session, radiant_mapping, minio_instance, seq_ids=[1, 64])
    before = _rows(starrocks_session, radiant_mapping)

    broken_new_file()
    errors = _import(starrocks_session, radiant_mapping, minio_instance, seq_ids=[1, 64])

    assert [(e["filepath"], e["task_id"]) for e in errors] == [(BROKEN_URL, 1)]
    assert _rows(starrocks_session, radiant_mapping) == before
