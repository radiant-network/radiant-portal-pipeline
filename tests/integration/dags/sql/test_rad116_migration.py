"""RAD-116 -- the manual StarRocks migration, `migrations/RAD-116_exomiser_task_id.sql`, run on fixture rows.

Each numbered section of the file runs in the database it names, with the placeholders substituted, in two fresh
databases: the base one (`staging_sequencing_experiment`, `raw_exomiser`) and one tenant database (`exomiser`), both
in their pre-RAD-116 shape.
"""

import os
import re
import time

import pymysql
import pytest

from radiant.dags import DAGS_DIR

_MIGRATION = os.path.join(DAGS_DIR, "sql", "radiant", "migrations", "RAD-116_exomiser_task_id.sql")
_TENANT = "test"

_LEGACY_RAW_EXOMISER = """
CREATE TABLE raw_exomiser
(
    part                 INT,
    seq_id               INT,
    id                   VARCHAR(2000),
    tenant_code          VARCHAR(50),
    rank                 INT,
    symbol               VARCHAR(200),
    entrez_gene_id       VARCHAR(200),
    moi                  VARCHAR(10),
    variant_score        FLOAT,
    gene_combined_score  FLOAT,
    contributing_variant BOOLEAN,
    chromosome           VARCHAR(10),
    start                INT,
    end                  INT,
    reference            VARCHAR(2000),
    alternate            VARCHAR(2000),
    acmg_classification  VARCHAR(300),
    acmg_evidence        ARRAY<VARCHAR (10)>,
    locus                VARCHAR(2000) AS (concat_ws('-', chromosome, start, reference, alternate)),
    locus_hash           VARCHAR(256) AS (sha2(concat_ws('-', chromosome, start, reference, alternate), 256))
)
ENGINE = OLAP DUPLICATE KEY(`part`, `seq_id`, `id`)
PARTITION BY (`part`)
DISTRIBUTED BY HASH(`locus_hash`) BUCKETS 10
"""

_LEGACY_EXOMISER = """
CREATE TABLE exomiser
(
    part                 INT,
    seq_id               INT,
    locus_id             BIGINT,
    id                   VARCHAR(2000),
    locus_hash           VARCHAR(256),
    moi                  VARCHAR(10),
    variant_score        FLOAT,
    gene_combined_score  FLOAT,
    variant_rank         TINYINT,
    rank                 INT,
    symbol               VARCHAR(200),
    acmg_classification  VARCHAR(300),
    acmg_evidence array< VARCHAR (10)>
)
ENGINE = OLAP
DUPLICATE KEY(`part`, `seq_id`, `locus_id`,  `id`)
PARTITION BY (`part`)
DISTRIBUTED BY HASH(`locus_id`)
BUCKETS 10
PROPERTIES (
    "colocate_with" = "{colocate_group}"
)
"""

_STAGING_COLUMNS = (
    "case_id, seq_id, task_id, task_type, part, analysis_type, aliquot, patient_id, experimental_strategy, "
    "tenant_code, vcf_filepath, exomiser_filepath, family_id, family_role, created_at, updated_at, ingested_at, "
    "deleted"
)
_INGESTED_AT = "2026-01-02 00:00:00"


def _staging(case_id, seq_id, task_id, task_type, family_role, exomiser_filepath=None):
    vcf_filepath = f"s3://test/{task_id}.vcf.gz" if task_type == "radiant_germline_annotation" else None
    return (
        case_id,
        seq_id,
        task_id,
        task_type,
        1,
        "germline",
        f"SA{seq_id}",
        str(seq_id),
        "wgs",
        _TENANT,
        vcf_filepath,
        exomiser_filepath,
        case_id,
        family_role,
        "2026-01-01 00:00:00",
        "2026-01-01 00:00:00",
        _INGESTED_AT,
        False,
    )


def _sections(substitutions):
    """{section number: [statements]}: the SQL of each numbered section (`0a`, `1`, `3.B2`...), comments left out."""
    with open(_MIGRATION) as f:
        text = f.read()
    for placeholder, value in substitutions.items():
        text = text.replace(placeholder, value)

    sections, current, previous = {}, None, ""
    for line in text.splitlines():
        header = re.match(r"^-- (\d+[a-z]?|\d+\.B\d+)\. ", line)
        if header and previous.startswith("-- ---"):
            current = header.group(1)
            sections[current] = []
        elif current and not line.lstrip().startswith("--"):
            sections[current].append(line)
        previous = line
    return {
        number: [statement.strip() for statement in "\n".join(lines).split(";") if statement.strip()]
        for number, lines in sections.items()
    }


class _Migration:
    def __init__(self, sections, names, connections):
        self.sections = sections
        self.names = names
        self.databases = connections

    def run(self, number, database):
        """Run every statement of a section; return the rows of the last one."""
        rows = None
        with self.databases[database].cursor() as cursor:
            for statement in self.sections[number]:
                cursor.execute(statement)
                rows = cursor.fetchall()
        return rows


@pytest.fixture
def migration(starrocks_database, radiant_mapping):
    names = {"shared": f"{starrocks_database.database}_rad116", "tenant": f"{starrocks_database.database}_rad116_t"}
    connections = {}
    with pymysql.connect(
        host=starrocks_database.host,
        port=int(starrocks_database.query_port),
        user=starrocks_database.user,
        password=starrocks_database.password,
        autocommit=True,
    ) as admin:
        with admin.cursor() as cursor:
            for name in names.values():
                cursor.execute(f"CREATE DATABASE {name}")
        try:
            for key, name in names.items():
                connections[key] = pymysql.connect(
                    host=starrocks_database.host,
                    port=int(starrocks_database.query_port),
                    user=starrocks_database.user,
                    password=starrocks_database.password,
                    database=name,
                    autocommit=True,
                )
            with open(
                os.path.join(DAGS_DIR, "sql", "radiant", "init", "staging_sequencing_experiment_create_table.sql")
            ) as f:
                staging_ddl = f.read().replace(
                    "{{ mapping.starrocks_staging_sequencing_experiment }}", "staging_sequencing_experiment"
                )
            with connections["shared"].cursor() as cursor:
                cursor.execute(staging_ddl)
                cursor.execute(_LEGACY_RAW_EXOMISER)
            with connections["tenant"].cursor() as cursor:
                cursor.execute(_LEGACY_EXOMISER.format(colocate_group=radiant_mapping["colocate_query_group"]))

            sections = _sections(
                {
                    "<shared_db>": names["shared"],
                    "<tenant>": _TENANT,
                    "<NAMESPACE>.query_group": radiant_mapping["colocate_query_group"],
                }
            )
            yield _Migration(sections, names, connections)
        finally:
            for connection in connections.values():
                connection.close()
            with admin.cursor() as cursor:
                for name in names.values():
                    cursor.execute(f"DROP DATABASE IF EXISTS {name} FORCE")


def _insert(migration, staging_rows, exomiser_seq_ids):
    with migration.databases["shared"].cursor() as cursor:
        cursor.executemany(
            f"INSERT INTO staging_sequencing_experiment ({_STAGING_COLUMNS}) "
            f"VALUES ({', '.join(['%s'] * len(_STAGING_COLUMNS.split(',')))})",
            staging_rows,
        )
    with migration.databases["tenant"].cursor() as cursor:
        cursor.executemany(
            "INSERT INTO exomiser (part, seq_id, locus_id, id, locus_hash, moi, variant_score, gene_combined_score, "
            "variant_rank, rank, symbol, acmg_classification) "
            "VALUES (1, %s, %s, %s, 'hash', 'AD', 0.5, 0.5, 1, 1, 'GENE1', 'pathogenic')",
            [
                (seq_id, locus_id, f"{seq_id}-{locus_id}")
                for seq_id, count in exomiser_seq_ids.items()
                for locus_id in range(1, count + 1)
            ],
        )


def _wait_for_schema_change(migration):
    with migration.databases["shared"].cursor() as cursor:
        for _ in range(60):
            cursor.execute(f"SHOW ALTER TABLE COLUMN FROM {migration.names['shared']}")
            columns = [d[0] for d in cursor.description]
            state, msg = columns.index("State"), columns.index("Msg")
            jobs = cursor.fetchall()
            cancelled = [row[msg] for row in jobs if row[state] == "CANCELLED"]
            if cancelled:
                raise RuntimeError(f"raw_exomiser ADD COLUMN was cancelled: {cancelled}")
            if all(row[state] == "FINISHED" for row in jobs):
                return
            time.sleep(1)
    raise TimeoutError("raw_exomiser ADD COLUMN did not finish")


def test_migration_moves_the_files_and_rebuilds_exomiser_per_task(migration):
    _insert(
        migration,
        [
            # Case 1, a trio: annotation task 101, legacy Exomiser task 102 on the proband.
            _staging(1, 11, 101, "radiant_germline_annotation", "proband"),
            _staging(1, 12, 101, "radiant_germline_annotation", "mother"),
            _staging(1, 13, 101, "radiant_germline_annotation", "father"),
            _staging(1, 11, 102, "exomiser", "proband", "s3://test/CA1.exomiser.variants.tsv"),
            # Sequencing 21 analysed twice: a duo (case 2, tasks 201/202), then a trio (case 3, tasks 301/302).
            _staging(2, 21, 201, "radiant_germline_annotation", "proband"),
            _staging(2, 24, 201, "radiant_germline_annotation", "mother"),
            _staging(2, 21, 202, "exomiser", "proband", "s3://test/CA2.exomiser.variants.tsv"),
            _staging(3, 21, 301, "radiant_germline_annotation", "proband"),
            _staging(3, 24, 301, "radiant_germline_annotation", "mother"),
            _staging(3, 25, 301, "radiant_germline_annotation", "father"),
            _staging(3, 21, 302, "exomiser", "proband", "s3://test/CA3.exomiser.variants.tsv"),
        ],
        exomiser_seq_ids={11: 2, 21: 3},
    )

    # 0. The checks, before any write.
    assert migration.run("0a", "shared") == ()
    assert migration.run("0b", "shared") == ()
    assert migration.run("0c", "tenant") == ()
    assert migration.run("0d", "shared") == ((21, 2),)

    # 1. Staging rows.
    migration.run("1", "shared")
    with migration.databases["shared"].cursor() as cursor:
        cursor.execute(
            "SELECT task_id, seq_id, exomiser_filepath, ingested_at FROM staging_sequencing_experiment "
            "ORDER BY task_id, seq_id"
        )
        staging = {(task_id, seq_id): filepath for task_id, seq_id, filepath, _ in cursor.fetchall()}
    assert staging == {
        (101, 11): "s3://test/CA1.exomiser.variants.tsv",
        (101, 12): None,
        (101, 13): None,
        (201, 21): "s3://test/CA2.exomiser.variants.tsv",
        (201, 24): None,
        (301, 21): "s3://test/CA3.exomiser.variants.tsv",
        (301, 24): None,
        (301, 25): None,
    }

    # 2. raw_exomiser: task_id after acmg_evidence, the generated columns still last.
    migration.run("2", "shared")
    _wait_for_schema_change(migration)
    with migration.databases["shared"].cursor() as cursor:
        cursor.execute("DESC raw_exomiser")
        assert [row[0] for row in cursor.fetchall()][-4:] == ["acmg_evidence", "task_id", "locus", "locus_hash"]

    # 3. exomiser rebuilt with the task id (appendix B steps 1 and 2), then swapped in (step 4).
    for number in ("3.B1", "3.B2", "3.B4"):
        migration.run(number, "tenant")
    with migration.databases["tenant"].cursor() as cursor:
        cursor.execute("DESC exomiser")
        assert [row[0] for row in cursor.fetchall()][:5] == ["part", "seq_id", "task_id", "locus_id", "id"]
        cursor.execute("SELECT seq_id, task_id, count(1) FROM exomiser GROUP BY seq_id, task_id")
        # The trio's rows get its only Exomiser task; the re-run sequencing 21 is left out, for re-import.
        assert cursor.fetchall() == ((11, 101, 2),)
        cursor.execute("SELECT count(1) FROM exomiser_task_id")
        assert cursor.fetchall() == ((5,),)

    # 4. Re-import marks: every row of the re-run sequencing's annotation tasks, for every family member.
    migration.run("4", "shared")
    with migration.databases["shared"].cursor() as cursor:
        cursor.execute(
            "SELECT task_id, seq_id FROM staging_sequencing_experiment "
            "WHERE ingested_at IS NULL ORDER BY task_id, seq_id"
        )
        assert cursor.fetchall() == ((201, 21), (201, 24), (301, 21), (301, 24), (301, 25))


def test_checks_list_the_rows_the_migration_cannot_place(migration):
    _insert(
        migration,
        [
            # A legacy Exomiser row with no proband annotation row of its case and sequencing.
            _staging(4, 41, 402, "exomiser", "proband", "s3://test/CA4.exomiser.variants.tsv"),
            _staging(4, 41, 403, "alignment_germline_variant_calling", "proband"),
            # A legacy Exomiser row with two proband annotation rows of its case and sequencing.
            _staging(5, 51, 501, "radiant_germline_annotation", "proband"),
            _staging(5, 51, 503, "radiant_germline_annotation", "proband"),
            _staging(5, 51, 502, "exomiser", "proband", "s3://test/CA5.exomiser.variants.tsv"),
            # Two legacy Exomiser rows matching one proband annotation row.
            _staging(7, 71, 701, "radiant_germline_annotation", "proband"),
            _staging(7, 71, 702, "exomiser", "proband", "s3://test/CA7.exomiser.variants.tsv"),
            _staging(7, 71, 704, "exomiser", "proband", "s3://test/CA7.rerun.exomiser.variants.tsv"),
        ],
        # Sequencing 61 has Exomiser rows and no staging row at all.
        exomiser_seq_ids={41: 1, 61: 2},
    )

    assert set(migration.run("0a", "shared")) == {(4, 41, 402, 0), (5, 51, 502, 2)}
    assert migration.run("0b", "shared") == ((7, 71, 701, 2),)
    assert migration.run("0c", "tenant") == ((61, 2),)
