"""Render checks for the re-annotation SQL (SJRA-1811 §5).

The point of the re-annotation statements is what they *stop* reading: the Radiant Iceberg tables, which
the design treats as transient. That is a property no other test would catch, because a statement that
reads the wrong table is still valid SQL.
"""

import re

import jinja2
import pytest

from radiant.dags import DAGS_DIR
from radiant.tasks.data.radiant_tables import get_radiant_mapping

_RADIANT_SQL = DAGS_DIR / "sql" / "radiant"
_CONF = {"RADIANT_TABLES_DATABASE": "radiant"}

_REANNOTATE_SQL = (
    "snv_staging_variant_reannotate.sql",
    "snv_consequence_reannotate.sql",
    "germline_cnv_occurrence_reannotate_partition.sql",
    "somatic_cnv_occurrence_reannotate_partition.sql",
)


def _text(name: str) -> str:
    return (_RADIANT_SQL / name).read_text()


def _statement(name: str) -> str:
    """The file with its comments removed.

    These files carry long rationale comments that quote the very table and predicate names the
    assertions below look for, so matching against the raw text would pass on a comment.
    """
    # `/*+ ... */` is an optimizer hint, not a comment -- StarRocks reads it, so it stays.
    sql = re.sub(r"/\*(?!\+).*?\*/", " ", _text(name), flags=re.DOTALL)
    return "\n".join(line for line in sql.splitlines() if not line.lstrip().startswith("--"))


def _render(name: str) -> str:
    return jinja2.Template(_text(name)).render(mapping=get_radiant_mapping(_CONF))


@pytest.mark.parametrize("name", _REANNOTATE_SQL)
def test_renders_with_no_unresolved_mapping_key(name):
    # A typo in `{{ mapping.x }}` renders as the empty string, so the statement silently loses a table.
    sql = _render(name)
    assert "{{" not in sql
    assert not re.search(r"\bFROM\s+(JOIN|WHERE|$)", sql, re.IGNORECASE | re.MULTILINE)


@pytest.mark.parametrize("name", _REANNOTATE_SQL)
def test_reads_no_radiant_iceberg_table(name):
    """The Radiant-side Iceberg tables are the dependency this ticket removes.

    `iceberg_gnomad_sv` is deliberately not covered: it is an OpenDataLake reference table, the source of
    truth for the CNV gnomAD columns, and §5's own diagram lists it as an input.
    """
    referenced = set(re.findall(r"mapping\.(iceberg_\w+)", _text(name)))
    radiant_iceberg = {
        key
        for key in referenced
        # `iceberg_gnomad_sv` is an OpenDataLake reference table, and `*_is_contract` is a flag, not a
        # table reference.
        if key != "iceberg_gnomad_sv" and not key.endswith("_is_contract")
    }
    assert not radiant_iceberg, f"{name} still reads {sorted(radiant_iceberg)}"


def test_staging_variant_reannotates_from_the_accumulator_not_the_batch():
    sql = jinja2.Template(_statement("snv_staging_variant_reannotate.sql")).render(mapping=get_radiant_mapping(_CONF))
    # `snv__tmp_variant` holds only the batch import_part is processing.
    assert "snv__tmp_variant" not in sql
    assert re.search(r"FROM\s+\S*snv__staging_variant\s+v", sql)
    assert "task_ids" not in sql


def test_consequence_reannotation_keeps_the_mane_pair_score_lookup():
    """§5's snippet joins dbNSFP on `transcript_id`; that would drop every RefSeq row's scores.

    The ingest rule (`snv_consequence_insert.sql`) reads a RefSeq row's scores under the Ensembl twin its
    MANE pair names, and the target's `scores_from_mane_pair` column records that it did.
    """
    sql = jinja2.Template(_statement("snv_consequence_reannotate.sql")).render(mapping=get_radiant_mapping(_CONF))
    assert "score_transcript_id" in sql
    assert "mane_pair_transcript_id" in sql
    assert "d.ensembl_transcript_id = c.score_transcript_id" in sql
    assert "scores_from_mane_pair" in sql


@pytest.mark.parametrize("kind", ("germline", "somatic"))
def test_cnv_reannotation_is_scoped_by_part_not_by_seq_ids(kind):
    sql = _statement(f"{kind}_cnv_occurrence_reannotate_partition.sql")
    assert "%(part)s" in sql
    # A delta's scoping. A re-annotation rebuilds whole partitions.
    assert "%(seq_ids)s" not in sql
    # The StarRocks occurrence tables are per-tenant, so the tenant is implied by `mapping`.
    assert "tenant_code" not in sql


@pytest.mark.parametrize("kind", ("germline", "somatic"))
def test_cnv_reannotation_overwrites_exactly_one_partition(kind):
    """`dynamic_overwrite` replaces only the partitions the result set contains, and every row here
    carries the same `part` -- which is also what makes reading the overwritten table safe."""
    sql = _statement(f"{kind}_cnv_occurrence_reannotate_partition.sql")
    assert "set_var(dynamic_overwrite = true)" in sql
    assert sql.count("INSERT") == 1


def test_open_data_release_is_current_state_not_history():
    """One row per source, latest run wins, retry-safe.

    Keyed on `source_name` rather than `table_name`: a held-back source is read under its pre-contract
    name, so a table-name key would leave a stale row behind the first time one flips to the contract.

    Two StarRocks rules make this fragile to edit: a PRIMARY KEY must be the table's leading column, and
    the declared length of the key columns is capped (128 bytes), which is why catalog and database are
    attributes rather than part of the key.
    """
    ddl = (_RADIANT_SQL / "init" / "open_data_release_create_table.sql").read_text()
    body = ddl[ddl.index("(") + 1 :]
    first_column = next(line.split()[0] for line in body.splitlines() if line.strip())

    assert "PRIMARY KEY(source_name)" in ddl
    assert "DUPLICATE KEY" not in ddl
    assert first_column == "source_name", "the primary key must be the leading column"


def test_open_data_release_insert_upserts_every_column_by_name():
    """A positional insert would silently shift once the DDL is reordered, and it was reordered to put
    the primary key first."""
    sql = _text("open_data_release_insert.sql")
    assert "INSERT INTO" in sql
    columns = re.search(r"INSERT INTO[^(]*\(([^)]*)\)", sql, re.DOTALL).group(1)
    assert [c.strip() for c in columns.split(",")] == [
        "source_name",
        "recorded_at",
        "dag_run_id",
        "table_name",
        "catalog_name",
        "database_name",
        "iceberg_ref",
        "dataset_version",
    ]


@pytest.mark.parametrize("kind", ("germline", "somatic"))
def test_cnv_reannotation_projects_every_target_column(kind):
    """Positional insert: the projection has to match the DDL column-for-column, and the somatic and
    germline column orders differ (`cn` sits in a different block)."""
    ddl = (_RADIANT_SQL / "init" / f"{kind}_cnv_occurrence_create_table.sql").read_text()
    body = ddl[ddl.index("(") + 1 : ddl.rindex(") ENGINE=OLAP")]
    ddl_columns = [
        match.group(1)
        for line in body.splitlines()
        if (match := re.match(r"\s*`?(\w+)`?\s+(?:int|bigint|varchar|char|float|double|boolean|array)", line, re.I))
    ]

    sql = _statement(f"{kind}_cnv_occurrence_reannotate_partition.sql")
    projection = sql[sql.rindex("SELECT o.part") : sql.rindex("FROM")]
    projected = re.findall(r"(?:AS\s+(\w+)|(?:o|cytoband|genes|snv)\.(\w+))\s*(?:,|$)", projection, re.M)
    projected = [alias or column for alias, column in projected]

    assert projected == ddl_columns, f"{kind} projection does not line up with its DDL"
