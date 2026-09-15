"""Render checks for the OpenDataLake-backed open-data SQL (SJRA-1811).

These assert the two things the migration to OpenDataLake changed and that nothing in CI would otherwise
catch: every contract table is read through an Iceberg ref, and the columns whose names differ upstream are
read under the upstream name.
"""

import re

import jinja2
import pytest

from radiant.dags import DAGS_DIR
from radiant.tasks.data.radiant_tables import (
    ICEBERG_OPEN_DATA_CONTRACT_MAPPING,
    ICEBERG_OPEN_DATA_LEGACY_MAPPING,
    RadiantConfigKeys,
    get_radiant_mapping,
)

_OPEN_DATA_SQL = DAGS_DIR / "sql" / "open_data"
_RADIANT_SQL = DAGS_DIR / "sql" / "radiant"
_CONF = {"RADIANT_TABLES_DATABASE": "radiant"}
_REF = RadiantConfigKeys.OPEN_DATA_REF.default

# The sources OpenDataLake does not publish a `locus_hash` for, so Radiant recomputes it. dbnsfp is
# deliberately absent: it is the one contract that does publish the column.
# The `table_prefix` each of these is named by in RADIANT_OPEN_DATA_USE_LEGACY_TABLES -- `gnomad` is the
# SQL file stem, `gnomad_joint` the source.
_SOURCE_NAMES = {
    "clinvar": "clinvar",
    "dbsnp": "dbsnp",
    "spliceai": "spliceai",
    "1000_genomes": "1000_genomes",
    "topmed_bravo": "topmed_bravo",
    "gnomad": "gnomad_joint",
}

_RECOMPUTES_LOCUS_HASH = (
    "clinvar",
    "dbsnp",
    "spliceai",
    "1000_genomes",
    "topmed_bravo",
    "gnomad",
)


def _statement(sql: str) -> str:
    """Rendered SQL with its comments stripped -- these files quote table and column names in their
    rationale, so matching against the raw text would pass on a comment."""
    sql = re.sub(r"/\*(?!\+).*?\*/", " ", sql, flags=re.DOTALL)
    return "\n".join(line for line in sql.splitlines() if not line.lstrip().startswith("--"))


def _render(path) -> str:
    return jinja2.Template(path.read_text()).render(mapping=get_radiant_mapping(_CONF), partition=5)


def _open_data_sql_files():
    return sorted(p for p in _OPEN_DATA_SQL.glob("*.sql"))


def test_every_mapping_key_used_in_sql_exists():
    """A typo in `{{ mapping.iceberg_x }}` renders as the empty string rather than failing, so the
    resulting SQL is silently wrong. Catch unknown keys here instead."""
    known = set(ICEBERG_OPEN_DATA_CONTRACT_MAPPING) | set(ICEBERG_OPEN_DATA_LEGACY_MAPPING)
    mapping = get_radiant_mapping(_CONF)
    referenced = set()
    for path in list(_open_data_sql_files()) + sorted(_RADIANT_SQL.glob("*.sql")):
        referenced.update(re.findall(r"mapping\.(iceberg_\w+)", path.read_text()))

    unknown = {key for key in referenced if key not in mapping}
    assert not unknown, f"SQL references mapping keys that do not exist: {sorted(unknown)}"
    # And every open-data key the SQL uses is one of the two open-data families, not a Radiant table.
    assert referenced & known


def _contract_reading_sql_files():
    return sorted(_open_data_sql_files()) + sorted(_RADIANT_SQL.glob("*.sql"))


@pytest.mark.parametrize("path", _contract_reading_sql_files(), ids=lambda p: p.name)
def test_contract_tables_are_always_read_through_a_ref(path):
    """OpenDataLake leaves `main` empty, so a read that names no ref returns zero rows."""
    sql = _render(path)
    for table in ICEBERG_OPEN_DATA_CONTRACT_MAPPING.values():
        for occurrence in re.finditer(rf"\.{re.escape(table)}\b", sql):
            tail = sql[occurrence.end() : occurrence.end() + 40]
            assert tail.lstrip().startswith(f"VERSION AS OF '{_REF}'"), (
                f"{path.name} reads {table} without a ref: ...{sql[occurrence.start() : occurrence.end() + 40]}"
            )


def _contract_relation_aliases(sql: str) -> list[str]:
    """Aliases bound directly to a contract table, i.e. the `src` in `FROM cat.db.t VERSION AS OF 'x' src`."""
    return re.findall(r"VERSION AS OF '[^']*'\s+(\w+)", sql)


@pytest.mark.parametrize("source", _RECOMPUTES_LOCUS_HASH)
def test_locus_hash_is_recomputed_when_read_from_opendatalake(source):
    for name in (f"{source}_insert.sql", f"{source}_insert_hashes.sql"):
        sql = _render(_OPEN_DATA_SQL / name)
        assert "sha2(concat_ws('-'" in sql, f"{name} does not recompute locus_hash"
        aliases = _contract_relation_aliases(sql)
        assert aliases, f"{name} binds no alias to its contract relation"
        for alias in aliases:
            # Left-anchored: the lookup alias `vd` ends in `d`, so a plain substring test would
            # read `vd.locus_hash` as the source alias `d`'s.
            assert not re.search(rf"(?<![\w.]){re.escape(alias)}\.locus_hash", sql), (
                f"{name} reads {alias}.locus_hash, which the contract does not publish"
            )


@pytest.mark.parametrize("source", _RECOMPUTES_LOCUS_HASH)
def test_locus_hash_is_read_not_recomputed_when_the_source_is_held_back(source):
    """The pre-contract tables store `locus_hash`. Hashing every row again would be the same value at
    the cost of a SHA-256 per row -- on `1000_genomes` and `dbsnp` that is the whole table."""
    legacy = {**_CONF, "RADIANT_OPEN_DATA_USE_LEGACY_TABLES": _SOURCE_NAMES[source]}
    for name in (f"{source}_insert.sql", f"{source}_insert_hashes.sql"):
        text = (_OPEN_DATA_SQL / name).read_text()
        sql = _statement(jinja2.Template(text).render(mapping=get_radiant_mapping(legacy), partition=5))
        assert "sha2(" not in sql, f"{name} still hashes a source that already stores locus_hash"
        assert ".locus_hash" in sql


@pytest.mark.parametrize("path", _open_data_sql_files(), ids=lambda p: p.name)
def test_locus_hash_is_computed_at_most_once_per_statement(path):
    """StarRocks plans the join key and the projected column as two separate projections and does not
    reuse the expression between them, so a second `sha2(...)` in a statement is a second SHA-256 over
    every row. Where the hash is needed twice it belongs in a subquery."""
    assert _render(path).count("sha2(") <= 1, f"{path.name} evaluates the locus hash more than once"


def test_dbnsfp_still_reads_the_locus_hash_it_publishes():
    """dbnsfp is the one contract that publishes `locus_hash`, and OpenDataLake pins it byte-identical to
    the StarRocks form, so recomputing it here would only cost a hash per row."""
    sql = _render(_OPEN_DATA_SQL / "dbnsfp_insert.sql")
    assert "d.locus_hash" in sql
    assert "sha2(" not in sql


def test_gnomad_reads_the_joint_callset_columns():
    sql = _render(_OPEN_DATA_SQL / "gnomad_insert.sql")
    assert "gnomad_joint_v1" in sql
    for column in ("af_joint", "ac_joint", "an_joint", "hom_joint"):
        assert f"t.{column}" in sql
    # The v3-era unsuffixed names exist nowhere upstream, so reading one would resolve to nothing.
    # `\b` after the name keeps `t.af_joint` from matching, since `_` is a word character.
    for column in ("af", "ac", "an", "nhomalt"):
        assert not re.search(rf"\bt\.{column}\b", sql), f"gnomad_insert.sql still reads the v3 column t.{column}"


def test_hpo_gene_panel_reads_the_upstream_column_names():
    sql = _render(_OPEN_DATA_SQL / "hpo_gene_panel_insert.sql")
    assert "hpo_genes_v1" in sql
    for column in ("h.gene_symbol", "h.hpo_name", "h.hpo_id"):
        assert column in sql
    for column in ("h.symbol", "h.hpo_term_name", "h.hpo_term_id"):
        assert column not in sql


_CNV_SQL = [
    f"{kind}_cnv_occurrence_{suffix}.sql"
    for kind in ("germline", "somatic")
    for suffix in ("insert_partition_delta", "reannotate_partition")
]


@pytest.mark.parametrize("name", _CNV_SQL)
def test_cnv_enrichment_filters_gnomad_sv_only_when_it_is_held_back(name):
    """`gnomad_sv_v1` publishes PASS rows only and drops `filters`, so the contract side must not
    filter. The pre-contract table still carries every call, so a source held back via
    RADIANT_OPEN_DATA_USE_LEGACY_TABLES has to filter as it always did -- otherwise it would quietly
    annotate against non-PASS calls."""
    path = _RADIANT_SQL / name
    contract = jinja2.Template(path.read_text()).render(mapping=get_radiant_mapping(_CONF), partition=5)
    # Assert on the resolved relation, not the bare table name: the rationale comment quotes it too.
    assert "gnomad.filters" not in _statement(contract)
    assert "JOIN opendatalake_catalog.reference.gnomad_sv_v1" in contract

    held_back = jinja2.Template(path.read_text()).render(
        mapping=get_radiant_mapping({**_CONF, "RADIANT_OPEN_DATA_USE_LEGACY_TABLES": "gnomad_sv"}),
        partition=5,
    )
    assert "AND gnomad.filters = 'PASS'" in _statement(held_back)
    assert "JOIN radiant_iceberg_catalog.radiant.gnomad_sv" in held_back
