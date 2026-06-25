import jinja2

from radiant.dags import DAGS_DIR
from radiant.tasks.data.radiant_tables import get_radiant_mapping

_RADIANT_SQL = DAGS_DIR / "sql" / "radiant"
_CONF = {"RADIANT_TABLES_DATABASE": "radiant"}


def _render(filename: str) -> str:
    """Render a pooled-build SQL with a 2-tenant context, mirroring the operator's Jinja context."""
    text = (_RADIANT_SQL / filename).read_text()
    ctx = {
        "mapping": get_radiant_mapping(_CONF),
        "per_tenant_mapping": lambda t: get_radiant_mapping(_CONF, tenant_code=t),
        "tenants": ["chop", "chusj"],
        "partition": 5,
    }
    return jinja2.Template(text).render(**ctx)


def test_snv_variant_part_unions_occurrences_across_tenant_dbs():
    sql = _render("snv_variant_part_insert_part.sql")
    assert "chop_db.germline__snv__occurrence" in sql
    assert "chusj_db.germline__snv__occurrence" in sql
    assert "UNION ALL" in sql
    assert "radiant.snv__variant_partitioned" in sql  # shared target


def test_consequence_filter_part_unions_occurrences_across_tenant_dbs():
    sql = _render("snv_consequence_filter_insert_part.sql")
    assert "chop_db.germline__snv__occurrence" in sql
    assert "chusj_db.germline__snv__occurrence" in sql
    assert "UNION ALL" in sql
    assert "radiant.snv__consequence_filter_partitioned" in sql  # shared target


def test_snv_variant_pools_freqs_across_tenant_dbs():
    sql = _render("snv_variant_insert.sql")
    assert "chop_db.germline__snv__variant_frequency" in sql
    assert "chusj_db.germline__snv__variant_frequency" in sql
    assert "chop_db.somatic__snv__variant_frequency" in sql
    assert "chusj_db.somatic__snv__variant_frequency" in sql
    assert "UNION ALL" in sql
    assert "radiant.snv__variant" in sql  # shared target + staging source
