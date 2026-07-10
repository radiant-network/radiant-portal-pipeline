import jinja2

from radiant.dags import DAGS_DIR
from radiant.tasks.data.radiant_tables import get_radiant_mapping

_RADIANT_SQL = DAGS_DIR / "sql" / "radiant"
_CONF = {"RADIANT_TABLES_DATABASE": "radiant"}


def _render(filename: str, tenants: list[str] | None = None) -> str:
    """Render a pooled-build SQL with a multi-tenant context, mirroring the operator's Jinja context."""
    text = (_RADIANT_SQL / filename).read_text()
    ctx = {
        "mapping": get_radiant_mapping(_CONF),
        "per_tenant_mapping": lambda t: get_radiant_mapping(_CONF, tenant_code=t),
        "tenants": tenants if tenants is not None else ["chop", "chusj"],
        "partition": 5,
    }
    return jinja2.Template(text).render(**ctx)


def _assert_balanced_unions(sql: str, n_selects: int):
    assert sql.count("SELECT locus_id") == n_selects
    assert sql.count("UNION ALL") == n_selects - 1


def test_snv_variant_part_unions_occurrences_across_tenant_dbs():
    sql = _render("snv_variant_part_insert_part.sql")
    for tenant in ("chop", "chusj"):
        assert f"{tenant}_tenant.germline__snv__occurrence" in sql
        assert f"{tenant}_tenant.somatic__snv__occurrence" in sql
    assert "UNION ALL" in sql
    assert "radiant.snv__variant_partitioned" in sql  # base target
    _assert_balanced_unions(sql, n_selects=4)  # (germline + somatic) x 2 tenants


def test_consequence_filter_part_unions_occurrences_across_tenant_dbs():
    sql = _render("snv_consequence_filter_insert_part.sql")
    for tenant in ("chop", "chusj"):
        assert f"{tenant}_tenant.germline__snv__occurrence" in sql
        assert f"{tenant}_tenant.somatic__snv__occurrence" in sql
    assert "UNION ALL" in sql
    assert "radiant.snv__consequence_filter_partitioned" in sql  # base target
    _assert_balanced_unions(sql, n_selects=4)  # (germline + somatic) x 2 tenants


def test_snv_variant_part_single_tenant_has_no_trailing_union():
    sql = _render("snv_variant_part_insert_part.sql", tenants=["chop"])
    assert "chop_tenant.somatic__snv__occurrence" in sql
    _assert_balanced_unions(sql, n_selects=2)


def test_consequence_filter_part_single_tenant_has_no_trailing_union():
    sql = _render("snv_consequence_filter_insert_part.sql", tenants=["chop"])
    assert "chop_tenant.somatic__snv__occurrence" in sql
    _assert_balanced_unions(sql, n_selects=2)


def test_snv_variant_pools_freqs_across_tenant_dbs():
    sql = _render("snv_variant_insert.sql")
    assert "chop_tenant.germline__snv__variant_frequency" in sql
    assert "chusj_tenant.germline__snv__variant_frequency" in sql
    assert "chop_tenant.somatic__snv__variant_frequency" in sql
    assert "chusj_tenant.somatic__snv__variant_frequency" in sql
    assert "UNION ALL" in sql
    assert "radiant.snv__variant" in sql  # base target + staging source
