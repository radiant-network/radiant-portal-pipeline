import jinja2
import pytest

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


def test_consequence_filter_part_unions_occurrences_across_tenant_dbs():
    sql = _render("snv_consequence_filter_insert_part.sql")
    for tenant in ("chop", "chusj"):
        assert f"{tenant}_tenant.germline__snv__occurrence" in sql
        assert f"{tenant}_tenant.somatic__snv__occurrence" in sql
    assert "UNION ALL" in sql
    assert "radiant.snv__consequence_filter_partitioned" in sql  # base target
    _assert_balanced_unions(sql, n_selects=4)  # (germline + somatic) x 2 tenants


def test_consequence_filter_part_single_tenant_has_no_trailing_union():
    sql = _render("snv_consequence_filter_insert_part.sql", tenants=["chop"])
    assert "chop_tenant.somatic__snv__occurrence" in sql
    _assert_balanced_unions(sql, n_selects=2)


# The variant catalog is built once per tenant, from that tenant's own tables only. These renders
# mirror the operator's Jinja context, which exposes `mapping` but neither `tenants` nor
# `per_tenant_mapping`.
_TENANT_SCOPED = ["snv_variant_insert.sql", "snv_variant_part_insert_part.sql"]


def _render_for_tenant(filename: str, tenant_code: str = "chop") -> str:
    text = (_RADIANT_SQL / filename).read_text()
    return jinja2.Template(text).render(mapping=get_radiant_mapping(_CONF, tenant_code=tenant_code))


@pytest.mark.parametrize("filename", _TENANT_SCOPED)
def test_no_other_tenant_leaks_into_the_render(filename):
    sql = _render_for_tenant(filename)
    assert "chusj" not in sql
    assert sql.count("_tenant.") == sql.count("chop_tenant.")


def test_snv_variant_reads_only_the_tenant_frequencies():
    sql = _render_for_tenant("snv_variant_insert.sql")
    assert "INSERT OVERWRITE chop_tenant.snv__variant\n" in sql
    assert "chop_tenant.germline__snv__variant_frequency" in sql
    assert "chop_tenant.somatic__snv__variant_frequency" in sql
    # A cross-tenant union is what made every frequency a pooled number. The one union left combines
    # this tenant's own germline and somatic loci, so it must stay at exactly two selects — and
    # `UNION ALL` rather than `UNION`, which StarRocks mis-plans on the right of a LEFT SEMI JOIN.
    _assert_balanced_unions(sql, n_selects=2)
    # The catalog only holds the loci this tenant carries.
    assert "LEFT SEMI JOIN tenant_loci" in sql
    # The annotation source is open data, so it stays shared.
    assert "radiant.snv__staging_variant" in sql


def test_snv_variant_part_reads_only_the_tenant_occurrences():
    sql = _render_for_tenant("snv_variant_part_insert_part.sql")
    assert "OVERWRITE chop_tenant.snv__variant_partitioned" in sql
    assert "chop_tenant.snv__variant v" in sql
    _assert_balanced_unions(sql, n_selects=2)  # germline + somatic, current tenant only
