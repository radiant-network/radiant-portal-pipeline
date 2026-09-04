"""The variant catalog must never expose another tenant's cohort.

`snv_variant_insert.sql` used to pool every tenant's frequency tables, so `germline_pn_*` was the
sum of all cohorts and a locus carried by one tenant showed up in everyone's catalog. This test
seeds two real tenant databases with overlapping and disjoint loci, runs the real insert once per
tenant, and asserts each catalog only ever sees its own numbers.
"""

import os

import jinja2
import pytest

from radiant.dags import DAGS_DIR
from radiant.tasks.data.radiant_tables import RadiantConfigKeys, get_radiant_mapping
from tests.integration.conftest import TENANT_CODES

_SQL_DIR = os.path.join(DAGS_DIR, "sql", "radiant")

_TENANT_A, _TENANT_B = TENANT_CODES

# Locus 4 is carried by no tenant: it exists in the shared staging catalog (open-data annotations
# have no tenant), so it proves the insert restricts to the loci the tenant actually carries.
_STAGING_LOCI = (1, 2, 3, 4)

# Locus 2 is the one both tenants carry, so giving it the odd value proves pick_source travels with
# its own row rather than being broadcast.
_STAGING_PICK_SOURCE = {1: "Ensembl", 2: "RefSeq", 3: "Ensembl", 4: "Ensembl"}

# locus_id -> (pc_wgs, pn_wgs). `pn_*` is the tenant's whole cohort, broadcast onto every row by
# germline_snv_variant_frequency_insert.sql.
_GERMLINE_FREQ = {
    _TENANT_A: {1: (3, 10), 2: (5, 10)},
    _TENANT_B: {2: (1, 4), 3: (2, 4)},
}
# locus_id -> (pc_tn_wgs, pn_tn_wgs, pc_to_wgs, pn_to_wgs). Tenant B has no somatic experiments at
# all. The four values are deliberately distinct and the `*_to_wxs` columns are left unseeded, so a
# wgs/wxs or tn/to mix-up in the positional projections of snv_variant_insert.sql shows up as a
# wrong number rather than passing silently — every somatic column there is INT(11) or DOUBLE, so
# the column count alone would still line up.
_SOMATIC_FREQ_COLUMNS = ("pc_tn_wgs", "pn_tn_wgs", "pc_to_wgs", "pn_to_wgs")
_SOMATIC_FREQ = {
    _TENANT_A: {2: (1, 7, 3, 12)},
    _TENANT_B: {},
}

# The catalog columns this test reads back, by name. Reading by name rather than by position keeps
# the test stable as the catalog gains columns (it grew six `*_to_*` ones in SJRA-1751).
_CATALOG_COLUMNS = (
    "germline_pc_wgs",
    "germline_pn_wgs",
    "somatic_pc_tn_wgs",
    "somatic_pn_tn_wgs",
    "somatic_pf_tn_wgs",
    "somatic_pc_to_wgs",
    "somatic_pn_to_wgs",
    "somatic_pf_to_wgs",
    "somatic_pc_to_wxs",
    "somatic_pn_to_wxs",
    "somatic_pf_to_wxs",
    # SJRA-1833. Not a frequency, but it sits next to `transcript_id` in the middle of the positional
    # projection, so a wrong ordinal there surfaces here rather than as a silent column swap.
    "pick_source",
)


def _reset_table(cursor, table, mapping):
    with open(os.path.join(_SQL_DIR, "init", f"{table}_create_table.sql")) as f_in:
        cursor.execute(jinja2.Template(f_in.read()).render({"mapping": mapping}))
    cursor.execute(f"TRUNCATE TABLE {mapping[f'starrocks_{table}']};")


def _seed(cursor, table, columns, rows):
    if not rows:
        return
    placeholders = ", ".join(["%s"] * len(columns))
    cursor.executemany(f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})", rows)


def test_snv_variant_is_isolated_per_tenant(starrocks_session, mapping_conf, starrocks_tenant_database):
    conf = {**mapping_conf, RadiantConfigKeys.RADIANT_TENANT_DB_TEMPLATE.env_key: starrocks_tenant_database}
    base_mapping = get_radiant_mapping(conf)
    mappings = {tenant: get_radiant_mapping(conf, tenant_code=tenant) for tenant in TENANT_CODES}

    with starrocks_session.cursor() as cursor:
        # The annotation source is shared: both tenants read the same staging catalog.
        _reset_table(cursor, "snv_staging_variant", base_mapping)
        _seed(
            cursor,
            base_mapping["starrocks_snv_staging_variant"],
            ("locus_id", "chromosome", "start", "reference", "alternate", "pick_source"),
            [(locus_id, "1", 1000 + locus_id, "A", "T", _STAGING_PICK_SOURCE[locus_id]) for locus_id in _STAGING_LOCI],
        )

        for tenant, mapping in mappings.items():
            for table in ("germline_snv_variant_frequency", "somatic_snv_variant_frequency", "snv_variant"):
                _reset_table(cursor, table, mapping)

            _seed(
                cursor,
                mapping["starrocks_germline_snv_variant_frequency"],
                ("locus_id", "pc_wgs", "pn_wgs"),
                [(locus_id, pc, pn) for locus_id, (pc, pn) in _GERMLINE_FREQ[tenant].items()],
            )
            _seed(
                cursor,
                mapping["starrocks_somatic_snv_variant_frequency"],
                ("locus_id", *_SOMATIC_FREQ_COLUMNS),
                [(locus_id, *values) for locus_id, values in _SOMATIC_FREQ[tenant].items()],
            )

        with open(os.path.join(_SQL_DIR, "snv_variant_insert.sql")) as f_in:
            insert_template = f_in.read()

        catalogs = {}
        for tenant, mapping in mappings.items():
            cursor.execute(jinja2.Template(insert_template).render({"mapping": mapping}))
            cursor.execute(
                f"SELECT locus_id, {', '.join(_CATALOG_COLUMNS)} "
                f"FROM {mapping['starrocks_snv_variant']} ORDER BY locus_id"
            )
            catalogs[tenant] = {row[0]: dict(zip(_CATALOG_COLUMNS, row[1:], strict=True)) for row in cursor.fetchall()}

    # Each catalog holds exactly the loci that tenant carries — never the other tenant's, and never
    # locus 4, which no tenant carries.
    assert set(catalogs[_TENANT_A]) == {1, 2}
    assert set(catalogs[_TENANT_B]) == {2, 3}

    # pn_* is the tenant's own cohort. Pooled, locus 2 would have read 10 + 4 = 14 patients.
    assert {row["germline_pn_wgs"] for row in catalogs[_TENANT_A].values()} == {10}
    assert {row["germline_pn_wgs"] for row in catalogs[_TENANT_B].values()} == {4}

    # pc_* on the shared locus counts only the tenant's own carriers (pooled it would have been 6).
    assert catalogs[_TENANT_A][2]["germline_pc_wgs"] == 5
    assert catalogs[_TENANT_B][2]["germline_pc_wgs"] == 1

    # Tenant A's somatic cohort stays out of tenant B, which has no somatic experiments.
    tenant_a_locus_2 = catalogs[_TENANT_A][2]
    assert (tenant_a_locus_2["somatic_pc_tn_wgs"], tenant_a_locus_2["somatic_pn_tn_wgs"]) == (1, 7)
    assert {row["somatic_pc_tn_wgs"] for row in catalogs[_TENANT_B].values()} == {0}
    assert {row["somatic_pn_tn_wgs"] for row in catalogs[_TENANT_B].values()} == {0}

    # The tumor-only columns are carried through with their own cohort, distinct from tumor-normal's
    # (SJRA-1751). `*_to_wxs` was never seeded, so it must read as an all-zero cohort.
    assert (
        tenant_a_locus_2["somatic_pc_to_wgs"],
        tenant_a_locus_2["somatic_pn_to_wgs"],
        tenant_a_locus_2["somatic_pf_to_wgs"],
    ) == (3, 12, 0.25)
    assert float(tenant_a_locus_2["somatic_pf_tn_wgs"]) == pytest.approx(1 / 7, rel=1e-6)
    assert (
        tenant_a_locus_2["somatic_pc_to_wxs"],
        tenant_a_locus_2["somatic_pn_to_wxs"],
        tenant_a_locus_2["somatic_pf_to_wxs"],
    ) == (0, 0, 0)
    assert {row["somatic_pc_to_wgs"] for row in catalogs[_TENANT_B].values()} == {0}
    assert {row["somatic_pn_to_wgs"] for row in catalogs[_TENANT_B].values()} == {0}

    # pick_source is copied from the shared staging catalog, per row (SJRA-1833).
    assert catalogs[_TENANT_A][1]["pick_source"] == "Ensembl"
    assert catalogs[_TENANT_A][2]["pick_source"] == "RefSeq"
    assert catalogs[_TENANT_B][2]["pick_source"] == "RefSeq"
    assert catalogs[_TENANT_B][3]["pick_source"] == "Ensembl"
