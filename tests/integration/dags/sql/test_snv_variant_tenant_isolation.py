"""The variant catalog must never expose another tenant's cohort.

`snv_variant_insert.sql` used to pool every tenant's frequency tables, so `germline_pn_*` was the
sum of all cohorts and a locus carried by one tenant showed up in everyone's catalog. This test
seeds two real tenant databases with overlapping and disjoint loci, runs the real insert once per
tenant, and asserts each catalog only ever sees its own numbers.
"""

import os

import jinja2

from radiant.dags import DAGS_DIR
from radiant.tasks.data.radiant_tables import RadiantConfigKeys, get_radiant_mapping
from tests.integration.conftest import TENANT_CODES

_SQL_DIR = os.path.join(DAGS_DIR, "sql", "radiant")

_TENANT_A, _TENANT_B = TENANT_CODES

# Locus 4 is carried by no tenant: it exists in the shared staging catalog (open-data annotations
# have no tenant), so it proves the insert restricts to the loci the tenant actually carries.
_STAGING_LOCI = (1, 2, 3, 4)

# locus_id -> (pc_wgs, pn_wgs). `pn_*` is the tenant's whole cohort, broadcast onto every row by
# germline_snv_variant_frequency_insert.sql.
_GERMLINE_FREQ = {
    _TENANT_A: {1: (3, 10), 2: (5, 10)},
    _TENANT_B: {2: (1, 4), 3: (2, 4)},
}
# locus_id -> (pc_tn_wgs, pn_tn_wgs). Tenant B has no somatic experiments at all.
_SOMATIC_FREQ = {
    _TENANT_A: {2: (1, 7)},
    _TENANT_B: {},
}


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
            ("locus_id", "chromosome", "start", "reference", "alternate"),
            [(locus_id, "1", 1000 + locus_id, "A", "T") for locus_id in _STAGING_LOCI],
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
                ("locus_id", "pc_tn_wgs", "pn_tn_wgs"),
                [(locus_id, pc, pn) for locus_id, (pc, pn) in _SOMATIC_FREQ[tenant].items()],
            )

        with open(os.path.join(_SQL_DIR, "snv_variant_insert.sql")) as f_in:
            insert_template = f_in.read()

        catalogs = {}
        for tenant, mapping in mappings.items():
            cursor.execute(jinja2.Template(insert_template).render({"mapping": mapping}))
            cursor.execute(
                "SELECT locus_id, germline_pc_wgs, germline_pn_wgs, somatic_pc_tn_wgs, somatic_pn_tn_wgs "
                f"FROM {mapping['starrocks_snv_variant']} ORDER BY locus_id"
            )
            catalogs[tenant] = {row[0]: row[1:] for row in cursor.fetchall()}

    # Each catalog holds exactly the loci that tenant carries — never the other tenant's, and never
    # locus 4, which no tenant carries.
    assert set(catalogs[_TENANT_A]) == {1, 2}
    assert set(catalogs[_TENANT_B]) == {2, 3}

    # pn_* is the tenant's own cohort. Pooled, locus 2 would have read 10 + 4 = 14 patients.
    assert {pn for _, pn, _, _ in catalogs[_TENANT_A].values()} == {10}
    assert {pn for _, pn, _, _ in catalogs[_TENANT_B].values()} == {4}

    # pc_* on the shared locus counts only the tenant's own carriers (pooled it would have been 6).
    assert catalogs[_TENANT_A][2][0] == 5
    assert catalogs[_TENANT_B][2][0] == 1

    # Tenant A's somatic cohort stays out of tenant B, which has no somatic experiments.
    assert catalogs[_TENANT_A][2][2:] == (1, 7)
    assert {row[3] for row in catalogs[_TENANT_B].values()} == {0}
