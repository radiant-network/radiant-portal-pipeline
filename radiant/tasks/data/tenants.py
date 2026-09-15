"""Tenant and partition discovery against StarRocks.

Shared by the DAGs that need the tenant set rather than a single tenant code: the pooled
consequence-filter render in `import_part`, and the data-QA run in
`data_integrity_starrocks`. Keeping one definition avoids two copies of "all tenants"
drifting apart.
"""


def _query(sql: str) -> list[tuple]:
    from airflow.hooks.base import BaseHook

    conn = BaseHook.get_connection("starrocks_conn")
    with conn.get_hook().get_conn().cursor() as cursor:
        cursor.execute(sql)
        return list(cursor.fetchall())


def _staging_table(conf: dict | None) -> str:
    from radiant.tasks.data.radiant_tables import get_radiant_mapping

    return get_radiant_mapping(conf)["starrocks_staging_sequencing_experiment"]


def list_all_parts(conf: dict | None = None) -> list[int]:
    rows = _query(f"SELECT DISTINCT part FROM {_staging_table(conf)} WHERE part IS NOT NULL")
    return sorted({int(row[0]) for row in rows})


def list_tenant_parts(conf: dict | None = None) -> list[dict]:
    rows = _query(
        f"SELECT DISTINCT tenant_code, part FROM {_staging_table(conf)} "
        "WHERE tenant_code IS NOT NULL AND part IS NOT NULL"
    )
    pairs = sorted({(row[0], int(row[1])) for row in rows})
    return [{"tenant_code": tenant_code, "part": part} for tenant_code, part in pairs]


def list_all_tenants(conf: dict | None = None) -> list[str]:
    """Every tenant known to the platform — not just the ones in the current batch.

    Discovery is by `tenant_code` on the staging table rather than by enumerating
    `%_tenant` databases, because the database name is built from the configurable
    RADIANT_TENANT_DB_TEMPLATE and matching on it would hardcode the default.
    """
    rows = _query(f"SELECT DISTINCT tenant_code FROM {_staging_table(conf)}")
    return sorted({row[0] for row in rows if row[0]})
