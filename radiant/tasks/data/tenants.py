"""Tenant discovery against StarRocks.

Shared by the DAGs that need the tenant set rather than a single tenant code: the pooled
consequence-filter render in `import_part`, and the data-QA run in
`data_integrity_starrocks`. Keeping one definition avoids two copies of "all tenants"
drifting apart.
"""


def list_all_tenants(conf: dict | None = None) -> list[str]:
    """Every tenant known to the platform — not just the ones in the current batch.

    Discovery is by `tenant_code` on the staging table rather than by enumerating
    `%_tenant` databases, because the database name is built from the configurable
    RADIANT_TENANT_DB_TEMPLATE and matching on it would hardcode the default.
    """
    from airflow.hooks.base import BaseHook

    from radiant.tasks.data.radiant_tables import get_radiant_mapping

    table = get_radiant_mapping(conf)["starrocks_staging_sequencing_experiment"]
    conn = BaseHook.get_connection("starrocks_conn")
    with conn.get_hook().get_conn().cursor() as cursor:
        cursor.execute(f"SELECT DISTINCT tenant_code FROM {table}")
        return sorted({row[0] for row in cursor.fetchall() if row[0]})
