from collections.abc import Iterable


def resolve_iceberg_source_tables(conf: dict | None = None) -> dict[str, str]:
    from radiant.tasks.data.radiant_tables import (
        ICEBERG_OPEN_DATA_CONTRACT_MAPPING,
        ICEBERG_OPEN_DATA_LEGACY_MAPPING,
        ICEBERG_OPEN_DATA_PRE_CONTRACT_MAPPING,
        RadiantConfigKeys,
        get_config_value,
        get_open_data_contract_keys,
    )
def _iceberg_schemas(conf: dict | None) -> tuple[str, str]:
    """The OpenDataLake and legacy Radiant schemas, each as `catalog.database`."""
    from radiant.tasks.data.radiant_tables import RadiantConfigKeys, get_config_value

    odl_schema = (
        f"{get_config_value(conf, RadiantConfigKeys.OPEN_DATA_CATALOG)}."
        f"{get_config_value(conf, RadiantConfigKeys.OPEN_DATA_DATABASE)}"
    )
    legacy_schema = (
        f"{get_config_value(conf, RadiantConfigKeys.ICEBERG_CATALOG)}."
        f"{get_config_value(conf, RadiantConfigKeys.ICEBERG_NAMESPACE)}"
    )
    return odl_schema, legacy_schema


def resolve_iceberg_source_tables(conf: dict | None = None) -> dict[str, str]:
    from radiant.tasks.data.radiant_tables import (
        ICEBERG_OPEN_DATA_CONTRACT_MAPPING,
        ICEBERG_OPEN_DATA_LEGACY_MAPPING,
        ICEBERG_OPEN_DATA_PRE_CONTRACT_MAPPING,
        get_open_data_contract_keys,
    )

    odl_schema, legacy_schema = _iceberg_schemas(conf)
    contract_keys = get_open_data_contract_keys(conf)

    tables = {
        key: (
            f"{odl_schema}.{table}"
            if key in contract_keys
            else f"{legacy_schema}.{ICEBERG_OPEN_DATA_PRE_CONTRACT_MAPPING[key]}"
        )
        for key, table in ICEBERG_OPEN_DATA_CONTRACT_MAPPING.items()
    }
    tables.update({key: f"{legacy_schema}.{table}" for key, table in ICEBERG_OPEN_DATA_LEGACY_MAPPING.items()})
    return tables


def list_iceberg_source_tables(conf: dict | None = None, keys: Iterable[str] | None = None) -> list[str]:
    tables = resolve_iceberg_source_tables(conf)
    if keys is None:
        return sorted(tables.values())

    keys = set(keys)
    unknown = sorted(keys - set(tables))
    if unknown:
        raise KeyError(f"not open-data mapping keys: {unknown}. Known: {sorted(tables)}")
    return sorted(tables[key] for key in keys)


def _tables_in(schema: str) -> set[str]:
    from airflow.hooks.base import BaseHook

    conn = BaseHook.get_connection("starrocks_conn")
    with conn.get_hook().get_conn().cursor() as cursor:
        cursor.execute(f"SHOW TABLES FROM {schema}")
        return {row[0] for row in cursor.fetchall()}


def _iceberg_groups(conf: dict | None) -> dict[str, tuple[str, set[str]]]:
    odl_schema, legacy_schema = _iceberg_schemas(conf)

    contract, legacy = set(), set()
    for relation in resolve_iceberg_source_tables(conf).values():
        schema, _, table = relation.rpartition(".")
        (contract if schema == odl_schema else legacy).add(table)

    groups = {}
    if contract:
        groups["OpenDataLake contract tables"] = (odl_schema, contract)
    if legacy:
        groups["Legacy Iceberg tables"] = (legacy_schema, legacy)
    return groups


def list_missing_open_data_tables(conf: dict | None = None) -> dict[str, list[str]]:
    from radiant.tasks.data.radiant_tables import (
        STARROCKS_OPEN_DATA_MAPPING,
        RadiantConfigKeys,
        get_config_value,
    )

    groups = _iceberg_groups(conf) | {
        "StarRocks target tables": (
            get_config_value(conf, RadiantConfigKeys.RADIANT_DATABASE),
            set(STARROCKS_OPEN_DATA_MAPPING.values()),
        ),
    }

    missing = {}
    for label, (schema, expected) in groups.items():
        absent = expected - _tables_in(schema)
        if absent:
            missing[label] = sorted(f"{schema}.{table}" for table in absent)
    return missing


def format_missing_tables(missing: dict[str, list[str]]) -> str:
    lines = [f"{sum(len(tables) for tables in missing.values())} table(s) the open-data refresh needs are missing:"]
    for label, tables in missing.items():
        lines.append(f"  {label}:")
        lines.extend(f"    - {table}" for table in tables)
    return "\n".join(lines)
