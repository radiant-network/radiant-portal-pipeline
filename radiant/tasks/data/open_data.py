def _iceberg_groups(conf: dict | None) -> dict[str, tuple[str, set[str]]]:
    from radiant.tasks.data.radiant_tables import (
        ICEBERG_OPEN_DATA_CONTRACT_MAPPING,
        ICEBERG_OPEN_DATA_LEGACY_MAPPING,
        ICEBERG_OPEN_DATA_PRE_CONTRACT_MAPPING,
        RadiantConfigKeys,
        get_config_value,
        get_open_data_contract_keys,
    )

    contract_keys = get_open_data_contract_keys(conf)
    contract = {table for key, table in ICEBERG_OPEN_DATA_CONTRACT_MAPPING.items() if key in contract_keys}
    fell_back = {table for key, table in ICEBERG_OPEN_DATA_PRE_CONTRACT_MAPPING.items() if key not in contract_keys}

    odl_schema = (
        f"{get_config_value(conf, RadiantConfigKeys.OPEN_DATA_CATALOG)}."
        f"{get_config_value(conf, RadiantConfigKeys.OPEN_DATA_DATABASE)}"
    )
    legacy_schema = (
        f"{get_config_value(conf, RadiantConfigKeys.ICEBERG_CATALOG)}."
        f"{get_config_value(conf, RadiantConfigKeys.ICEBERG_NAMESPACE)}"
    )

    groups = {}
    if contract:
        groups["OpenDataLake contract tables"] = (odl_schema, contract)
    groups["Legacy Iceberg tables"] = (legacy_schema, set(ICEBERG_OPEN_DATA_LEGACY_MAPPING.values()) | fell_back)
    return groups


def list_iceberg_source_tables(conf: dict | None = None) -> list[str]:
    return sorted(f"{schema}.{table}" for schema, tables in _iceberg_groups(conf).values() for table in tables)
