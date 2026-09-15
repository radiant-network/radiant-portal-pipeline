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

    odl_schema = (
        f"{get_config_value(conf, RadiantConfigKeys.OPEN_DATA_CATALOG)}."
        f"{get_config_value(conf, RadiantConfigKeys.OPEN_DATA_DATABASE)}"
    )
    legacy_schema = (
        f"{get_config_value(conf, RadiantConfigKeys.ICEBERG_CATALOG)}."
        f"{get_config_value(conf, RadiantConfigKeys.ICEBERG_NAMESPACE)}"
    )
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


def list_missing_open_data_tables(conf: dict | None = None) -> dict[str, list[str]]:
    """Every table the open-data refresh needs and StarRocks cannot see, grouped by cause.

    Returns a mapping of group label to sorted, fully-qualified names. An empty mapping means
    everything the refresh reads and writes is present.
    """
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
    """A message an operator can act on: what is absent, grouped by what would fix it."""
    lines = [f"{sum(len(tables) for tables in missing.values())} table(s) the open-data refresh needs are missing:"]
    for label, tables in missing.items():
        lines.append(f"  {label}:")
        lines.extend(f"    - {table}" for table in tables)
    return "\n".join(lines)
