import logging
from collections.abc import Iterable

LOGGER = logging.getLogger(__name__)


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
        ICEBERG_OPEN_DATA_PRE_CONTRACT_MAPPING,
        get_open_data_contract_keys,
    )

    odl_schema, legacy_schema = _iceberg_schemas(conf)
    contract_keys = get_open_data_contract_keys(conf)

    return {
        key: (
            f"{odl_schema}.{table}"
            if key in contract_keys
            else f"{legacy_schema}.{ICEBERG_OPEN_DATA_PRE_CONTRACT_MAPPING[key]}"
        )
        for key, table in ICEBERG_OPEN_DATA_CONTRACT_MAPPING.items()
        # Held back with no pre-contract table (`clinvar_rcv`): no Iceberg table anywhere, so nothing
        # to refresh and nothing to report as missing.
        if key in contract_keys or key in ICEBERG_OPEN_DATA_PRE_CONTRACT_MAPPING
    }


def list_iceberg_source_tables(conf: dict | None = None, keys: Iterable[str] | None = None) -> list[str]:
    tables = resolve_iceberg_source_tables(conf)
    if keys is None:
        return sorted(tables.values())

    keys = set(keys)
    unknown = sorted(keys - set(tables))
    if unknown:
        raise KeyError(f"not open-data mapping keys: {unknown}. Known: {sorted(tables)}")
    return sorted(tables[key] for key in keys)


def build_open_data_release_rows(
    conf: dict | None = None,
    refs: dict[str, dict] | None = None,
    annotated: dict[str, int | str | None] | None = None,
) -> list[dict[str, str]]:
    from radiant.tasks.data.radiant_tables import RadiantConfigKeys, get_config_value

    odl_schema, _ = _iceberg_schemas(conf)
    ref = get_config_value(conf, RadiantConfigKeys.OPEN_DATA_REF)
    refs = refs or {}
    annotated = annotated or {}

    rows = []
    for key, relation in resolve_iceberg_source_tables(conf).items():
        schema, _, table = relation.rpartition(".")
        catalog, _, database = schema.partition(".")
        state = refs.get(key) or {}

        if schema == odl_schema:
            iceberg_ref = ref
            dataset_version = state.get("dataset_version") or ("" if ref in ("", "latest") else ref)
        else:
            iceberg_ref = dataset_version = LEGACY

        snapshot = state.get("snapshot")
        rows.append(
            {
                "source_name": key.removeprefix("iceberg_"),
                "table_name": table,
                "catalog_name": catalog,
                "database_name": database,
                "iceberg_ref": iceberg_ref,
                "dataset_version": dataset_version,
                "imported_snapshot_id": _snapshot_literal(snapshot),
                "reannotated_snapshot_id": _snapshot_literal(annotated.get(key)),
            }
        )
    return sorted(rows, key=lambda row: row["source_name"])


LEGACY = "LEGACY"
IMPORTED_SNAPSHOT = "imported_snapshot_id"
ANNOTATED_SNAPSHOT = "reannotated_snapshot_id"


def _snapshot_literal(snapshot: int | str | None) -> str:
    return "" if snapshot in (None, LEGACY) else str(snapshot)

REANNOTATION_SOURCES = {
    # snv_staging_variant_reannotate.sql:42-47
    "snv_variant": {
        "iceberg_1000_genomes",
        "iceberg_clinvar",
        "iceberg_dbsnp",
        "iceberg_gnomad_joint",
        "iceberg_omim_gene_set",
        "iceberg_topmed_bravo",
    },
    # snv_consequence_reannotate.sql:66-72
    "snv_consequence": {
        "iceberg_dbnsfp",
        "iceberg_gnomad_constraint",
        "iceberg_spliceai",
    },
    # {germline,somatic}_cnv_occurrence_reannotate_partition.sql -- cytoband is a broker load with
    # no Iceberg source at all, so it cannot be watched here and never gates anything.
    "cnv_occurrence": {
        "iceberg_ensembl_gene",
        "iceberg_gnomad_sv",
    },
}


def _query(sql: str, params: tuple = ()) -> list[tuple]:
    from airflow.hooks.base import BaseHook

    conn = BaseHook.get_connection("starrocks_conn")
    with conn.get_hook().get_conn().cursor() as cursor:
        cursor.execute(sql, params)
        return list(cursor.fetchall())

MAIN_BRANCH = "main"
AUDIT_BRANCH_PREFIX = "audit_"

def _dataset_version_of(rows: list[tuple], ref: str, snapshot: int | None) -> str:
    """The version branch `ref` resolves to, given every row of one table's `$refs`."""
    if snapshot is None:
        return ""

    named = [(str(name), str(kind or ""), snap) for name, kind, snap in rows]
    # A ref that is itself a branch already names its version -- a pinned run.
    if any(name == ref and kind.upper() == "BRANCH" for name, kind, _ in named):
        return ref

    candidates = sorted(
        name
        for name, kind, snap in named
        if kind.upper() == "BRANCH"
        and snap is not None
        and int(snap) == snapshot
        and name != ref
        and name != MAIN_BRANCH
        and not name.startswith(AUDIT_BRANCH_PREFIX)
    )
    if len(candidates) > 1:
        LOGGER.warning(f"Snapshot {snapshot} carries several version branches {candidates}; recording the first.")
    return candidates[0] if candidates else ""


def resolve_current_refs(conf: dict | None = None) -> dict[str, dict]:
    from radiant.tasks.data.radiant_tables import RadiantConfigKeys, get_config_value, get_open_data_contract_keys

    contract_keys = get_open_data_contract_keys(conf)
    ref = get_config_value(conf, RadiantConfigKeys.OPEN_DATA_REF)

    refs: dict[str, dict] = {}
    for key, relation in resolve_iceberg_source_tables(conf).items():
        if key not in contract_keys:
            refs[key] = {"snapshot": LEGACY, "dataset_version": LEGACY}
            continue
        try:
            rows = _query(f"SELECT name, type, snapshot_id FROM {relation}$refs")
        except Exception as error:
            LOGGER.warning(f"Could not read {relation}$refs: {error}. Treating {key} as changed.")
            refs[key] = {"snapshot": None, "dataset_version": ""}
            continue

        resolved = next((snap for name, _, snap in rows if str(name) == ref and snap is not None), None)
        snapshot = int(resolved) if resolved is not None else None
        refs[key] = {"snapshot": snapshot, "dataset_version": _dataset_version_of(rows, ref, snapshot)}
    return refs


def resolve_current_snapshots(
    conf: dict | None = None, refs: dict[str, dict] | None = None
) -> dict[str, int | str | None]:
    """Just the snapshot per source. Pass `refs` to reuse a resolution already made."""
    refs = refs if refs is not None else resolve_current_refs(conf)
    return {key: state.get("snapshot") for key, state in refs.items()}


def last_recorded_snapshots(
    conf: dict | None = None, column: str = ANNOTATED_SNAPSHOT
) -> dict[str, tuple[str | None, int | None]]:
    from radiant.tasks.data.radiant_tables import get_radiant_mapping

    if column not in (IMPORTED_SNAPSHOT, ANNOTATED_SNAPSHOT):
        raise ValueError(f"not a snapshot column: {column!r}")

    table = get_radiant_mapping(conf)["starrocks_open_data_release"]
    try:
        rows = _query(f"SELECT source_name, iceberg_ref, {column} FROM {table}")
    except Exception as error:
        LOGGER.warning(f"Could not read {table}.{column}: {error}. Treating every source as changed.")
        return {}
    return {f"iceberg_{name}": (ref, int(snapshot) if snapshot is not None else None) for name, ref, snapshot in rows}


def annotated_snapshots(conf: dict | None = None) -> dict[str, int | str | None]:
    return {key: snapshot for key, (_, snapshot) in last_recorded_snapshots(conf, column=ANNOTATED_SNAPSHOT).items()}


def changed_sources(
    conf: dict | None = None,
    column: str = ANNOTATED_SNAPSHOT,
    current: dict[str, int | str | None] | None = None,
) -> set[str]:
    if current is None:
        current = resolve_current_snapshots(conf)
    recorded = last_recorded_snapshots(conf, column=column)

    changed = set()
    for key, snapshot in current.items():
        if key not in recorded:
            changed.add(key)
            continue

        recorded_ref, recorded_snapshot = recorded[key]
        if snapshot == LEGACY:
            if recorded_ref != LEGACY:
                changed.add(key)
        elif snapshot is None or snapshot != recorded_snapshot:
            changed.add(key)
    return changed


def branches_to_reannotate(conf: dict | None = None, changed: set[str] | None = None) -> dict[str, bool]:
    if changed is None:
        changed = changed_sources(conf)
    gates = {branch: bool(changed & sources) for branch, sources in REANNOTATION_SOURCES.items()}
    gates["cnv_occurrence"] = gates["cnv_occurrence"] or gates["snv_variant"]
    return gates


def _tables_in(schema: str) -> set[str]:
    return {row[0] for row in _query(f"SHOW TABLES FROM {schema}")}


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
