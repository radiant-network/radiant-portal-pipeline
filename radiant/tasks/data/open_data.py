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
    conf: dict | None = None, snapshots: dict[str, int | str | None] | None = None
) -> list[dict[str, str]]:
    """The rows P4 writes. Pure -- pass `snapshots` from `resolve_current_snapshots`, which reads
    the database; a caller that omits them records no snapshot and disables change detection for
    the next run."""
    from radiant.tasks.data.radiant_tables import RadiantConfigKeys, get_config_value

    odl_schema, _ = _iceberg_schemas(conf)
    ref = get_config_value(conf, RadiantConfigKeys.OPEN_DATA_REF)
    snapshots = snapshots or {}

    rows = []
    for key, relation in resolve_iceberg_source_tables(conf).items():
        schema, _, table = relation.rpartition(".")
        catalog, _, database = schema.partition(".")

        if schema == odl_schema:
            iceberg_ref = ref
            dataset_version = "" if ref in ("", "latest") else ref
        else:
            iceberg_ref = dataset_version = LEGACY

        snapshot = snapshots.get(key)
        rows.append(
            {
                "source_name": key.removeprefix("iceberg_"),
                "table_name": table,
                "catalog_name": catalog,
                "database_name": database,
                "iceberg_ref": iceberg_ref,
                "dataset_version": dataset_version,
                # NULL for a legacy source (no ref to resolve) and for a contract source whose
                # ref could not be read; `changed_sources` tells those two apart by `iceberg_ref`.
                "snapshot_id": "" if snapshot in (None, LEGACY) else str(snapshot),
            }
        )
    return sorted(rows, key=lambda row: row["source_name"])


# --- Change detection ------------------------------------------------------------------------
#
# OpenDataLake moves a `latest` tag on each publish, and `dataset_version` is NULL whenever the
# ref is a moving tag -- so the ledger cannot answer "did this source change?" on its own. The
# snapshot the ref resolves to can, and StarRocks exposes it: Iceberg metadata tables landed in
# StarRocks 3.4.1, and `<relation>$refs` carries `name`, `type` and `snapshot_id`.
# Docs: https://docs.starrocks.io/docs/data_source/catalog/iceberg/iceberg_meta_table/

LEGACY = "LEGACY"

# Which open-data sources each re-annotation branch actually reads, by `iceberg_*` mapping key.
# The branch names match the task groups in `reannotate_open_data.py`. Derived from the FROM/JOIN
# lists of the statements each group runs -- keep it in step with them.
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


def resolve_current_snapshots(conf: dict | None = None) -> dict[str, int | str | None]:
    """The snapshot each source's ref currently points at.

    Returns the literal `LEGACY` for a source held back on the Radiant catalog -- it is read
    without time travel, so it has no ref and cannot move during a refresh. Returns None when a
    contract source's ref cannot be resolved, which `changed_sources` treats as changed.
    """
    from radiant.tasks.data.radiant_tables import RadiantConfigKeys, get_config_value, get_open_data_contract_keys

    contract_keys = get_open_data_contract_keys(conf)
    ref = get_config_value(conf, RadiantConfigKeys.OPEN_DATA_REF)

    snapshots: dict[str, int | str | None] = {}
    for key, relation in resolve_iceberg_source_tables(conf).items():
        if key not in contract_keys:
            snapshots[key] = LEGACY
            continue
        try:
            rows = _query(f"SELECT snapshot_id FROM {relation}$refs WHERE name = %s", (ref,))
        except Exception as error:
            # A `$refs` read that raises -- a catalog hiccup, a StarRocks older than 3.4.1, a
            # relation that is not an Iceberg table -- must not take the run down. None reads as
            # "unknown", and `changed_sources` re-annotates on unknown.
            LOGGER.warning(f"Could not read {relation}$refs for ref {ref!r}: {error}. Treating {key} as changed.")
            snapshots[key] = None
            continue
        snapshots[key] = int(rows[0][0]) if rows and rows[0][0] is not None else None
    return snapshots


def last_recorded_snapshots(conf: dict | None = None) -> dict[str, tuple[str | None, int | None]]:
    """Per source, the `(iceberg_ref, snapshot_id)` the last recorded release was annotated against.

    An empty map on any read failure. The column arrived with this feature (migrations/
    SJRA-1950_*.sql), so a database that has not taken the migration yet answers with an error, and
    the honest reading of that is "nothing recorded" -- which re-annotates everything, exactly what
    the DAG did before the gate existed. P4's INSERT still fails loudly on the missing column, so
    this degrades the gate without hiding the migration.
    """
    from radiant.tasks.data.radiant_tables import get_radiant_mapping

    table = get_radiant_mapping(conf)["starrocks_open_data_release"]
    try:
        rows = _query(f"SELECT source_name, iceberg_ref, snapshot_id FROM {table}")
    except Exception as error:
        LOGGER.warning(f"Could not read {table}: {error}. Treating every source as changed.")
        return {}
    return {f"iceberg_{name}": (ref, int(snapshot) if snapshot is not None else None) for name, ref, snapshot in rows}


def changed_sources(conf: dict | None = None) -> set[str]:
    """Sources whose ref moved since the release the warehouse is currently annotated against.

    Unknown means changed. A source with no recorded row (a first run, or one added since the
    last one) and a contract source whose snapshot cannot be read both count as changed, so the
    gate fails open and re-annotates rather than silently skipping.
    """
    current = resolve_current_snapshots(conf)
    recorded = last_recorded_snapshots(conf)

    changed = set()
    for key, snapshot in current.items():
        if key not in recorded:
            changed.add(key)
            continue

        recorded_ref, recorded_snapshot = recorded[key]
        if snapshot == LEGACY:
            # Held back now. Only a change if it was on OpenDataLake last time -- a rollback
            # replaces the values just as surely as a publish does.
            if recorded_ref != LEGACY:
                changed.add(key)
        elif snapshot is None or snapshot != recorded_snapshot:
            changed.add(key)
    return changed


def branches_to_reannotate(conf: dict | None = None, changed: set[str] | None = None) -> dict[str, bool]:
    """Which re-annotation branches have a source that moved.

    Pass `changed` to reuse a set already computed -- resolving it costs one `$refs` read per
    contract source, and the caller usually wants to log it as well.
    """
    if changed is None:
        changed = changed_sources(conf)
    gates = {branch: bool(changed & sources) for branch, sources in REANNOTATION_SOURCES.items()}

    # `nb_snv` counts rows in `snv__variant`, which the variant chain rebuilds with INSERT
    # OVERWRITE -- so a variant rebuild forces a CNV rebuild even when no CNV source moved.
    # Same dependency the DAG's `insert_snv_variant >> cnv_occurrence` edge exists for.
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
