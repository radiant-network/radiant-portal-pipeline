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
    """The rows the import writes. Pure -- pass `refs` from `resolve_current_refs`, which reads the
    database; a caller that omits them records no snapshot and disables change detection for the
    next run.

    `annotated` carries `reannotated_snapshot_id` forward. The row is a whole-row upsert on a
    PRIMARY KEY table, so a column left out is a column set back to NULL -- and
    `reannotated_snapshot_id` belongs to the re-annotation, which has no business being reset by an
    import.
    """
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
            # `$refs` names the version branch outright, including when the ref is the moving tag.
            # The ref itself is the floor: it is the answer when the ref already pins a branch, and
            # all there is when the read failed.
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
                # NULL for a legacy source (no ref to resolve) and for a contract source whose
                # ref could not be read; `changed_sources` tells those two apart by `iceberg_ref`.
                "imported_snapshot_id": _snapshot_literal(snapshot),
                # Not this writer's column -- carried through so the upsert does not blank it.
                "reannotated_snapshot_id": _snapshot_literal(annotated.get(key)),
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

# `open_data_release` carries two snapshots per source, because two things drift apart: what
# StarRocks holds and what the portal tables were annotated from. A standalone import moves the
# first without the second, so one column cannot answer both gates.
#
#   imported_snapshot_id     what the copies in StarRocks were loaded from  -- written by the import
#   reannotated_snapshot_id  what the portal tables were annotated against  -- written by P4
#
# P4 sets `reannotated_snapshot_id = imported_snapshot_id` rather than re-reading `$refs`: what the rebuilds
# annotate is whatever the import loaded, and a publish landing mid-run must not be stamped as
# annotated when only the older data was ever read.
IMPORTED_SNAPSHOT = "imported_snapshot_id"
ANNOTATED_SNAPSHOT = "reannotated_snapshot_id"


def _snapshot_literal(snapshot: int | str | None) -> str:
    """A snapshot as the insert template wants it: empty renders NULL. `LEGACY` is not a snapshot --
    a held-back source is read without time travel and has none."""
    return "" if snapshot in (None, LEGACY) else str(snapshot)


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


# OpenDataLake publishes each dataset version onto a branch of its own and moves the `latest` tag
# onto the newest one (radiant-open-datalake -- `WapLoader.publishVersionBranch`,
# `IcebergTable.LatestTag`). So the version is not a property to look up: it is the name of the
# branch sitting on the same snapshot as the ref, which is why one `$refs` read answers both
# "did it move?" and "which release is this?".
#
# `main` is left empty by the loader and `audit_<version>` is the staging branch it drops after
# publishing, so neither is ever the answer.
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
    """Per source, the snapshot its ref resolves to and the dataset version that snapshot is.

    `{"snapshot": ..., "dataset_version": ...}` per mapping key. The snapshot is the literal
    `LEGACY` for a source held back on the Radiant catalog -- it is read without time travel, so it
    has no ref and cannot move during a refresh -- and None when a contract source's ref cannot be
    resolved, which `changed_sources` treats as changed.

    Plain dicts rather than a tuple or a dataclass because this crosses an XCom.
    """
    from radiant.tasks.data.radiant_tables import RadiantConfigKeys, get_config_value, get_open_data_contract_keys

    contract_keys = get_open_data_contract_keys(conf)
    ref = get_config_value(conf, RadiantConfigKeys.OPEN_DATA_REF)

    refs: dict[str, dict] = {}
    for key, relation in resolve_iceberg_source_tables(conf).items():
        if key not in contract_keys:
            refs[key] = {"snapshot": LEGACY, "dataset_version": LEGACY}
            continue
        try:
            # Every ref, not just `name = ref`: the tag gives the snapshot, and a branch on that
            # same snapshot gives the version. One read, two answers.
            rows = _query(f"SELECT name, type, snapshot_id FROM {relation}$refs")
        except Exception as error:
            # A `$refs` read that raises -- a catalog hiccup, a StarRocks older than 3.4.1, a
            # relation that is not an Iceberg table -- must not take the run down. None reads as
            # "unknown", and `changed_sources` re-annotates on unknown.
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
    """Per source, the `(iceberg_ref, <column>)` last recorded -- `column` picking which of the two
    snapshots is meant.

    An empty map on any read failure. Both columns arrived with this feature (migrations/
    SJRA-1950_*.sql), so a database that has not taken the migration yet answers with an error, and
    the honest reading of that is "nothing recorded" -- which re-imports and re-annotates
    everything, exactly what the DAGs did before the gates existed. The writes still fail loudly on
    the missing column, so this degrades the gates without hiding the migration.
    """
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
    """Just the `reannotated_snapshot_id` per source, for the import to carry through its upsert."""
    return {key: snapshot for key, (_, snapshot) in last_recorded_snapshots(conf, column=ANNOTATED_SNAPSHOT).items()}


def changed_sources(
    conf: dict | None = None,
    column: str = ANNOTATED_SNAPSHOT,
    current: dict[str, int | str | None] | None = None,
) -> set[str]:
    """Sources whose ref moved since `column` last recorded them.

    Unknown means changed. A source with no recorded row (a first run, or one added since the
    last one) and a contract source whose snapshot cannot be read both count as changed, so the
    gate fails open and re-annotates rather than silently skipping.

    Pass `current` to reuse snapshots already resolved. The import gate does, because the values it
    gates on are the ones it must record afterwards -- resolving twice is how a publish landing
    mid-run gets recorded as imported when it was not.
    """
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
