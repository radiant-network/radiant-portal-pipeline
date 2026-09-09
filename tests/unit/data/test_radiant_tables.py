from radiant.dags import DAGS_DIR
from radiant.tasks.data.radiant_tables import (
    ICEBERG_OPEN_DATA_CONTRACT_MAPPING,
    ICEBERG_OPEN_DATA_LEGACY_MAPPING,
    STARROCKS_RADIANT_BASE_MAPPING,
    STARROCKS_RADIANT_PER_TENANT_MAPPING,
    get_iceberg_open_data_mapping,
    get_starrocks_mapping,
)

# Pin RADIANT_TABLES_DATABASE in the conf so the assertions don't depend on the process environment.
# tenant_code is an explicit argument (never inferred from conf).
_SHARED = {"RADIANT_TABLES_DATABASE": "radiant"}


def test_no_tenant_routes_everything_to_shared_database():
    mapping = get_starrocks_mapping(_SHARED)
    # Legacy single-database behaviour: per-tenant tables fall back to the shared database.
    assert mapping["starrocks_germline_snv_occurrence"] == "radiant.germline__snv__occurrence"
    assert mapping["starrocks_snv_variant"] == "radiant.snv__variant"
    assert mapping["starrocks_snv_staging_variant"] == "radiant.snv__staging_variant"


def test_tenant_routes_per_tenant_tables_to_tenant_database():
    mapping = get_starrocks_mapping(_SHARED, tenant_code="chop")
    # Per-tenant tables land in <tenant>_db ...
    assert mapping["starrocks_germline_snv_occurrence"] == "chop_tenant.germline__snv__occurrence"
    assert mapping["starrocks_somatic_snv_occurrence"] == "chop_tenant.somatic__snv__occurrence"
    assert mapping["starrocks_exomiser"] == "chop_tenant.exomiser"
    assert mapping["starrocks_snv_variant"] == "chop_tenant.snv__variant"
    assert mapping["starrocks_snv_variant_partitioned"] == "chop_tenant.snv__variant_partitioned"
    # ... while base tables stay in RADIANT_DATABASE.
    assert mapping["starrocks_snv_staging_variant"] == "radiant.snv__staging_variant"
    assert mapping["starrocks_staging_sequencing_experiment"] == "radiant.staging_sequencing_experiment"


def test_tenant_database_name_uses_template():
    mapping = get_starrocks_mapping({**_SHARED, "RADIANT_TENANT_DB_TEMPLATE": "tenant_{tenant}"}, tenant_code="chop")
    assert mapping["starrocks_germline_snv_occurrence"] == "tenant_chop.germline__snv__occurrence"


def test_per_tenant_and_base_keys_route_consistently():
    mapping = get_starrocks_mapping(_SHARED, tenant_code="chop")
    for key in STARROCKS_RADIANT_PER_TENANT_MAPPING:
        assert mapping[key].startswith("chop_tenant.")
    for key in STARROCKS_RADIANT_BASE_MAPPING:
        assert mapping[key].startswith("radiant.")


def test_every_per_tenant_key_has_a_create_table_template():
    # `prepare_tenants_tables` derives the DDL filename from the mapping key and opens it unguarded, so a
    # key without a matching template fails tenant preparation for the whole DAG run. Catch it here instead.
    init_dir = DAGS_DIR / "sql" / "radiant" / "init"
    for key in STARROCKS_RADIANT_PER_TENANT_MAPPING:
        template = init_dir / f"{key.removeprefix('starrocks_')}_create_table.sql"
        assert template.is_file(), f"[{key}] has no create-table template at {template}"


# --- OpenDataLake open-data tables (design/SJRA-1811-opendatalake-integration.md)
# Pin every open-data key so the assertions don't depend on the process environment.
_OPEN_DATA = {
    "RADIANT_ICEBERG_CATALOG": "radiant_iceberg_catalog",
    "RADIANT_ICEBERG_NAMESPACE": "radiant",
    "RADIANT_OPEN_DATA_CATALOG": "odl_catalog",
    "RADIANT_OPEN_DATA_DATABASE": "opendatalake_qa",
    "RADIANT_OPEN_DATA_REF": "latest",
}


def test_contract_tables_resolve_to_the_open_data_catalog_pinned_to_the_ref():
    mapping = get_iceberg_open_data_mapping(_OPEN_DATA)
    assert mapping["iceberg_clinvar"] == ("odl_catalog.opendatalake_qa.clinvar_v1 VERSION AS OF 'latest'")
    assert mapping["iceberg_gnomad_joint"] == ("odl_catalog.opendatalake_qa.gnomad_joint_v1 VERSION AS OF 'latest'")


def test_legacy_tables_stay_on_the_radiant_catalog_with_no_ref():
    # No OpenDataLake contract exists for these, so they must not move or acquire a ref.
    mapping = get_iceberg_open_data_mapping(_OPEN_DATA)
    assert mapping["iceberg_ensembl_gene"] == "radiant_iceberg_catalog.radiant.ensembl_gene"
    assert mapping["iceberg_cosmic_gene_set"] == "radiant_iceberg_catalog.radiant.cosmic_gene_set"
    for key in ICEBERG_OPEN_DATA_LEGACY_MAPPING:
        assert "VERSION AS OF" not in mapping[key]


def test_an_empty_ref_reads_the_table_with_no_time_travel():
    # The escape hatch for a deployment that pins nothing; every other value names a tag or a branch.
    mapping = get_iceberg_open_data_mapping({**_OPEN_DATA, "RADIANT_OPEN_DATA_REF": ""})
    assert mapping["iceberg_clinvar"] == "odl_catalog.opendatalake_qa.clinvar_v1"


def test_a_dataset_version_can_be_pinned_instead_of_latest():
    mapping = get_iceberg_open_data_mapping({**_OPEN_DATA, "RADIANT_OPEN_DATA_REF": "20260715"})
    assert mapping["iceberg_clinvar"] == ("odl_catalog.opendatalake_qa.clinvar_v1 VERSION AS OF '20260715'")


def test_every_contract_table_carries_the_major_suffix():
    # The table name is `{table_prefix}_v{MAJOR}`: a missing suffix would silently read another table.
    for key, table in ICEBERG_OPEN_DATA_CONTRACT_MAPPING.items():
        assert table.split("_")[-1].startswith("v"), f"[{key}] {table} has no v{{MAJOR}} suffix"


def test_contract_and_legacy_families_do_not_overlap():
    assert not set(ICEBERG_OPEN_DATA_CONTRACT_MAPPING) & set(ICEBERG_OPEN_DATA_LEGACY_MAPPING)
