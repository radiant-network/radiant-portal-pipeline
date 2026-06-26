from radiant.tasks.data.radiant_tables import (
    STARROCKS_RADIANT_BASE_MAPPING,
    STARROCKS_RADIANT_PER_TENANT_MAPPING,
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


def test_tenant_routes_per_tenant_tables_to_tenant_database():
    mapping = get_starrocks_mapping(_SHARED, tenant_code="chop")
    # Per-tenant tables land in <tenant>_db ...
    assert mapping["starrocks_germline_snv_occurrence"] == "chop_db.germline__snv__occurrence"
    assert mapping["starrocks_somatic_snv_occurrence"] == "chop_db.somatic__snv__occurrence"
    assert mapping["starrocks_exomiser"] == "chop_db.exomiser"
    # ... while base tables (including the global variant catalog) stay in RADIANT_DATABASE.
    assert mapping["starrocks_snv_variant"] == "radiant.snv__variant"
    assert mapping["starrocks_snv_variant_partitioned"] == "radiant.snv__variant_partitioned"
    assert mapping["starrocks_staging_sequencing_experiment"] == "radiant.staging_sequencing_experiment"


def test_tenant_database_name_uses_template():
    mapping = get_starrocks_mapping({**_SHARED, "RADIANT_TENANT_DB_TEMPLATE": "tenant_{tenant}"}, tenant_code="chop")
    assert mapping["starrocks_germline_snv_occurrence"] == "tenant_chop.germline__snv__occurrence"


def test_per_tenant_and_base_keys_route_consistently():
    mapping = get_starrocks_mapping(_SHARED, tenant_code="chop")
    for key in STARROCKS_RADIANT_PER_TENANT_MAPPING:
        assert mapping[key].startswith("chop_db.")
    for key in STARROCKS_RADIANT_BASE_MAPPING:
        assert mapping[key].startswith("radiant.")
