import os
from enum import Enum

from radiant.dags import NAMESPACE

OPEN_DATA_ALL_LEGACY = "*"  # "*" targets all tables


class RadiantConfigKeys(Enum):
    ICEBERG_CATALOG = ("RADIANT_ICEBERG_CATALOG", "radiant_iceberg_catalog")
    ICEBERG_NAMESPACE = ("RADIANT_ICEBERG_NAMESPACE", "radiant")
    RADIANT_DATABASE = ("RADIANT_TABLES_DATABASE", "radiant")
    RADIANT_TENANT_DB_TEMPLATE = ("RADIANT_TENANT_DB_TEMPLATE", "{tenant}_tenant")
    CLINICAL_CATALOG = ("RADIANT_CLINICAL_CATALOG", "radiant_jdbc")
    CLINICAL_DATABASE = ("RADIANT_CLINICAL_DATABASE", "public")
    OPEN_DATA_CATALOG = ("RADIANT_OPEN_DATA_CATALOG", "opendatalake_catalog")
    OPEN_DATA_DATABASE = ("RADIANT_OPEN_DATA_DATABASE", "reference")
    OPEN_DATA_REF = ("RADIANT_OPEN_DATA_REF", "latest")
    OPEN_DATA_USE_LEGACY_TABLES = ("RADIANT_OPEN_DATA_USE_LEGACY_TABLES", OPEN_DATA_ALL_LEGACY)

    @property
    def env_key(self):
        return self.value[0]

    @property
    def default(self):
        return self.value[1]


def get_config_value(conf, config: RadiantConfigKeys):
    return (conf or {}).get(config.env_key, os.getenv(config.env_key, config.default))


STARROCKS_COLOCATE_GROUP_MAPPING = {"colocate_query_group": f"{NAMESPACE}.query_group"}

# --- Clinical tables
CLINICAL_MAPPING = {
    "clinical_case": "`cases`",
    "clinical_case_has_sequencing_experiment": "case_has_sequencing_experiment",
    "clinical_analysis_catalog": "analysis_catalog",
    "clinical_sequencing_experiment": "sequencing_experiment",
    "clinical_task_context": "task_context",
    "clinical_task": "task",
    "clinical_sample": "sample",
    "clinical_patient": "patient",
    "clinical_family": "family",
    "clinical_document": "document",
    "clinical_task_has_document": "task_has_document",
    "clinical_organization": "organization",
    "clinical_project": "project",
    "clinical_obs_categorical": "obs_categorical",
    "clinical_panel": "panel",
    "clinical_obs_string": "obs_string",
    "clinical_family_history": "family_history",
}

# --- Iceberg tables
ICEBERG_RADIANT_MAPPING = {
    "iceberg_germline_cnv_occurrence": "germline_cnv_occurrence",
    "iceberg_germline_snv_occurrence": "germline_snv_occurrence",
    "iceberg_snv_variant": "snv_variant",
    "iceberg_snv_consequence": "snv_consequence",
    "iceberg_somatic_cnv_occurrence": "somatic_cnv_occurrence",
    "iceberg_somatic_snv_occurrence": "somatic_snv_occurrence",
}

ICEBERG_OPEN_DATA_CONTRACT_MAPPING = {
    "iceberg_1000_genomes": "1000_genomes_v1",
    "iceberg_clinvar": "clinvar_v1",
    "iceberg_dbnsfp": "dbnsfp_v1",
    "iceberg_dbsnp": "dbsnp_v1",
    "iceberg_ddd_gene_set": "ddd_v1",
    "iceberg_gnomad_constraint": "gnomad_constraint_v1",
    "iceberg_gnomad_joint": "gnomad_joint_v1",
    "iceberg_gnomad_sv": "gnomad_sv_v1",
    "iceberg_hpo_gene_set": "hpo_genes_v1",
    "iceberg_hpo_term": "hpo_terms_v1",
    "iceberg_mondo_term": "mondo_v1",
    "iceberg_omim_gene_set": "omim_v1",
    "iceberg_orphanet_gene_set": "orphanet_v1",
    "iceberg_spliceai": "spliceai_v1",
    "iceberg_topmed_bravo": "topmed_bravo_v1",
}

ICEBERG_OPEN_DATA_LEGACY_MAPPING = {
    "iceberg_ensembl_gene": "ensembl_gene",
    "iceberg_ensembl_exon_by_gene": "ensembl_exon_by_gene",
    "iceberg_cosmic_gene_set": "cosmic_gene_set",
}

ICEBERG_OPEN_DATA_PRE_CONTRACT_MAPPING = {
    "iceberg_1000_genomes": "1000_genomes",
    "iceberg_clinvar": "clinvar",
    "iceberg_dbnsfp": "dbnsfp",
    "iceberg_dbsnp": "dbsnp",
    "iceberg_ddd_gene_set": "ddd_gene_set",
    "iceberg_gnomad_constraint": "gnomad_constraint_v_2_1_1",
    "iceberg_gnomad_joint": "gnomad_genomes_v3",
    "iceberg_gnomad_sv": "gnomad_sv",
    "iceberg_hpo_gene_set": "hpo_gene_set",
    "iceberg_hpo_term": "hpo_term",
    "iceberg_mondo_term": "mondo_term",
    "iceberg_omim_gene_set": "omim_gene_set",
    "iceberg_orphanet_gene_set": "orphanet_gene_set",
    "iceberg_spliceai": "spliceai_enriched",
    "iceberg_topmed_bravo": "topmed_bravo",
}

IS_CONTRACT_SUFFIX = "_is_contract"


def contract_table_prefix(table: str) -> str:
    """`ddd_v1` -> `ddd`. The name OpenDataLake's `contracts.yml` calls the source."""
    return table.rsplit("_v", 1)[0]


ICEBERG_CATALOG_DATABASE = {
    "iceberg_catalog": os.getenv("RADIANT_ICEBERG_CATALOG", "radiant_iceberg_catalog"),
    "iceberg_database": os.getenv("RADIANT_ICEBERG_NAMESPACE", "radiant"),
}

STARROCKS_RADIANT_BASE_MAPPING = {
    "starrocks_staging_sequencing_experiment": "staging_sequencing_experiment",
    "starrocks_staging_external_sequencing_experiment": "staging_external_sequencing_experiment",
    "starrocks_staging_sequencing_experiment_delta": "staging_sequencing_experiment_delta",
    "starrocks_variant_lookup": "variant_lookup",
    # Audit log of which OpenDataLake release each re-annotation ran against (SJRA-1811 P4).
    # Shared, like the open-data tables it describes.
    "starrocks_open_data_release": "open_data_release",
    "starrocks_snv_consequence": "snv__consequence",
    "starrocks_snv_consequence_filter": "snv__consequence_filter",
    "starrocks_snv_consequence_filter_partitioned": "snv__consequence_filter_partitioned",
    "starrocks_snv_tmp_variant": "snv__tmp_variant",
    "starrocks_snv_staging_variant": "snv__staging_variant",
    # Tenant partitioned staging tables
    "starrocks_staging_exomiser": "raw_exomiser",
    "starrocks_germline_snv_staging_variant_frequency": "germline__snv__staging_variant_frequency_part",
    "starrocks_somatic_snv_staging_variant_frequency": "somatic__snv__staging_variant_frequency_part",
}

STARROCKS_RADIANT_PER_TENANT_MAPPING = {
    "starrocks_exomiser": "exomiser",
    "starrocks_germline_cnv_occurrence": "germline__cnv__occurrence",
    "starrocks_germline_snv_occurrence": "germline__snv__occurrence",
    "starrocks_germline_snv_variant_frequency": "germline__snv__variant_frequency",
    "starrocks_snv_variant": "snv__variant",
    "starrocks_snv_variant_partitioned": "snv__variant_partitioned",
    "starrocks_somatic_cnv_occurrence": "somatic__cnv__occurrence",
    "starrocks_somatic_snv_occurrence": "somatic__snv__occurrence",
    "starrocks_somatic_snv_variant_frequency": "somatic__snv__variant_frequency",
}

STARROCKS_RADIANT_MAPPING = STARROCKS_RADIANT_BASE_MAPPING | STARROCKS_RADIANT_PER_TENANT_MAPPING


STARROCKS_OPEN_DATA_MAPPING = {
    "starrocks_1000_genomes": "1000_genomes",
    "starrocks_clinvar": "clinvar",
    "starrocks_dbnsfp": "dbnsfp",
    "starrocks_dbsnp": "dbsnp",
    "starrocks_gnomad_genomes_v3": "gnomad_genomes_v3",
    "starrocks_spliceai": "spliceai",
    "starrocks_topmed_bravo": "topmed_bravo",
    "starrocks_gnomad_constraint": "gnomad_constraint",
    "starrocks_omim_gene_panel": "omim_gene_panel",
    "starrocks_hpo_gene_panel": "hpo_gene_panel",
    "starrocks_ensembl_gene": "ensembl_gene",
    "starrocks_ensembl_exon_by_gene": "ensembl_exon_by_gene",
    "starrocks_cytoband": "cytoband",
    "starrocks_hpo_term": "hpo_term",
    "starrocks_mondo_term": "mondo_term",
    "starrocks_orphanet_gene_panel": "orphanet_gene_panel",
    "starrocks_cosmic_gene_panel": "cosmic_gene_panel",
    "starrocks_ddd_gene_panel": "ddd_gene_panel",
    "starrocks_clinvar_rcv_summary": "clinvar_rcv_summary",
    "starrocks_raw_clinvar_rcv_summary": "raw_clinvar_rcv_summary",
}

CLINICAL_TRANSFORM_LAYER_MAPPING = {
    "starrocks_patient_access": "patient_access",
    "starrocks_brim": "brim",
}


def get_iceberg_radiant_mapping(conf=None) -> dict:
    _catalog = get_config_value(conf, RadiantConfigKeys.ICEBERG_CATALOG)
    _database = get_config_value(conf, RadiantConfigKeys.ICEBERG_NAMESPACE)
    return {key: f"{_catalog}.{_database}.{value}" for key, value in ICEBERG_RADIANT_MAPPING.items()}


def _open_data_relation(catalog: str, database: str, table: str, ref: str) -> str:
    if not ref:
        # An undefined `ref` defaults to `main`, which should never be used
        raise ValueError(
            f"{RadiantConfigKeys.OPEN_DATA_REF.env_key} is empty. OpenDataLake publishes on refs and leaves "
            f"`main` empty, so an unpinned read of `{table}` returns no rows. Set it to a ref "
            f"(`{RadiantConfigKeys.OPEN_DATA_REF.default}`, or a dataset_version to pin one release), or "
            f"hold the source back with {RadiantConfigKeys.OPEN_DATA_USE_LEGACY_TABLES.env_key}."
        )
    return f"{catalog}.{database}.{table} VERSION AS OF '{ref}'"


def _open_data_source_aliases() -> dict[str, str]:
    aliases = {contract_table_prefix(table): key for key, table in ICEBERG_OPEN_DATA_CONTRACT_MAPPING.items()}
    aliases.update({table: key for key, table in ICEBERG_OPEN_DATA_PRE_CONTRACT_MAPPING.items()})
    return aliases


def get_open_data_legacy_keys(conf=None) -> set[str]:
    from radiant.dags import parse_list

    raw = get_config_value(conf, RadiantConfigKeys.OPEN_DATA_USE_LEGACY_TABLES)
    if raw.strip() == OPEN_DATA_ALL_LEGACY:
        return set(ICEBERG_OPEN_DATA_CONTRACT_MAPPING)

    names = parse_list(raw)
    if not names:
        return set()

    aliases = _open_data_source_aliases()
    unknown = sorted({name for name in names if name not in aliases})
    if unknown:
        raise ValueError(
            f"{RadiantConfigKeys.OPEN_DATA_USE_LEGACY_TABLES.env_key} names unknown sources: {unknown}. "
            f"Known: {sorted(set(aliases))}"
        )
    return {aliases[name] for name in names}


def get_open_data_contract_keys(conf=None) -> set[str]:
    return set(ICEBERG_OPEN_DATA_CONTRACT_MAPPING) - get_open_data_legacy_keys(conf)


def get_iceberg_open_data_mapping(conf=None) -> dict:
    _legacy_catalog = get_config_value(conf, RadiantConfigKeys.ICEBERG_CATALOG)
    _legacy_database = get_config_value(conf, RadiantConfigKeys.ICEBERG_NAMESPACE)
    _catalog = get_config_value(conf, RadiantConfigKeys.OPEN_DATA_CATALOG)
    _database = get_config_value(conf, RadiantConfigKeys.OPEN_DATA_DATABASE)
    _ref = get_config_value(conf, RadiantConfigKeys.OPEN_DATA_REF)
    _contract_keys = get_open_data_contract_keys(conf)

    def _legacy(table: str) -> str:
        return f"{_legacy_catalog}.{_legacy_database}.{table}"

    mapping = {
        key: (
            _open_data_relation(_catalog, _database, value, _ref)
            if key in _contract_keys
            else _legacy(ICEBERG_OPEN_DATA_PRE_CONTRACT_MAPPING[key])
        )
        for key, value in ICEBERG_OPEN_DATA_CONTRACT_MAPPING.items()
    }
    mapping.update({key: _legacy(value) for key, value in ICEBERG_OPEN_DATA_LEGACY_MAPPING.items()})
    mapping.update(
        {
            f"{key}{IS_CONTRACT_SUFFIX}": ("true" if key in _contract_keys else "")
            for key in ICEBERG_OPEN_DATA_CONTRACT_MAPPING
        }
    )
    return mapping


def get_iceberg_tables(conf=None) -> dict:
    return {
        **get_iceberg_radiant_mapping(conf),
        **get_iceberg_open_data_mapping(conf),
    }


def _resolve_radiant_databases(conf=None, tenant_code=None) -> tuple[str, str]:
    base_db = get_config_value(conf, RadiantConfigKeys.RADIANT_DATABASE)
    if not tenant_code:
        return base_db, base_db

    template = get_config_value(conf, RadiantConfigKeys.RADIANT_TENANT_DB_TEMPLATE)
    return base_db, template.format(tenant=tenant_code)


def get_starrocks_mapping(conf=None, tenant_code=None) -> dict:
    base_db, tenant_db = _resolve_radiant_databases(conf, tenant_code)
    base_tables = STARROCKS_RADIANT_BASE_MAPPING | STARROCKS_OPEN_DATA_MAPPING | CLINICAL_TRANSFORM_LAYER_MAPPING

    mapping = {key: f"{base_db}.{value}" for key, value in base_tables.items()}
    mapping.update({key: f"{tenant_db}.{value}" for key, value in STARROCKS_RADIANT_PER_TENANT_MAPPING.items()})

    return mapping


def get_clinical_mapping(conf=None) -> dict:
    _catalog = get_config_value(conf, RadiantConfigKeys.CLINICAL_CATALOG)
    _database = get_config_value(conf, RadiantConfigKeys.CLINICAL_DATABASE)
    return {key: f"{_catalog}.{_database}.{value}" for key, value in CLINICAL_MAPPING.items()}


def get_radiant_mapping(conf=None, tenant_code=None) -> dict:
    namespace = get_config_value(conf, RadiantConfigKeys.ICEBERG_NAMESPACE)
    namespace = f"{namespace}_" if namespace else "germline__snv__"
    mapping = {
        key: f"{namespace}{value}"
        for key, value in {
            **STARROCKS_COLOCATE_GROUP_MAPPING,
        }.items()
    }
    mapping.update(get_starrocks_mapping(conf=conf, tenant_code=tenant_code))
    mapping.update(get_iceberg_tables(conf))
    mapping.update(get_clinical_mapping(conf))
    return mapping
