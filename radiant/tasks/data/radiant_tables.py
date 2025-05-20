import os

from radiant.dags import NAMESPACE

RADIANT_TABLES_PREFIX_ENV_KEY = "RADIANT_TABLES_NAMESPACE"
ICEBERG_CATALOG_ENV_KEY = "RADIANT_ICEBERG_CATALOG"
ICEBERG_DATABASE_ENV_KEY = "RADIANT_ICEBERG_DATABASE"

STARROCKS_COLOCATE_GROUP_MAPPING = {"colocate_query_group": f"{NAMESPACE}.query_group"}

GERMLINE_SNV_NAMESPACE_STARROCKS_PREFIX = "germline__snv__"


# --- Iceberg tables

ICEBERG_GERMLINE_SNV_MAPPING = {
    "iceberg_consequences": "germline_snv_consequences",
    "iceberg_occurrences": "germline_snv_occurrences",
    "iceberg_variants": "germline_snv_variants",
}

ICEBERG_OPEN_DATA_MAPPING = {
    "iceberg_1000_genomes": "1000_genomes",
    "iceberg_clinvar": "clinvar",
    "iceberg_dbnsfp": "dbnsfp",
    "iceberg_gnomad_genomes_v3": "gnomad_genomes_v3",
    "iceberg_spliceai": "spliceai_enriched",
    "iceberg_topmed_bravo": "topmed_bravo",
    "iceberg_gnomad_constraints": "gnomad_constraint_v_2_1_1",
    "iceberg_omim_gene_set": "omim_gene_set",
    "iceberg_hpo_gene_set": "hpo_gene_set",
    "iceberg_orphanet_gene_set": "orphanet_gene_set",
    "iceberg_cosmic_gene_set": "cosmic_gene_set",
    "iceberg_ddd_gene_set": "ddd_gene_set",
}

ICEBERG_CATALOG_DATABASE = {
    "iceberg_catalog": os.getenv("RADIANT_ICEBERG_CATALOG", "radiant_iceberg_catalog"),
    "iceberg_database": os.getenv("RADIANT_ICEBERG_DATABASE", "radiant"),
}


# --- StarRocks tables

STARROCKS_COMMON_MAPPING = {
    "starrocks_sequencing_experiments": "sequencing_experiments",
    "starrocks_variants_lookup": "variants_lookup",
}

STARROCKS_GERMLINE_SNV_MAPPING = {
    "starrocks_consequences": "consequences",
    "starrocks_consequences_filter": "consequences_filter",
    "starrocks_consequences_filter_partitioned": "consequences_filter_partitioned",
    "starrocks_occurrences": "occurrences",
    "starrocks_tmp_variants": "tmp_variants",
    "starrocks_variants": "variants",
    "starrocks_variants_frequencies": "variants_frequencies",
    "starrocks_variants_partitioned": "variants_partitioned",
    "starrocks_staging_variants": "staging_variants",
    "starrocks_staging_variants_frequencies": "staging_variants_frequencies_part",
}

STARROCKS_OPEN_DATA_MAPPING = {
    "starrocks_1000_genomes": "1000_genomes",
    "starrocks_clinvar": "clinvar",
    "starrocks_dbnsfp": "dbnsfp",
    "starrocks_gnomad_genomes_v3": "gnomad_genomes_v3",
    "starrocks_spliceai": "spliceai",
    "starrocks_topmed_bravo": "topmed_bravo",
    "starrocks_gnomad_constraints": "gnomad_constraints",
    "starrocks_omim_gene_panel": "omim_gene_panel",
    "starrocks_hpo_gene_panel": "hpo_gene_panel",
    "starrocks_orphanet_gene_panel": "orphanet_gene_panel",
    "starrocks_cosmic_gene_panel": "cosmic_gene_panel",
    "starrocks_ddd_gene_panel": "ddd_gene_panel",
}


def get_iceberg_germline_snv_mapping() -> dict:
    return {
        key: f"{ICEBERG_CATALOG_DATABASE['iceberg_catalog']}.{ICEBERG_CATALOG_DATABASE['iceberg_database']}.{value}"
        for key, value in ICEBERG_GERMLINE_SNV_MAPPING.items()
    }


def get_iceberg_open_data_mapping() -> dict:
    _catalog = os.getenv(ICEBERG_CATALOG_ENV_KEY, ICEBERG_CATALOG_DATABASE["iceberg_catalog"])
    _database = os.getenv(ICEBERG_DATABASE_ENV_KEY, ICEBERG_CATALOG_DATABASE["iceberg_database"])
    return {
        key: f"{ICEBERG_CATALOG_DATABASE['iceberg_catalog']}.{_database}.{value}"
        for key, value in ICEBERG_OPEN_DATA_MAPPING.items()
    }


def get_iceberg_tables() -> dict:
    return {
        **get_iceberg_germline_snv_mapping(),
        **get_iceberg_open_data_mapping(),
    }


def get_starrocks_germline_snv_mapping() -> dict:
    namespace = os.environ.get(RADIANT_TABLES_PREFIX_ENV_KEY)
    namespace = f"{namespace}_" if namespace else GERMLINE_SNV_NAMESPACE_STARROCKS_PREFIX
    return {key: f"{namespace}{value}" for key, value in STARROCKS_GERMLINE_SNV_MAPPING.items()}


def get_starrocks_open_data_mapping() -> dict:
    namespace = os.environ.get(RADIANT_TABLES_PREFIX_ENV_KEY)
    namespace = f"{namespace}_" if namespace else ""
    return {key: f"{namespace}{value}" for key, value in STARROCKS_OPEN_DATA_MAPPING.items()}


def get_starrocks_common_mapping() -> dict:
    namespace = os.environ.get(RADIANT_TABLES_PREFIX_ENV_KEY)
    namespace = f"{namespace}_" if namespace else ""
    return {key: f"{namespace}{value}" for key, value in STARROCKS_COMMON_MAPPING.items()}


def get_radiant_mapping() -> dict:
    namespace = os.environ.get(RADIANT_TABLES_PREFIX_ENV_KEY)
    namespace = f"{namespace}_" if namespace else GERMLINE_SNV_NAMESPACE_STARROCKS_PREFIX
    mapping = {
        key: f"{namespace}{value}"
        for key, value in {
            **STARROCKS_COLOCATE_GROUP_MAPPING,
        }.items()
    }
    mapping.update(get_starrocks_common_mapping())
    mapping.update(get_starrocks_germline_snv_mapping())
    mapping.update(get_starrocks_open_data_mapping())
    mapping.update(get_iceberg_tables())
    return mapping
