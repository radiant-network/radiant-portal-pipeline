import os

from radiant.dags import NAMESPACE

RADIANT_TABLES_PREFIX_ENV_KEY = "RADIANT_TABLES_NAMESPACE"
ICEBERG_CATALOG_ENV_KEY = "RADIANT_ICEBERG_CATALOG"
ICEBERG_DATABASE_ENV_KEY = "RADIANT_ICEBERG_DATABASE"

CLINICAL_CATALOG_ENV_KEY = "RADIANT_CLINICAL_CATALOG"
CLINICAL_DATABASE_ENV_KEY = "RADIANT_CLINICAL_DATABASE"

STARROCKS_COLOCATE_GROUP_MAPPING = {"colocate_query_group": f"{NAMESPACE}.query_group"}

GERMLINE_SNV_NAMESPACE_STARROCKS_PREFIX = "germline__snv__"


# --- Clinical tables
CLINICAL_MAPPING = {
    "clinical_case": "`case`",
    "clinical_case_analysis": "case_analysis",
    "clinical_sequencing_experiment": "sequencing_experiment",
    "clinical_experiment": "experiment",
    "clinical_task": "task",
    "clinical_sample": "sample",
    "clinical_patient": "patient",
    "clinical_family": "family",
    "clinical_document": "document",
    "clinical_task_has_sequencing_experiment": "task_has_sequencing_experiment",
    "clinical_task_has_document": "task_has_document",
    "clinical_request": "request",
    "clinical_organization": "organization",
    "clinical_project": "project",
    "clinical_observation_coding": "observation_coding",
    "clinical_pipeline": "pipeline",
}

CLINICAL_CATALOG_DATABASE = {
    "clinical_catalog": os.getenv(CLINICAL_CATALOG_ENV_KEY, "radiant_jdbc"),
    "clinical_database": os.getenv(CLINICAL_DATABASE_ENV_KEY, "public"),
}

# --- Iceberg tables

ICEBERG_GERMLINE_SNV_MAPPING = {
    "iceberg_consequence": "germline_snv_consequence",
    "iceberg_occurrence": "germline_snv_occurrence",
    "iceberg_variant": "germline_snv_variant",
}

ICEBERG_OPEN_DATA_MAPPING = {
    "iceberg_1000_genomes": "1000_genomes",
    "iceberg_clinvar": "clinvar",
    "iceberg_dbnsfp": "dbnsfp",
    "iceberg_gnomad_genomes_v3": "gnomad_genomes_v3",
    "iceberg_spliceai": "spliceai_enriched",
    "iceberg_topmed_bravo": "topmed_bravo",
    "iceberg_gnomad_constraint": "gnomad_constraint_v_2_1_1",
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
    "starrocks_staging_sequencing_experiment": "staging_sequencing_experiment",
    "starrocks_staging_external_sequencing_experiment": "staging_external_sequencing_experiment",
    "starrocks_staging_sequencing_experiment_delta": "staging_sequencing_experiment_delta",
    "starrocks_variant_lookup": "variant_lookup",
}

STARROCKS_GERMLINE_SNV_MAPPING = {
    "starrocks_consequence": "consequence",
    "starrocks_consequence_filter": "consequence_filter",
    "starrocks_consequence_filter_partitioned": "consequence_filter_partitioned",
    "starrocks_occurrence": "occurrence",
    "starrocks_tmp_variant": "tmp_variant",
    "starrocks_variant": "variant",
    "starrocks_variant_frequency": "variant_frequency",
    "starrocks_variant_partitioned": "variant_partitioned",
    "starrocks_staging_variant": "staging_variant",
    "starrocks_staging_variant_frequency": "staging_variant_frequency_part",
}

STARROCKS_OPEN_DATA_MAPPING = {
    "starrocks_1000_genomes": "1000_genomes",
    "starrocks_clinvar": "clinvar",
    "starrocks_dbnsfp": "dbnsfp",
    "starrocks_gnomad_genomes_v3": "gnomad_genomes_v3",
    "starrocks_spliceai": "spliceai",
    "starrocks_topmed_bravo": "topmed_bravo",
    "starrocks_gnomad_constraint": "gnomad_constraint",
    "starrocks_omim_gene_panel": "omim_gene_panel",
    "starrocks_hpo_gene_panel": "hpo_gene_panel",
    "starrocks_orphanet_gene_panel": "orphanet_gene_panel",
    "starrocks_cosmic_gene_panel": "cosmic_gene_panel",
    "starrocks_ddd_gene_panel": "ddd_gene_panel",
}


def get_iceberg_germline_snv_mapping() -> dict:
    _catalog = ICEBERG_CATALOG_DATABASE["iceberg_catalog"]
    _database = ICEBERG_CATALOG_DATABASE["iceberg_database"]
    return {key: f"{_catalog}.{_database}.{value}" for key, value in ICEBERG_GERMLINE_SNV_MAPPING.items()}


def get_iceberg_open_data_mapping() -> dict:
    _catalog = ICEBERG_CATALOG_DATABASE["iceberg_catalog"]
    _database = ICEBERG_CATALOG_DATABASE["iceberg_database"]
    return {key: f"{_catalog}.{_database}.{value}" for key, value in ICEBERG_OPEN_DATA_MAPPING.items()}


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


def get_clinical_mapping() -> dict:
    _catalog = CLINICAL_CATALOG_DATABASE["clinical_catalog"]
    _database = CLINICAL_CATALOG_DATABASE["clinical_database"]
    return {key: f"{_catalog}.{_database}.{value}" for key, value in CLINICAL_MAPPING.items()}


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
    mapping.update(get_clinical_mapping())
    return mapping
