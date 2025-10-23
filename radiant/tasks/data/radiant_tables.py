import os

from radiant.dags import NAMESPACE

RADIANT_ICEBERG_DATABASE_ENV_KEY = "RADIANT_ICEBERG_NAMESPACE"

RADIANT_ICEBERG_CATALOG_ENV_KEY = "RADIANT_ICEBERG_CATALOG"

RADIANT_DATABASE_ENV_KEY = "RADIANT_TABLES_DATABASE"

CLINICAL_CATALOG_ENV_KEY = "RADIANT_CLINICAL_CATALOG"
CLINICAL_DATABASE_ENV_KEY = "RADIANT_CLINICAL_DATABASE"

STARROCKS_COLOCATE_GROUP_MAPPING = {"colocate_query_group": f"{NAMESPACE}.query_group"}

GERMLINE_SNV_NAMESPACE_STARROCKS_PREFIX = "germline__snv__"

# --- Clinical tables
CLINICAL_MAPPING = {
    "clinical_case": "`cases`",
    "clinical_case_analysis": "case_analysis",
    "clinical_sequencing_experiment": "sequencing_experiment",
    "clinical_experiment": "experiment",
    "clinical_task": "task",
    "clinical_sample": "sample",
    "clinical_patient": "patient",
    "clinical_family": "family",
    "clinical_document": "document",
    "clinical_document_has_patient": "document_has_patient",
    "clinical_task_has_sequencing_experiment": "task_has_sequencing_experiment",
    "clinical_task_has_document": "task_has_document",
    "clinical_request": "request",
    "clinical_organization": "organization",
    "clinical_project": "project",
    "clinical_observation_coding": "observation_coding",
    "clinical_pipeline": "pipeline",
}

# --- Iceberg tables

ICEBERG_GERMLINE_SNV_MAPPING = {
    "iceberg_consequence": "germline_snv_consequence",
    "iceberg_occurrence": "germline_snv_occurrence",
    "iceberg_variant": "germline_snv_variant",
    "iceberg_germline_cnv_occurrence": "germline_cnv_occurrence",
}

ICEBERG_OPEN_DATA_MAPPING = {
    "iceberg_1000_genomes": "1000_genomes",
    "iceberg_clinvar": "clinvar",
    "iceberg_dbnsfp": "dbnsfp",
    "iceberg_dbsnp": "dbsnp",
    "iceberg_gnomad_genomes_v3": "gnomad_genomes_v3",
    "iceberg_spliceai": "spliceai_enriched",
    "iceberg_topmed_bravo": "topmed_bravo",
    "iceberg_gnomad_constraint": "gnomad_constraint_v_2_1_1",
    "iceberg_omim_gene_set": "omim_gene_set",
    "iceberg_hpo_gene_set": "hpo_gene_set",
    "iceberg_ensembl_gene": "ensembl_gene",
    "iceberg_ensembl_exon_by_gene": "ensembl_exon_by_gene",
    "iceberg_orphanet_gene_set": "orphanet_gene_set",
    "iceberg_cosmic_gene_set": "cosmic_gene_set",
    "iceberg_ddd_gene_set": "ddd_gene_set",
    "iceberg_mondo_term": "mondo_term",
    "iceberg_hpo_term": "hpo_term",
    "iceberg_gnomad_sv": "gnomad_sv",
}

ICEBERG_CATALOG_DATABASE = {
    "iceberg_catalog": os.getenv("RADIANT_ICEBERG_CATALOG", "radiant_iceberg_catalog"),
    "iceberg_database": os.getenv("RADIANT_ICEBERG_NAMESPACE", "radiant"),
}

# --- StarRocks tables

STARROCKS_COMMON_MAPPING = {
    "starrocks_staging_sequencing_experiment": "staging_sequencing_experiment",
    "starrocks_staging_external_sequencing_experiment": "staging_external_sequencing_experiment",
    "starrocks_staging_sequencing_experiment_delta": "staging_sequencing_experiment_delta",
    "starrocks_variant_lookup": "variant_lookup",
    "starrocks_staging_exomiser": "raw_exomiser",
    "starrocks_exomiser": "exomiser",
    "starrocks_germline_cnv_occurrence": "germline__cnv__occurrence",
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


def get_iceberg_germline_snv_mapping(conf=None) -> dict:
    _catalog = (
        conf.get(
            RADIANT_ICEBERG_CATALOG_ENV_KEY, os.getenv(RADIANT_ICEBERG_CATALOG_ENV_KEY, "radiant_iceberg_catalog")
        )
        if conf
        else os.getenv(RADIANT_ICEBERG_CATALOG_ENV_KEY, "radiant_iceberg_catalog")
    )
    _database = (
        conf.get(RADIANT_ICEBERG_DATABASE_ENV_KEY, os.getenv(RADIANT_ICEBERG_DATABASE_ENV_KEY, "radiant"))
        if conf
        else os.getenv(RADIANT_ICEBERG_DATABASE_ENV_KEY, "radiant")
    )
    return {key: f"{_catalog}.{_database}.{value}" for key, value in ICEBERG_GERMLINE_SNV_MAPPING.items()}


def get_iceberg_open_data_mapping(conf=None) -> dict:
    _catalog = (
        conf.get(
            RADIANT_ICEBERG_CATALOG_ENV_KEY, os.getenv(RADIANT_ICEBERG_CATALOG_ENV_KEY, "radiant_iceberg_catalog")
        )
        if conf
        else os.getenv(RADIANT_ICEBERG_CATALOG_ENV_KEY, "radiant_iceberg_catalog")
    )
    _database = (
        conf.get(RADIANT_ICEBERG_DATABASE_ENV_KEY, os.getenv(RADIANT_ICEBERG_DATABASE_ENV_KEY, "radiant"))
        if conf
        else os.getenv(RADIANT_ICEBERG_DATABASE_ENV_KEY, "radiant")
    )
    return {key: f"{_catalog}.{_database}.{value}" for key, value in ICEBERG_OPEN_DATA_MAPPING.items()}


def get_iceberg_tables(conf=None) -> dict:
    return {
        **get_iceberg_germline_snv_mapping(conf),
        **get_iceberg_open_data_mapping(conf),
    }


def get_starrocks_mapping(tables, prefix="", conf=None) -> dict:
    _database = (
        conf.get(RADIANT_DATABASE_ENV_KEY, os.getenv(RADIANT_DATABASE_ENV_KEY, "radiant"))
        if conf
        else os.getenv(RADIANT_DATABASE_ENV_KEY, "radiant")
    )
    return {key: f"{_database}.{prefix}{value}" for key, value in tables.items()}


def get_starrocks_germline_snv_mapping(conf=None) -> dict:
    return get_starrocks_mapping(STARROCKS_GERMLINE_SNV_MAPPING, GERMLINE_SNV_NAMESPACE_STARROCKS_PREFIX, conf)


def get_starrocks_open_data_mapping(conf=None) -> dict:
    return get_starrocks_mapping(STARROCKS_OPEN_DATA_MAPPING, conf=conf)


def get_starrocks_common_mapping(conf=None) -> dict:
    return get_starrocks_mapping(STARROCKS_COMMON_MAPPING, conf=conf)


def get_clinical_mapping(conf=None) -> dict:
    _catalog = (
        conf.get(CLINICAL_CATALOG_ENV_KEY, os.getenv(CLINICAL_CATALOG_ENV_KEY, "radiant_jdbc"))
        if conf
        else os.getenv(CLINICAL_CATALOG_ENV_KEY, "radiant_jdbc")
    )
    _database = (
        conf.get(CLINICAL_DATABASE_ENV_KEY, os.getenv(CLINICAL_DATABASE_ENV_KEY, "public"))
        if conf
        else os.getenv(CLINICAL_DATABASE_ENV_KEY, "public")
    )
    return {key: f"{_catalog}.{_database}.{value}" for key, value in CLINICAL_MAPPING.items()}


def get_radiant_mapping(conf=None) -> dict:
    namespace = os.environ.get(RADIANT_DATABASE_ENV_KEY)
    namespace = f"{namespace}_" if namespace else GERMLINE_SNV_NAMESPACE_STARROCKS_PREFIX
    mapping = {
        key: f"{namespace}{value}"
        for key, value in {
            **STARROCKS_COLOCATE_GROUP_MAPPING,
        }.items()
    }
    mapping.update(get_starrocks_common_mapping(conf))
    mapping.update(get_starrocks_germline_snv_mapping(conf))
    mapping.update(get_starrocks_open_data_mapping(conf))
    mapping.update(get_iceberg_tables(conf))
    mapping.update(get_clinical_mapping(conf))
    return mapping
