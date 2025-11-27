import os
from enum import Enum

from radiant.dags import NAMESPACE


class RadiantConfigKeys(Enum):
    ICEBERG_CATALOG = ("RADIANT_ICEBERG_CATALOG", "radiant_iceberg_catalog")
    ICEBERG_NAMESPACE = ("RADIANT_ICEBERG_NAMESPACE", "radiant")
    RADIANT_DATABASE = ("RADIANT_TABLES_DATABASE", "radiant")
    CLINICAL_CATALOG = ("RADIANT_CLINICAL_CATALOG", "radiant_jdbc")
    CLINICAL_DATABASE = ("RADIANT_CLINICAL_DATABASE", "public")

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
STARROCKS_RADIANT_MAPPING = {
    "starrocks_staging_sequencing_experiment": "staging_sequencing_experiment",
    "starrocks_staging_external_sequencing_experiment": "staging_external_sequencing_experiment",
    "starrocks_staging_sequencing_experiment_delta": "staging_sequencing_experiment_delta",
    "starrocks_variant_lookup": "variant_lookup",
    "starrocks_staging_exomiser": "raw_exomiser",
    "starrocks_exomiser": "exomiser",
    "starrocks_germline_cnv_occurrence": "germline__cnv__occurrence",
    "starrocks_germline_snv_consequence": "germline__snv__consequence",
    "starrocks_germline_snv_consequence_filter": "germline__snv__consequence_filter",
    "starrocks_germline_snv_consequence_filter_partitioned": "germline__snv__consequence_filter_partitioned",
    "starrocks_germline_snv_occurrence": "germline__snv__occurrence",
    "starrocks_germline_snv_tmp_variant": "germline__snv__tmp_variant",
    "starrocks_germline_snv_variant": "germline__snv__variant",
    "starrocks_germline_snv_variant_frequency": "germline__snv__variant_frequency",
    "starrocks_germline_snv_variant_partitioned": "germline__snv__variant_partitioned",
    "starrocks_germline_snv_staging_variant": "germline__snv__staging_variant",
    "starrocks_germline_snv_staging_variant_frequency": "germline__snv__staging_variant_frequency_part",
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
    _catalog = get_config_value(conf, RadiantConfigKeys.ICEBERG_CATALOG)
    _database = get_config_value(conf, RadiantConfigKeys.ICEBERG_NAMESPACE)
    return {key: f"{_catalog}.{_database}.{value}" for key, value in ICEBERG_GERMLINE_SNV_MAPPING.items()}


def get_iceberg_open_data_mapping(conf=None) -> dict:
    _catalog = get_config_value(conf, RadiantConfigKeys.ICEBERG_CATALOG)
    _database = get_config_value(conf, RadiantConfigKeys.ICEBERG_NAMESPACE)
    return {key: f"{_catalog}.{_database}.{value}" for key, value in ICEBERG_OPEN_DATA_MAPPING.items()}


def get_iceberg_tables(conf=None) -> dict:
    return {
        **get_iceberg_germline_snv_mapping(conf),
        **get_iceberg_open_data_mapping(conf),
    }


def get_starrocks_mapping(conf=None) -> dict:
    tables = STARROCKS_RADIANT_MAPPING | STARROCKS_OPEN_DATA_MAPPING
    _database = get_config_value(conf, RadiantConfigKeys.RADIANT_DATABASE)
    return {key: f"{_database}.{value}" for key, value in tables.items()}


def get_clinical_mapping(conf=None) -> dict:
    _catalog = get_config_value(conf, RadiantConfigKeys.CLINICAL_CATALOG)
    _database = get_config_value(conf, RadiantConfigKeys.CLINICAL_DATABASE)
    return {key: f"{_catalog}.{_database}.{value}" for key, value in CLINICAL_MAPPING.items()}


def get_radiant_mapping(conf=None) -> dict:
    namespace = get_config_value(conf, RadiantConfigKeys.ICEBERG_NAMESPACE)
    namespace = f"{namespace}_" if namespace else "germline__snv__"
    mapping = {
        key: f"{namespace}{value}"
        for key, value in {
            **STARROCKS_COLOCATE_GROUP_MAPPING,
        }.items()
    }
    mapping.update(get_starrocks_mapping(conf=conf))
    mapping.update(get_iceberg_tables(conf))
    mapping.update(get_clinical_mapping(conf))
    return mapping
