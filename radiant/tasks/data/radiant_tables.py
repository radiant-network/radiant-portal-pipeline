import os

from radiant.dags import NAMESPACE

RADIANT_TABLES_PREFIX_ENV_KEY = "RADIANT_TABLES_NAMESPACE"

STARROCKS_COLOCATE_GROUP_MAPPING = {"colocate_query_group": f"{NAMESPACE}.query_group"}


SOURCE_SEQUENCING_EXPERIMENT_MAPPING = {
    "source_sequencing_experiments": "sequencing_experiments",
}

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
}

STARROCKS_GERMLINE_SNV_MAPPING = {
    "starrocks_consequences": "consequences",
    "starrocks_consequences_filter": "consequences_filter",
    "starrocks_consequences_filter_partitioned": "consequences_filter_partitioned",
    "starrocks_occurrences": "occurrences",
    "starrocks_variants": "variants",
    "starrocks_variants_frequencies": "variants_frequencies",
    "starrocks_variants_partitioned": "variants_partitioned",
    "starrocks_variants_lookup": "variants_lookup",
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
}

ICEBERG_CATALOG_DATABASE = {
    "iceberg_catalog": os.getenv("RADIANT_ICEBERG_CATALOG", "radiant_iceberg_catalog"),
    "iceberg_database": os.getenv("RADIANT_ICEBERG_DATABASE", "radiant"),
}


def get_iceberg_germline_snv_mapping() -> dict:
    return {
        key: f"{ICEBERG_CATALOG_DATABASE['iceberg_catalog']}.{ICEBERG_CATALOG_DATABASE['iceberg_database']}.{value}"
        for key, value in ICEBERG_GERMLINE_SNV_MAPPING.items()
    }


def get_iceberg_open_data_mapping() -> dict:
    return {
        key: f"{ICEBERG_CATALOG_DATABASE['iceberg_catalog']}.{ICEBERG_CATALOG_DATABASE['iceberg_database']}.{value}"
        for key, value in ICEBERG_OPEN_DATA_MAPPING.items()
    }


def get_iceberg_tables() -> dict:
    return {
        **get_iceberg_germline_snv_mapping(),
        **get_iceberg_open_data_mapping(),
    }


def get_radiant_mapping() -> dict:
    namespace = os.environ.get(RADIANT_TABLES_PREFIX_ENV_KEY)
    namespace = f"{namespace}_" if namespace else ""
    mapping = {
        key: f"{namespace}{value}"
        for key, value in {
            **SOURCE_SEQUENCING_EXPERIMENT_MAPPING,
            **STARROCKS_GERMLINE_SNV_MAPPING,
            **STARROCKS_OPEN_DATA_MAPPING,
            **STARROCKS_COLOCATE_GROUP_MAPPING,
        }.items()
    }
    mapping.update(get_iceberg_tables())
    return mapping
