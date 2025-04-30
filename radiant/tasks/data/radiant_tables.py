import os

from radiant.dags import NAMESPACE

RADIANT_TABLES_PREFIX_ENV_KEY = "RADIANT_TABLES_NAMESPACE"

STARROCKS_COLOCATE_GROUP_MAPPING = {"colocate_query_group": f"{NAMESPACE}.query_group"}

ICEBERG_GERMLINE_SNV_MAPPING = {
    "iceberg_consequences": "germline_snv_consequences",
    "iceberg_occurrences": "germline_snv_occurrences",
    "iceberg_variants": "germline_snv_variants",
}

ICEBERG_SEQUENCING_EXPERIMENT_MAPPING = {
    "iceberg_sequencing_experiment": "sequencing_experiment",
}

ICEBERG_OPEN_DATA_MAPPING = {
    "iceberg_1000_genomes": "open_data_1000_genomes",
    "iceberg_clinvar": "open_data_clinvar",
    "iceberg_dbnsfp": "open_data_dbnsfp",
    "iceberg_gnomad_genomes_v3": "open_data_gnomad_genomes_v3",
    "iceberg_spliceai": "open_data_spliceai_enriched",
    "iceberg_topmed_bravo": "open_data_topmed_bravo",
}

STARROCKS_GERMLINE_SNV_MAPPING = {
    "starrocks_consequences": "consequences",
    "starrocks_consequences_filter": "consequences_filter",
    "starrocks_occurrences": "occurrences",
    "starrocks_variants": "variants",
    "starrocks_variants_frequencies": "variants_frequencies",
    "starrocks_variants_lookup": "variants_lookup",
    "starrocks_staging_variants": "staging_variants",
}

STARROCKS_SEQUENCING_EXPERIMENT_MAPPING = {
    "starrocks_sequencing_experiment": "sequencing_experiment",
    "starrocks_sequencing_experiment_delta": "sequencing_experiment_delta",
}

STARROCKS_OPEN_DATA_MAPPING = {
    "starrocks_1000_genomes": "1000_genomes",
    "starrocks_clinvar": "clinvar",
    "starrocks_dbnsfp": "dbnsfp",
    "starrocks_gnomad_genomes_v3": "gnomad_genomes_v3",
    "starrocks_spliceai": "spliceai_enriched",
    "starrocks_topmed_bravo": "topmed_bravo",
}


def get_radiant_mapping() -> dict:
    namespace = os.environ.get(RADIANT_TABLES_PREFIX_ENV_KEY, "radiant")
    namespace = f"{namespace}_"
    mapping = {
        key: f"{namespace}{value}"
        for key, value in {
            **ICEBERG_GERMLINE_SNV_MAPPING,
            **ICEBERG_SEQUENCING_EXPERIMENT_MAPPING,
            **ICEBERG_OPEN_DATA_MAPPING,
            **STARROCKS_GERMLINE_SNV_MAPPING,
            **STARROCKS_SEQUENCING_EXPERIMENT_MAPPING,
            **STARROCKS_OPEN_DATA_MAPPING,
            **STARROCKS_COLOCATE_GROUP_MAPPING,
        }.items()
    }
    return mapping
