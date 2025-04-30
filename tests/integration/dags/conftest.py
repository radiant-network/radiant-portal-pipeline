import pytest

from tests.utils.dags import get_pyarrow_table_from_csv


def create_and_append_table(iceberg_client, namespace, table_name, file_path, json_fields=None, is_clinvar=False):
    content = get_pyarrow_table_from_csv(csv_path=file_path, sep="\t", json_fields=json_fields, is_clinvar=is_clinvar)
    iceberg_client.create_table_if_not_exists(f"{namespace}.{table_name}", schema=content.schema)
    iceberg_client.load_table(f"{namespace}.{table_name}").append(df=content)


@pytest.fixture(scope="session")
def open_data_iceberg_tables(
    starrocks_iceberg_catalog, iceberg_client, setup_namespace, resources_dir, random_test_id
):
    # Json fields are required for certain .tsv files to properly handle types
    tables = {
        "1000_genomes": None,
        "clinvar": [
            "interpretations",
            "clin_sig",
            "clin_sig_co",
            "clnvi",
            "clndisdb",
            "clnrevstat",
            "origin",
            "clndnincl",
            "rs",
            "clnhgvs",
            "clndisdbinc",
            "conditions",
            "inheritance",
            "clnsigscv",
        ],
        "dbnsfp": None,
        "gnomad_genomes_v3": None,
        "spliceai_enriched": ["max_score"],
        "topmed_bravo": None,
    }
    for table, json_fields in tables.items():
        create_and_append_table(
            iceberg_client,
            setup_namespace,
            f"test_{random_test_id}_open_data_{table}",
            resources_dir / "open_data" / f"{table}.tsv",
            json_fields=json_fields,
            is_clinvar=(table == "clinvar"),
        )


@pytest.fixture(scope="session")
def iceberg_tables(starrocks_iceberg_catalog, iceberg_client, setup_namespace, resources_dir, random_test_id):
    create_and_append_table(
        iceberg_client,
        setup_namespace,
        f"test_{random_test_id}_germline_snv_variants",
        resources_dir / "radiant" / "variants.tsv",
    )


@pytest.fixture(scope="session")
def init_iceberg_tables(open_data_iceberg_tables, iceberg_tables):
    yield
