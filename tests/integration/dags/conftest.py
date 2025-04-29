import pytest

from tests.utils.dags import get_pyarrow_table_from_csv


@pytest.fixture(scope="session")
def open_data_iceberg_tables(iceberg_client, setup_namespace, resources_dir):
    for table in ["1000_genomes", "clinvar", "dbnsfp", "gnomad_genomes_v3", "spliceai_enriched", "topmed_bravo"]:
        _path = resources_dir / "open_data" / f"{table}.tsv"

        _json_fields = None
        if table == "spliceai_enriched":
            _json_fields = ["max_score"]

        elif table == "clinvar":
            _json_fields = [
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
            ]

        _content = get_pyarrow_table_from_csv(
            csv_path=_path, sep="\t", json_fields=_json_fields, is_clinvar=table == "clinvar"
        )
        iceberg_client.create_table_if_not_exists(
            f"{setup_namespace}.{table}",
            schema=_content.schema,
        )

        _table = iceberg_client.load_table(identifier=f"{setup_namespace}.{table}")
        _table.append(df=_content)


@pytest.fixture(scope="session")
def kf_iceberg_tables(iceberg_client, setup_namespace, resources_dir):
    for table in ["kf_variants"]:
        _path = resources_dir / "kf" / f"{table}.tsv"

        _content = get_pyarrow_table_from_csv(csv_path=_path, sep="\t")

        iceberg_client.create_table_if_not_exists(
            f"{setup_namespace}.{table}",
            schema=_content.schema,
        )

        _table = iceberg_client.load_table(identifier=f"{setup_namespace}.{table}")
        _table.append(df=_content)


@pytest.fixture(scope="session")
def init_iceberg_tables(open_data_iceberg_tables, kf_iceberg_tables):
    yield
