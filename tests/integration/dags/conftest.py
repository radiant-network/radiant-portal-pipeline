import json

import pandas as pd
import pyarrow as pa
import pytest


def get_pyarrow_table_from_csv(
    csv_path, sep: str | None, json_fields: list[str] | None = None, is_clinvar: bool = False
) -> pa.Table:
    """
    Get the pyarrow schema from a CSV file.
    """
    df = pd.read_csv(csv_path, sep=sep)

    if is_clinvar:
        df = df.applymap(lambda x: [""] if pd.isna(x) else x)

    if json_fields:
        for field in json_fields:
            if field in df.columns:
                df[field] = df[field].apply(lambda x: json.loads(x) if isinstance(x, str) else x)

    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, list)).all():
            df[col] = pa.array(df[col].tolist(), type=pa.list_(pa.string()))

    return pa.Table.from_pandas(df)


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
            csv_path=_path, sep="\t", json_fields=_json_fields, is_clinvar=True if table == "clinvar" else False
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
