import datetime
import json

import pyarrow as pa
import pytest

from radiant.dags import NAMESPACE
from radiant.tasks.data.radiant_tables import RadiantConfigKeys
from tests.utils.dags import get_pyarrow_table_from_csv, poll_dag_until_success, trigger_dag, unpause_dag

# The ref the contract tables are tagged with here. `mapping_conf` pins RADIANT_OPEN_DATA_REF to the same
# value, so the fixture's tag and the ref the rendered SQL asks for cannot drift apart.
OPEN_DATA_REF = RadiantConfigKeys.OPEN_DATA_REF.default


def create_and_append_arrow_table(iceberg_client, namespace, table_name, content, tag=None):
    if not iceberg_client.namespace_exists(namespace):
        return
    if iceberg_client.table_exists(f"{namespace}.{table_name}"):
        return
    iceberg_client.create_table(f"{namespace}.{table_name}", schema=content.schema)
    table = iceberg_client.load_table(f"{namespace}.{table_name}")
    table.append(df=content)
    if tag:
        # Every read of an OpenDataLake contract table is pinned to a ref (RADIANT_OPEN_DATA_REF,
        # `latest` by default), so the fixture has to carry that ref or the rendered SQL resolves
        # nothing. Upstream this tag is moved by each publish; here one append is the whole history.
        table.manage_snapshots().create_tag(snapshot_id=table.current_snapshot().snapshot_id, tag_name=tag).commit()


def create_and_append_table(
    iceberg_client, namespace, table_name, file_path, json_fields=None, na_fill=None, tag=None
):
    content = get_pyarrow_table_from_csv(csv_path=file_path, sep="\t", json_fields=json_fields, na_fill=na_fill)
    create_and_append_arrow_table(iceberg_client, namespace, table_name, content, tag=tag)


# Tables OpenDataLake publishes under contract: named `<table_prefix>_v{MAJOR}` and read through the
# `latest` tag. Keys are the fixture .tsv basenames, which match the published table names.
_OPEN_DATA_CONTRACT_TABLES = {
    "1000_genomes_v1": None,
    "clinvar_v1": [
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
    "dbnsfp_v1": None,
    "dbsnp_v1": None,
    "gnomad_joint_v1": None,
    "gnomad_constraint_v1": None,
    # Read straight from Iceberg by the CNV enrichment -- there is no StarRocks DDL for it. The columns
    # in the .tsv are the ones *_cnv_occurrence_insert_partition_delta.sql touches, not the full
    # gnomAD-SV column set.
    "gnomad_sv_v1": None,
    "spliceai_v1": ["max_score"],
    "topmed_bravo_v1": None,
    "omim_v1": ["symbols", "phenotype"],
    "ddd_v1": None,
    # `alias` is an array upstream. One row carries a value so pyarrow infers list<string>, not list<null>.
    "ensembl_gene_v1": ["alias"],
    "ensembl_exon_by_gene_v1": ["transcript_ids"],
    "hpo_genes_v1": None,
    "hpo_terms_v1": None,
    "mondo_v1": None,
    "orphanet_v1": ["type_of_inheritance"],
}

# `clinvar_rcv_v1` reuses the NDJSON the broker load is fed, so one file serves both ways into
# `raw_clinvar_rcv_summary`. Not a .tsv like its neighbours: the CSV reader would flatten its array of
# structs into an array of strings. Typed explicitly, to match the contract and the StarRocks DDL.
_CLINVAR_RCV_SCHEMA = pa.schema(
    [
        ("clinvar_id", pa.string()),
        ("accession", pa.string()),
        ("clinical_significance", pa.list_(pa.string())),
        ("date_last_evaluated", pa.date32()),
        ("submission_count", pa.int32()),
        ("review_status", pa.string()),
        ("review_status_stars", pa.int32()),
        ("version", pa.int32()),
        ("traits", pa.list_(pa.string())),
        ("origins", pa.list_(pa.string())),
        (
            "submissions",
            pa.list_(
                pa.struct(
                    [
                        ("submitter", pa.string()),
                        ("scv", pa.string()),
                        ("version", pa.int32()),
                        ("review_status", pa.string()),
                        ("review_status_stars", pa.int32()),
                        ("clinical_significance", pa.string()),
                        ("date_last_evaluated", pa.date32()),
                    ]
                )
            ),
        ),
        ("clinical_significance_count", pa.map_(pa.string(), pa.int32())),
    ]
)


def get_pyarrow_table_from_clinvar_rcv_ndjson(path) -> pa.Table:
    rows = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    for row in rows:
        for record in [row, *(row["submissions"] or [])]:
            record["date_last_evaluated"] = datetime.date.fromisoformat(record["date_last_evaluated"])
    return pa.Table.from_pylist(rows, schema=_CLINVAR_RCV_SCHEMA)


_OPEN_DATA_NA_FILL = {
    "clinvar_v1": [""],
    "ensembl_gene_v1": "",
    "ensembl_exon_by_gene_v1": "",
}


@pytest.fixture(scope="session")
def open_data_iceberg_tables(s3_fs, iceberg_client, iceberg_namespace, resources_dir, random_test_id):
    # Json fields are required for certain .tsv files to properly handle types
    for table, json_fields in _OPEN_DATA_CONTRACT_TABLES.items():
        create_and_append_table(
            iceberg_client,
            iceberg_namespace,
            f"{table}",
            resources_dir / "open_data" / f"{table}.tsv",
            json_fields=json_fields,
            na_fill=_OPEN_DATA_NA_FILL.get(table),
            tag=OPEN_DATA_REF,
        )

    create_and_append_arrow_table(
        iceberg_client,
        iceberg_namespace,
        "clinvar_rcv_v1",
        get_pyarrow_table_from_clinvar_rcv_ndjson(resources_dir / "open_data" / "clinvar_rcv_summary.ndjson"),
        tag=OPEN_DATA_REF,
    )


@pytest.fixture(scope="session")
def init_iceberg_tables(radiant_airflow_container, iceberg_namespace, random_test_id):
    dag_id = f"{NAMESPACE}-init-iceberg-tables"
    dag_conf = {
        RadiantConfigKeys.ICEBERG_NAMESPACE.value[0]: iceberg_namespace,
    }
    unpause_dag(radiant_airflow_container, dag_id)
    trigger_dag(radiant_airflow_container, dag_id, random_test_id, conf=dag_conf)
    assert poll_dag_until_success(
        airflow_container=radiant_airflow_container, dag_id=dag_id, run_id=random_test_id, timeout=180
    )
    yield


@pytest.fixture(scope="session")
def init_starrocks_tables(radiant_airflow_container, starrocks_database, starrocks_jdbc_catalog, random_test_id):
    dag_id = f"{NAMESPACE}-init-starrocks-base-tables"
    unpause_dag(radiant_airflow_container, dag_id)
    dag_conf = {
        RadiantConfigKeys.RADIANT_DATABASE.value[0]: starrocks_database.database,
        RadiantConfigKeys.CLINICAL_DATABASE.value[0]: starrocks_jdbc_catalog.database,
    }
    trigger_dag(radiant_airflow_container, dag_id, random_test_id, conf=dag_conf)
    assert poll_dag_until_success(
        airflow_container=radiant_airflow_container, dag_id=dag_id, run_id=random_test_id, timeout=180
    )
    yield


@pytest.fixture(scope="session")
def init_all_tables(init_iceberg_tables, init_starrocks_tables, open_data_iceberg_tables):
    yield
