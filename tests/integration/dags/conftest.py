import pytest

from radiant.dags import NAMESPACE
from radiant.tasks.data.radiant_tables import RadiantConfigKeys
from tests.utils.dags import get_pyarrow_table_from_csv, poll_dag_until_success, trigger_dag, unpause_dag

# The ref the contract tables are tagged with here. `mapping_conf` pins RADIANT_OPEN_DATA_REF to the same
# value, so the fixture's tag and the ref the rendered SQL asks for cannot drift apart.
OPEN_DATA_REF = RadiantConfigKeys.OPEN_DATA_REF.default


def create_and_append_table(
    iceberg_client, namespace, table_name, file_path, json_fields=None, na_fill=None, tag=None
):
    content = get_pyarrow_table_from_csv(csv_path=file_path, sep="\t", json_fields=json_fields, na_fill=na_fill)
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
    "hpo_genes_v1": None,
    "hpo_terms_v1": None,
    "mondo_v1": None,
    "orphanet_v1": ["type_of_inheritance"],
}

# No OpenDataLake contract exists for these, so they keep their legacy names and carry no ref.
_OPEN_DATA_LEGACY_TABLES = {
    "ensembl_gene": None,
    "ensembl_exon_by_gene": ["transcript_ids"],
    "cosmic_gene_set": ["tumour_types_germline"],
}

_OPEN_DATA_NA_FILL = {
    "clinvar_v1": [""],
    "ensembl_gene": "",
    "ensembl_exon_by_gene": "",
}


@pytest.fixture(scope="session")
def open_data_iceberg_tables(s3_fs, iceberg_client, iceberg_namespace, resources_dir, random_test_id):
    # Json fields are required for certain .tsv files to properly handle types
    for tables, tag in ((_OPEN_DATA_CONTRACT_TABLES, OPEN_DATA_REF), (_OPEN_DATA_LEGACY_TABLES, None)):
        for table, json_fields in tables.items():
            create_and_append_table(
                iceberg_client,
                iceberg_namespace,
                f"{table}",
                resources_dir / "open_data" / f"{table}.tsv",
                json_fields=json_fields,
                na_fill=_OPEN_DATA_NA_FILL.get(table),
                tag=tag,
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
