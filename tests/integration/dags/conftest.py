import pytest

from radiant.dags import NAMESPACE
from radiant.tasks.data.radiant_tables import (
    RadiantConfigKeys
)
from tests.utils.dags import get_pyarrow_table_from_csv, poll_dag_until_success, trigger_dag, unpause_dag


def create_and_append_table(iceberg_client, namespace, table_name, file_path, json_fields=None, is_clinvar=False):
    content = get_pyarrow_table_from_csv(csv_path=file_path, sep="\t", json_fields=json_fields, is_clinvar=is_clinvar)
    if iceberg_client.namespace_exists(namespace):
        if iceberg_client.table_exists(f"{namespace}.{table_name}"):
            return
        iceberg_client.create_table(f"{namespace}.{table_name}", schema=content.schema)
        iceberg_client.load_table(f"{namespace}.{table_name}").append(df=content)


@pytest.fixture(scope="session")
def open_data_iceberg_tables(iceberg_client, iceberg_namespace, resources_dir, random_test_id):
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
        "dbsnp": None,
        "gnomad_genomes_v3": None,
        "gnomad_constraint_v_2_1_1": None,
        "ensembl_gene": None,
        "ensembl_exon_by_gene": ["transcript_ids"],
        "spliceai_enriched": ["max_score"],
        "topmed_bravo": None,
        "omim_gene_set": ["symbols", "phenotype"],
        "cosmic_gene_set": ["tumour_types_germline"],
        "ddd_gene_set": None,
        "hpo_gene_set": None,
        "hpo_term": None,
        "mondo_term": None,
        "orphanet_gene_set": ["type_of_inheritance"],
    }

    for table, json_fields in tables.items():
        create_and_append_table(
            iceberg_client,
            iceberg_namespace,
            f"{table}",
            resources_dir / "open_data" / f"{table}.tsv",
            json_fields=json_fields,
            is_clinvar=(table == "clinvar"),
        )


@pytest.fixture(scope="session")
def init_iceberg_tables(radiant_airflow_container, iceberg_namespace, random_test_id):
    dag_id = f"{NAMESPACE}-init-iceberg-tables"
    dag_conf = {
        RadiantConfigKeys.ICEBERG_NAMESPACE: iceberg_namespace,
    }
    unpause_dag(radiant_airflow_container, dag_id)
    trigger_dag(radiant_airflow_container, dag_id, random_test_id, conf=dag_conf)
    assert poll_dag_until_success(
        airflow_container=radiant_airflow_container, dag_id=dag_id, run_id=random_test_id, timeout=180
    )
    yield


@pytest.fixture(scope="session")
def init_starrocks_tables(radiant_airflow_container, starrocks_database, starrocks_jdbc_catalog, random_test_id):
    dag_id = f"{NAMESPACE}-init-starrocks-tables"
    unpause_dag(radiant_airflow_container, dag_id)
    dag_conf = {
        RadiantConfigKeys.RADIANT_DATABASE: starrocks_database.database,
        RadiantConfigKeys.CLINICAL_DATABASE: starrocks_jdbc_catalog.database,
    }
    trigger_dag(radiant_airflow_container, dag_id, random_test_id, conf=dag_conf)
    assert poll_dag_until_success(
        airflow_container=radiant_airflow_container, dag_id=dag_id, run_id=random_test_id, timeout=180
    )
    yield


@pytest.fixture(scope="session")
def init_all_tables(init_iceberg_tables, init_starrocks_tables, open_data_iceberg_tables):
    yield
