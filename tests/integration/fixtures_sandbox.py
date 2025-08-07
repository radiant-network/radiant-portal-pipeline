import os

import pytest

from tests.integration.fixtures_common import *


@pytest.fixture(scope="session")
def minio_instance():
    instance = MinioInstance("localhost", 9000, 9001, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, "radiant-minio", 9000)
    os.environ["HTS_S3_HOST"] = "localhost:9000"
    os.environ["HTS_S3_ADDRESS_STYLE"] = "path"
    os.environ["AWS_ACCESS_KEY_ID"] = instance.access_key
    os.environ["AWS_SECRET_ACCESS_KEY"] = instance.secret_key
    os.environ["AWS_REGION"] = "us-east-1"
    return instance


@pytest.fixture(scope="session")
def rest_iceberg_catalog_instance(minio_instance):
    return RestIcebergCatalogInstance(
        "localhost", 8181, ICEBERG_REST_CATALOG_NAME, ICEBERG_REST_TOKEN, "radiant-iceberg-rest", 8181
    )


@pytest.fixture(scope="session")
def starrocks_instance():
    return StarRocksInstance(
        "localhost",
        STARROCKS_QUERY_PORT,
        STARROCKS_FE_HTTP_PORT,
        None,
        STARROCKS_USER,
        STARROCKS_PWD,
        "radiant-starrocks-fe",
        9030,
    )


# Note on the Postgres fixture:
# Airflow cannot be run in standalone mode for proper DAG testing because the SequentialExecutor
# does not support parallelism, making it unsuitable for testing. A LocalExecutor requires a non-sqlite database.
@pytest.fixture(scope="session")
def postgres_instance(random_test_id):
    return PostgresInstance(
        host="localhost",
        port=5432,
        user="postgres",
        password="postgres",
        airflow_db="",
        radiant_db="radiant",
        radiant_db_schema=f"test_{random_test_id}",
        internal_host="radiant-postgres",
        internal_port=5432,
    )


@pytest.fixture(scope="session")
def starrocks_iceberg_catalog(iceberg_namespace):
    catalog_name = "radiant_iceberg_catalog"
    return StarRocksIcebergCatalog(catalog=catalog_name, database=iceberg_namespace)


@pytest.fixture(scope="session")
def starrocks_jdbc_catalog(
    postgres_instance,
    postgres_clinical_seeds,
):
    catalog_name = "radiant_jdbc"
    yield StarrocksJDBCCatalog(catalog_name, postgres_instance.radiant_db_schema)
    return
