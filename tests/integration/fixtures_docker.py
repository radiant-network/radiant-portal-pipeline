import os
from pathlib import Path

import pytest
from testcontainers.core.container import DockerContainer
from testcontainers.core.network import Network
from testcontainers.core.waiting_utils import wait_for_logs

from tests.integration.fixtures_common import *

# Base path of the current file
CURRENT_DIR = Path(__file__).parent

# Path to the resources directory
RESOURCES_DIR = CURRENT_DIR.parent / "resources" / "integration"

RADIANT_DIR = CURRENT_DIR.parent.parent / "radiant"

# Constants
MINIO_IMAGE = "minio/minio:latest"

ICEBERG_REST_IMAGE = "apache/polaris:1.3.0-incubating"
ICEBERG_INIT_IMAGE = "apache/polaris-admin-tool:1.3.0-incubating"

STARROCKS_FE_HOSTNAME = "radiant-starrocks-fe"
STARROCKS_ALLIN1_HOSTNAME = "radiant-starrocks-allin1"
STARROCKS_IMAGE = "starrocks/allin1-ubuntu:3.4.2"


@pytest.fixture(scope="session")
def network():
    network = Network()
    network.create()
    yield network
    network.remove()


# Fixtures
@pytest.fixture(scope="session")
def minio_instance(network):
    minio_name = "radiant-minio"
    container = (
        DockerContainer(MINIO_IMAGE)
        .with_name(minio_name)
        .with_env("MINIO_ROOT_USER", MINIO_ACCESS_KEY)
        .with_env("MINIO_ROOT_PASSWORD", MINIO_SECRET_KEY)
        .with_exposed_ports(MINIO_API_PORT, MINIO_CONSOLE_PORT)
        .with_network(network)
        .with_command("server /data --console-address ':9001'")
    )
    container.start()
    wait_for_logs(container, "API:", timeout=30)

    api_port = container.get_exposed_port(MINIO_API_PORT)
    console_port = container.get_exposed_port(MINIO_CONSOLE_PORT)

    instance = MinioInstance(
        container.get_container_host_ip(), api_port, console_port, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, minio_name, 9000
    )

    os.environ["HTS_S3_HOST"] = f"{instance.host}:{instance.api_port}"
    os.environ["HTS_S3_ADDRESS_STYLE"] = "path"
    os.environ["AWS_ACCESS_KEY_ID"] = instance.access_key
    os.environ["AWS_SECRET_ACCESS_KEY"] = instance.secret_key
    os.environ["AWS_REGION"] = "us-east-1"
    yield instance

    container.stop()


@pytest.fixture(scope="session")
def rest_iceberg_service(network, minio_instance):
    container = (
        DockerContainer(ICEBERG_REST_IMAGE)
        .with_name("radiant-polaris-rest")
        .with_env("AWS_REGION", "us-east-1")
        .with_env("AWS_ACCESS_KEY_ID", minio_instance.access_key)
        .with_env("AWS_SECRET_ACCESS_KEY", minio_instance.secret_key)
        .with_env("AWS_ENDPOINT_URL", minio_instance.internal_endpoint)
        .with_env("POLARIS_AUTHENTICATION_TOKEN_BROKER_SYMMETRIC_KEY_SECRET", "password")
        .with_env("POLARIS_REALM_CONTEXT_REALMS", ICEBERG_REST_REALM_NAME)
        .with_env("POLARIS_AUTHENTICATION_TOKEN_BROKER_MAX_TOKEN_GENERATION", "PT1H")
        .with_env("POLARIS_AUTHENTICATION_TOKEN_BROKER_TYPE", "symmetric-key")
        .with_env("POLARIS_FEATURES_ALLOW_SETTING_S3_ENDPOINTS", "true")
        .with_env("POLARIS_FEATURES_SUPPORTED_CATALOG_STORAGE_TYPES", "S3")
        .with_env(
            "POLARIS_BOOTSTRAP_CREDENTIALS", f"{ICEBERG_REST_REALM_NAME},{ICEBERG_REST_USER},{ICEBERG_REST_PASSWORD}"
        )
        .with_exposed_ports(ICEBERG_REST_PORT)
        .with_network(network)
    )
    container.start()
    wait_for_logs(container, "Installed features", timeout=60)

    rest_port = container.get_exposed_port(ICEBERG_REST_PORT)

    yield RestIcebergServiceInstance(
        host=container.get_container_host_ip(),
        port=rest_port,
        internal_host="radiant-polaris-rest",
        internal_port=ICEBERG_REST_PORT,
    )
    container.stop()


@pytest.fixture(scope="session")
def rest_iceberg_catalog_instance(network, rest_iceberg_service, minio_instance):
    container = (
        DockerContainer(ICEBERG_INIT_IMAGE)
        .with_name("radiant-polaris-init")
        .with_env("POLARIS_SERVICE_HOST", f"{rest_iceberg_service.internal_host}:{rest_iceberg_service.internal_port}")
        .with_env("REALM_NAME", ICEBERG_REST_REALM_NAME)
        .with_env("REALM_USER", ICEBERG_REST_USER)
        .with_env("REALM_PASSWORD", ICEBERG_REST_PASSWORD)
        .with_env("CATALOG_NAME", ICEBERG_REST_CATALOG_NAME)
        .with_env("CATALOG_ENDPOINT", minio_instance.internal_endpoint)
        .with_env("CATALOG_WAREHOUSE", "s3://warehouse")
        .with_env("QUARKUS_OTEL_SDK_DISABLED", "true")
        .with_network(network)
        .with_volume_mapping(
            f"{RESOURCES_DIR}/polaris/init_polaris_catalog.sh", "/opt/scripts/init_polaris_catalog.sh"
        )
    )
    container.start()
    container.exec(["/bin/bash", "/opt/scripts/init_polaris_catalog.sh"])
    container.stop()

    yield RestIcebergCatalogInstance(
        host=rest_iceberg_service.host,
        port=rest_iceberg_service.port,
        realm=ICEBERG_REST_REALM_NAME,
        catalog_name=ICEBERG_REST_CATALOG_NAME,
        client_id=ICEBERG_REST_USER,
        client_secret=ICEBERG_REST_PASSWORD,
        internal_host=rest_iceberg_service.internal_host,
        internal_port=rest_iceberg_service.internal_port,
    )


@pytest.fixture(scope="session")
def starrocks_instance(network):
    container = (
        DockerContainer(STARROCKS_IMAGE)
        .with_name(STARROCKS_ALLIN1_HOSTNAME)
        .with_exposed_ports(STARROCKS_QUERY_PORT, STARROCKS_FE_HTTP_PORT, STARROCKS_BE_HTTP_PORT)
        .with_network(network)
    )
    container.start()
    wait_for_logs(container, "Enjoy the journey to StarRocks blazing-fast lake-house engine!", timeout=60)

    query_port = container.get_exposed_port(STARROCKS_QUERY_PORT)
    fe_http_port = container.get_exposed_port(STARROCKS_FE_HTTP_PORT)
    print(f"StarRocks FE HTTP Port: {fe_http_port}")
    print(f"StarRocks Query Port: {query_port}")
    be_http_port = container.get_exposed_port(STARROCKS_BE_HTTP_PORT)

    yield StarRocksInstance(
        host=container.get_container_host_ip(),
        query_port=query_port,
        fe_port=fe_http_port,
        be_port=be_http_port,
        user=STARROCKS_USER,
        password=STARROCKS_PWD,
        internal_host=STARROCKS_ALLIN1_HOSTNAME,
        internal_port=STARROCKS_QUERY_PORT,
    )

    container.stop()


# Note on the Postgres fixture:
# Airflow cannot be run in standalone mode for proper DAG testing because the SequentialExecutor
# does not support parallelism, making it unsuitable for testing. A LocalExecutor requires a non-sqlite database.
@pytest.fixture(scope="session")
def postgres_instance(network, random_test_id):
    pg_container = (
        DockerContainer("postgres:latest")
        .with_name("radiant-postgres")
        .with_env("POSTGRES_USER", "postgres")
        .with_env("POSTGRES_PASSWORD", "postgres")
        .with_env("POSTGRES_DB", "radiant")
        .with_env("POSTGRES_HOST_AUTH_METHOD", "trust")
        .with_exposed_ports(5432)
        .with_command("postgres -c max_connections=1000")
        .with_network(network)
    )
    pg_container.start()
    wait_for_logs(pg_container, "PostgreSQL init process complete", timeout=60)

    pg_port = pg_container.get_exposed_port(5432)
    yield PostgresInstance(
        host=pg_container.get_container_host_ip(),
        port=pg_port,
        user="postgres",
        password="postgres",
        radiant_db="radiant",
        airflow_db="airflow",
        radiant_db_schema=f"test_{random_test_id}",
        internal_host="radiant-postgres",
        internal_port=5432,
    )
    pg_container.stop()


@pytest.fixture(scope="session")
def starrocks_iceberg_catalog(starrocks_session, rest_iceberg_catalog_instance, minio_instance, iceberg_namespace):
    with starrocks_session.cursor() as cursor:
        catalog_name = "radiant_iceberg_catalog"

        cursor.execute(f"""
        CREATE EXTERNAL CATALOG '{catalog_name}'
        COMMENT 'External catalog to Apache Iceberg on MinIO'
        PROPERTIES
        (
            'type'='iceberg',
            'iceberg.catalog.type'='rest',
            'iceberg.catalog.uri'='http://{rest_iceberg_catalog_instance.internal_host}:{rest_iceberg_catalog_instance.internal_port}/api/catalog',
            'iceberg.catalog.warehouse'='{rest_iceberg_catalog_instance.catalog_name}',
            'iceberg.catalog.security'='oauth2',
            'iceberg.catalog.oauth2.credential'='{rest_iceberg_catalog_instance.client_id}:{rest_iceberg_catalog_instance.client_secret}',
            'iceberg.catalog.oauth2.scope'='PRINCIPAL_ROLE:CATALOG_MANAGE_CONTENT',
            'iceberg.catalog.vended-credentials-enabled'='false',
            'aws.s3.region'='us-east-1',
            'aws.s3.access_key'='{minio_instance.access_key}',
            'aws.s3.secret_key'='{minio_instance.secret_key}',
            'aws.s3.endpoint'='{minio_instance.internal_endpoint}',
            'aws.s3.enable_path_style_access'='true',
            'client.factory'='com.starrocks.connector.share.iceberg.IcebergAwsClientFactory'
        );""")
        starrocks_session.commit()
        yield StarRocksIcebergCatalog(catalog=catalog_name, database=iceberg_namespace)


@pytest.fixture(scope="session")
def starrocks_jdbc_catalog(
    starrocks_session,
    postgres_instance,
    postgres_clinical_seeds,
    random_test_id,
):
    with starrocks_session.cursor() as cursor:
        catalog_name = "radiant_jdbc"
        cursor.execute(f"""
        CREATE EXTERNAL CATALOG '{catalog_name}'
        COMMENT 'External Clinical Catalog'
        PROPERTIES (
            'driver_class'='org.postgresql.Driver',
            'checksum'='bef0b2e1c6edcd8647c24bed31e1a4ac',
            'driver_url'='https://repo1.maven.org/maven2/org/postgresql/postgresql/42.3.3/postgresql-42.3.3.jar',
            'type'='jdbc',
            'user'='{postgres_instance.user}',
            'password'='{postgres_instance.password}',
            'jdbc_uri'='jdbc:postgresql://{postgres_instance.internal_host}:{postgres_instance.internal_port}/{postgres_instance.radiant_db}'
        );""")
        starrocks_session.commit()
        yield StarrocksJDBCCatalog(catalog_name, postgres_instance.radiant_db_schema)
