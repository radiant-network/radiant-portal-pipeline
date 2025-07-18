import os
import tempfile
import time
import uuid
from pathlib import Path

import docker
import fsspec
import pymysql
import pysam
import pytest
from pyiceberg.catalog.rest import RestCatalog
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

from radiant.dags import ICEBERG_NAMESPACE
from radiant.tasks.vcf.consequence import SCHEMA as CONSEQUENCE_SCHEMA
from radiant.tasks.vcf.occurrence import SCHEMA as OCCURRENCE_SCHEMA
from radiant.tasks.vcf.variant import SCHEMA as VARIANT_SCHEMA

# Base path of the current file
CURRENT_DIR = Path(__file__).parent

# Path to the resources directory
RESOURCES_DIR = CURRENT_DIR.parent / "resources" / "integration"

RADIANT_DIR = CURRENT_DIR.parent.parent / "radiant"

# Constants
MINIO_IMAGE = "minio/minio:latest"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password"
MINIO_API_PORT = 9000
MINIO_CONSOLE_PORT = 9001

ICEBERG_REST_IMAGE = "ferlabcrsj/iceberg-rest-catalog:1.0.1"
ICEBERG_REST_PORT = 8181
ICEBERG_REST_CATALOG_NAME = "radiant"
ICEBERG_REST_TOKEN = "mysecret"

POSTGRES_PORT = 5432

STARROCKS_FE_HOSTNAME = "radiant-starrocks-fe"
STARROCKS_ALLIN1_HOSTNAME = "radiant-starrocks-allin1"
STARROCKS_IMAGE = "starrocks/allin1-ubuntu:3.4.2"
STARROCKS_FE_HTTP_PORT = 8030
STARROCKS_BE_HTTP_PORT = 8040
STARROCKS_QUERY_PORT = 9030
STARROCKS_USER = "root"
STARROCKS_PWD = ""
STARROCKS_DATABASE_PREFIX = "test"
STARROCKS_ICEBERG_CATALOG_NAME_PREFIX = "iceberg_catalog"
STARROCKS_JDBC_CATALOG_NAME_PREFIX = "jdbc_catalog"
STARROCKS_ICEBERG_DB_NAME_PREFIX = "ns"

AIRFLOW_API_PORT = 8080


# Utility classes
class MinioInstance:
    def __init__(self, host, api_port, console_port, access_key, secret_key):
        self.host = host
        self.api_port = api_port
        self.console_port = console_port
        self.endpoint = f"http://{host}:{api_port}"
        self.console_url = f"http://{host}:{console_port}"
        self.access_key = access_key
        self.secret_key = secret_key


class IcebergInstance:
    def __init__(self, host, port, catalog_name, token):
        self.host = host
        self.port = port
        self.endpoint = f"http://{host}:{port}"
        self.token = token
        self.catalog_name = catalog_name


class StarRocksEnvironment:
    def __init__(self, host, query_port, fe_port, be_port, user, password, database):
        self.host = host
        self.query_port = query_port
        self.fe_port = fe_port
        self.be_port = be_port
        self.endpoint = f"http://{host}:{query_port}"
        self.user = user
        self.password = password
        self.database = database


class StarRocksIcebergCatalog:
    def __init__(self, name):
        self.name = name


class StarRocksIcebergDatabase:
    def __init__(self, catalog, name):
        self.catalog = catalog
        self.name = name


class PostgresInstance:
    def __init__(self, host, port, user, password, airflow_db, radiant_db):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.airflow_db = airflow_db
        self.radiant_db = radiant_db


class RadiantAirflowInstance:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.endpoint = f"http://{host}:{port}"


@pytest.fixture(scope="session")
def random_test_id():
    """
    Fixture to provide a random test ID for the session.
    """
    return uuid.uuid4().hex[:8]


@pytest.fixture(scope="session")
def resources_dir():
    """
    Fixture to provide the path to the resources directory.
    """
    return RESOURCES_DIR


# Fixtures
@pytest.fixture(scope="session")
def minio_container():
    client = docker.from_env()

    for container in client.containers.list():
        if "radiant-minio" in container.name:
            ports = container.attrs["NetworkSettings"]["Ports"]
            api_port = ports[f"{MINIO_API_PORT}/tcp"][0]["HostPort"]
            console_port = ports[f"{MINIO_CONSOLE_PORT}/tcp"][0]["HostPort"]
            instance = MinioInstance("localhost", api_port, console_port, MINIO_ACCESS_KEY, MINIO_SECRET_KEY)
            os.environ["HTS_S3_HOST"] = f"localhost:{api_port}"
            os.environ["HTS_S3_ADDRESS_STYLE"] = "path"
            os.environ["AWS_ACCESS_KEY_ID"] = instance.access_key
            os.environ["AWS_SECRET_ACCESS_KEY"] = instance.secret_key
            os.environ["AWS_REGION"] = "us-east-1"
            yield instance
            return

    container = (
        DockerContainer(MINIO_IMAGE)
        .with_name("radiant-minio")
        .with_env("MINIO_ROOT_USER", MINIO_ACCESS_KEY)
        .with_env("MINIO_ROOT_PASSWORD", MINIO_SECRET_KEY)
        .with_exposed_ports(MINIO_API_PORT, MINIO_CONSOLE_PORT)
        .with_command("server /data --console-address ':9001'")
    )
    container.start()
    wait_for_logs(container, "API:", timeout=30)

    api_port = container.get_exposed_port(MINIO_API_PORT)
    console_port = container.get_exposed_port(MINIO_CONSOLE_PORT)

    instance = MinioInstance("localhost", api_port, console_port, MINIO_ACCESS_KEY, MINIO_SECRET_KEY)
    os.environ["HTS_S3_HOST"] = f"localhost:{api_port}"
    os.environ["HTS_S3_ADDRESS_STYLE"] = "path"
    os.environ["AWS_ACCESS_KEY_ID"] = instance.access_key
    os.environ["AWS_SECRET_ACCESS_KEY"] = instance.secret_key
    os.environ["AWS_REGION"] = "us-east-1"
    yield instance

    container.stop()


@pytest.fixture(scope="session")
def iceberg_container(minio_container):
    client = docker.from_env()

    for container in client.containers.list():
        if "radiant-iceberg-rest" in container.name:
            ports = container.attrs["NetworkSettings"]["Ports"]
            rest_port = ports[f"{ICEBERG_REST_PORT}/tcp"][0]["HostPort"]
            yield IcebergInstance("localhost", rest_port, ICEBERG_REST_CATALOG_NAME, ICEBERG_REST_TOKEN)
            return

    container = (
        DockerContainer(ICEBERG_REST_IMAGE)
        .with_name("radiant-iceberg-rest")
        .with_env("CATALOG_WAREHOUSE", "s3a://warehouse")
        .with_env("CATALOG_CATALOG__NAME", ICEBERG_REST_CATALOG_NAME)
        .with_env("CATALOG_S3_ENDPOINT", minio_container.endpoint)
        .with_env("CATALOG_S3_ACCESS_KEY_ID", minio_container.access_key)
        .with_env("CATALOG_S3_SECRET_ACCESS_KEY", minio_container.secret_key)
        .with_env("CATALOG_SECRET", ICEBERG_REST_TOKEN)
        .with_env("CATALOG_CATALOG__IMPL", "org.apache.iceberg.inmemory.InMemoryCatalog")
        .with_exposed_ports(ICEBERG_REST_PORT)
    )
    container.start()
    wait_for_logs(container, "Started ServerConnector", timeout=60)

    rest_port = container.get_exposed_port(ICEBERG_REST_PORT)

    yield IcebergInstance("localhost", rest_port, ICEBERG_REST_CATALOG_NAME, ICEBERG_REST_TOKEN)

    container.stop()


@pytest.fixture(scope="session")
def starrocks_container(minio_container, random_test_id):
    client = docker.from_env()

    test_db_name = f"{STARROCKS_DATABASE_PREFIX}_{random_test_id}"

    for container in client.containers.list():
        if STARROCKS_FE_HOSTNAME in container.name:
            ports = container.attrs["NetworkSettings"]["Ports"]
            query_port = ports[f"{STARROCKS_QUERY_PORT}/tcp"][0]["HostPort"]
            fe_http_port = ports[f"{STARROCKS_FE_HTTP_PORT}/tcp"][0]["HostPort"]

            with (
                pymysql.connect(
                    host="localhost",
                    port=int(query_port),
                    user=STARROCKS_USER,
                    password=STARROCKS_PWD,
                ) as connection,
                connection.cursor() as cursor,
            ):
                cursor.execute(f"CREATE DATABASE IF NOT EXISTS {test_db_name};")
                connection.commit()
                yield StarRocksEnvironment(
                    "localhost", query_port, fe_http_port, None, STARROCKS_USER, STARROCKS_PWD, test_db_name
                )
                cursor.execute(f"DROP DATABASE IF EXISTS {test_db_name};")
                connection.commit()
            return

    container = (
        DockerContainer(STARROCKS_IMAGE)
        .with_name(STARROCKS_ALLIN1_HOSTNAME)
        .with_exposed_ports(STARROCKS_QUERY_PORT, STARROCKS_FE_HTTP_PORT, STARROCKS_BE_HTTP_PORT)
    )
    container.start()
    wait_for_logs(container, "Enjoy the journey to StarRocks blazing-fast lake-house engine!", timeout=60)

    query_port = container.get_exposed_port(STARROCKS_QUERY_PORT)
    fe_http_port = container.get_exposed_port(STARROCKS_FE_HTTP_PORT)
    be_http_port = container.get_exposed_port(STARROCKS_BE_HTTP_PORT)

    with (
        pymysql.connect(
            host="localhost",
            port=int(query_port),
            user=STARROCKS_USER,
            password=STARROCKS_PWD,
        ) as connection,
        connection.cursor() as cursor,
    ):
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {test_db_name};")
        connection.commit()

    yield StarRocksEnvironment(
        host="localhost",
        query_port=query_port,
        fe_port=fe_http_port,
        be_port=be_http_port,
        user=STARROCKS_USER,
        password=STARROCKS_PWD,
        database=test_db_name,
    )

    container.stop()


# Note on the Postgres fixture:
# Airflow cannot be run in standalone mode for proper DAG testing because the SequentialExecutor
# does not support parallelism, making it unsuitable for testing. A LocalExecutor requires a non-sqlite database.
@pytest.fixture(scope="session")
def postgres_container(host_internal_address, random_test_id):
    client = docker.from_env()

    for container in client.containers.list():
        if "postgres" in container.name:
            ports = container.attrs["NetworkSettings"]["Ports"]
            pg_port = ports[f"{POSTGRES_PORT}/tcp"][0]["HostPort"]
            pg_host = ports[f"{POSTGRES_PORT}/tcp"][0]["HostIp"]

            _common_args = ["psql", "postgresql://postgres:postgres@localhost:5432/postgres", "-c"]
            container.exec_run(_common_args + [f"CREATE DATABASE test_{random_test_id}_airflow WITH OWNER postgres;"])
            container.exec_run(_common_args + [f"CREATE DATABASE test_{random_test_id}_radiant WITH OWNER postgres;"])
            container.exec_run(_common_args + [f"CREATE SCHEMA IF NOT EXISTS test_{random_test_id}_iceberg_catalog;"])

            yield PostgresInstance(
                host=pg_host,
                port=pg_port,
                user="postgres",
                password="postgres",
                airflow_db=f"test_{random_test_id}_airflow",
                radiant_db=f"test_{random_test_id}_radiant",
            )

            container.exec_run(_common_args + [f"DROP DATABASE test_{random_test_id}_airflow WITH (force);"])
            container.exec_run(_common_args + [f"DROP DATABASE test_{random_test_id}_radiant WITH (force);"])
            container.exec_run(_common_args + [f"DROP SCHEMA test_{random_test_id}_iceberg_catalog WITH (force);"])
            return

    pg_container = (
        DockerContainer("postgres:latest")
        .with_name("radiant-postgres")
        .with_env("POSTGRES_USER", "postgres")
        .with_env("POSTGRES_PASSWORD", "postgres")
        .with_env("POSTGRES_DB", "postgres")
        .with_exposed_ports(5432)
        .with_command("postgres -c max_connections=1000")
    )
    pg_container.start()
    wait_for_logs(pg_container, "PostgreSQL init process complete", timeout=60)

    _common_args = ["psql", "postgresql://postgres:postgres@localhost:5432/postgres", "-c"]
    pg_container.exec(_common_args + ["CREATE DATABASE airflow WITH OWNER postgres;"])
    pg_container.exec(_common_args + ["CREATE DATABASE radiant WITH OWNER postgres;"])
    pg_container.exec(_common_args + ["CREATE SCHEMA IF NOT EXISTS iceberg_catalog;"])

    pg_port = pg_container.get_exposed_port(5432)
    yield PostgresInstance(
        host=host_internal_address,
        port=pg_port,
        user="postgres",
        password="postgres",
        radiant_db="radiant",
        airflow_db="airflow",
    )
    pg_container.stop()


@pytest.fixture(scope="session")
def postgres_clinical_seeds(postgres_container):
    import psycopg2

    sql_path = Path(__file__).parent.parent / "resources" / "clinical" / "create_clinical_tables.sql"
    with open(sql_path) as f:
        tables_sql = f.read()

    sql_path = Path(__file__).parent.parent / "resources" / "clinical" / "seeds.sql"
    with open(sql_path) as f:
        seeds_sql = f.read()

    with (
        psycopg2.connect(
            host="localhost",
            port=postgres_container.port,
            user=postgres_container.user,
            password=postgres_container.password,
            database=postgres_container.radiant_db,
        ) as conn,
        conn.cursor() as cur,
    ):
        cur.execute(tables_sql)
        cur.execute(seeds_sql)
        conn.commit()
    yield


@pytest.fixture(scope="session")
def radiant_airflow_container(
    host_internal_address, postgres_container, starrocks_container, iceberg_container, minio_container, random_test_id
):
    client = docker.from_env()

    for container in client.containers.list():
        if "airflow-webserver" in container.name:
            container.exec_run(["airflow", "connections", "delete", "starrocks_conn"])
            container.exec_run(
                [
                    "airflow",
                    "connections",
                    "add",
                    "starrocks_conn",
                    "--conn-uri",
                    (
                        f"mysql://{starrocks_container.user}:{starrocks_container.password}"
                        f"@{host_internal_address}:{starrocks_container.query_port}"
                        f"/{starrocks_container.database}"
                    ),
                ]
            )
            yield container
            container.exec_run(["airflow", "connections", "delete", "starrocks_conn"])
            container.exec_run(
                [
                    "airflow",
                    "connections",
                    "add",
                    "starrocks_conn",
                    "--conn-uri",
                    (
                        f"mysql://{starrocks_container.user}:{starrocks_container.password}"
                        f"@{host_internal_address}:{starrocks_container.query_port}"
                        f"/radiant"
                    ),
                ]
            )
            return

    env_vars = {
        "PYICEBERG_CATALOG__DEFAULT__URI": f"http://{host_internal_address}:{iceberg_container.port}",
        "PYICEBERG_CATALOG__DEFAULT__S3__ENDPOINT": f"http://{host_internal_address}:{minio_container.api_port}",
        "PYICEBERG_CATALOG__DEFAULT__TOKEN": ICEBERG_REST_TOKEN,
        "AIRFLOW__CORE__DAGS_FOLDER": "/opt/airflow/radiant/dags",
        "AIRFLOW__CORE__EXECUTOR": "LocalExecutor",
        "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": f"postgresql+psycopg2://{postgres_container.user}:{postgres_container.password}@{postgres_container.host}:{postgres_container.port}/{postgres_container.airflow_db}",
        "PYTHONPATH": "$PYTHONPATH:/opt/airflow",
        "HTS_S3_HOST": f"{host_internal_address}:{minio_container.api_port}",
        "HTS_S3_ADDRESS_STYLE": "path",
        "AWS_ACCESS_KEY_ID": "admin",
        "AWS_SECRET_ACCESS_KEY": "password",
        "AWS_REGION": "us-east-1",
    }

    container = (
        DockerContainer("radiant-airflow:latest")
        .with_name("radiant-airflow")
        .with_command("standalone")
        .with_volume_mapping(host=str(RADIANT_DIR), container="/opt/airflow/radiant")
        .with_exposed_ports(AIRFLOW_API_PORT)
    )
    container.env |= env_vars
    container.start()
    wait_for_logs(container, "Starting gunicorn", timeout=60)

    # Add delay and configure Airflow
    time.sleep(20)
    container.exec(
        [
            "airflow",
            "connections",
            "add",
            "starrocks_conn",
            "--conn-uri",
            (
                f"mysql://{starrocks_container.user}:{starrocks_container.password}"
                f"@{host_internal_address}:{starrocks_container.query_port}"
                f"/{starrocks_container.database}"
            ),
        ]
    )
    container.exec(["airflow", "pools", "set", "starrocks_insert_pool", "1", "StarRocks insert pool"])
    container.exec(["airflow", "pools", "set", "import_vcf", "8", "VCF import pool"])
    container.exec(["airflow", "pools", "set", "import_part", "1", "Partition import pool"])

    yield container
    container.stop()


@pytest.fixture(scope="session")
def starrocks_session(starrocks_container):
    with pymysql.connect(
        host=starrocks_container.host,
        port=int(starrocks_container.query_port),
        password=starrocks_container.password,
        user=starrocks_container.user,
        database=starrocks_container.database,
    ) as connection:
        yield connection


@pytest.fixture(scope="session")
def s3_fs(minio_container):
    fs = fsspec.filesystem(
        "s3",
        key=minio_container.access_key,
        secret=minio_container.secret_key,
        client_kwargs={"endpoint_url": minio_container.endpoint},
    )
    fs.mkdirs("warehouse", exist_ok=True)
    fs.mkdirs("vcf", exist_ok=True)
    fs.mkdirs("opendata", exist_ok=True)
    fs.mkdirs("exomiser", exist_ok=True)
    return fs


@pytest.fixture(scope="session")
def iceberg_catalog_properties(iceberg_container, minio_container):
    return {
        "uri": iceberg_container.endpoint,
        "token": iceberg_container.token,
        "s3.endpoint": minio_container.endpoint,
        "s3.access-key-id": minio_container.access_key,
        "s3.secret-access-key": minio_container.secret_key,
    }


@pytest.fixture(scope="session")
def iceberg_client(iceberg_container, iceberg_catalog_properties):
    return RestCatalog(name=iceberg_container.catalog_name, **iceberg_catalog_properties)


@pytest.fixture(scope="session")
def starrocks_iceberg_catalog(
    host_internal_address, starrocks_session, iceberg_container, minio_container, random_test_id
):
    with starrocks_session.cursor() as cursor:
        catalog_name = "radiant_iceberg_catalog"

        cursor.execute("SHOW CATALOGS;")
        catalog_exists = catalog_name in [row[0] for row in cursor.fetchall()]
        if catalog_exists:
            cursor.execute(f"DROP CATALOG {catalog_name};")
            starrocks_session.commit()

        cursor.execute(f"""
        CREATE EXTERNAL CATALOG '{catalog_name}'
        COMMENT 'External catalog to Apache Iceberg on MinIO'
        PROPERTIES
        (
            'type'='iceberg',
            'iceberg.catalog.type'='rest',
            'iceberg.catalog.uri'='http://{host_internal_address}:{iceberg_container.port}',
            'iceberg.catalog.token' = '{iceberg_container.token}',
            'aws.s3.region'='us-east-1',
            'aws.s3.access_key'='{minio_container.access_key}',
            'aws.s3.secret_key'='{minio_container.secret_key}',
            'aws.s3.endpoint'='http://{host_internal_address}:{minio_container.api_port}',
            'aws.s3.enable_path_style_access'='true',
            'client.factory'='com.starrocks.connector.share.iceberg.IcebergAwsClientFactory'
        );""")
        starrocks_session.commit()
        yield StarRocksIcebergCatalog(name=catalog_name)
        cursor.execute(f"DROP CATALOG {catalog_name};")
        starrocks_session.commit()
        if catalog_exists:
            cursor.execute("""CREATE EXTERNAL CATALOG 'radiant_iceberg_catalog'
              COMMENT 'External catalog to Apache Iceberg on MinIO'
              PROPERTIES
              (
                'type'='iceberg',
                'iceberg.catalog.type'='rest',
                'iceberg.catalog.uri'='http://radiant-iceberg-rest:8181',
                'iceberg.catalog.token' = 'mysecret',
                'aws.s3.region'='us-east-1',
                'aws.s3.access_key'='admin',
                'aws.s3.secret_key'='password',
                'aws.s3.endpoint'='http://radiant-minio:9000',
                'aws.s3.enable_path_style_access'='true',
                'client.factory'='com.starrocks.connector.share.iceberg.IcebergAwsClientFactory'
              );
            """)
            starrocks_session.commit()


@pytest.fixture(scope="session")
def starrocks_jdbc_catalog(
    host_internal_address,
    starrocks_session,
    postgres_container,
    postgres_clinical_seeds,
    minio_container,
    random_test_id,
):
    with starrocks_session.cursor() as cursor:
        catalog_name = "radiant_jdbc"
        cursor.execute("SHOW CATALOGS;")
        catalog_exists = catalog_name in [row[0] for row in cursor.fetchall()]
        if catalog_exists:
            cursor.execute(f"DROP CATALOG {catalog_name};")
            starrocks_session.commit()

        cursor.execute(f"""
        CREATE EXTERNAL CATALOG '{catalog_name}'
        COMMENT 'External Clinical Catalog'
        PROPERTIES (
            'driver_class'='org.postgresql.Driver',
            'checksum'='bef0b2e1c6edcd8647c24bed31e1a4ac',
            'driver_url'='https://repo1.maven.org/maven2/org/postgresql/postgresql/42.3.3/postgresql-42.3.3.jar',
            'type'='jdbc',
            'user'='{postgres_container.user}',
            'password'='{postgres_container.password}',
            'jdbc_uri'='jdbc:postgresql://{host_internal_address}:{postgres_container.port}/{postgres_container.radiant_db}'
        );""")
        starrocks_session.commit()
        yield catalog_name
        cursor.execute(f"DROP CATALOG {catalog_name};")
        starrocks_session.commit()
        if catalog_exists:
            cursor.execute("""CREATE EXTERNAL CATALOG radiant_jdbc
              PROPERTIES (
                'driver_class'='org.postgresql.Driver',
                'checksum'='bef0b2e1c6edcd8647c24bed31e1a4ac',
                'driver_url'='https://repo1.maven.org/maven2/org/postgresql/postgresql/42.3.3/postgresql-42.3.3.jar',
                'type'='jdbc',
                'user'='postgres',
                'password'='postgres',
                'jdbc_uri'='jdbc:postgresql://postgres:5432/radiant'
                )
            """)
            starrocks_session.commit()


@pytest.fixture(scope="session")
def setup_namespace(s3_fs, iceberg_client, random_test_id):
    namespace = ICEBERG_NAMESPACE
    _exists = iceberg_client.namespace_exists(namespace)
    if _exists:
        iceberg_client.create_namespace_if_not_exists(f"tmp_{random_test_id}_{namespace}")
        for table in ["germline_snv_occurrence", "germline_snv_variant", "germline_snv_consequence"]:
            if iceberg_client.table_exists(f"{namespace}.{table}"):
                iceberg_client.rename_table(f"{namespace}.{table}", f"tmp_{random_test_id}_{namespace}.{table}")
    else:
        iceberg_client.create_namespace(namespace)

    iceberg_client.create_table_if_not_exists(f"{namespace}.germline_snv_occurrence", schema=OCCURRENCE_SCHEMA)
    iceberg_client.create_table_if_not_exists(f"{namespace}.germline_snv_variant", schema=VARIANT_SCHEMA)
    iceberg_client.create_table_if_not_exists(f"{namespace}.germline_snv_consequence", schema=CONSEQUENCE_SCHEMA)
    yield namespace

    if _exists:
        for table in ["germline_snv_occurrence", "germline_snv_variant", "germline_snv_consequence"]:
            if iceberg_client.table_exists(f"{namespace}.{table}"):
                iceberg_client.drop_table(f"{namespace}.{table}")
            iceberg_client.rename_table(f"tmp_{random_test_id}_{namespace}.{table}", f"{namespace}.{table}")
        iceberg_client.drop_namespace(f"tmp_{random_test_id}_{namespace}")


VCF_SOURCE_DIR = "resources/vcf"


def compress_and_index_vcf(source_path, dest_path):
    # Read original VCF
    vcf_in = pysam.VariantFile(source_path, "r")

    # Write compressed VCF with tabix-compatible BGZF format
    vcf_out = pysam.VariantFile(dest_path, "wz", header=vcf_in.header)
    for rec in vcf_in:
        vcf_out.write(rec)

    vcf_out.close()
    vcf_in.close()
    # Index
    pysam.tabix_index(dest_path, preset="vcf", force=True)


@pytest.fixture(scope="session")
def indexed_vcfs(s3_fs):
    """
    Compress and index all VCFs in test/resources/vcf into a temp directory.
    Yields a dict of {filename: path_to_compressed_vcf}
    """

    with tempfile.TemporaryDirectory() as tmpdir:
        output = {}
        for filename in os.listdir(RESOURCES_DIR):
            if filename.endswith(".vcf"):
                src_path = RESOURCES_DIR / filename
                dest_path = os.path.join(tmpdir, filename + ".gz")
                compress_and_index_vcf(src_path, dest_path)
                s3_fs.put(dest_path, "vcf/" + filename + ".gz")
                s3_fs.put(dest_path + ".tbi", "vcf/" + filename + ".gz.tbi")
                output[filename] = "s3+http://vcf/" + filename + ".gz"

        yield output


@pytest.fixture(scope="session")
def clinical_vcf(s3_fs, starrocks_session, starrocks_jdbc_catalog):
    """
    Creates "mock" VCFs for clinical documents.
    """
    reference_vcf = "test.vcf"
    with tempfile.TemporaryDirectory() as tmpdir:
        with open(RESOURCES_DIR / reference_vcf) as f:
            vcf_content = f.read()

        with starrocks_session.cursor() as cursor:
            cursor.execute(f"""
                SELECT 
                     aliquot,
                     d.name
                FROM {starrocks_jdbc_catalog}.public.sequencing_experiment se
                LEFT JOIN {starrocks_jdbc_catalog}.public.task_has_sequencing_experiment thse 
                ON se.id = thse.sequencing_experiment_id
                LEFT JOIN {starrocks_jdbc_catalog}.public.task_has_document thd ON thse.task_id = thd.task_id
                LEFT JOIN {starrocks_jdbc_catalog}.public.document d ON thd.document_id = d.id
                WHERE REGEXP(d.name, '\\.vcf\\.gz$')
                """)
            results = cursor.fetchall()

        for aliquot, document_name in results:
            _path = document_name.replace(".gz", "")
            src_path = os.path.join(tmpdir, f"source_{_path}")
            dest_path = os.path.join(tmpdir, f"{_path}.gz")

            with open(src_path, "w") as f:
                _new_content = vcf_content.replace("SA0001", str(aliquot))
                f.write(_new_content)

            compress_and_index_vcf(src_path, dest_path)
            s3_fs.put(dest_path, "vcf/" + document_name)
            s3_fs.put(dest_path + ".tbi", "vcf/" + document_name + ".tbi")


@pytest.fixture(scope="session")
def sample_exomiser_tsv(s3_fs):
    """
    Uploads the sample Exomiser TSV to S3.
    """
    src_path = RESOURCES_DIR / "exomiser" / "sample.variants.tsv"
    dest_path = "exomiser/"
    s3_fs.put(src_path, dest_path)
    yield f"{dest_path}sample.variants.tsv"


@pytest.fixture(scope="session")
def clinvar_rcv_summary_ndjson(s3_fs):
    """
    Uploads the ClinVar RCV summary JSON to S3.
    """
    src_path = RESOURCES_DIR / "open_data" / "clinvar_rcv_summary.ndjson"
    dest_path = "opendata/"
    s3_fs.put(src_path, dest_path)
    yield dest_path
