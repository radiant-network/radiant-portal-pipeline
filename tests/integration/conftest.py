import os
import tempfile
import uuid
from pathlib import Path

import fsspec
import jinja2
import pymysql
import pysam
from pyiceberg.catalog.rest import RestCatalog

from radiant.dags import ICEBERG_NAMESPACE
from radiant.tasks.data.radiant_tables import CLINICAL_DATABASE_ENV_KEY, get_clinical_mapping
from radiant.tasks.vcf.snv.germline.consequence import SCHEMA as CONSEQUENCE_SCHEMA
from radiant.tasks.vcf.snv.germline.occurrence import SCHEMA as OCCURRENCE_SCHEMA
from radiant.tasks.vcf.snv.germline.variant import SCHEMA as VARIANT_SCHEMA

USE_DOCKER_FIXTURES = os.getenv("USE_DOCKER_FIXTURES", "false").lower() == "true"
if USE_DOCKER_FIXTURES:
    from tests.integration.fixtures_docker import *
else:
    from tests.integration.fixtures_sandbox import *

# Base path of the current file
CURRENT_DIR = Path(__file__).parent

# Path to the resources directory
RESOURCES_DIR = CURRENT_DIR.parent / "resources" / "integration"

RADIANT_DIR = CURRENT_DIR.parent.parent / "radiant"


class StarRocksDatabase:
    def __init__(self, host, query_port, user, password, database):
        self.host = host
        self.query_port = query_port
        self.user = user
        self.password = password
        self.database = database


@pytest.fixture(scope="session")
def starrocks_database(starrocks_instance, random_test_id):
    test_db_name = f"{STARROCKS_DATABASE_PREFIX}_{random_test_id}"

    with (
        pymysql.connect(
            host=starrocks_instance.host,
            port=int(starrocks_instance.query_port),
            user=starrocks_instance.user,
            password=starrocks_instance.password,
        ) as connection,
        connection.cursor() as cursor,
    ):
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {test_db_name};")
        connection.commit()
        yield StarRocksDatabase(
            host=starrocks_instance.host,
            query_port=starrocks_instance.query_port,
            user=starrocks_instance.user,
            password=starrocks_instance.password,
            database=test_db_name,
        )
        cursor.execute(f"DROP DATABASE IF EXISTS {test_db_name};")
        connection.commit()
        return


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
def postgres_clinical_seeds(postgres_instance):
    from pathlib import Path

    import psycopg2

    sql_path = Path(__file__).parent.parent / "resources" / "clinical" / "create_clinical_tables.sql"
    with open(sql_path) as f:
        tables_sql = f.read()

    sql_path = Path(__file__).parent.parent.parent / "radiant" / "dags" / "sql" / "clinical" / "seeds.sql"
    with open(sql_path) as f:
        template_seeds_sql = f.read()
        _query_params = get_clinical_mapping({CLINICAL_DATABASE_ENV_KEY: postgres_instance.radiant_db_schema})
        _query_params = {
            key: value.replace("radiant_jdbc.", "").replace("`", "") for key, value in _query_params.items()
        }
        _query_params["vcf_bucket_prefix"] = "s3://test-vcf"
        seeds_sql = jinja2.Template(template_seeds_sql).render({"params": _query_params})

    # Step 1: Create the new database (autocommit required)
    conn = psycopg2.connect(
        host=postgres_instance.host,
        port=postgres_instance.port,
        user=postgres_instance.user,
        password=postgres_instance.password,
        database="radiant",
    )
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(f"CREATE SCHEMA {postgres_instance.radiant_db_schema};")
    cur.close()
    conn.close()

    # Connect to the new database to run schema + seed
    with psycopg2.connect(
        host="localhost",
        port=postgres_instance.port,
        user=postgres_instance.user,
        password=postgres_instance.password,
        database=postgres_instance.radiant_db,
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SET search_path TO {postgres_instance.radiant_db_schema};")
            cur.execute(tables_sql)
            cur.execute(seeds_sql)
            conn.commit()
            yield

        # Step 3: Teardown (drop the database)
    conn = psycopg2.connect(
        host="localhost",
        port=postgres_instance.port,
        user=postgres_instance.user,
        password=postgres_instance.password,
        database="radiant",
    )
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(f"DROP SCHEMA IF EXISTS {postgres_instance.radiant_db_schema} CASCADE;")
    cur.close()
    conn.close()


@pytest.fixture(scope="session")
def radiant_airflow_container(
    host_internal_address,
    postgres_instance,
    starrocks_database,
    rest_iceberg_catalog_instance,
    minio_instance,
    random_test_id,
):
    return RadiantAirflowInstance(host="localhost", port="8080", username="airflow", password="airflow")


@pytest.fixture(scope="session")
def starrocks_session(starrocks_database):
    with pymysql.connect(
        host=starrocks_database.host,
        port=int(starrocks_database.query_port),
        password=starrocks_database.password,
        user=starrocks_database.user,
        database=starrocks_database.database,
    ) as connection:
        yield connection


@pytest.fixture(scope="session")
def s3_fs(minio_instance):
    fs = fsspec.filesystem(
        "s3",
        key=minio_instance.access_key,
        secret=minio_instance.secret_key,
        client_kwargs={"endpoint_url": minio_instance.endpoint},
    )
    fs.mkdirs("warehouse", exist_ok=True)
    fs.mkdirs("test-vcf", exist_ok=True)
    fs.mkdirs("opendata", exist_ok=True)
    fs.mkdirs("exomiser", exist_ok=True)
    return fs


@pytest.fixture(scope="session")
def iceberg_catalog_properties(rest_iceberg_catalog_instance, minio_instance):
    return {
        "uri": rest_iceberg_catalog_instance.endpoint,
        "token": rest_iceberg_catalog_instance.token,
        "s3.endpoint": minio_instance.endpoint,
        "s3.access-key-id": minio_instance.access_key,
        "s3.secret-access-key": minio_instance.secret_key,
    }


@pytest.fixture(scope="session")
def iceberg_client(rest_iceberg_catalog_instance, iceberg_catalog_properties):
    return RestCatalog(name=rest_iceberg_catalog_instance.catalog_name, **iceberg_catalog_properties)


@pytest.fixture(scope="session")
def iceberg_namespace(random_test_id, iceberg_client):
    """
    Generates a namespace for the Iceberg catalog.
    """
    ns = f"test_{random_test_id}_{ICEBERG_NAMESPACE}"
    yield ns
    if iceberg_client.namespace_exists(ns):
        for table in iceberg_client.list_tables(ns):
            iceberg_client.drop_table(table)
        iceberg_client.drop_namespace(namespace=ns)


@pytest.fixture(scope="session")
def setup_iceberg_namespace(s3_fs, iceberg_client, iceberg_namespace, random_test_id):
    iceberg_client.create_namespace(namespace=iceberg_namespace)
    iceberg_client.create_table_if_not_exists(f"{iceberg_namespace}.germline_snv_occurrence", schema=OCCURRENCE_SCHEMA)
    iceberg_client.create_table_if_not_exists(f"{iceberg_namespace}.germline_snv_variant", schema=VARIANT_SCHEMA)
    iceberg_client.create_table_if_not_exists(
        f"{iceberg_namespace}.germline_snv_consequence", schema=CONSEQUENCE_SCHEMA
    )
    yield iceberg_namespace


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
def indexed_vcfs():
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
                output[filename] = dest_path

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
                FROM {starrocks_jdbc_catalog.catalog}.{starrocks_jdbc_catalog.database}.sequencing_experiment se
                LEFT JOIN {starrocks_jdbc_catalog.catalog}.{starrocks_jdbc_catalog.database}.task_has_sequencing_experiment thse 
                ON se.id = thse.sequencing_experiment_id
                LEFT JOIN {starrocks_jdbc_catalog.catalog}.{starrocks_jdbc_catalog.database}.task_has_document thd ON thse.task_id = thd.task_id
                LEFT JOIN {starrocks_jdbc_catalog.catalog}.{starrocks_jdbc_catalog.database}.document d ON thd.document_id = d.id
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
            s3_fs.put(dest_path, "test-vcf/" + document_name)
            s3_fs.put(dest_path + ".tbi", "test-vcf/" + document_name + ".tbi")


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
