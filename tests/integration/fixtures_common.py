# Constants
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password"
MINIO_API_PORT = 9000
MINIO_CONSOLE_PORT = 9001

ICEBERG_REST_PORT = 8181
ICEBERG_REST_CATALOG_NAME = "radiant"
ICEBERG_REST_TOKEN = "mysecret"

POSTGRES_PORT = 5432

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
    def __init__(self, host, api_port, console_port, access_key, secret_key, internal_host, internal_port):
        self.host = host
        self.api_port = api_port
        self.console_port = console_port
        self.endpoint = f"http://{host}:{api_port}"
        self.console_url = f"http://{host}:{console_port}"
        self.access_key = access_key
        self.secret_key = secret_key
        self.internal_host = internal_host
        self.internal_port = internal_port
        self.internal_endpoint = f"http://{internal_host}:{internal_port}"


class RestIcebergCatalogInstance:
    def __init__(self, host, port, catalog_name, token, internal_host, internal_port):
        self.host = host
        self.port = port
        self.endpoint = f"http://{host}:{port}"
        self.token = token
        self.catalog_name = catalog_name
        self.internal_host = internal_host
        self.internal_port = internal_port


class StarRocksInstance:
    def __init__(self, host, query_port, fe_port, be_port, user, password, internal_host, internal_port):
        self.host = host
        self.query_port = query_port
        self.fe_port = fe_port
        self.be_port = be_port
        self.endpoint = f"http://{host}:{query_port}"
        self.user = user
        self.password = password
        self.internal_host = internal_host
        self.internal_port = internal_port


class RadiantAirflowInstance:
    def __init__(self, host, port, username, password):
        self.host = host
        self.port = port
        self.endpoint = f"http://{host}:{port}"
        self.username = username
        self.password = password


class PostgresInstance:
    def __init__(
        self, host, port, user, password, airflow_db, radiant_db, radiant_db_schema, internal_host, internal_port
    ):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.airflow_db = airflow_db
        self.radiant_db = radiant_db
        self.radiant_db_schema = radiant_db_schema
        self.internal_host = internal_host
        self.internal_port = internal_port


class StarrocksJDBCCatalog:
    def __init__(self, catalog, database):
        self.catalog = catalog
        self.database = database


class StarRocksIcebergCatalog:
    def __init__(self, catalog, database):
        self.catalog = catalog
        self.database = database
