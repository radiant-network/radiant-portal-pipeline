from airflow.models import Param

NAMESPACE = "radiant"

ICEBERG_COMMON_PARAMS = {
    "iceberg_catalog": Param(
        default="iceberg_catalog",
        description="The iceberg catalog to use.",
        type="string",
    ),
    "iceberg_database": Param(
        default="iceberg_database",
        description="The iceberg database to use.",
        type="string",
    ),
}
