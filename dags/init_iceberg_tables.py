from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from pyiceberg.catalog import load_catalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.transforms import IdentityTransform

from tasks.vcf.consequence import SCHEMA as CONSEQUENCE_SCHEMA
from tasks.vcf.occurrence import SCHEMA as OCCURRENCE_SCHEMA
from tasks.vcf.variant import SCHEMA as VARIANT_SCHEMA

NAMESPACE = "radiant"

default_args = {
    "owner": "ferlab",
}

with DAG(
    dag_id="init_iceberg_tables",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
) as dag:
    catalog = load_catalog(
        "default",
        **{
            "uri": "http://radiant-iceberg-rest:8181",
            "token": "mysecret",
            "s3.endpoint": "http://minio:9000",
            "s3.access-key-id": "admin",
            "s3.secret-access-key": "password",
        },
    )

    @task
    def init_database():
        catalog.create_namespace_if_not_exists(NAMESPACE)

    @task
    def create_germline_occurrences_table():
        table_name = f"{NAMESPACE}.germline_snv_occurrences"
        if catalog.table_exists(table_name):
            print(f"Deleting existing table {table_name}")
            catalog.drop_table(table_name)

        case_id_field = OCCURRENCE_SCHEMA.find_field("case_id")
        seq_id_field = OCCURRENCE_SCHEMA.find_field("seq_id")
        chromosome_field = OCCURRENCE_SCHEMA.find_field("chromosome")

        partition_spec = PartitionSpec(
            fields=[
                PartitionField(
                    field_id=1001,
                    source_id=case_id_field.field_id,
                    name=case_id_field.name,
                    transform=IdentityTransform(),
                ),
                PartitionField(
                    field_id=1002,
                    source_id=seq_id_field.field_id,
                    name=seq_id_field.name,
                    transform=IdentityTransform(),
                ),
                PartitionField(
                    field_id=1003,
                    source_id=chromosome_field.field_id,
                    name=chromosome_field.name,
                    transform=IdentityTransform(),
                ),
            ]
        )
        catalog.create_table_if_not_exists(table_name, schema=OCCURRENCE_SCHEMA, partition_spec=partition_spec)

    @task
    def create_germline_variants_table():
        table_name = f"{NAMESPACE}.germline_snv_variants"
        if catalog.table_exists(table_name):
            catalog.drop_table(table_name)

        case_id_field = VARIANT_SCHEMA.find_field("case_id")
        chromosome_field = VARIANT_SCHEMA.find_field("chromosome")

        partition_spec = PartitionSpec(
            fields=[
                PartitionField(
                    field_id=1001,
                    source_id=case_id_field.field_id,
                    name="case_id",
                    transform=IdentityTransform(),
                ),
                PartitionField(
                    field_id=1003,
                    source_id=chromosome_field.field_id,
                    name="chromosome",
                    transform=IdentityTransform(),
                ),
            ]
        )
        catalog.create_table_if_not_exists(table_name, schema=VARIANT_SCHEMA, partition_spec=partition_spec)

    @task
    def create_germline_consequences_table():
        table_name = f"{NAMESPACE}.germline_snv_consequences"
        if catalog.table_exists(table_name):
            catalog.drop_table(table_name)

        case_id_field = CONSEQUENCE_SCHEMA.find_field("case_id")
        chromosome_field = CONSEQUENCE_SCHEMA.find_field("chromosome")

        partition_spec = PartitionSpec(
            fields=[
                PartitionField(
                    field_id=1001,
                    source_id=case_id_field.field_id,
                    name="case_id",
                    transform=IdentityTransform(),
                ),
                PartitionField(
                    field_id=1003,
                    source_id=chromosome_field.field_id,
                    name="chromosome",
                    transform=IdentityTransform(),
                ),
            ]
        )
        catalog.create_table_if_not_exists(table_name, schema=CONSEQUENCE_SCHEMA, partition_spec=partition_spec)

    (
        init_database()
        >> create_germline_occurrences_table()
        >> create_germline_variants_table()
        >> create_germline_consequences_table()
    )
