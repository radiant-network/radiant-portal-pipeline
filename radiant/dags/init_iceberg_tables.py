from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago

from radiant.dags import NAMESPACE

PATH_TO_PYTHON_BINARY = "/home/airflow/.venv/radiant/bin/python"
default_args = {
    "owner": "radiant",
}

with DAG(
    dag_id=f"{NAMESPACE}-init-iceberg-tables",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    tags=["radiant", "iceberg", "manual"],
    dag_display_name="Radiant - Init Iceberg Tables",
    catchup=False,
) as dag:

    @task.external_python(task_id="init_database", python=PATH_TO_PYTHON_BINARY)
    def init_database(namespace):
        from pyiceberg.catalog import load_catalog

        catalog = load_catalog("default")
        catalog.create_namespace_if_not_exists(namespace)

    @task.external_python(task_id="create_germline_occurrence_table", python=PATH_TO_PYTHON_BINARY)
    def create_germline_occurrence_table(namespace):
        from pyiceberg.catalog import load_catalog
        from pyiceberg.partitioning import PartitionField, PartitionSpec
        from pyiceberg.transforms import IdentityTransform

        from radiant.tasks.vcf.occurrence import SCHEMA as OCCURRENCE_SCHEMA

        catalog = load_catalog("default")
        table_name = f"{namespace}.germline_snv_occurrence"
        if catalog.table_exists(table_name):
            print(f"Deleting existing table {table_name}")
            catalog.drop_table(table_name)

        part_field = OCCURRENCE_SCHEMA.find_field("part")
        case_id_field = OCCURRENCE_SCHEMA.find_field("case_id")
        seq_id_field = OCCURRENCE_SCHEMA.find_field("seq_id")
        chromosome_field = OCCURRENCE_SCHEMA.find_field("chromosome")

        partition_spec = PartitionSpec(
            fields=[
                PartitionField(
                    field_id=1001,
                    source_id=part_field.field_id,
                    name=part_field.name,
                    transform=IdentityTransform(),
                ),
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

    @task.external_python(task_id="create_germline_variant_table", python=PATH_TO_PYTHON_BINARY)
    def create_germline_variant_table(namespace):
        from pyiceberg.catalog import load_catalog
        from pyiceberg.partitioning import PartitionField, PartitionSpec
        from pyiceberg.transforms import IdentityTransform

        from radiant.tasks.vcf.variant import SCHEMA as VARIANT_SCHEMA

        catalog = load_catalog("default")
        table_name = f"{namespace}.germline_snv_variant"
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

    @task.external_python(task_id="create_germline_consequence_table", python=PATH_TO_PYTHON_BINARY)
    def create_germline_consequences_table(namespace):
        from pyiceberg.catalog import load_catalog
        from pyiceberg.partitioning import PartitionField, PartitionSpec
        from pyiceberg.transforms import IdentityTransform

        from radiant.tasks.vcf.consequence import SCHEMA as CONSEQUENCE_SCHEMA

        catalog = load_catalog("default")
        table_name = f"{namespace}.germline_snv_consequence"
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
        init_database(NAMESPACE)
        >> create_germline_occurrence_table(NAMESPACE)
        >> create_germline_variant_table(NAMESPACE)
        >> create_germline_consequences_table(NAMESPACE)
    )
