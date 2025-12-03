def init_database():
    import os

    from pyiceberg.catalog import load_catalog

    namespace = os.environ["RADIANT_ICEBERG_NAMESPACE"]

    catalog = load_catalog("default")
    catalog.create_namespace_if_not_exists(namespace)


def create_germline_cnv_occurrence_table():
    import os

    from pyiceberg.catalog import load_catalog

    from radiant.tasks.vcf.cnv.germline.occurrence import SCHEMA as OCCURRENCE_SCHEMA

    namespace = os.environ["RADIANT_ICEBERG_NAMESPACE"]
    catalog = load_catalog("default")
    table_name = f"{namespace}.germline_cnv_occurrence"
    if catalog.table_exists(table_name):
        catalog.drop_table(table_name)

    catalog.create_table_if_not_exists(table_name, schema=OCCURRENCE_SCHEMA)


def create_germline_consequences_table():
    import os

    from pyiceberg.catalog import load_catalog
    from pyiceberg.partitioning import PartitionField, PartitionSpec
    from pyiceberg.transforms import IdentityTransform

    from radiant.tasks.vcf.snv.germline.consequence import SCHEMA as CONSEQUENCE_SCHEMA

    namespace = os.environ["RADIANT_ICEBERG_NAMESPACE"]
    catalog = load_catalog("default")
    table_name = f"{namespace}.germline_snv_consequence"
    if catalog.table_exists(table_name):
        catalog.drop_table(table_name)

    task_id_field = CONSEQUENCE_SCHEMA.find_field("task_id")

    partition_spec = PartitionSpec(
        fields=[
            PartitionField(
                field_id=1001,
                source_id=task_id_field.field_id,
                name="task_id",
                transform=IdentityTransform(),
            )
        ]
    )
    catalog.create_table_if_not_exists(table_name, schema=CONSEQUENCE_SCHEMA, partition_spec=partition_spec)


def create_germline_variant_table():
    import os

    from pyiceberg.catalog import load_catalog
    from pyiceberg.partitioning import PartitionField, PartitionSpec
    from pyiceberg.transforms import IdentityTransform

    from radiant.tasks.vcf.snv.germline.variant import SCHEMA as VARIANT_SCHEMA

    namespace = os.environ["RADIANT_ICEBERG_NAMESPACE"]
    catalog = load_catalog("default")
    table_name = f"{namespace}.germline_snv_variant"
    if catalog.table_exists(table_name):
        catalog.drop_table(table_name)

    task_id_field = VARIANT_SCHEMA.find_field("task_id")

    partition_spec = PartitionSpec(
        fields=[
            PartitionField(
                field_id=1001,
                source_id=task_id_field.field_id,
                name="task_id",
                transform=IdentityTransform(),
            )
        ]
    )
    catalog.create_table_if_not_exists(table_name, schema=VARIANT_SCHEMA, partition_spec=partition_spec)


def create_germline_snv_occurrence_table():
    import os

    from pyiceberg.catalog import load_catalog
    from pyiceberg.partitioning import PartitionField, PartitionSpec
    from pyiceberg.transforms import IdentityTransform

    from radiant.tasks.vcf.snv.germline.occurrence import SCHEMA as OCCURRENCE_SCHEMA

    namespace = os.environ["RADIANT_ICEBERG_NAMESPACE"]
    catalog = load_catalog("default")
    table_name = f"{namespace}.germline_snv_occurrence"
    if catalog.table_exists(table_name):
        catalog.drop_table(table_name)

    part_field = OCCURRENCE_SCHEMA.find_field("part")
    task_id_field = OCCURRENCE_SCHEMA.find_field("task_id")

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
                source_id=task_id_field.field_id,
                name=task_id_field.name,
                transform=IdentityTransform(),
            ),
        ]
    )
    catalog.create_table_if_not_exists(table_name, schema=OCCURRENCE_SCHEMA, partition_spec=partition_spec)
