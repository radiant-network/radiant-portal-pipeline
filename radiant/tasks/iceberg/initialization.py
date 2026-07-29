def init_database():
    import os

    from pyiceberg.catalog import load_catalog

    namespace = os.environ["RADIANT_ICEBERG_NAMESPACE"]

    catalog = load_catalog("default")
    catalog.create_namespace_if_not_exists(namespace)


def evolve_table(table_name: str):
    """
    Add any column missing from an existing Iceberg table, in place.

    This is the non-destructive counterpart to the `create_*_table` functions below, which
    drop and recreate. Iceberg records the change as metadata only: existing Parquet files
    are left untouched and read NULL for the added columns, so no data is lost and nothing
    is rewritten.

    Only additive changes are applied. Dropping a column, narrowing a type or making a
    column required is out of reach here and still needs a recreate.

    Note that the catalog assigns its own field ids to the added columns, so they will not
    match the literal ids declared in the Python schema. That is harmless: writers resolve
    field ids from the table metadata through the name mapping, never from the literals.

    Parameters:
        table_name (str): The table to evolve, without the namespace prefix.

    Raises:
        ValueError: If the table name is unknown.
    """
    import os

    from pyiceberg.catalog import load_catalog

    if table_name == "germline_cnv_occurrence":
        from radiant.tasks.vcf.cnv.germline.occurrence import SCHEMA
    elif table_name == "germline_snv_occurrence":
        from radiant.tasks.vcf.snv.germline.occurrence import SCHEMA
    elif table_name == "snv_consequence":
        from radiant.tasks.vcf.snv.consequence import SCHEMA
    elif table_name == "snv_variant":
        from radiant.tasks.vcf.snv.variant import SCHEMA
    elif table_name == "somatic_snv_occurrence":
        from radiant.tasks.vcf.snv.somatic.occurrence import SCHEMA
    else:
        raise ValueError(
            f"Unknown table name: {table_name}, possible values are: 'germline_snv_occurrence', "
            "'snv_variant', 'snv_consequence', 'germline_cnv_occurrence', 'somatic_snv_occurrence'"
        )

    namespace = os.environ["RADIANT_ICEBERG_NAMESPACE"]
    catalog = load_catalog("default")
    table = catalog.load_table(f"{namespace}.{table_name}")

    with table.update_schema() as update:
        update.union_by_name(SCHEMA)


def create_germline_cnv_occurrence_table():
    import os

    from pyiceberg.catalog import load_catalog
    from pyiceberg.partitioning import PartitionField, PartitionSpec
    from pyiceberg.transforms import IdentityTransform

    from radiant.tasks.vcf.cnv.germline.occurrence import SCHEMA as OCCURRENCE_SCHEMA

    namespace = os.environ["RADIANT_ICEBERG_NAMESPACE"]
    catalog = load_catalog("default")
    table_name = f"{namespace}.germline_cnv_occurrence"
    if catalog.table_exists(table_name):
        catalog.drop_table(table_name)

    tenant_code_field = OCCURRENCE_SCHEMA.find_field("tenant_code")
    partition_spec = PartitionSpec(
        fields=[
            PartitionField(
                field_id=1001,
                source_id=tenant_code_field.field_id,
                name=tenant_code_field.name,
                transform=IdentityTransform(),
            ),
        ]
    )
    catalog.create_table_if_not_exists(table_name, schema=OCCURRENCE_SCHEMA, partition_spec=partition_spec)


def create_consequences_table():
    import os

    from pyiceberg.catalog import load_catalog
    from pyiceberg.partitioning import PartitionField, PartitionSpec
    from pyiceberg.transforms import IdentityTransform

    from radiant.tasks.vcf.snv.consequence import SCHEMA as CONSEQUENCE_SCHEMA

    namespace = os.environ["RADIANT_ICEBERG_NAMESPACE"]
    catalog = load_catalog("default")
    table_name = f"{namespace}.snv_consequence"
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


def create_variant_table():
    import os

    from pyiceberg.catalog import load_catalog
    from pyiceberg.partitioning import PartitionField, PartitionSpec
    from pyiceberg.transforms import IdentityTransform

    from radiant.tasks.vcf.snv.variant import SCHEMA as VARIANT_SCHEMA

    namespace = os.environ["RADIANT_ICEBERG_NAMESPACE"]
    catalog = load_catalog("default")
    table_name = f"{namespace}.snv_variant"
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
                field_id=1002,
                source_id=task_id_field.field_id,
                name=task_id_field.name,
                transform=IdentityTransform(),
            ),
        ]
    )
    catalog.create_table_if_not_exists(table_name, schema=OCCURRENCE_SCHEMA, partition_spec=partition_spec)


def create_somatic_snv_occurrence_table():
    import os

    from pyiceberg.catalog import load_catalog
    from pyiceberg.partitioning import PartitionField, PartitionSpec
    from pyiceberg.transforms import IdentityTransform

    from radiant.tasks.vcf.snv.somatic.occurrence import SCHEMA as OCCURRENCE_SCHEMA

    namespace = os.environ["RADIANT_ICEBERG_NAMESPACE"]
    catalog = load_catalog("default")
    table_name = f"{namespace}.somatic_snv_occurrence"
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
                field_id=1002,
                source_id=task_id_field.field_id,
                name=task_id_field.name,
                transform=IdentityTransform(),
            ),
        ]
    )
    catalog.create_table_if_not_exists(table_name, schema=OCCURRENCE_SCHEMA, partition_spec=partition_spec)
