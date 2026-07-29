import pyarrow as pa
import pytest
from pyiceberg.schema import Schema
from pyiceberg.types import IntegerType, StringType

from radiant.tasks.iceberg.initialization import evolve_table
from radiant.tasks.vcf.snv.somatic.occurrence import SCHEMA as SOMATIC_SNV_OCCURRENCE_SCHEMA

# The columns SJRA-1751 added to the somatic occurrence schema. A table created before that
# change has everything else, so evolving it must add exactly these.
NEW_COLUMNS = ("info_hotspot", "info_aq", "tumor_sq", "normal_sq")


def _drop_namespace(iceberg_client, namespace):
    if iceberg_client.namespace_exists(namespace):
        for table in iceberg_client.list_tables(namespace):
            iceberg_client.drop_table(table)
        iceberg_client.drop_namespace(namespace)


@pytest.fixture
def legacy_namespace(s3_fs, iceberg_client, random_test_id):
    """A namespace holding a `somatic_snv_occurrence` table on the pre-SJRA-1751 schema.

    Depends on `s3_fs` because that is what provisions the MinIO warehouse bucket the
    catalog writes table metadata into.
    """
    namespace = f"test_{random_test_id}_evolve"
    _drop_namespace(iceberg_client, namespace)  # leftovers from a previously failed run
    iceberg_client.create_namespace(namespace)

    schema = Schema(*[f for f in SOMATIC_SNV_OCCURRENCE_SCHEMA.fields if f.name not in NEW_COLUMNS])
    iceberg_client.create_table(f"{namespace}.somatic_snv_occurrence", schema=schema)

    yield namespace

    _drop_namespace(iceberg_client, namespace)


def _seed_one_row(table):
    """Write a single row under whatever schema the table currently has."""
    row = {
        field.name: (
            1
            if isinstance(field.field_type, IntegerType)
            else "x"
            if isinstance(field.field_type, StringType)
            else None
        )
        for field in table.schema().fields
    }
    table.append(pa.Table.from_pylist([row], schema=table.schema().as_arrow()))


def test_evolve_table_adds_missing_columns_and_keeps_data(monkeypatch, iceberg_client, legacy_namespace):
    monkeypatch.setenv("RADIANT_ICEBERG_NAMESPACE", legacy_namespace)
    monkeypatch.setattr("pyiceberg.catalog.load_catalog", lambda *args, **kwargs: iceberg_client)

    table_name = f"{legacy_namespace}.somatic_snv_occurrence"
    table = iceberg_client.load_table(table_name)
    for column in NEW_COLUMNS:
        assert column not in table.schema().column_names
    _seed_one_row(table)

    evolve_table("somatic_snv_occurrence")

    evolved = iceberg_client.load_table(table_name)
    assert set(evolved.schema().column_names) == set(SOMATIC_SNV_OCCURRENCE_SCHEMA.column_names)

    # existing data survives, and reads NULL for the added columns
    df = evolved.scan().to_arrow().to_pandas()
    assert len(df) == 1, "the pre-existing row was lost"
    for column in NEW_COLUMNS:
        assert df[column].isna().all()


def test_evolve_table_is_idempotent(monkeypatch, iceberg_client, legacy_namespace):
    monkeypatch.setenv("RADIANT_ICEBERG_NAMESPACE", legacy_namespace)
    monkeypatch.setattr("pyiceberg.catalog.load_catalog", lambda *args, **kwargs: iceberg_client)

    evolve_table("somatic_snv_occurrence")
    first = iceberg_client.load_table(f"{legacy_namespace}.somatic_snv_occurrence").schema()

    evolve_table("somatic_snv_occurrence")
    second = iceberg_client.load_table(f"{legacy_namespace}.somatic_snv_occurrence").schema()

    assert first.column_names == second.column_names
    assert first.schema_id == second.schema_id, "a no-op evolution should not create a new schema version"


def test_evolve_table_rejects_unknown_table():
    with pytest.raises(ValueError, match="Unknown table name: nope"):
        evolve_table("nope")
