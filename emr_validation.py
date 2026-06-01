"""
Inspect an Iceberg table registered in the AWS Glue Data Catalog.

Prints schema, partitioning, sort order, properties, snapshots, refs,
manifests/files/partition stats, and a small sample of rows.

Usage:
    pip install "pyiceberg[glue,pyarrow]"
    python inspect_iceberg.py
"""

from __future__ import annotations

import pandas as pd
from pyiceberg.catalog.glue import GlueCatalog


# ---------- helpers ----------------------------------------------------------

def hr(title: str) -> None:
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)


def show_df(df: pd.DataFrame, max_rows: int = 20, max_cols: int = 12) -> None:
    """Print a dataframe with sane defaults so wide inspect tables stay readable."""
    if df is None or len(df) == 0:
        print("(empty)")
        return
    with pd.option_context(
        "display.max_rows", max_rows,
        "display.max_columns", max_cols,
        "display.width", 200,
        "display.max_colwidth", 60,
    ):
        print(df)


# ---------- catalog & table --------------------------------------------------

catalog = GlueCatalog(
    name="glue",
    **{
        "region_name": "us-east-1",
    },
)

TABLE_IDENT = "opendatalake_poc.clinvar"
table = catalog.load_table(TABLE_IDENT)


# ---------- identity & location ---------------------------------------------

hr("IDENTITY")
print(f"Identifier        : {TABLE_IDENT}")
print(f"Metadata location : {table.metadata_location}")
print(f"Data location     : {table.location()}")
print(f"Format version    : {table.metadata.format_version}")
print(f"Table UUID        : {table.metadata.table_uuid}")
print(f"Last updated (ms) : {table.metadata.last_updated_ms}")


# ---------- schema -----------------------------------------------------------

hr("SCHEMA (current)")
schema = table.schema()
print(schema)
print(f"\nSchema id   : {schema.schema_id}")
print(f"Field count : {len(schema.fields)}")
print(f"Identifier  : {list(schema.identifier_field_names())}")

hr("SCHEMA (all)")
for s in table.schemas().values():
    marker = "  <-- current" if s.schema_id == schema.schema_id else ""
    print(f"- schema_id={s.schema_id}{marker}  fields={len(s.fields)}")


# ---------- partitioning & sort ---------------------------------------------

hr("PARTITION SPEC")
spec = table.spec()
print(spec)
print(f"\nSpec id     : {spec.spec_id}")
print(f"Is unpartitioned: {spec.is_unpartitioned()}")

hr("SORT ORDER")
sort = table.sort_order()
print(sort)
print(f"\nOrder id : {sort.order_id}")


# ---------- properties -------------------------------------------------------

hr("TABLE PROPERTIES")
props = dict(table.properties)
if props:
    for k in sorted(props):
        print(f"{k:40s} = {props[k]}")
else:
    print("(none)")


# ---------- snapshots & refs -------------------------------------------------

hr("CURRENT SNAPSHOT")
cur = table.current_snapshot()
print(cur)
if cur is not None:
    print(f"\nsnapshot_id   : {cur.snapshot_id}")
    print(f"parent_id     : {cur.parent_snapshot_id}")
    print(f"timestamp_ms  : {cur.timestamp_ms}")
    print(f"operation     : {cur.summary.operation if cur.summary else None}")
    # print(f"summary       : {dict(cur.summary) if cur.summary else {}}")

hr("SNAPSHOT HISTORY")
snaps = list(table.snapshots())
print(f"Total snapshots: {len(snaps)}")
for s in snaps[-10:]:  # last 10
    op = s.summary.operation if s.summary else None
    print(f"  {s.snapshot_id}  ts={s.timestamp_ms}  op={op}  parent={s.parent_snapshot_id}")

hr("REFS (branches & tags)")
refs = table.refs()
if refs:
    for name, ref in refs.items():
        print(f"  {name:20s}  type={ref.snapshot_ref_type}  snapshot_id={ref.snapshot_id}")
else:
    print("(none)")


# ---------- inspect metadata tables -----------------------------------------

hr("inspect.snapshots()")
show_df(table.inspect.snapshots().to_pandas())

hr("inspect.history()")
show_df(table.inspect.history().to_pandas())

hr("inspect.manifests()")
show_df(table.inspect.manifests().to_pandas())

hr("inspect.partitions()")
try:
    show_df(table.inspect.partitions().to_pandas())
except Exception as e:
    print(f"(unavailable: {e})")

hr("inspect.files()  [first 20]")
files_df = table.inspect.files().to_pandas()
print(f"Total data files: {len(files_df)}")
show_df(files_df.head(20))

if len(files_df) and "file_size_in_bytes" in files_df.columns:
    total_bytes = int(files_df["file_size_in_bytes"].sum())
    print(f"\nTotal size : {total_bytes:,} bytes  (~{total_bytes / 1e9:.2f} GB)")
if len(files_df) and "record_count" in files_df.columns:
    print(f"Total rows (from manifests): {int(files_df['record_count'].sum()):,}")


# ---------- sample rows ------------------------------------------------------

hr("SAMPLE ROWS  (scan limit=10)")
arrow_table = table.scan(limit=10).to_arrow()
print(f"Columns: {arrow_table.column_names}")
print(f"Rows   : {arrow_table.num_rows}")
show_df(arrow_table.to_pandas())