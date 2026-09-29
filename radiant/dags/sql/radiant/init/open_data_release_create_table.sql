CREATE TABLE IF NOT EXISTS {{ mapping.starrocks_open_data_release }} (
    source_name      VARCHAR(64)     NOT NULL COMMENT "Open-data source; stable across the pre-contract -> contract flip",
    recorded_at      DATETIME        NOT NULL COMMENT "NOW() when this row was last written, by an import or by P4",
    dag_run_id       VARCHAR(250)    NOT NULL COMMENT "The run that last wrote this row: an import, or a re-annotation P4",
    table_name       VARCHAR(64)     NOT NULL COMMENT "Table actually read: `{source}_v{MAJOR}`, or the pre-contract name when held back",
    catalog_name     VARCHAR(100)    NOT NULL COMMENT "Catalog it was read from: OpenDataLake, or the Radiant one when held back",
    database_name    VARCHAR(100)    NOT NULL COMMENT "Database it was read from",
    iceberg_ref      VARCHAR(200)    NULL     COMMENT "RADIANT_OPEN_DATA_REF, or `LEGACY` when the source was held back and read without time travel",
    dataset_version  VARCHAR(200)    NULL     COMMENT "The OpenDataLake release this row was read from -- the version branch `iceberg_ref` resolved to, which is a real answer even on the moving `latest` tag. `LEGACY` when held back, NULL when the ref could not be resolved",
    imported_snapshot_id BIGINT      NULL     COMMENT "Iceberg snapshot the copy in StarRocks was loaded from. Written by radiant-import-open-data, and what its own gate compares against. NULL when held back or unresolvable",
    reannotated_snapshot_id BIGINT   NULL     COMMENT "Iceberg snapshot the portal-facing tables were annotated from. Written by P4 as a copy of `imported_snapshot_id`, and what the re-annotation gates compare against. Two columns because a standalone import moves the first without the second"
) ENGINE=OLAP
PRIMARY KEY(source_name)
DISTRIBUTED BY HASH(source_name) BUCKETS 1;
