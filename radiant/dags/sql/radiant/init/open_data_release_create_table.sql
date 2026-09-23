CREATE TABLE IF NOT EXISTS {{ mapping.starrocks_open_data_release }} (
    source_name      VARCHAR(64)     NOT NULL COMMENT "Open-data source; stable across the pre-contract -> contract flip",
    recorded_at      DATETIME        NOT NULL COMMENT "NOW() when P4 executed, i.e. once every rebuild had succeeded",
    dag_run_id       VARCHAR(250)    NOT NULL COMMENT "The re-annotation run that wrote this row",
    table_name       VARCHAR(64)     NOT NULL COMMENT "Table actually read: `{source}_v{MAJOR}`, or the pre-contract name when held back",
    catalog_name     VARCHAR(100)    NOT NULL COMMENT "Catalog it was read from: OpenDataLake, or the Radiant one when held back",
    database_name    VARCHAR(100)    NOT NULL COMMENT "Database it was read from",
    iceberg_ref      VARCHAR(200)    NULL     COMMENT "RADIANT_OPEN_DATA_REF, or `LEGACY` when the source was held back and read without time travel",
    dataset_version  VARCHAR(200)    NULL     COMMENT "Concrete release when the ref names one, `LEGACY` when held back, NULL on a moving tag"
) ENGINE=OLAP
PRIMARY KEY(source_name)
DISTRIBUTED BY HASH(source_name) BUCKETS 1;
