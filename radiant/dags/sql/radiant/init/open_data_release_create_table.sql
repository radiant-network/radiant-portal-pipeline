CREATE TABLE IF NOT EXISTS {{ mapping.starrocks_open_data_release }} (
    table_name       VARCHAR(64)     NOT NULL COMMENT "Contract table, `{table_prefix}_v{MAJOR}`",
    recorded_at      DATETIME        NOT NULL COMMENT "When P4 ran, i.e. when the rebuild completed",
    dag_run_id       VARCHAR(250)    NOT NULL COMMENT "The re-annotation run that wrote this row",
    catalog_name     VARCHAR(100)    NOT NULL COMMENT "RADIANT_OPEN_DATA_CATALOG at the time of the run",
    database_name    VARCHAR(100)    NOT NULL COMMENT "RADIANT_OPEN_DATA_DATABASE at the time of the run",
    iceberg_ref      VARCHAR(200)    NULL     COMMENT "RADIANT_OPEN_DATA_REF; empty means no time travel",
    dataset_version  VARCHAR(200)    NULL     COMMENT "Concrete release, when the ref names one"
) ENGINE=OLAP
PRIMARY KEY(table_name)
DISTRIBUTED BY HASH(table_name) BUCKETS 1;
