LOAD LABEL {database_name}.{label} (
    DATA INFILE %(rcv_summary_filepaths)s
    INTO TABLE {{ params.starrocks_raw_clinvar_rcv_summary }}
    COLUMNS TERMINATED BY "\t"
    FORMAT AS "json"
)
 WITH BROKER
 (
        {broker_configuration}
 )
PROPERTIES
(
    'timeout' = '{{ params.broker_load_timeout }}'
);