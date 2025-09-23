LOAD LABEL {{ database_name }}.{{ load_label }} (
    DATA INFILE %(rcv_summary_filepaths)s
    INTO TABLE {{ table_name }}
    COLUMNS TERMINATED BY "\t"
    FORMAT AS "json"
)
 WITH BROKER
 (
        {{ broker_configuration }}
 )
PROPERTIES
(
    'timeout' = '{{ broker_load_timeout }}'
);