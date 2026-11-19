-- RAD-315. One DRAGEN `coverage_by_gene` CSV. The operator reads the header first and fills `file_columns` and
-- `type_value` for its format: the new format has a `type` column (14 columns), the old one has none (13 columns,
-- every row is CDS).
LOAD LABEL {{ database_name }}.{{ load_label }}
(
    DATA INFILE(%(filepath)s)
    INTO TABLE {{ table_name }} {{ temporary_partition_clause }}
    COLUMNS TERMINATED BY ","
    FORMAT AS "CSV"
    (
        skip_header = 1
    )
    ({{ file_columns }})
    SET
    (
        part = %(part)s,
        seq_id = %(seq_id)s,
        task_id = %(task_id)s,
        tenant_code = %(tenant_code)s,
        type = {{ type_value }}
    )
)
WITH BROKER
(
    {{ broker_configuration }}
)
PROPERTIES
(
    'timeout' = '{{ broker_load_timeout }}'
);
