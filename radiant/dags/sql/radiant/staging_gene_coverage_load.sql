-- RAD-315. One DRAGEN `coverage_by_gene` CSV. The operator reads the header first and fills `file_columns` and
-- `type_value` for its format: the new format has a `type` column (14 columns), the old one has none (13 columns,
-- every row is CDS). The broker load's WHERE filters after the SET, so it reads the `type` of both formats
-- (starrocks docs BROKER_LOAD.md, `WHERE`).
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
    -- Only the CDS rows are ever read (about 20k of the 177k rows of a new-format file): the others would only grow
    -- the staging copy of every run.
    WHERE type = 'CDS'
)
WITH BROKER
(
    {{ broker_configuration }}
)
PROPERTIES
(
    'timeout' = '{{ broker_load_timeout }}'
);
