-- RAD-315. The DRAGEN `coverage_by_gene` CSVs of one format, in one load job: one `data_desc` per file, each with its
-- own experiment, task and tenant in its SET. The data_descs of one table and one partition make one atomic job (starrocks
-- docs BROKER_LOAD.md, `data_desc`; fe `BrokerFileGroupAggInfo.addFileGroup`): a bad file cancels the whole job, and the
-- operator then loads the files of the job one by one.
-- The operator reads the header of each file first, and fills `file_columns` and `type_value` for its format: the new
-- format has a `type` column (14 columns), the old one has none (13 columns, every row is CDS). The broker load's WHERE
-- filters after the SET, so it reads the `type` of both formats (starrocks docs BROKER_LOAD.md, `WHERE`). Only the CDS
-- rows are ever read (about 20k of the 177k rows of a new-format file): the others would only grow the staging copy of
-- every run.
-- The `raw` block is rendered at the second render only (per job, in run_insert), with `file_count`, `table_name`,
-- `temporary_partition_clause`, `file_columns` and `type_value`.
LOAD LABEL {{ database_name }}.{{ load_label }}
(
{% raw %}{% for i in range(file_count) %}
    DATA INFILE(%(filepath_{{ i }})s)
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
        seq_id = %(seq_id_{{ i }})s,
        task_id = %(task_id_{{ i }})s,
        tenant_code = %(tenant_code_{{ i }})s,
        type = {{ type_value }}
    )
    WHERE type = 'CDS'{{ "," if not loop.last }}
{% endfor %}{% endraw %}
)
WITH BROKER
(
    {{ broker_configuration }}
)
PROPERTIES
(
    'timeout' = '{{ broker_load_timeout }}'
);
