LOAD LABEL {{ database_name }}.{{load_label}}
(
    DATA INFILE %(tsv_filepath)s
    INTO TABLE {{table_name}}
    COLUMNS TERMINATED BY "\t"
    FORMAT AS "CSV"
    (
     skip_header = 1
    )
    (`name`, `scope`, patient_id, document_id, generated_value, `value`, raw_text,
     reasoning, review_status, review, reviewing_user, reviewed_at, validation_value, validation_agreement)
 )
 WITH BROKER
 (
        {{ broker_configuration }}
 )
PROPERTIES
(
    'timeout' = '{{ broker_load_timeout }}'
);