CREATE TABLE IF NOT EXISTS {{ mapping.starrocks_brim }} (
    `name` varchar(500) NULL COMMENT "",
    `scope` varchar(128) NULL COMMENT "",
    patient_id varchar(128) NULL COMMENT "",
    document_id varchar(128) NULL COMMENT "",
    generated_value varchar(128) NULL COMMENT "",
    `value` varchar(128) NULL COMMENT "",
    raw_text varchar(128) NULL COMMENT "",
    reasoning varchar(500) NULL COMMENT "",
    review_status varchar(128) NULL COMMENT "",
    review varchar(128) NULL COMMENT "",
    reviewing_user varchar(128) NULL COMMENT "",
    reviewed_at varchar(128) NULL COMMENT "",
    validation_value varchar(128) NULL COMMENT "",
    validation_agreement varchar(128) NULL COMMENT ""
    ) ENGINE=OLAP