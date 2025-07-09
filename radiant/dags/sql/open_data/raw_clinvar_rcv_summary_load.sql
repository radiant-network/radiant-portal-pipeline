LOAD LABEL radiant.{label} (
    DATA INFILE(%()s)
    INTO TABLE {{ params.starrocks_raw_clinvar_rcv_summary }}
    COLUMNS TERMINATED BY "\t"
    FORMAT AS "json"
)
 WITH BROKER
 (
        'aws.s3.region' = '{{ params.starrocks_aws_region }}',
        'aws.s3.endpoint'  =  '{{ params.starrocks_aws_endpoint }}',
        'aws.s3.enable_path_style_access'  =  'true',
        'aws.s3.access_key' = '{{ params.starrocks_aws_access_key }}',
        'aws.s3.secret_key' = '{{ params.starrocks_aws_secret_key }}'
 )
PROPERTIES
(
    'timeout' = '{{ params.broker_load_timeout }}'
);