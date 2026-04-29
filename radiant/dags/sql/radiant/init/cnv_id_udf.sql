CREATE OR REPLACE
    GLOBAL FUNCTION GET_CNV_ID
(
    string,
    bigint,
    bigint,
    string
) RETURNS bigint
    PROPERTIES
(
    "symbol" =
    "org.radiant.CNVIdUDF",
    "type" =
    "StarrocksJar",
    "file" =
    "https://github.com/radiant-network/radiant-starrocks-udf/releases/download/{{ params.udf_release_version }}/radiant-starrocks-udf-{{ params.udf_release_version | replace('v', '') }}-jar-with-dependencies.jar"
);