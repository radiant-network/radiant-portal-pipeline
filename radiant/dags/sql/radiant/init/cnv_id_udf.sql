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
    "https://github.com/radiant-network/radiant-starrocks-udf/releases/download/v1.1.0/radiant-starrocks-udf-1.1.0-jar-with-dependencies.jar"
);