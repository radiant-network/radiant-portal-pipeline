CREATE OR REPLACE
    GLOBAL FUNCTION GET_VARIANT_ID
(
    string,
    bigint,
    string,
    string
) RETURNS bigint
    PROPERTIES
(
    "symbol" =
    "org.radiant.VariantIdUDF",
    "type" =
    "StarrocksJar",
    "file" =
    "https://github.com/radiant-network/radiant-starrocks-udf/releases/download/v1.0.0/radiant-starrocks-udf-1.0.0-jar-with-dependencies.jar"
);