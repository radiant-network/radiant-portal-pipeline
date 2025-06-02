CREATE
    GLOBAL FUNCTION GET_VARIANT_ID
(
    string,
    bigint,
    string,
    string
) RETURNS bigint
    PROPERTIES
(
    "symbol" = "org.radiant.VariantIdUDF",
    "type" = "StarrocksJar",
    "file" = "{{ params.starrocks_udf_jar_path }}"
);