CREATE
    GLOBAL FUNCTION GET_SEQUENCING_EXPERIMENT_PARTITION
(
    int,
    int
) RETURNS int
    PROPERTIES
(
    "symbol" = "org.radiant.GetSequencingExperimentPartitionUDF",
    "type" = "StarrocksJar",
    "file" = "{{ params.starrocks_udf_jar_path }}"
);