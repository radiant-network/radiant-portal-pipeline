CREATE
GLOBAL FUNCTION INIT_SEQUENCING_EXPERIMENT_PARTITION
(
    string
) RETURNS int
    PROPERTIES
(
    "symbol" = "org.radiant.InitSequencingExperimentPartitionUDF",
    "type" = "StarrocksJar",
    "file" = "{{ params.starrocks_udf_jar_path }}"
);
