ALTER TABLE open_data_release
    ADD COLUMN imported_snapshot_id BIGINT NULL COMMENT "Iceberg snapshot the copy in StarRocks was loaded from. Written by radiant-import-open-data, and what its own gate compares against. NULL when held back or unresolvable" AFTER dataset_version;

ALTER TABLE open_data_release
    ADD COLUMN reannotated_snapshot_id BIGINT NULL COMMENT "Iceberg snapshot the portal-facing tables were annotated from. Written by P4 as a copy of `imported_snapshot_id`, and what the re-annotation gates compare against. Two columns because a standalone import moves the first without the second" AFTER imported_snapshot_id;
