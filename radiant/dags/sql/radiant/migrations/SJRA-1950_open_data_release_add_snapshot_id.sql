-- SJRA-1950 — add `open_data_release.snapshot_id`, the key the re-annotation gates compare on.
--
-- WHY THE COLUMN EXISTS. `reannotate_open_data` now skips a branch whose sources did not move
-- (radiant/tasks/data/open_data.py -- `changed_sources`). The ledger could not answer "did this
-- move?" on its own: `dataset_version` is NULL whenever the ref is a moving tag, and `latest` is
-- the default. What can answer is the Iceberg snapshot the ref resolved to, read from the
-- `<relation>$refs` metadata table StarRocks exposes from 3.4.1. P4 records it here, and the next
-- run compares against it.
--   Docs: https://docs.starrocks.io/docs/data_source/catalog/iceberg/iceberg_meta_table/
--
-- Run manually, ONCE per database that holds `open_data_release`. It is in
-- STARROCKS_RADIANT_BASE_MAPPING (radiant/tasks/data/radiant_tables.py), so the base database
-- only -- there is nothing to do in the `{tenant}_tenant` databases.
--
-- New deployments get the column from init/open_data_release_create_table.sql and must NOT run this
-- script. StarRocks has no `ADD COLUMN IF NOT EXISTS`, so re-running it fails with
--
--   Can not add column which already exists in base table: snapshot_id
--
-- That is the safe failure: the FE raises it before the ALTER touches anything (SchemaChangeHandler
-- .addColumnInternal), so a second run leaves no duplicate column and no half-applied schema -- it
-- just means the migration was already taken. Check `DESC open_data_release` first either way.
--
-- UNTIL IT IS RUN, the DAG still works but the gate does nothing useful: the `SELECT ...
-- snapshot_id` errors, `last_recorded_snapshots` logs the failure and returns nothing recorded, and
-- every branch re-annotates -- the behaviour that predates the gate. P4's INSERT names
-- `snapshot_id` in its column list, so the run then fails at `record_open_data_release` rather than
-- stamping a row that claims the warehouse is current. Read that failure as "take this migration".
--
-- `AFTER dataset_version` is cosmetic here, unlike SJRA-1751/1833/1854. open_data_release_insert.sql
-- names every target column, so nothing in the pipeline depends on the position. It is named only
-- so `DESC open_data_release` matches the init DDL on a migrated and a fresh deployment alike.
--
-- NULL, not NOT NULL. The column is legitimately empty for a source held back on the legacy Radiant
-- catalog (read without time travel, so there is no ref to resolve) and for a contract source whose
-- `$refs` read failed. `changed_sources` tells those two apart by `iceberg_ref`.
--
-- Cost. Adding a nullable column is a metadata change on a table with one row per open-data source
-- -- fewer than 20 rows. Expect it to finish immediately. It is still an ALTER, so the table state
-- must be NORMAL: do not run it while `reannotate_open_data` holds the import mutex.


-- ---------------------------------------------------------------------------------------------------
-- 0. Confirm this database still needs the migration.
-- ---------------------------------------------------------------------------------------------------
--
--   DESC open_data_release;
--       -- there must be no `snapshot_id`. If there is, stop -- nothing to do.
--
--   SELECT count(*) FROM open_data_release;
--       -- record it; step 3 must match. The ALTER touches no row.
--


-- ---------------------------------------------------------------------------------------------------
-- 1. The column.
-- ---------------------------------------------------------------------------------------------------

ALTER TABLE open_data_release
    ADD COLUMN snapshot_id BIGINT NULL COMMENT "Iceberg snapshot `iceberg_ref` resolved to when this release was annotated; NULL when held back or unresolvable. The change-detection key -- `dataset_version` is NULL on a moving tag and cannot serve" AFTER dataset_version;


-- ---------------------------------------------------------------------------------------------------
-- 2. Let it reach FINISHED.
-- ---------------------------------------------------------------------------------------------------
--
--   SHOW ALTER TABLE COLUMN WHERE TableName = 'open_data_release' ORDER BY CreateTime DESC LIMIT 1;
--       -- State must be FINISHED.
--


-- ---------------------------------------------------------------------------------------------------
-- 3. Post-checks, read-only.
-- ---------------------------------------------------------------------------------------------------
--
--   DESC open_data_release;
--       -- `snapshot_id` bigint YES, sitting after `dataset_version`.
--
--   SELECT count(*) FROM open_data_release;
--       -- must match step 0.
--
--   SELECT source_name, iceberg_ref, dataset_version, snapshot_id FROM open_data_release
--   ORDER BY source_name;
--       -- every `snapshot_id` is NULL. The existing rows were annotated before the column existed
--       -- and there is no way to recover which snapshot they used, so the first run after this
--       -- migration re-annotates every branch -- `changed_sources` counts an unknown snapshot as
--       -- changed. That is one full rebuild, once, and it is the correct one: it is also what
--       -- re-establishes the baseline the following runs gate against.
--
