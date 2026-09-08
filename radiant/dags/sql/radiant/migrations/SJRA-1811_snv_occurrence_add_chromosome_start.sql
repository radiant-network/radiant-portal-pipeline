-- Run once per tenant DB (both tables are per-tenant); StarRocks has no ADD COLUMN IF NOT EXISTS, so re-running fails.
ALTER TABLE germline__snv__occurrence
    ADD COLUMN chromosome VARCHAR(20) NULL AFTER locus_id;

ALTER TABLE germline__snv__occurrence
    ADD COLUMN start INT(11) NULL AFTER chromosome;

ALTER TABLE somatic__snv__occurrence
    ADD COLUMN chromosome VARCHAR(20) NULL AFTER locus_id;

ALTER TABLE somatic__snv__occurrence
    ADD COLUMN start INT(11) NULL AFTER chromosome;