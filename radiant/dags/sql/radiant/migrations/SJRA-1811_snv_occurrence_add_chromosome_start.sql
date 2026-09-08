ALTER TABLE germline__snv__occurrence
    ADD COLUMN chromosome VARCHAR(20) NULL AFTER locus_id;

ALTER TABLE germline__snv__occurrence
    ADD COLUMN start INT(11) NULL AFTER chromosome;

ALTER TABLE somatic__snv__occurrence
    ADD COLUMN chromosome VARCHAR(20) NULL AFTER locus_id;

ALTER TABLE somatic__snv__occurrence
    ADD COLUMN start INT(11) NULL AFTER chromosome;