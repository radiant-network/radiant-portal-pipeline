INSERT INTO {{ mapping.starrocks_pipeline_journal }}
    (case_id, pipeline, item_key, decided_at, run_id, decision, reasons)
VALUES (%(case_id)s, %(pipeline)s, %(item_key)s, %(decided_at)s, %(run_id)s, %(decision)s, PARSE_JSON(%(reasons)s))
