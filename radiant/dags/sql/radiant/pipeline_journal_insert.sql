INSERT INTO {{ mapping.starrocks_pipeline_journal }}
    (case_id, pipeline, item_key, decided_at, run_id, decision, reasons)
VALUES (%s, %s, %s, %s, %s, %s, PARSE_JSON(%s))
