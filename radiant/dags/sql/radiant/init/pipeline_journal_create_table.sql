-- What the scheduler decided for each pipeline item, and why, and how the pipeline ended. Append-only: `run` is
-- written on every pass, any other decision only when it differs from the item's last row (`pipeline_state`).
CREATE TABLE IF NOT EXISTS {{ mapping.starrocks_pipeline_journal }} (
    case_id     INT          NOT NULL COMMENT "The case",
    pipeline    VARCHAR(32)  NOT NULL COMMENT "germline_snv, germline_cnv, quality_control, quality_control_somatic, somatic_snv, import, case_status",
    item_key    VARCHAR(64)  NOT NULL DEFAULT "" COMMENT "Empty for a case-level pipeline; the calling task for somatic_snv, the sequencing task for import",
    decided_at  DATETIME     NOT NULL COMMENT "UTC, to the microsecond; one value per write",
    run_id      VARCHAR(250) NOT NULL COMMENT "The Airflow run that wrote the row",
    decision    VARCHAR(16)  NOT NULL COMMENT "run, wait, blocked, skip (scheduler); succeeded, failed (pipeline)",
    reasons     JSON         NULL     COMMENT "List of {code, detail, patient_id}; patient_id only on a reason about one member; NULL when none"
) ENGINE=OLAP
DUPLICATE KEY(case_id, pipeline, item_key, decided_at)
PARTITION BY date_trunc('month', decided_at)
DISTRIBUTED BY HASH(case_id);
