-- What each pipeline decided for each case, at each stage of a case scheduler run, and why: the changes.
-- Written by `radiant.tasks.nextflow.decision.write_decisions`; append-only. A pass writes every `run` (the work of
-- the run, read back by the "from Cases" DAGs) and any other decision only when it differs from the unit's last state,
-- the view `case_pipeline_state`. The table grows with the changes, not with the cases: no retention.
CREATE TABLE IF NOT EXISTS {{ mapping.starrocks_pipeline_decision_log }} (
    run_id      VARCHAR(250) NOT NULL COMMENT "The case scheduler run, sanitized as the child run ids (`sanitize_run_tag`)",
    stage       VARCHAR(16)  NOT NULL COMMENT "discover (before the pipelines) or after (after them)",
    pipeline    VARCHAR(32)  NOT NULL COMMENT "germline_snv, germline_cnv, somatic_snv, qc, import",
    case_id     INT          NOT NULL,
    scope       VARCHAR(8)   NOT NULL COMMENT "case: the decision of the unit; member: one member at fault, with its own reason",
    tenant_code VARCHAR(50)  NULL,
    patient_id  INT          NULL     COMMENT "member rows: the member at fault",
    seq_id      INT          NULL     COMMENT "member rows: its current sequencing experiment, NULL while it has none",
    task_id     INT          NULL     COMMENT "member rows: its current alignment task, when it has one",
    unit_id     INT          NULL     COMMENT "somatic_snv: the calling task; the unit is the (calling task, case) pair",
    decision    VARCHAR(16)  NOT NULL COMMENT "run, wait (the data will come), skipped (left out by configuration or nothing to process), excluded (something needs fixing), done",
    reason      VARCHAR(64)  NULL     COMMENT "the reason code; NULL for run, except the import's",
    detail      VARCHAR(2048) NULL    COMMENT "the readable reason, written for the ETL team",
    decided_at  DATETIME     NOT NULL COMMENT "UTC, like the staging timestamps; one value per pass"
) ENGINE=OLAP
DUPLICATE KEY(run_id, stage, pipeline, case_id)
PARTITION BY date_trunc('day', decided_at)
DISTRIBUTED BY HASH(case_id) BUCKETS 1;
