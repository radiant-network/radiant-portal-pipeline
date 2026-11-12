-- What each pipeline decided for each candidate case, at each stage of a case scheduler run, and why.
-- Written by `radiant.tasks.nextflow.decision.write_decisions`; append-only.
--
-- No retention for now: a few hundred rows a day. The daily partitions keep one possible later, with a
-- `partition_live_number` property or by dropping the old partitions.
CREATE TABLE IF NOT EXISTS {{ mapping.starrocks_pipeline_decision_log }} (
    run_id      VARCHAR(250) NOT NULL COMMENT "The case scheduler run, sanitized as the child run ids (`sanitize_run_tag`); a child run by hand logs under its own",
    stage       VARCHAR(16)  NOT NULL COMMENT "discover (the scheduler, before the pipelines), launch (a child, what it ran), after (the scheduler, after the pipelines)",
    pipeline    VARCHAR(32)  NOT NULL COMMENT "germline_snv, germline_cnv, somatic_snv, qc, import",
    case_id     INT          NOT NULL,
    scope       VARCHAR(8)   NOT NULL COMMENT "case: the decision of the unit; member: one member at fault, with its own reason",
    tenant_code VARCHAR(50)  NULL,
    patient_id  INT          NULL     COMMENT "member rows: the member at fault",
    seq_id      INT          NULL     COMMENT "member rows: its current sequencing experiment, NULL while it has none",
    task_id     INT          NULL     COMMENT "member rows: its current alignment task, when it has one",
    unit_id     INT          NULL     COMMENT "somatic_snv: the calling task; the unit is the (calling task, case) pair",
    decision    VARCHAR(16)  NOT NULL COMMENT "run, wait (the data will come), skipped (left out by configuration), excluded (something needs fixing)",
    reason      VARCHAR(64)  NULL     COMMENT "the reason code; NULL for run, except the import's",
    detail      VARCHAR(2048) NULL    COMMENT "the readable reason, written for the ETL team",
    decided_at  DATETIME     NOT NULL COMMENT "UTC, like the staging timestamps"
) ENGINE=OLAP
DUPLICATE KEY(run_id, stage, pipeline, case_id)
PARTITION BY date_trunc('day', decided_at)
DISTRIBUTED BY HASH(case_id) BUCKETS 1;
