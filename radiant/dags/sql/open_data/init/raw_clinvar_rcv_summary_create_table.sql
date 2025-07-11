CREATE TABLE IF NOT EXISTS {{ params.starrocks_raw_clinvar_rcv_summary }}
(
    `clinvar_id`            VARCHAR(32)  NULL,
    `accession`             VARCHAR(32)  NULL,
    `clinical_significance` ARRAY<VARCHAR (64)> NULL,
    `date_last_evaluated`   DATE         NULL,
    `submission_count`      INT(11)      NULL,
    `review_status`         VARCHAR(128) NULL,
    `review_status_stars`   INT(11)      NULL,
    `version`               INT(11)      NULL,
    `traits`                ARRAY< VARCHAR (128)> NULL,
    `origins`               ARRAY< VARCHAR (64)> NULL,
    `submissions`           ARRAY<
        struct<
            submitter               VARCHAR (128),
            scv                     VARCHAR(32),
            version                 INT(11),
            review_status           VARCHAR(128),
            review_status_stars     INT(11),
            clinical_significance   VARCHAR(128),
            date_last_evaluated     DATE
        >
    >,
    `clinical_significance_count` MAP<VARCHAR (64), INT(11)> NULL
)
ENGINE = OLAP
DUPLICATE KEY(`clinvar_id`, `accession`);