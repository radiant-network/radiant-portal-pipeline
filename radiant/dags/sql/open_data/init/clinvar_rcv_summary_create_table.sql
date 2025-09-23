CREATE TABLE IF NOT EXISTS {{ mapping.starrocks_clinvar_rcv_summary }}
(
    `locus_id`              BIGINT(20)   NOT NULL,
    `clinvar_id`            VARCHAR(32)  NOT NULL,
    `accession`             VARCHAR(32)  NOT NULL,
    `clinical_significance` ARRAY<VARCHAR (64)> NULL,
    `date_last_evaluated`   DATE         NULL,
    `submission_count`      INT(11)      NULL,
    `review_status`         VARCHAR(128) NULL,
    `review_status_stars`   INT(11)      NULL,
    `version`               INT(11)      NULL,
    `traits`                ARRAY< VARCHAR (128)> NULL,
    `origins`               ARRAY< VARCHAR (64)> NULL,
    `submissions` ARRAY<
        STRUCT<
            submitter             VARCHAR(128),
            scv                   VARCHAR(32),
            version               INT(11),
            review_status         VARCHAR(128),
            review_status_stars   INT(11),
            clinical_significance VARCHAR(128),
            date_last_evaluated   DATE
        >
    > NULL,
    `clinical_significance_count` MAP<VARCHAR(64), INT(11)> NULL
)
ENGINE = OLAP
DISTRIBUTED BY HASH(`locus_id`)
BUCKETS 10
PROPERTIES (
    "colocate_with" = "{{ mapping.colocate_query_group }}"
);