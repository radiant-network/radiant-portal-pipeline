CREATE TABLE IF NOT EXISTS {{ mapping.starrocks_cytoband }}
(
    `chromosome`            CHAR(2)   NOT NULL,
    `cytoband`              VARCHAR(20) NOT NULL,
    `start`                 BIGINT    NOT NULL,
    `end`                   BIGINT    NOT NULL,
    `gie_stain`              VARCHAR(20) NULL
)
ENGINE = OLAP
DUPLICATE KEY(`chromosome`, `cytoband`);