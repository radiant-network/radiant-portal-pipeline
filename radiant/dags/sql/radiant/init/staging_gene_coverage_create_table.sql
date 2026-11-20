-- RAD-315. The DRAGEN `coverage_by_gene` CSV (`covgene` document) of each current alignment, as read from the file:
-- every row of the file, `gene` still the raw name, `type` set to `CDS` for the old format (no `type` column). Shared
-- by all tenants (`tenant_code` set during the broker load), one partition per part, like `raw_exomiser`. The
-- per-tenant `gene_coverage` insert keeps the CDS rows and applies the gene name rule.
CREATE TABLE IF NOT EXISTS {{ mapping.starrocks_staging_gene_coverage }}
(
    part             INT,
    seq_id           INT,
    task_id          INT,
    tenant_code      VARCHAR(50),
    gene             VARCHAR(200),
    type             VARCHAR(50),
    size             INT,
    average_coverage DOUBLE,
    coverage_5x      FLOAT,
    coverage_15x     FLOAT,
    coverage_30x     FLOAT,
    coverage_50x     FLOAT,
    coverage_100x    FLOAT,
    coverage_200x    FLOAT,
    coverage_300x    FLOAT,
    coverage_400x    FLOAT,
    coverage_500x    FLOAT,
    coverage_1000x   FLOAT
)
ENGINE = OLAP DUPLICATE KEY(`part`, `seq_id`, `task_id`)
PARTITION BY (`part`)
DISTRIBUTED BY HASH(`gene`) BUCKETS 10;
