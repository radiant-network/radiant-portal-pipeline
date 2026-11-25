-- Local stand-ins of the pipeline tables, reduced to the columns facts.sql reads (same names and types).

-- Flight clients fetch results from the BEs, not through the FE. A GLOBAL variable only: in QA it is an infra change.
SET GLOBAL arrow_flight_proxy_enabled = false;

CREATE DATABASE IF NOT EXISTS ${shared_db};
CREATE DATABASE IF NOT EXISTS ${tenant_db};
-- The engines' output tables: never in the pipeline databases.
CREATE DATABASE IF NOT EXISTS ${shared_db}_out;

CREATE TABLE IF NOT EXISTS ${shared_db}.snv__consequence (
    locus_id BIGINT,
    symbol VARCHAR(30),
    transcript_id VARCHAR(100),
    consequences ARRAY<VARCHAR(50)>,
    biotype VARCHAR(50) NULL,
    exon_rank VARCHAR(10) NULL,
    exon_total VARCHAR(10) NULL,
    spliceai_ds FLOAT NULL,
    is_canonical BOOLEAN NULL,
    is_picked BOOLEAN NULL,
    is_mane_select BOOLEAN NULL,
    revel_score FLOAT NULL,
    gnomad_loeuf FLOAT NULL
)
PRIMARY KEY (locus_id, symbol, transcript_id)
DISTRIBUTED BY HASH(locus_id) BUCKETS 10;

CREATE TABLE IF NOT EXISTS ${shared_db}.gnomad_genomes_v3 (
    locus_id BIGINT NOT NULL,
    af DOUBLE NULL,
    ac INT NULL,
    an INT NULL,
    hom INT NULL
)
DUPLICATE KEY (locus_id)
DISTRIBUTED BY HASH(locus_id) BUCKETS 10;

CREATE TABLE IF NOT EXISTS ${tenant_db}.snv__variant (
    locus_id BIGINT NOT NULL,
    chromosome CHAR(2),
    start BIGINT,
    germline_pn_wgs_not_affected INT,
    germline_pf_wgs_not_affected DOUBLE,
    germline_pn_wxs_not_affected INT,
    germline_pf_wxs_not_affected DOUBLE,
    germline_hom_wgs_not_affected INT,
    germline_hom_wxs_not_affected INT,
    germline_pc_wgs_affected INT,
    germline_pc_wxs_affected INT,
    germline_pn_wgs_affected INT,
    germline_pn_wxs_affected INT,
    germline_pc_wgs_not_affected INT,
    germline_pc_wxs_not_affected INT,
    omim_inheritance_code ARRAY<VARCHAR(5)>
)
PRIMARY KEY (locus_id)
DISTRIBUTED BY HASH(locus_id) BUCKETS 10;

CREATE TABLE IF NOT EXISTS ${tenant_db}.germline__snv__occurrence (
    part INT NOT NULL,
    seq_id INT NOT NULL,
    task_id INT NOT NULL,
    locus_id BIGINT NOT NULL,
    gq INT,
    ad_alt INT,
    zygosity CHAR(3),
    filter VARCHAR(255),
    father_calls ARRAY<INT>,
    mother_calls ARRAY<INT>,
    transmission_mode VARCHAR(50),
    transmission_mode_source VARCHAR(10)
)
DUPLICATE KEY (part, seq_id, task_id, locus_id)
DISTRIBUTED BY HASH(locus_id) BUCKETS 10;

CREATE TABLE IF NOT EXISTS ${tenant_db}.germline__snv__comphet (
    part INT NOT NULL,
    seq_id INT NOT NULL,
    task_id INT NOT NULL,
    locus_id BIGINT NOT NULL,
    partner_locus_id BIGINT NOT NULL,
    symbol VARCHAR(200) NOT NULL
)
DUPLICATE KEY (part, seq_id, task_id, locus_id)
DISTRIBUTED BY HASH(locus_id) BUCKETS 10;

CREATE TABLE IF NOT EXISTS ${shared_db}.staging_sequencing_experiment (
    case_id INT NOT NULL,
    seq_id INT NOT NULL,
    task_id INT NOT NULL,
    analysis_type VARCHAR(50),
    patient_id VARCHAR(255),
    experimental_strategy VARCHAR(50),
    tenant_code VARCHAR(50),
    family_id INT,
    family_role VARCHAR(50),
    affected_status VARCHAR(50),
    deleted BOOLEAN NOT NULL DEFAULT "false"
)
PRIMARY KEY (case_id, seq_id, task_id)
DISTRIBUTED BY HASH(seq_id) BUCKETS 10;

CREATE TABLE IF NOT EXISTS ${shared_db}.clinvar (
    locus_id BIGINT NOT NULL,
    chromosome VARCHAR(10),
    start BIGINT,
    clin_sig ARRAY<VARCHAR(100)>,
    mc ARRAY<VARCHAR(100)>,
    geneinfo VARCHAR(1000)
)
DUPLICATE KEY (locus_id)
DISTRIBUTED BY HASH(locus_id) BUCKETS 10;

CREATE TABLE IF NOT EXISTS ${shared_db}.clinvar_rcv_summary (
    locus_id BIGINT NOT NULL,
    review_status_stars INT
)
DUPLICATE KEY (locus_id)
DISTRIBUTED BY HASH(locus_id) BUCKETS 10;
