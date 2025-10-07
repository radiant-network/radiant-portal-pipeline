CREATE TABLE IF NOT EXISTS {{ mapping.starrocks_ensembl_exon_by_gene }} (
    `gene_id` varchar(128) NULL COMMENT "",
    `exon_id` varchar(128)  NULL COMMENT "",
    `chromosome` varchar(10) NULL COMMENT "",
    `start` bigint(20) NULL COMMENT "",
    `end` bigint(20) NULL COMMENT "",
    `transcript_ids` array<varchar(128)> DEFAULT NULL,
    `version` tinyint NULL COMMENT "",
    `type` varchar(128) NULL COMMENT "",
    `strand` char(1) NULL COMMENT "",
    `phase` tinyint NULL COMMENT "",
    `name` varchar(128) NULL COMMENT "",
    `alias` array<varchar(128)> NULL COMMENT "",
    `constitutive` varchar(128) NULL COMMENT "",
    `description` varchar(500) NULL COMMENT "",
    `ensembl_end_phase` varchar(10) NULL COMMENT "",
    `ensembl_phase` varchar(10) NULL COMMENT "",
    `external_name` varchar(128) NULL COMMENT "",
    `logic_name` varchar(500) NULL COMMENT "",
    `length` bigint(20) NULL COMMENT ""
    ) ENGINE=OLAP
    DUPLICATE KEY(`gene_id`, `exon_id`, `chromosome`);

