CREATE TABLE IF NOT EXISTS {{ mapping.starrocks_gene_reference_transcript }} (
    `ensembl_gene_id` varchar(128) NULL COMMENT "",
    `ensembl_transcript_id` varchar(128) NULL COMMENT "",
    `ensembl_transcript_version` int NULL COMMENT "",
    `is_mane_select` boolean NULL COMMENT "",
    `refseq_transcript_id` varchar(128) NULL COMMENT ""
    ) ENGINE=OLAP
    DUPLICATE KEY(`ensembl_gene_id`);
