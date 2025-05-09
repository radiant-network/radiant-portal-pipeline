CREATE TABLE IF NOT EXISTS {{ params.starrocks_orphanet_gene_panel }} (
  `symbol` varchar(30) NOT NULL COMMENT "",
  `panel` varchar(250) NOT NULL COMMENT "",
  `disorder_id` bigint NULL COMMENT "",
  `type_of_inheritance` array<varchar(200)> NULL COMMENT ""
)
ENGINE=OLAP
DUPLICATE KEY(`symbol`, `panel`)
