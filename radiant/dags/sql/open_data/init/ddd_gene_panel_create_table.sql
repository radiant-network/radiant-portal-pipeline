CREATE TABLE IF NOT EXISTS {{ mapping.starrocks_ddd_gene_panel }} (
  `symbol` varchar(30) NOT NULL COMMENT "",
  `panel` varchar(250) NOT NULL COMMENT ""
)
ENGINE=OLAP
DUPLICATE KEY(`symbol`, `panel`)
