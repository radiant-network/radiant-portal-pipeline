CREATE TABLE IF NOT EXISTS {{ params.starrocks_hpo_gene_panel }} (
  `symbol` varchar(30) NOT NULL COMMENT "",
  `panel` varchar(250) NOT NULL COMMENT "",
  `hpo_term_name` varchar(200)>  NOT NULL COMMENT "",
  `hpo_term_id` varchar(200)>  NOT NULL COMMENT ""
)
ENGINE=OLAP
DUPLICATE KEY(`symbol`, `panel`)
