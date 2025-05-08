CREATE TABLE IF NOT EXISTS {{ params.starrocks_omim_gene_panel }} (
  `symbol` varchar(30) NOT NULL COMMENT "",
  `panel` varchar(200) NOT NULL COMMENT "",
  `inheritance_code` array<varchar(5)>  NULL COMMENT "",
  `inheritance` array<varchar(50)>  NULL COMMENT "",
  `omim_gene_id` int  NULL COMMENT "",
  `omim_phenotype_id` int  NULL COMMENT ""
)
ENGINE=OLAP
DUPLICATE KEY(`symbol`, `panel`)
