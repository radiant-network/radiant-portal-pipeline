CREATE TABLE IF NOT EXISTS {{ params.starrocks_gnomad_constraint }} (
  `transcript_id` varchar(100) NOT NULL COMMENT "",
  `pli` float,
  `loeuf` float
)
ENGINE=OLAP
COMMENT "OLAP"
