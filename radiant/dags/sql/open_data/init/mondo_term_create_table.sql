CREATE TABLE IF NOT EXISTS {{ mapping.starrocks_mondo_term }} (
                              `id` varchar(65533) NULL COMMENT "",
                              `name` varchar(65533) NULL COMMENT "",
                              `term` varchar(65533) NULL AS concat(`id`, ' ', `name`) COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`id`)
DISTRIBUTED BY RANDOM