INSERT OVERWRITE {{ mapping.starrocks_ensembl_gene }}
SELECT
gene_id,
chromosome,
start,
end,
version,
type,
strand,
phase,
name,
alias,
biotype,
ccdsid,
constitutive,
description,
ensembl_end_phase,
ensembl_phase,
external_name,
logic_name,
length
FROM {{ mapping.iceberg_ensembl_gene }}
WHERE name IS NOT NULL
;

