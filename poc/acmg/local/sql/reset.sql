-- The stand-ins follow the pipeline columns: drop them so that schema.sql recreates them with the current ones.
CREATE DATABASE IF NOT EXISTS ${shared_db};
CREATE DATABASE IF NOT EXISTS ${tenant_db};
DROP TABLE IF EXISTS ${shared_db}.snv__consequence FORCE;
DROP TABLE IF EXISTS ${shared_db}.gnomad_genomes_v3 FORCE;
DROP TABLE IF EXISTS ${shared_db}.clinvar FORCE;
DROP TABLE IF EXISTS ${shared_db}.clinvar_rcv_summary FORCE;
DROP TABLE IF EXISTS ${shared_db}.staging_sequencing_experiment FORCE;
DROP TABLE IF EXISTS ${tenant_db}.snv__variant FORCE;
DROP TABLE IF EXISTS ${tenant_db}.germline__snv__occurrence FORCE;
DROP TABLE IF EXISTS ${tenant_db}.germline__snv__comphet FORCE;
DROP TABLE IF EXISTS ${shared_db}.obs_categorical FORCE;
DROP TABLE IF EXISTS ${shared_db}.omim_gene_panel FORCE;
DROP TABLE IF EXISTS ${shared_db}.ensembl_exon_by_gene FORCE;
