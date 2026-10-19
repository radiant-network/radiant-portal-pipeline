from radiant.tasks.vcf.snv.somatic.process import create_parquet_files
from radiant.tasks.vcf.snv.writer_cli import run_writer

if __name__ == "__main__":
    run_writer(create_parquet_files, "Import Somatic SNV VCF for task")
