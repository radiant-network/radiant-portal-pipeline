"""
Pseudo-code for the process_expression task following the proposed schema:
https://docs.google.com/spreadsheets/d/18znLGx23qknUWXaqgMQ5cRNK3JZfUU9A7LwwdsGvfis/edit?usp=sharing

Pulls from these files:
- rsem.genes.results*
- rsem.isoforms.results*
- kallisto.abundance.tsv*
- ReadsPerGene.out.tab*
- RNASeQC.counts.tar.gz

Docs also proposes a schema for transcript annotation table to be added to open data
- gencode.v39.primary_assembly.annotation.gtf
"""

import pandas as pd
import shutil
import tarfile
from pathlib import Path
from typing import List, Dict, Any, Optional
from pyiceberg.schema import Schema
from pyiceberg.types import FloatType, IntegerType, StringType, NestedField

from radiant.tasks.iceberg.partition_commit import PartitionCommit
from radiant.tasks.iceberg.table_accumulator import TableAccumulator

EXPRESSION_SCHEMA = Schema(
    NestedField(1, "bs_id", StringType(), required=True),
    NestedField(2, "case_id", StringType(), required=False), 
    NestedField(3, "tool", StringType(), required=True), 
    NestedField(4, "feature_type", StringType(), required=True), 
    NestedField(5, "feature_id", StringType(), required=True), 
    NestedField(6, "feature_id_base", StringType(), required=False),
    NestedField(7, "feature_symbol", StringType(), required=False),
    # Quantitative Metrics
    NestedField(10, "expected_count", FloatType(), required=False),
    NestedField(11, "tpm", FloatType(), required=False),
    NestedField(12, "fpkm", FloatType(), required=False),
    NestedField(13, "iso_pct", FloatType(), required=False),
    # Metadata/Physical metrics
    NestedField(20, "length", FloatType(), required=False),
    NestedField(21, "effective_length", FloatType(), required=False),
    NestedField(22, "strandedness", StringType(), required=False),
    # STAR-specific stranded counts
    NestedField(30, "star_count_unstranded", IntegerType(), required=False),
    NestedField(31, "star_count_first", IntegerType(), required=False),
    NestedField(32, "star_count_second", IntegerType(), required=False),
    NestedField(40, "task_id", IntegerType(), required=False),
)

def load_rsem_genes(path: Path, bs_id):
    df = pd.read_csv(path, sep="\t", compression="infer")

    # RSEM gene file usually has: gene_id, transcript_id(s), length, effective_length, expected_count, TPM, FPKM, (IsoPct optional)
    iso_pct_col = "IsoPct" if "IsoPct" in df.columns else None

    out = pd.DataFrame({
        "bs_id": bs_id,
        "tool": "RSEM",
        "feature_type": "gene",
        "feature_id": df["gene_id"],        # we will append symbols later
        "feature_id_base": df["gene_id"],
        "feature_symbol": None,
        "strandedness": None,
        "length": df["length"],
        "effective_length": df["effective_length"],
        "expected_count": df["expected_count"],
        "iso_pct": df[iso_pct_col] if iso_pct_col else None,
        "tpm": df["TPM"],
        "fpkm": df["FPKM"],
        "star_count_unstranded": None,
        "star_count_first": None,
        "star_count_second": None,
    })

    return out


def load_rsem_isoforms(path: Path, bs_id):
    df = pd.read_csv(path, sep="\t", compression="infer")

    # RSEM isoform file usually has: transcript_id, gene_id, length, effective_length, expected_count, TPM, FPKM, IsoPct
    iso_pct_col = "IsoPct" if "IsoPct" in df.columns else None

    out = pd.DataFrame({
        "bs_id": bs_id,
        "tool": "RSEM",
        "feature_type": "transcript",
        "feature_id": df["transcript_id"],
        "feature_id_base": df["transcript_id"],
        "feature_symbol": None,
        "strandedness": None,
        "length": df["length"],
        "effective_length": df["effective_length"],
        "expected_count": df["expected_count"],
        "iso_pct": df[iso_pct_col] if iso_pct_col else None,
        "tpm": df["TPM"],
        "fpkm": df["FPKM"],
        "star_count_unstranded": None,
        "star_count_first": None,
        "star_count_second": None,
    })

    return out


def load_kallisto(path: Path, bs_id):
    df = pd.read_csv(path, sep="\t", compression="infer")

    # Kallisto abundance.tsv usually has: target_id, length, eff_length, est_counts, tpm
    out = pd.DataFrame({
        "bs_id": bs_id,
        "tool": "KALLISTO",
        "feature_type": "transcript",
        "feature_id": df["target_id"],
        "feature_id_base": df["target_id"],
        "feature_symbol": None,
        "strandedness": None,
        "length": df["length"],
        "effective_length": df["eff_length"],
        "expected_count": df["est_counts"],
        "iso_pct": None,
        "tpm": df["tpm"],
        "fpkm": None,
        "star_count_unstranded": None,
        "star_count_first": None,
        "star_count_second": None,
    })

    return out


def load_star_counts(path: Path, bs_id):
    df = pd.read_csv(path, sep="\t", header=None, compression="infer",
                     names=["feature_id", "unstranded", "first", "second"])

    # STAR has special rows starting with "N_"
    specials = df[df["feature_id"].str.startswith("N_")].copy()
    genes = df[~df["feature_id"].str.startswith("N_")].copy()

    # Special rows (e.g., N_unmapped)
    special_rows = pd.DataFrame({
        "bs_id": bs_id,
        "tool": "STAR",
        "feature_type": "special",
        "feature_id": specials["feature_id"],
        "feature_id_base": specials["feature_id"],
        "feature_symbol": None,
        "strandedness": None,
        "length": None,
        "effective_length": None,
        "expected_count": None,
        "iso_pct": None,
        "tpm": None,
        "fpkm": None,
        "star_count_unstranded": specials["unstranded"],
        "star_count_first": specials["first"],
        "star_count_second": specials["second"],
    })

    # Gene rows
    gene_rows = pd.DataFrame({
        "bs_id": bs_id,
        "tool": "STAR",
        "feature_type": "gene",
        "feature_id": genes["feature_id"],
        "feature_id_base": genes["feature_id"],
        "feature_symbol": None,
        "strandedness": None,
        "length": None,
        "effective_length": None,
        "expected_count": None,
        "iso_pct": None,
        "tpm": None,
        "fpkm": None,
        "star_count_unstranded": genes["unstranded"],
        "star_count_first": genes["first"],
        "star_count_second": genes["second"],
    })

    return pd.concat([special_rows, gene_rows], ignore_index=True)


# RNA-SeQC from counts tarball
def _parse_gct_filelike(fileobj):
    df = pd.read_csv(fileobj, sep="\t", comment="#")
    # First column = feature ID, last column = sample value (Counts or TPM)
    feature_col = df.columns[0]
    sample_col = df.columns[-1]
    df = df[[feature_col, sample_col]]
    df = df.rename(columns={feature_col: "feature_id", sample_col: "value"})
    return df


def load_rnaseqc_from_tar(tar_path: Path, bs_id):
    """
    tar_path: *.RNASeQC.counts.tar.gz containing gene_reads.gct and gene_tpm.gct
    """
    with tarfile.open(tar_path, "r:gz") as tar:
        reads_member = None
        tpm_member = None

        for m in tar.getmembers():
            name = m.name
            if "gene_reads.gct" in name:
                reads_member = m
            elif "gene_tpm.gct" in name:
                tpm_member = m

        if reads_member is None or tpm_member is None:
            raise RuntimeError(f"Missing gene_reads.gct or gene_tpm.gct in {tar_path}")

        with tar.extractfile(reads_member) as f_reads:
            df_reads = _parse_gct_filelike(f_reads)
        with tar.extractfile(tpm_member) as f_tpm:
            df_tpm = _parse_gct_filelike(f_tpm)

    merged = df_reads.merge(df_tpm, on="feature_id", suffixes=("_reads", "_tpm"))

    out = pd.DataFrame({
        "bs_id": bs_id,
        "tool": "RNASEQC",
        "feature_type": "gene",
        "feature_id": merged["feature_id"],
        "feature_id_base": merged["feature_id"],
        "feature_symbol": None,
        "strandedness": None,
        "length": None,
        "effective_length": None,
        "expected_count": merged["value_reads"],
        "iso_pct": None,
        "tpm": merged["value_tpm"],
        "fpkm": None,
        "star_count_unstranded": None,
        "star_count_first": None,
        "star_count_second": None,
    })

    return out


def get_sample_files(sample_dir: Path, bs_id: str):
    """
    Collects available expression files and returns a unified DataFrame 
    aligned with the EXPRESSION_SCHEMA.
    """
    dfs = []
    tool_loaders = [
        ("*.rsem.genes.results*", load_rsem_genes),
        ("*.rsem.isoforms.results*", load_rsem_isoforms),
        ("*.kallisto.abundance.tsv*", load_kallisto),
        ("*.ReadsPerGene.out.tab*", load_star_counts),
        ("*.RNASeQC.counts.tar.gz", load_rnaseqc_from_tar)
    ]

    for pattern, loader_func in tool_loaders:
        file_path = next(sample_dir.glob(pattern), None)
        if file_path:
            dfs.append(loader_func(file_path, bs_id))

    if not dfs:
        raise FileNotFoundError(f"No valid expression files found in {sample_dir}")

    return pd.concat(dfs, ignore_index=True)


def process_expression_case(case, sample_dir: Path, catalog, namespace="radiant"):
    """
    Entry point for the Airflow task.
    'case' is the dictionary passed from the DAG.
    """
    case_id = case.get("case_id")
    bs_id = case.get("bs_id")
    
    try:
        combined = get_sample_files(sample_dir, bs_id=bs_id)
        combined["case_id"] = case_id
        combined["task_id"] = case.get("task_id")

        schema_cols = [field.name for field in EXPRESSION_SCHEMA.fields]
        combined = combined.reindex(columns=schema_cols)

        table_name = f"{namespace}.expression_occurrence"
        table = catalog.load_table(table_name)
        buffer = TableAccumulator(table, partition_filter={"case_id": case_id})
        
        buffer.extend(combined.to_dict(orient="records"))
        buffer.write_files()
        
        return [
            PartitionCommit(
                parquet_files=buffer.parquet_paths,
                partition_filter=buffer.partition_filter
            )
        ]
        
    except Exception as e:
        print(f"Error processing case {case_id}: {e}")
        raise