"""Case-driven inputs and outputs for the somatic SNV annotation.

Sibling of `cnv/` and `qc/`, built the same way: the DAG in
`radiant/dags/nextflow_somatic_snv_annotation_cases.py` supplies the I/O, these modules supply the
rules. `paths`, `portal` and `register` are shared. The pipeline is the germline one
(snv-post-processing), entered at its `normalize` step with `tools = vep`, so the unit of work is a
calling task and its raw VCF, not a family.

Design: radiant-hub `docs/features/RAD-23-somatic-vep-annotation.md`.
"""
