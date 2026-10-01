"""Case-driven inputs and outputs for the Ferlab snv-post-processing.

Everything here is deliberately Airflow-free and side-effect-free apart from
`portal.py` (HTTP) -- the DAG in `radiant/dags/nextflow_postprocessing_cases.py`
supplies the I/O, these modules supply the rules.
"""
