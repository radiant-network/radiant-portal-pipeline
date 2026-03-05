import logging

logger = logging.getLogger("airflow.task")


def create_parquet_files(tasks: list[dict], namespace: str) -> dict[str, list[dict]]:
    logger.info(f"somatic create_parquet_files function called with tasks: {tasks} and namespace: {namespace}")
    logger.info(
        "SKIPPED: Somatic SNV VCF processing is currently not implemented. "
        "This function is a placeholder and does not perform any operations."
    )
    return {}
