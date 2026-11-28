"""The head every pipeline DAG starts with: read its facts, decide each candidate, journal it, and move the cases it
will run to `processing`. The DAG then runs what was decided, `decided["run_case_ids"]` (`run_task_ids` for somatic
SNV), and closes each item with its outcome.
"""

import datetime
import logging
from typing import Any

from airflow.decorators import task

from radiant.tasks.journal import Pipeline
from radiant.tasks.starrocks.operator import RadiantStarRocksOperator, rows_output_processor

LOGGER = logging.getLogger(__name__)

# The status PATCH is guarded by the expected status (`submitted`), and the journal write by the item's last row, so a
# retry changes nothing twice.
RETRIES = 2
RETRY_DELAY = datetime.timedelta(minutes=1)


@task(retries=RETRIES, retry_delay=RETRY_DELAY)
def decide(fact_rows: Any, pipeline: str) -> dict:
    """Every candidate's decision, and what runs: the named items among those decided `run`, or all of them."""
    from airflow.sdk import get_current_context

    from radiant.tasks.nextflow.case_status import decide_run

    params = get_current_context()["params"]
    named = params["task_ids"] if pipeline == Pipeline.SOMATIC_SNV else params["case_ids"]
    return decide_run(Pipeline(pipeline), fact_rows, params["excluded_tenants"], named)


@task(retries=RETRIES, retry_delay=RETRY_DELAY)
def journal(decided: dict) -> int:
    from airflow.sdk import get_current_context

    from radiant.tasks.journal import Entry, record

    written = record([Entry(**e) for e in decided["entries"]], get_current_context()["run_id"])
    LOGGER.info("%d row(s) written", len(written))
    return len(written)


@task(task_display_name="[PyOp] List the cases to move, per tenant")
def list_processing(decided: dict) -> list[dict]:
    return decided["processing"]


@task(task_display_name="[PyOp] Move the cases to run to processing", retries=RETRIES, retry_delay=RETRY_DELAY)
def set_processing(batch: dict) -> Any:
    from radiant.tasks.nextflow.case_status import PROCESSING, change_tenant_status

    return change_tenant_status(batch, PROCESSING)


def decision_head(
    pipeline: Pipeline,
    facts_sql: str,
    label: str,
    suffix: str = "",
    move_to_processing: bool = True,
    parameters: dict | None = None,
):
    """`fetch_facts → decide → journal`, and `set_processing` mapped per tenant when `move_to_processing`. Returns the
    `decide` output and the tasks the run must wait for (the status moves). `suffix` tells apart the heads of a DAG
    that decides two pipelines. The somatic SNV query reads only the named calling tasks when the run names some
    (`params.task_ids`), so that a named task without its VCF gets its reason."""
    fetch_facts = RadiantStarRocksOperator(
        task_id=f"fetch_facts{suffix}",
        task_display_name=f"[StarRocks] Facts of the {label} candidates",
        sql=facts_sql,
        parameters=parameters,
        output_processor=rows_output_processor,
        do_xcom_push=True,
    )
    decided = decide.override(task_id=f"decide{suffix}", task_display_name=f"[PyOp] Decide each {label} candidate")(
        fetch_facts.output, str(pipeline)
    )
    journal.override(task_id=f"journal{suffix}", task_display_name=f"[PyOp] Journal the {label} decisions")(decided)
    if not move_to_processing:
        return decided, []
    batches = list_processing.override(task_id=f"list_processing{suffix}")(decided)
    return decided, [set_processing.override(task_id=f"set_processing{suffix}").expand(batch=batches)]
