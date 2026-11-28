"""The task a Nextflow pipeline registered for each item it ran: the journal's link from a pipeline run to its output.

The batch PATCH reports no task id, so the task is found back from the URL of one of its output documents, which
the run collected itself and which belongs to that task alone.
"""

from collections.abc import Callable

import jinja2

from radiant.tasks.decision import Decision
from radiant.tasks.journal import Entry

# A document is the output of one task only: the portal enforces it.
OUTPUT_TASK_SQL = """
SELECT d.url, thd.task_id
FROM {{ mapping.clinical_document }} d
JOIN {{ mapping.clinical_task_has_document }} thd ON thd.document_id = d.id AND thd.type = 'output'
WHERE d.url IN %(urls)s
"""


def with_output_tasks(
    entries: list[Entry],
    collected: dict[str, dict] | None,
    key_of: Callable[[Entry], str],
    task_ids_of: Callable[[list[str]], dict[str, int]] | None = None,
) -> list[Entry]:
    """`entries`, each `succeeded` one with the task registered for it. `collected` is the run's outputs, by the key
    `key_of` gives an entry (the samplesheet's family id)."""
    url_of = {key: min(doc["url"] for doc in documents.values()) for key, documents in (collected or {}).items()}
    wanted = {
        i: url_of[key_of(e)] for i, e in enumerate(entries) if e.decision == Decision.SUCCEEDED and key_of(e) in url_of
    }
    if not wanted:
        return entries
    task_of = (task_ids_of or output_task_ids)(sorted(set(wanted.values())))
    return [
        e.model_copy(update={"output_task_id": task_of.get(wanted[i])}) if i in wanted else e
        for i, e in enumerate(entries)
    ]


def output_task_ids(urls: list[str], conf: dict | None = None) -> dict[str, int]:
    """The task that has each of `urls` as output, over the Airflow StarRocks connection."""
    from airflow.hooks.base import BaseHook

    from radiant.tasks.data.radiant_tables import get_radiant_mapping

    sql = jinja2.Template(OUTPUT_TASK_SQL).render(mapping=get_radiant_mapping(conf))
    connection = BaseHook.get_connection("starrocks_conn").get_hook().get_conn()
    with connection.cursor() as cursor:
        cursor.execute(sql, {"urls": urls})
        return {url: int(task_id) for url, task_id in cursor.fetchall()}
