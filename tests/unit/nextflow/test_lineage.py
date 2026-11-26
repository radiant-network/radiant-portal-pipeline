from radiant.tasks.decision import Decision
from radiant.tasks.journal import Entry, Pipeline
from radiant.tasks.nextflow.lineage import with_output_tasks
from radiant.tasks.nextflow.model import family_id


def _entry(case_id, decision=Decision.SUCCEEDED, item_key=""):
    return Entry(case_id=case_id, pipeline=Pipeline.GERMLINE_SNV, item_key=item_key, decision=decision)


def _outputs(name):
    return {"vcf": {"url": f"s3://out/{name}.vcf.gz"}, "tbi": {"url": f"s3://out/{name}.vcf.gz.tbi"}}


def test_each_succeeded_item_names_the_task_registered_for_it():
    asked = []

    def task_ids_of(urls):
        asked.append(urls)
        return {"s3://out/a.vcf.gz": 103, "s3://out/b.vcf.gz": 104}

    entries = [_entry(500), _entry(501), _entry(502, Decision.FAILED)]
    collected = {family_id(500): _outputs("a"), family_id(501): _outputs("b"), family_id(502): _outputs("c")}
    result = with_output_tasks(entries, collected, lambda e: family_id(e.case_id), task_ids_of)

    assert [e.output_task_id for e in result] == [103, 104, None]
    # One lookup, one URL per item: the failed item is not looked up.
    assert asked == [["s3://out/a.vcf.gz", "s3://out/b.vcf.gz"]]


def test_an_item_keyed_by_its_calling_task():
    entries = [_entry(23, item_key="70"), _entry(24, item_key="70")]
    result = with_output_tasks(
        entries, {"70": _outputs("t70")}, lambda e: e.item_key, lambda urls: {"s3://out/t70.vcf.gz": 900}
    )
    assert [e.output_task_id for e in result] == [900, 900]


def test_no_output_collected_looks_nothing_up():
    def task_ids_of(urls):
        raise AssertionError("no lookup expected")

    entries = [_entry(500)]
    assert with_output_tasks(entries, None, lambda e: family_id(e.case_id), task_ids_of) == entries


def test_a_url_the_clinical_model_does_not_know_leaves_the_task_empty():
    entries = [_entry(500)]
    result = with_output_tasks(entries, {family_id(500): _outputs("a")}, lambda e: family_id(e.case_id), lambda u: {})
    assert result[0].output_task_id is None
