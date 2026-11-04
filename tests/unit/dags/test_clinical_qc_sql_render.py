"""Render `pending_quality_control_select.sql` the way Airflow will, and pin what it must say."""

import re

import jinja2

from radiant.dags import DAGS_DIR
from radiant.tasks.data.radiant_tables import get_radiant_mapping

_CLINICAL_SQL = DAGS_DIR / "sql" / "clinical"
_CONF = {"RADIANT_TABLES_DATABASE": "radiant"}
_TEMPLATE = "pending_quality_control_select.sql"
_ANNOTATION_TEMPLATE = "pending_annotation_select.sql"
_NO_PARAMS = {"task_ids": [], "excluded_tenants": []}


def _render(filename: str, params: dict | None = None) -> str:
    text = (_CLINICAL_SQL / filename).read_text()
    return jinja2.Template(text, undefined=jinja2.StrictUndefined).render(
        mapping=get_radiant_mapping(_CONF),
        params=params if params is not None else _NO_PARAMS,
    )


def _without_comments(sql: str) -> str:
    return "\n".join(line for line in sql.splitlines() if not line.lstrip().startswith("--"))


def _cte(sql: str, name: str) -> str:
    return sql.split(f"{name} AS (")[1].split("\n),\n")[0]


def test_every_mapping_key_used_exists():
    assert "{{" not in _render(_TEMPLATE)


def test_discovery_is_unscoped_by_default():
    sql = _without_comments(_render(_TEMPLATE))
    assert "%(task_ids)s" not in sql
    assert "%(excluded_tenants)s" not in sql


def test_named_tasks_narrow_candidacy_not_membership():
    sql = _without_comments(_render(_TEMPLATE, {"task_ids": [1], "excluded_tenants": []}))
    assert sql.count("%(task_ids)s") == 1
    assert "%(task_ids)s" in _cte(sql, "candidate")


def test_the_tenant_block_list_is_a_reason_not_a_filter():
    sql = _without_comments(_render(_TEMPLATE, {"task_ids": [], "excluded_tenants": ["radiant"]}))
    assert "'tenant_excluded'" in sql
    assert "WHERE cs.tenant_code" not in sql


def test_no_stray_percent_survives_parameter_binding():
    sql = _render(_TEMPLATE, {"task_ids": [1], "excluded_tenants": ["radiant"]})
    assert "%" not in re.sub(r"%\(\w+\)s", "", sql).replace("%%", "")


def _window(cte: str) -> str:
    return cte.split("ROW_NUMBER() OVER (")[1].split(") AS rn")[0]


def test_the_supersession_policy_is_the_annotation_query_s():
    """One policy, two templates: the newest completed experiment per member and the newest
    alignment per experiment must be selected the same way, or the two DAGs disagree on
    which experiment a case is 'about'. The QC query only ranks alignments per type too."""
    qc = _without_comments(_render(_TEMPLATE))
    annotation = _without_comments(_render(_ANNOTATION_TEMPLATE))
    assert _window(_cte(qc, "current_experiment")) == _window(_cte(annotation, "current_experiment"))
    assert "WHERE se.status_code = 'completed'" in _cte(qc, "current_experiment")
    assert _window(_cte(qc, "current_alignment")) == _window(_cte(annotation, "current_alignment")).replace(
        "PARTITION BY tc.sequencing_experiment_id", "PARTITION BY tc.sequencing_experiment_id, t.task_type_code"
    )


def test_the_trigger_document_is_the_cram_selected_on_type_fields():
    sql = _without_comments(_render(_TEMPLATE))
    cram = _cte(sql, "cram")
    assert "data_type_code = 'alignment'" in cram
    assert "format_code    = 'cram'" in cram
    assert "LIKE" not in sql


def test_the_anti_join_is_on_the_qc_task_type_and_scoped_to_the_case():
    sql = _without_comments(_render(_TEMPLATE))
    done = _cte(sql, "quality_controlled")
    assert "t.task_type_code = 'quality_control_metrics'" in done
    assert "tc.case_id IS NOT NULL" in done
    assert "radiant_germline_annotation" not in sql


def test_the_dragen_metrics_tarball_is_selected_on_type_fields_and_counted():
    sql = _without_comments(_render(_TEMPLATE))
    tarball = _cte(sql, "dragen_metrics")
    assert "data_type_code = 'ssup'" in tarball
    assert "format_code    = 'tgz'" in tarball
    assert "COUNT(DISTINCT url) AS matches" in tarball
    assert "dm.url                          AS dragen_metrics_url" in sql
    assert "WHEN dm.matches > 1     THEN 'ambiguous_dragen_metrics'" in sql


def test_one_row_per_member_not_per_document():
    """Each joined CTE has at most one row per experiment; joining the raw documents would
    multiply the members."""
    sql = _without_comments(_render(_TEMPLATE))
    assert "LEFT JOIN alignment_document" not in sql
    assert "cr.url                          AS cram_url" in sql
    assert "ci.url                          AS crai_url" in sql


def test_members_without_sequencing_come_back_carrying_a_reason():
    sql = _without_comments(_render(_TEMPLATE))
    for reason in (
        "pending_sequencing",
        "pending_alignment",
        "no_cram",
        "ambiguous_cram",
        "ambiguous_dragen_metrics",
        "no_metrics",
        "no_project_code",
    ):
        assert f"'{reason}'" in sql
    assert "LEFT JOIN current_experiment ce" in sql


def test_germline_and_somatic_cases_never_revoked_ones():
    cases = _cte(_without_comments(_render(_TEMPLATE)), "qc_case")
    assert "c.status_code NOT IN ('draft', 'revoked')" in cases
    assert "c.case_type_code IN ('germline', 'somatic')" in cases


def test_a_normal_experiment_never_excludes_a_somatic_case():
    """The tumor is QC'd whether or not the case also holds a normal experiment."""
    assert "'normal'" not in _without_comments(_render(_TEMPLATE))


def test_a_somatic_case_runs_its_tumor_on_its_somatic_alignment():
    sql = _without_comments(_render(_TEMPLATE))
    assert "WHEN 'somatic' THEN 'alignment_somatic_variant_calling'" in _cte(sql, "qc_case")
    assert "(qc.case_type = 'germline' OR s.histology_code = 'tumoral')" in _cte(sql, "current_experiment")
    assert "('alignment_germline_variant_calling', 'alignment_somatic_variant_calling')" in _cte(
        sql, "current_alignment"
    )
    for cte in ("cram", "crai", "dragen_metrics"):
        assert "GROUP BY seq_id, task_type" in _cte(sql, cte), cte
    for alias in ("ca", "tr", "cr", "ci", "dm"):
        assert f"AND {alias}.task_type = ce.alignment_type" in sql, alias
    assert "AND tr.task_type = ce.alignment_type" in _cte(sql, "candidate")


def test_a_tumor_s_cram_plays_no_role():
    """The CRAM CTEs are germline only, so a somatic row has NULL CRAM columns, and candidacy
    and a targeted rerun use the alignment task, which for a tumor is the one that holds the tarball."""
    sql = _without_comments(_render(_TEMPLATE, {"task_ids": [1], "excluded_tenants": []}))
    for cte in ("cram", "crai"):
        assert "task_type      = 'alignment_germline_variant_calling'" in _cte(sql, cte), cte
    assert "task_type = 'alignment_somatic_variant_calling'" in _cte(sql, "trigger_alignment")
    assert "FROM cram" in _cte(sql, "trigger_alignment")
    assert "AND tr.task_id IN %(task_ids)s" in _cte(sql, "candidate")
    assert "tr.task_id                      AS alignment_task_id" in sql
    assert "WHEN cs.case_type = 'germline' AND cr.matches IS NULL THEN 'no_cram'" in sql


def test_a_somatic_case_s_only_member_is_its_proband():
    # The last CTE: it ends where the final SELECT starts.
    members = _cte(_without_comments(_render(_TEMPLATE)), "qc_member").split("\n)\nSELECT")[0]
    somatic = members.split("UNION ALL")[1]
    assert "cs.proband_id                               AS patient_id" in somatic
    assert "'proband'                                   AS role" in somatic
    assert "COALESCE(f.affected_status_code, 'unknown') AS affected_status" in somatic
    assert "LEFT JOIN" in somatic
    assert "WHERE cs.case_type = 'germline'" in members.split("UNION ALL")[0]


def test_a_tumor_without_a_tarball_is_no_metrics_and_the_case_type_is_returned():
    sql = _without_comments(_render(_TEMPLATE))
    assert "WHEN cs.case_type = 'somatic' AND dm.matches IS NULL THEN 'no_metrics'" in sql
    assert "cs.case_type                    AS case_type" in sql
