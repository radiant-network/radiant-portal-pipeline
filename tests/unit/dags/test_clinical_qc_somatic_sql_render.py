"""Render `pending_quality_control_somatic_select.sql` the way Airflow will, and pin what it must say."""

import re

import jinja2

from radiant.dags import DAGS_DIR
from radiant.tasks.data.radiant_tables import get_radiant_mapping

_CLINICAL_SQL = DAGS_DIR / "sql" / "clinical"
_CONF = {"RADIANT_TABLES_DATABASE": "radiant"}
_TEMPLATE = "pending_quality_control_somatic_select.sql"
_GERMLINE_TEMPLATE = "pending_quality_control_select.sql"
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


def _window(cte: str) -> str:
    return cte.split("ROW_NUMBER() OVER (")[1].split(") AS rn")[0]


def _output_columns(sql: str) -> list[str]:
    final_select = sql.split("\nSELECT ")[-1].split("\nFROM ")[0]
    return re.findall(r"\bAS (\w+),?$", final_select, flags=re.MULTILINE)


def test_every_mapping_key_used_exists():
    assert "{{" not in _render(_TEMPLATE)


def test_discovery_is_unscoped_by_default():
    sql = _without_comments(_render(_TEMPLATE))
    assert "%(task_ids)s" not in sql
    assert "%(excluded_tenants)s" not in sql


def test_named_tasks_narrow_candidacy_on_the_alignment_task():
    sql = _without_comments(_render(_TEMPLATE, {"task_ids": [1], "excluded_tenants": []}))
    assert sql.count("%(task_ids)s") == 1
    assert "AND ca.task_id IN %(task_ids)s" in _cte(sql, "candidate")


def test_the_tenant_block_list_is_a_reason_not_a_filter():
    sql = _without_comments(_render(_TEMPLATE, {"task_ids": [], "excluded_tenants": ["radiant"]}))
    assert "'tenant_excluded'" in sql
    assert "WHERE sc.tenant_code" not in sql


def test_no_stray_percent_survives_parameter_binding():
    sql = _render(_TEMPLATE, {"task_ids": [1], "excluded_tenants": ["radiant"]})
    assert "%" not in re.sub(r"%\(\w+\)s", "", sql).replace("%%", "")


def test_the_output_columns_are_the_germline_query_s():
    """`QcMember` reads both queries' rows: same names, same order."""
    assert _output_columns(_render(_TEMPLATE)) == _output_columns(_render(_GERMLINE_TEMPLATE))


def test_the_supersession_policy_is_the_annotation_query_s():
    somatic = _without_comments(_render(_TEMPLATE))
    annotation = _without_comments(_render(_ANNOTATION_TEMPLATE))
    assert _window(_cte(somatic, "current_experiment")) == _window(_cte(annotation, "current_experiment"))
    assert _window(_cte(somatic, "current_alignment")) == _window(_cte(annotation, "current_alignment"))
    assert "WHERE se.status_code = 'completed'" in _cte(somatic, "current_experiment")


def test_only_somatic_cases_and_never_revoked_ones():
    cases = _cte(_without_comments(_render(_TEMPLATE)), "somatic_case")
    assert "c.case_type_code = 'somatic'" in cases
    assert "c.status_code NOT IN ('draft', 'revoked')" in cases


def test_the_member_is_the_proband_s_tumor_experiment_and_a_normal_one_is_ignored():
    experiment = _cte(_without_comments(_render(_TEMPLATE)), "current_experiment")
    assert "AND s.patient_id = sc.proband_id" in experiment
    assert "s.histology_code = 'tumoral'" in experiment
    assert "'normal'" not in experiment


def test_the_alignment_is_the_somatic_one_joined_through_the_experiment():
    sql = _without_comments(_render(_TEMPLATE))
    alignment = _cte(sql, "current_alignment")
    assert "t.task_type_code = 'alignment_somatic_variant_calling'" in alignment
    assert "tc.sequencing_experiment_id" in alignment
    assert "tc.case_id" not in alignment


def test_the_tarball_is_selected_on_type_fields_and_counted():
    sql = _without_comments(_render(_TEMPLATE))
    tarball = _cte(sql, "dragen_metrics")
    assert "d.data_type_code = 'ssup'" in tarball
    assert "d.format_code    = 'tgz'" in tarball
    assert "thd.type = 'output'" in tarball
    assert "COUNT(DISTINCT d.url)" in tarball
    assert "LIKE" not in sql


def test_the_anti_join_is_on_the_qc_task_type_and_scoped_to_the_case():
    done = _cte(_without_comments(_render(_TEMPLATE)), "quality_controlled")
    assert "t.task_type_code = 'quality_control_metrics'" in done
    assert "tc.case_id IS NOT NULL" in done


def test_a_tumor_s_cram_plays_no_role():
    sql = _without_comments(_render(_TEMPLATE))
    assert "CAST(NULL AS VARCHAR)                       AS cram_url" in sql
    assert "CAST(NULL AS VARCHAR)                       AS crai_url" in sql
    assert "0                                           AS cram_matches" in sql
    assert "no_cram" not in sql
    assert "ambiguous_cram" not in sql


def test_the_member_is_the_proband_and_the_family_row_only_gives_the_affected_status():
    sql = _without_comments(_render(_TEMPLATE))
    assert "'proband'                                   AS role" in sql
    assert "COALESCE(f.affected_status_code, 'unknown') AS affected_status" in sql
    assert re.search(r"LEFT JOIN \S+ f\s+ON f\.case_id = sc\.case_id\s+AND f\.family_member_id = sc\.proband_id", sql)


def test_a_tumor_without_a_tarball_is_no_metrics_and_reasons_come_most_specific_first():
    sql = _without_comments(_render(_TEMPLATE, {"task_ids": [], "excluded_tenants": ["radiant"]}))
    reasons = re.findall(r"THEN '(\w+)'", sql)
    assert reasons == [
        "pending_sequencing",
        "pending_alignment",
        "ambiguous_dragen_metrics",
        "no_metrics",
        "no_project_code",
        "tenant_excluded",
    ]
    assert "LEFT JOIN current_experiment ce" in sql
    assert "'somatic'                                   AS case_type" in sql
