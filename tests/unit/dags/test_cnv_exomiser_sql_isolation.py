"""RAD-181: the CNV Exomiser report never reaches an SNV, and the SNV one never reaches a CNV.

`raw_exomiser_cnv` (mapping key `starrocks_staging_exomiser_cnv`) is read by the tenant `exomiser_cnv` insert only.
`exomiser_cnv` (mapping key `starrocks_exomiser_cnv`) is read by the germline CNV occurrence insert only. Neither
CNV statement reads an SNV Exomiser table.
"""

import re

from radiant.dags import DAGS_DIR

_SQL_DIR = DAGS_DIR / "sql"
_RAW_CNV_KEY = re.compile(r"mapping\.starrocks_staging_exomiser_cnv\b")
_TENANT_CNV_KEY = re.compile(r"mapping\.starrocks_exomiser_cnv\b")
# `\b` does not match between `exomiser` and `_cnv`, so the CNV keys never match here.
_SNV_KEYS = re.compile(r"mapping\.starrocks_(staging_exomiser|exomiser)\b")

_CNV_INSERT = "radiant/exomiser_cnv_insert_partition_delta.sql"
_CNV_OCCURRENCE_INSERT = "radiant/germline_cnv_occurrence_insert_partition_delta.sql"


def _templates():
    return {str(p.relative_to(_SQL_DIR)): p.read_text() for p in _SQL_DIR.rglob("*.sql")}


def test_only_the_tenant_cnv_exomiser_insert_reads_raw_exomiser_cnv():
    readers = {path for path, text in _templates().items() if _RAW_CNV_KEY.search(text)}
    assert readers == {"radiant/init/staging_exomiser_cnv_create_table.sql", _CNV_INSERT}


def test_only_the_germline_cnv_insert_reads_exomiser_cnv():
    readers = {path for path, text in _templates().items() if _TENANT_CNV_KEY.search(text)}
    assert readers == {"radiant/init/exomiser_cnv_create_table.sql", _CNV_OCCURRENCE_INSERT}


def test_the_cnv_exomiser_statements_read_no_snv_exomiser_table():
    for path in (_CNV_INSERT, _CNV_OCCURRENCE_INSERT):
        assert not _SNV_KEYS.search((_SQL_DIR / path).read_text()), path


def test_no_snv_statement_reads_a_cnv_exomiser_table():
    for path, text in _templates().items():
        if _SNV_KEYS.search(text):
            assert not _RAW_CNV_KEY.search(text) and not _TENANT_CNV_KEY.search(text), path
