"""RAD-181: the CNV Exomiser report never reaches an SNV, and the SNV one never reaches a CNV.

`raw_exomiser_cnv` (mapping key `starrocks_staging_exomiser_cnv`) is read by the germline CNV occurrence insert only,
and that insert reads no SNV Exomiser table.
"""

import re

from radiant.dags import DAGS_DIR

_SQL_DIR = DAGS_DIR / "sql"
_CNV_KEY = re.compile(r"mapping\.starrocks_staging_exomiser_cnv\b")
_SNV_KEYS = re.compile(r"mapping\.starrocks_(staging_exomiser|exomiser)\b(?!_cnv)")

# The templates allowed to name `raw_exomiser_cnv`: its DDL, its copy step and its one reader.
_CNV_READERS = {
    "radiant/init/staging_exomiser_cnv_create_table.sql",
    "radiant/germline_cnv_occurrence_insert_partition_delta.sql",
}


def _templates():
    return {str(p.relative_to(_SQL_DIR)): p.read_text() for p in _SQL_DIR.rglob("*.sql")}


def test_only_the_germline_cnv_insert_reads_raw_exomiser_cnv():
    readers = {path for path, text in _templates().items() if _CNV_KEY.search(text)}
    assert readers == _CNV_READERS


def test_the_germline_cnv_insert_reads_no_snv_exomiser_table():
    text = (_SQL_DIR / "radiant" / "germline_cnv_occurrence_insert_partition_delta.sql").read_text()
    assert not _SNV_KEYS.search(text)
