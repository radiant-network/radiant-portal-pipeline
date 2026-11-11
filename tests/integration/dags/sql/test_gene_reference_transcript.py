"""The reference transcript of each gene, built from `ensembl_transcript_v1` and `mane_v1`.

Fixtures `tests/resources/integration/open_data/{ensembl_transcript_v1,mane_v1}.tsv`. Each gene gets its MANE
Select transcript, else its Ensembl canonical one, and the RefSeq id of the MANE Select row with the same
transcript id **and** version.
"""

import os

import jinja2
import pytest

from tests.integration.dags.sql.test_cnv_nb_snv import _SQL_DIR, _create_table

_ORM1 = "ENSG00000229314"
_CANONICAL_ONLY = "ENSG00000900001"
_VERSION_MISMATCH = "ENSG00000900002"
_NO_FLAG = "ENSG00000900003"

_COLUMNS = ("ensembl_transcript_id", "ensembl_transcript_version", "is_mane_select", "refseq_transcript_id")


@pytest.fixture
def reference_transcripts(starrocks_session, open_data_iceberg_tables, radiant_mapping):
    _create_table(starrocks_session, "open_data", "gene_reference_transcript", radiant_mapping, truncate=True)
    with open(os.path.join(_SQL_DIR, "open_data", "gene_reference_transcript_insert.sql")) as f_in:
        sql = jinja2.Template(f_in.read()).render({"mapping": radiant_mapping})

    target = radiant_mapping["starrocks_gene_reference_transcript"]
    with starrocks_session.cursor() as cursor:
        for table in ("iceberg_ensembl_transcript", "iceberg_mane"):
            # The mapping value carries the ref clause, which REFRESH does not take.
            cursor.execute(f"REFRESH EXTERNAL TABLE {radiant_mapping[table].split(' VERSION AS OF ')[0]}")
        cursor.execute(sql)
        cursor.execute(f"SELECT ensembl_gene_id, {', '.join(_COLUMNS)} FROM {target}")
        rows = {row[0]: dict(zip(_COLUMNS, row[1:], strict=True)) for row in cursor.fetchall()}

    yield rows

    with starrocks_session.cursor() as cursor:
        cursor.execute(f"TRUNCATE TABLE {target}")


def test_a_mane_select_gene_gets_its_refseq_id(reference_transcripts):
    """ORM1: its MANE Select transcript, not its other transcript (whose flags are null)."""
    assert reference_transcripts[_ORM1] == {
        "ensembl_transcript_id": "ENST00000259396",
        "ensembl_transcript_version": 9,
        "is_mane_select": True,
        "refseq_transcript_id": "NM_000607.4",
    }


def test_a_gene_without_mane_select_gets_its_canonical_transcript_and_no_refseq_id(reference_transcripts):
    """The MANE Plus Clinical row of its other transcript gives it nothing."""
    assert reference_transcripts[_CANONICAL_ONLY] == {
        "ensembl_transcript_id": "ENST00000900011",
        "ensembl_transcript_version": 2,
        "is_mane_select": False,
        "refseq_transcript_id": None,
    }


def test_a_mane_row_of_another_transcript_version_gives_no_refseq_id(reference_transcripts):
    """A MANE release built on another Ensembl release leaves the RefSeq id null, not a wrong one."""
    assert reference_transcripts[_VERSION_MISMATCH]["ensembl_transcript_id"] == "ENST00000900021"
    assert reference_transcripts[_VERSION_MISMATCH]["is_mane_select"]
    assert reference_transcripts[_VERSION_MISMATCH]["refseq_transcript_id"] is None


def test_a_gene_with_neither_flag_gets_no_row(reference_transcripts):
    assert _NO_FLAG not in reference_transcripts
    assert set(reference_transcripts) == {_ORM1, _CANONICAL_ONLY, _VERSION_MISMATCH}
