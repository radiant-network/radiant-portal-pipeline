from argparse import Namespace

import pytest
from acmg.db import HERE, connect, execute
from acmg.exchange import scope
from acmg.models import Ruleset

DB = "acmg_poc_scope_test"
RULESET = Ruleset.load(HERE / "ruleset.yaml")


@pytest.fixture(autouse=True, scope="module")
def local_starrocks():
    try:
        connect().close()
    except Exception:
        pytest.skip("no local StarRocks")
    yield


def test_classified_falls_back_to_ensembl_canonical_when_the_gene_has_no_ensembl_mane():
    execute(
        f"DROP DATABASE IF EXISTS {DB} FORCE; CREATE DATABASE {DB};\n"
        f"CREATE TABLE {DB}.snv__consequence (locus_id BIGINT, transcript_id VARCHAR(30), symbol VARCHAR(20),"
        " is_mane_select BOOLEAN, is_canonical BOOLEAN, consequences ARRAY<VARCHAR(50)>, spliceai_ds DOUBLE)"
        " DUPLICATE KEY (locus_id) DISTRIBUTED BY HASH(locus_id) BUCKETS 1;\n"
        f"CREATE TABLE {DB}.germline__snv__occurrence (seq_id BIGINT, locus_id BIGINT, part INT)"
        " DUPLICATE KEY (seq_id) DISTRIBUTED BY HASH(seq_id) BUCKETS 1;\n"
        f"CREATE TABLE {DB}.staging_sequencing_experiment (seq_id BIGINT, analysis_type VARCHAR(20),"
        " tenant_code VARCHAR(20), deleted BOOLEAN, family_role VARCHAR(20), affected_status VARCHAR(20))"
        " DUPLICATE KEY (seq_id) DISTRIBUTED BY HASH(seq_id) BUCKETS 1;\n"
        f"INSERT INTO {DB}.staging_sequencing_experiment VALUES (7, 'germline', 'x', false, 'proband', 'affected');\n"
        f"INSERT INTO {DB}.germline__snv__occurrence VALUES (7, 1, 0), (7, 2, 0), (7, 3, 0);\n"
        # Locus 1: gene A has its MANE flag on the RefSeq copy only; gene B (neighbour) has an Ensembl MANE.
        # Locus 2: gene C has an Ensembl MANE, its canonical copy is not taken twice.
        # Locus 3: gene D has no MANE at all (lncRNA): left out.
        f"INSERT INTO {DB}.snv__consequence VALUES"
        " (1, 'ENST_A', 'A', false, true, ['missense_variant'], NULL),"
        " (1, 'NM_A', 'A', true, false, ['missense_variant'], NULL),"
        " (1, 'ENST_B', 'B', true, true, ['downstream_gene_variant'], NULL),"
        " (2, 'ENST_C', 'C', true, true, ['missense_variant'], NULL),"
        " (2, 'ENST_C2', 'C', false, true, ['missense_variant'], NULL),"
        " (3, 'ENST_D', 'D', false, true, ['splice_region_variant'], NULL),"
        " (3, 'NR_D', 'D', false, true, ['splice_region_variant'], NULL)"
    )
    args = Namespace(
        shared_db=DB, tenant_db=DB, output_db=DB, tenant_code="x", clinical_db="c.d", part=None, seq_ids=None,
        all_transcripts=False,
    )
    with connect() as conn, conn.cursor() as cur:
        cur.execute(f"SELECT locus_id, transcript_id FROM ({scope(args, RULESET)['classified']}) k ORDER BY 1, 2")
        assert cur.fetchall() == ((1, "ENST_A"), (2, "ENST_C"))
