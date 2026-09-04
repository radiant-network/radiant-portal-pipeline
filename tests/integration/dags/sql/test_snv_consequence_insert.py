"""SJRA-1827 -- RefSeq MANE rows borrow their Ensembl twin's prediction scores.

dbNSFP and gnomAD constraint are keyed on unversioned Ensembl transcript ids, so a RefSeq annotation
(`NM_...`) matches neither on its own identifier. `snv_consequence_insert.sql` therefore keys those two
joins on `mane_pair_transcript_id` -- the version-free `MANE_SELECT` cross-reference -- whenever the row
came from the RefSeq catalogue.

The failure mode this file exists to catch is silent: if a versioned or wrong-namespace key reaches the
join, every score on every RefSeq row comes back null, which looks exactly like not having implemented
the borrow at all. `scores_from_mane_pair` cannot detect that -- it reports the provenance of the join
*key*, not that a value was found -- so the assertions below compare the borrowed values against the
twin's, and pin the two source tables independently.
"""

import os

import jinja2
import pyarrow as pa
import pytest
from pyiceberg.expressions import EqualTo

from radiant.dags import DAGS_DIR

_SQL_DIR = os.path.join(DAGS_DIR, "sql")

# Own task_id so the rows are addressable: the insert filters on it, and the teardown deletes on it.
_TASK_ID = 18270

# One locus for every consequence row, as a merged VCF produces. `snv__consequence` is keyed on
# (locus_id, symbol, transcript_id), so the rows stay distinct through their transcripts.
_LOCUS_ID = 1_827_001
_LOCUS = "17-7676154-C-T"
_LOCUS_HASH = "sjra1827-locus-hash"

# Every numeric value here is an exact binary fraction, so it survives the round trip through a
# StarRocks `float` (float32) unchanged and the assertions below can compare exactly.
_ENST1_SCORES = {
    "sift_score": 0.5,
    "sift_pred": "D",
    "polyphen2_hvar_score": 0.25,
    "polyphen2_hvar_pred": "D",
    "fathmm_score": 1.5,
    "fathmm_pred": "D",
    "cadd_score": 2.0,
    "cadd_phred": 4.0,
    "dann_score": 0.75,
    "revel_score": 0.125,
    "lrt_score": 0.0625,
    "lrt_pred": "D",
    "phyloP17way_primate": 3.5,
    "phyloP100way_vertebrate": 7.25,
}
_ENST2_SCORES = {**_ENST1_SCORES, "sift_score": 0.375, "cadd_score": 8.0, "revel_score": 0.875}

_DBNSFP_COLUMNS = ("locus_id", "ensembl_transcript_id", *_ENST1_SCORES)

# A dbNSFP row and a constraint row keyed on the *RefSeq* side of a pair. Nothing may ever join to
# these: they are the tripwire for the key being built with a COALESCE instead of a source-keyed CASE,
# which would hand every Ensembl row its paired `NM_...` and quietly replace the scores it gets today.
# They also catch the CASE being dropped altogether now that `transcript_id` is version-free on both
# catalogues: `ON d.ensembl_transcript_id = c.transcript_id` would match the RefSeq row straight onto
# this poison instead of borrowing its twin's scores.
_POISON_TRANSCRIPT = "NM_0001"
_POISON_SCORE = -99.0

_SCORE_COLUMNS = (
    "sift_score",
    "sift_pred",
    "polyphen2_hvar_score",
    "polyphen2_hvar_pred",
    "fathmm_score",
    "fathmm_pred",
    "cadd_score",
    "cadd_phred",
    "dann_score",
    "revel_score",
    "lrt_score",
    "lrt_pred",
    "phyloP17way_primate",
    "phyloP100way_vertebrate",
)


def _scores(row):
    """The score columns of one loaded row, as a plain dict for exact comparison."""
    return {column: row[column] for column in _SCORE_COLUMNS}


def _flag(row):
    """`scores_from_mane_pair` as a bool -- StarRocks hands BOOLEAN back over the wire as 0/1."""
    return bool(row["scores_from_mane_pair"])


def _consequence_row(
    *,
    symbol,
    transcript_id,
    transcript_version,
    source,
    mane_select,
    mane_pair,
    is_mane,
    consequences=("missense_variant",),
    vep_impact="MODERATE",
    impact_score=3,
):
    """One Iceberg consequence row, with every non-nullable field of the merged schema filled."""
    return {
        "task_id": _TASK_ID,
        "locus": _LOCUS,
        "locus_hash": _LOCUS_HASH,
        "chromosome": "17",
        "start": 7676154,
        "end": 7676154,
        "reference": "C",
        "alternate": "T",
        "variant_class": "SNV",
        "hgvsg": None,
        "hgvsp": None,
        "hgvsc": None,
        "symbol": symbol,
        "transcript_id": transcript_id,
        "transcript_version": transcript_version,
        "source": source,
        "biotype": "protein_coding",
        "strand": "1",
        "exon": None,
        "vep_impact": vep_impact,
        "consequences": list(consequences),
        "mane_select": mane_select,
        "mane_pair_transcript_id": mane_pair,
        "is_mane_select": is_mane,
        "is_mane_plus": False,
        "is_picked": False,
        "is_canonical": is_mane,
        "aa_change": None,
        "dna_change": None,
        "impact_score": impact_score,
    }


# `mane_select` deliberately keeps its version suffix while `mane_pair_transcript_id` and
# `transcript_id` do not. dbNSFP and gnomAD constraint only ever hold unversioned ids, so a join built
# on `mane_select` matches nothing -- these rows are what makes that mistake fail rather than pass with
# silent nulls. The RefSeq rows keep a non-null `transcript_version` for the same reason: it must stay
# out of the identifier, and a row that carried its version in both would let a regression pass.
_CONSEQUENCE_ROWS = [
    # 1. Ensembl MANE Select. Scored on its own transcript, as before this change.
    _consequence_row(
        symbol="TP53",
        transcript_id="ENST0001",
        transcript_version=None,
        source="Ensembl",
        mane_select="NM_0001.6",
        mane_pair="NM_0001",
        is_mane=True,
    ),
    # 2. Its RefSeq twin. Must end up with row 1's scores, from both source tables.
    _consequence_row(
        symbol="TP53",
        transcript_id="NM_0001",
        transcript_version="6",
        source="RefSeq",
        mane_select="ENST0001.2",
        mane_pair="ENST0001",
        is_mane=True,
    ),
    # 3. Non-MANE RefSeq. `MANE_SELECT` is empty for these, so there is no pair and nothing to borrow.
    _consequence_row(
        symbol="TP53",
        transcript_id="NM_9999",
        transcript_version="1",
        source="RefSeq",
        mane_select="",
        mane_pair="",
        is_mane=False,
    ),
    # 4. Ensembl MANE Select covered by dbNSFP but NOT by gnomAD constraint.
    _consequence_row(
        symbol="BRCA1",
        transcript_id="ENST0002",
        transcript_version=None,
        source="Ensembl",
        mane_select="NM_0002.1",
        mane_pair="NM_0002",
        is_mane=True,
    ),
    # 5. Its RefSeq twin -- isolates the dbNSFP leg: scores arrive, pLI/LOEUF stay null.
    _consequence_row(
        symbol="BRCA1",
        transcript_id="NM_0002",
        transcript_version="1",
        source="RefSeq",
        mane_select="ENST0002.4",
        mane_pair="ENST0002",
        is_mane=True,
    ),
    # 6. RefSeq MANE whose twin is in gnomAD constraint only -- isolates the constraint leg, so a total
    #    dbNSFP-leg failure cannot hide behind it (or the reverse).
    _consequence_row(
        symbol="EGFR",
        transcript_id="NM_0003",
        transcript_version="1",
        source="RefSeq",
        mane_select="ENST0003.7",
        mane_pair="ENST0003",
        is_mane=True,
    ),
    # 8. Non-MANE RefSeq reporting a consequence Ensembl does not report for this gene. This is the
    #    class SJRA-1828 exists to preserve: on the reference file it is 79 HIGH-impact keys that an
    #    impact filter would otherwise never surface. Contrast with row 3, same gene and the *same*
    #    consequence as the Ensembl row, which SJRA-1828 must drop.
    _consequence_row(
        symbol="TP53",
        transcript_id="NM_8888",
        transcript_version="1",
        source="RefSeq",
        mane_select="",
        mane_pair="",
        is_mane=False,
        consequences=("frameshift_variant",),
        vep_impact="HIGH",
        impact_score=4,
    ),
    # 7. Intergenic: `resolve_source()` rule 4 leaves the source NULL. `source = 'RefSeq'` then yields
    #    NULL, which is what the COALESCE around the flag exists to absorb -- the column is NOT NULL.
    _consequence_row(
        symbol="",
        transcript_id="",
        transcript_version=None,
        source=None,
        mane_select="",
        mane_pair="",
        is_mane=False,
    ),
]


def _reset_table(starrocks_session, sql_subdir, table_name, mapping):
    with open(os.path.join(_SQL_DIR, sql_subdir, "init", f"{table_name}_create_table.sql")) as f_in:
        create_table_sql = jinja2.Template(f_in.read()).render({"mapping": mapping})

    with starrocks_session.cursor() as cursor:
        cursor.execute(create_table_sql)
        cursor.execute(f"TRUNCATE TABLE {mapping[f'starrocks_{table_name}']};")


def _insert_rows(starrocks_session, table, columns, rows):
    sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))})"
    with starrocks_session.cursor() as cursor:
        cursor.executemany(sql, rows)


def _fetch_by_transcript(starrocks_session, table):
    """Read the loaded rows as dicts keyed on transcript_id.

    By name rather than by position on purpose: this test must not have to change every time a column is
    added to `snv__consequence`.
    """
    with starrocks_session.cursor() as cursor:
        cursor.execute(f"SELECT * FROM {table}")
        names = [column[0] for column in cursor.description]
        return {row[names.index("transcript_id")]: dict(zip(names, row, strict=True)) for row in cursor.fetchall()}


@pytest.fixture
def seeded_consequences(starrocks_session, iceberg_client, setup_iceberg_namespace, radiant_mapping):
    """Seed the Iceberg consequence rows and the two Ensembl-keyed score tables.

    The Iceberg table is shared with the VCF-processing tests, which assert *unfiltered* row counts on
    it, so the rows are added under a dedicated `task_id` and deleted again on teardown. Everything the
    insert reads is filtered on that same `task_id`.
    """
    for subdir, table in (
        ("radiant", "snv_consequence"),
        ("radiant", "snv_consequence_filter"),
        ("radiant", "snv_tmp_variant"),
        ("open_data", "dbnsfp"),
        ("open_data", "gnomad_constraint"),
        ("open_data", "spliceai"),
    ):
        _reset_table(starrocks_session, subdir, table, radiant_mapping)

    _insert_rows(
        starrocks_session,
        radiant_mapping["starrocks_snv_tmp_variant"],
        ("locus_id", "locus_hash", "chromosome", "start", "reference", "alternate"),
        [(_LOCUS_ID, _LOCUS_HASH, "17", 7676154, "C", "T")],
    )
    _insert_rows(
        starrocks_session,
        radiant_mapping["starrocks_dbnsfp"],
        _DBNSFP_COLUMNS,
        [
            (_LOCUS_ID, "ENST0001", *_ENST1_SCORES.values()),
            (_LOCUS_ID, "ENST0002", *_ENST2_SCORES.values()),
            # Tripwire: keyed on the RefSeq side of pair 1, so only a COALESCE-style key can reach it.
            (_LOCUS_ID, _POISON_TRANSCRIPT, *{**_ENST1_SCORES, "sift_score": _POISON_SCORE}.values()),
        ],
    )
    _insert_rows(
        starrocks_session,
        radiant_mapping["starrocks_gnomad_constraint"],
        ("transcript_id", "pli", "loeuf"),
        [
            ("ENST0001", 0.5, 0.25),
            # No ENST0002 row: that is what makes rows 4 and 5 isolate the dbNSFP leg.
            ("ENST0003", 0.875, 0.75),
            (_POISON_TRANSCRIPT, _POISON_SCORE, _POISON_SCORE),
        ],
    )

    iceberg_table = iceberg_client.load_table(f"{setup_iceberg_namespace}.snv_consequence")
    # The table's own schema, not `consequence.SCHEMA`: Iceberg renumbers field ids on create, and the
    # append is validated by field id rather than by name.
    iceberg_table.append(pa.Table.from_pylist(_CONSEQUENCE_ROWS, schema=iceberg_table.schema().as_arrow()))

    # StarRocks caches Iceberg table metadata, and `test_queries.py` may already have populated it by
    # EXPLAINing this same table. Without the refresh the insert can read a snapshot from before the
    # append and return nothing.
    with starrocks_session.cursor() as cursor:
        cursor.execute(f"REFRESH EXTERNAL TABLE {radiant_mapping['iceberg_snv_consequence']}")

    yield

    iceberg_table.delete(EqualTo("task_id", _TASK_ID))


def _run(starrocks_session, mapping, sql_file, params=None):
    with open(os.path.join(_SQL_DIR, sql_file)) as f_in:
        rendered_sql = jinja2.Template(f_in.read()).render({"mapping": mapping})

    with starrocks_session.cursor() as cursor:
        cursor.execute(rendered_sql, params)


def test_refseq_mane_rows_borrow_their_twins_scores(seeded_consequences, starrocks_session, radiant_mapping):
    _run(starrocks_session, radiant_mapping, "radiant/snv_consequence_insert.sql", {"task_ids": [_TASK_ID]})

    rows = _fetch_by_transcript(starrocks_session, radiant_mapping["starrocks_snv_consequence"])
    # Version-free on both catalogues: `transcript_id` is part of the primary key, so a RefSeq release
    # bump must replace a row rather than sit beside it.
    assert set(rows) == {
        "ENST0001",
        "NM_0001",
        "NM_9999",
        "NM_8888",
        "ENST0002",
        "NM_0002",
        "NM_0003",
        "",
    }

    ensembl_mane = rows["ENST0001"]
    refseq_twin = rows["NM_0001"]
    refseq_non_mane = rows["NM_9999"]
    dbnsfp_only_ensembl = rows["ENST0002"]
    dbnsfp_only_twin = rows["NM_0002"]
    constraint_only_twin = rows["NM_0003"]
    intergenic = rows[""]

    # The version survives the load beside the identifier, so the citable accession is reconstructible.
    # Null on the Ensembl row because VEP never emitted one, not because the load dropped it.
    assert refseq_twin["transcript_version"] == "6"
    assert ensembl_mane["transcript_version"] is None

    # The Ensembl row is unchanged by this ticket: scored on its own transcript, flag clear. The poison
    # dbNSFP row keyed on its MANE pair must not have displaced any of it.
    assert _scores(ensembl_mane) == _ENST1_SCORES
    assert (ensembl_mane["gnomad_pli"], ensembl_mane["gnomad_loeuf"]) == (0.5, 0.25)
    assert _flag(ensembl_mane) is False

    # The borrow itself: identical values from both source tables, and the flag set so the portal can
    # label them as the twin's rather than computed on the RefSeq transcript.
    assert _scores(refseq_twin) == _ENST1_SCORES
    assert (refseq_twin["gnomad_pli"], refseq_twin["gnomad_loeuf"]) == (0.5, 0.25)
    assert _flag(refseq_twin) is True
    # The stored cross-reference is version-free, which is what makes the join above possible at all.
    assert refseq_twin["mane_pair_transcript_id"] == "ENST0001"
    assert refseq_twin["mane_select"] == "ENST0001.2"

    # dbNSFP leg on its own: scores borrowed, constraint columns legitimately absent.
    assert _scores(dbnsfp_only_ensembl) == _ENST2_SCORES
    assert _scores(dbnsfp_only_twin) == _ENST2_SCORES
    assert dbnsfp_only_twin["gnomad_pli"] is None
    assert _flag(dbnsfp_only_twin) is True

    # gnomAD constraint leg on its own, so a dead dbNSFP leg cannot hide behind it.
    assert (constraint_only_twin["gnomad_pli"], constraint_only_twin["gnomad_loeuf"]) == (0.875, 0.75)
    assert constraint_only_twin["sift_score"] is None
    assert _flag(constraint_only_twin) is True

    # A non-MANE RefSeq transcript has no pair, so it borrows nothing -- and must not be labelled as if
    # it had. Same for an intergenic block, whose source is NULL.
    for row in (refseq_non_mane, rows["NM_8888"], intergenic):
        assert all(value is None for value in _scores(row).values())
        assert row["gnomad_pli"] is None
        assert _flag(row) is False

    # The non-zero-coverage guard SJRA-1827 asks for, stated over the table rather than per row: if the
    # join key were ever built from the versioned `mane_select`, this would be 0.
    with starrocks_session.cursor() as cursor:
        cursor.execute(
            f"SELECT count(*), count(sift_score), count(gnomad_pli) "
            f"FROM {radiant_mapping['starrocks_snv_consequence']} WHERE scores_from_mane_pair"
        )
        flagged, with_sift, with_pli = cursor.fetchone()
    assert (flagged, with_sift, with_pli) == (3, 2, 2)


def test_score_borrow_joins_are_hash_joins(seeded_consequences, starrocks_session, radiant_mapping):
    """The MANE-pair score key must stay an equi-join condition.

    Keying a join on a `CASE` is unusual enough to be worth pinning: the optimizer is expected to hoist
    the expression into a Project above the scan and keep both joins hash joins, which was measured on
    StarRocks 4.0.11 but not on the 3.4.2 the compose stack pins.

    It also guards against the tempting rewrite
    `ON d.ensembl_transcript_id IN (c.transcript_id, c.mane_pair_transcript_id)`. That is
    *correct* -- the two identifier namespaces are disjoint, so only one candidate can ever match -- but
    it plans as a NESTLOOP JOIN over the whole consequence table. Nothing else would catch it: the
    results would be identical and `EXPLAIN` would still succeed.

    This lives here rather than beside the other EXPLAIN checks in `test_queries.py` because those run
    against empty tables, where the optimizer prunes the joins away and there is no plan left to assert
    on.
    """
    with open(os.path.join(_SQL_DIR, "radiant/snv_consequence_insert.sql")) as f_in:
        insert_sql = jinja2.Template(f_in.read()).render({"mapping": radiant_mapping})

    with starrocks_session.cursor() as cursor:
        cursor.execute(f"EXPLAIN {insert_sql}", {"task_ids": [_TASK_ID]})  # noqa: S608
        plan = "\n".join(str(row[0]) for row in cursor.fetchall())

    assert "NESTLOOP" not in plan, f"the MANE-pair score key is no longer an equi-join condition:\n{plan}"
    # One per borrowed source table, plus the two locus joins onto snv__tmp_variant and spliceai.
    assert plan.count("HASH JOIN") >= 2, f"snv_consequence_insert.sql lost its hash joins:\n{plan}"


def test_filter_table_takes_only_the_refseq_annotations_ensembl_does_not_provide(
    seeded_consequences, starrocks_session, radiant_mapping
):
    """SJRA-1828 -- `snv__consequence_filter` loads Ensembl as before, plus only what RefSeq adds.

    The fixture holds both cases on the same gene: `NM_9999` reports the *same* consequence as the
    Ensembl transcript and must be dropped, while `NM_8888` reports one Ensembl never mentions and
    must survive.

    `NM_9999` is what makes this a real test. It carries no scores -- it is non-MANE, so SJRA-1827
    finds no twin to borrow from -- while the Ensembl row it duplicates has dbNSFP values. The
    aggregation below groups on the score columns, so the two land in different groups: without the
    restriction that duplicate survives as a second `(TP53, missense_variant)` row rather than
    collapsing away.
    """
    _run(starrocks_session, radiant_mapping, "radiant/snv_consequence_insert.sql", {"task_ids": [_TASK_ID]})
    _run(starrocks_session, radiant_mapping, "radiant/snv_consequence_filter_insert.sql")

    with starrocks_session.cursor() as cursor:
        cursor.execute(
            f"SELECT symbol, consequence, vep_impact, is_deleterious, sift_score, gnomad_pli "  # noqa: S608
            f"FROM {radiant_mapping['starrocks_snv_consequence_filter']} ORDER BY symbol, consequence"
        )
        rows = cursor.fetchall()

    assert rows == (
        # Intergenic (source NULL): must not be caught by the RefSeq restriction.
        ("", "missense_variant", "MODERATE", 0, None, None),
        # Ensembl, scored. Its MANE twin duplicates this key and contributes no second row.
        ("BRCA1", "missense_variant", "MODERATE", 1, 0.375, None),
        # RefSeq-only symbol: Ensembl says nothing about EGFR at this locus, so it is kept. pLI alone
        # does not make a row deleterious -- `is_deleterious` looks only at the prediction scores.
        ("EGFR", "missense_variant", "MODERATE", 0, None, 0.875),
        # The row SJRA-1828 exists to keep: a consequence only RefSeq reports for this gene.
        ("TP53", "frameshift_variant", "HIGH", 0, None, None),
        # Exactly one row for the duplicated key, and it is the scored Ensembl one. Two rows here would
        # mean the anti join stopped firing; a null `sift_score` would mean it dropped the wrong side.
        ("TP53", "missense_variant", "MODERATE", 1, 0.5, 0.5),
    )
