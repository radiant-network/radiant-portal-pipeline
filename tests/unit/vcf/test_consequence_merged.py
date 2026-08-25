"""Extraction tests against a real VEP `--merged --mane` file.

Every other fixture in `tests/resources/` was annotated against Ensembl alone, so none of them
exercises the merged-file code paths: no `SOURCE` column, no RefSeq blocks, no MANE flags.
`test_consequence_merged.vcf` is a 44-variant slice of the reference trio file -- see its
`##radiant_fixture` header line for provenance and the cases it was cut to cover.

What a real file adds over the synthetic fixtures is scale and a header whose 28 columns shadow
each other (`MANE` vs `MANE_SELECT`, the `RefSeq` column vs `RefSeq` the `SOURCE` value). So the
assertions below re-derive their expected values straight from the raw CSQ text, block by block,
rather than trusting the production parser.

One thing this module deliberately does *not* claim to test: that `SOURCE` beats a contradicting
transcript identifier. On a real merged file all three signals agree on every block, so reading
`SOURCE` from the wrong column still yields the right answer via the identifier fallback. That
precedence is pinned by `test_consequence_source_precedence.vcf` instead, where the two
deliberately disagree.
"""

import pytest

from radiant.tasks.vcf.snv.common import Common
from radiant.tasks.vcf.snv.consequence import (
    SOURCE_ENSEMBL,
    SOURCE_REFSEQ,
    parse_csq_header,
    process_consequence,
)
from tests.unit.vcf.vcf_test_utils import vcf

MERGED_VCF = "test_consequence_merged.vcf"

common = Common(1, 1, "1-1000-AC-A", "hash", "1", 1000, 1000, "AC", "A")

# Spanning-deletion records (ALT `*`), which VEP leaves with no CSQ field at all.
NO_CSQ_VARIANTS = {("chr1", 112653575), ("chr1", 154590148)}

# Column indexes in this fixture's CSQ header, taken from the `##INFO=<ID=CSQ ...Format:` line.
# Hardcoded on purpose: deriving them with `parse_csq_header` would make the oracle depend on
# the very lookup under test. The `merged` fixture pins them against the real header.
SOURCE_IDX = 27
MANE_IDX = 22
MANE_SELECT_IDX = 23
FEATURE_IDX = 7


@pytest.fixture(scope="module")
def merged():
    """Every record of the fixture as `(record, picked, consequences)`, plus its raw CSQ blocks.

    The raw blocks are the independent oracle: they are split straight out of the INFO field,
    so a mis-indexed column in `process_consequence` shows up as a mismatch.
    """
    with vcf(MERGED_VCF) as vcf_file:
        csq_header = parse_csq_header(vcf_file)
        # The oracle below indexes the raw CSQ by hand. If the fixture is ever recut with a
        # different column order those indexes would silently start reading a neighbouring
        # column and every assertion would compare the wrong pair of values, so pin them here.
        assert {
            "source": SOURCE_IDX,
            "mane": MANE_IDX,
            "mane_select": MANE_SELECT_IDX,
            "feature": FEATURE_IDX,
        }.items() <= csq_header.items(), f"CSQ column order changed: {csq_header}"
        rows = []
        for record in vcf_file:
            picked, consequences = process_consequence(record, csq_header, common)
            raw = record.INFO.get("CSQ")
            raw_blocks = [b.split("|") for b in raw.split(",")] if raw else []
            rows.append((record, picked, consequences, raw_blocks))
    return rows


@pytest.fixture(scope="module")
def blocks(merged):
    """Every consequence block, paired with the raw CSQ fields it was built from."""
    return [(c, raw) for _, _, consequences, raws in merged for c, raw in zip(consequences, raws, strict=True)]


def at(merged, chrom, pos):
    """The `(picked, consequences)` of one fixture record, by coordinate."""
    for record, picked, consequences, _ in merged:
        if (chrom, pos) == (record.CHROM, record.POS):
            return picked, consequences
    raise AssertionError(f"{chrom}:{pos} is not in {MERGED_VCF} -- the fixture was recut")


def test_the_resolved_source_matches_the_files_own_answer(blocks):
    # Every block of a merged file states its catalogue, and extraction must end up on that
    # value -- for all 1,500-odd blocks, not just the tidy ones.
    assert blocks
    classified = 0
    for consequence, raw in blocks:
        assert consequence["source"] == (raw[SOURCE_IDX] or None)
        classified += consequence["source"] is not None
    assert {c["source"] for c, _ in blocks} == {SOURCE_ENSEMBL, SOURCE_REFSEQ, None}
    assert classified, "no block resolved to a catalogue"


def test_intergenic_blocks_resolve_to_no_source(blocks):
    # Rule 4: a block with neither gene nor transcript belongs to no catalogue, and is kept
    # with a null source rather than dropped or forced into one.
    unclassified = [(c, raw) for c, raw in blocks if c["source"] is None]
    assert unclassified, "fixture no longer carries an intergenic block"
    for consequence, _ in unclassified:
        assert not consequence["transcript_id"]
        assert consequence["consequences"] == ["intergenic_variant"]


def test_mane_flags_are_derived_from_the_mane_column(blocks):
    # `MANE` holds the flag, `MANE_SELECT` holds an accession. Reading the latter as a flag
    # would light up `is_mane_select` on exactly the same rows here, so the assertion is tied
    # to the raw label rather than to a count.
    for consequence, raw in blocks:
        assert consequence["is_mane_select"] == (raw[MANE_IDX] == "MANE_Select")
        assert consequence["is_mane_plus"] == (raw[MANE_IDX] == "MANE_Plus_Clinical")

    select = [c for c, _ in blocks if c["is_mane_select"]]
    assert {c["source"] for c in select} == {SOURCE_ENSEMBL, SOURCE_REFSEQ}, "flags must populate on both catalogues"
    assert [c for c, _ in blocks if c["is_mane_plus"]], "fixture no longer carries a MANE Plus Clinical block"


def test_version_stripping_is_applied_to_both_sides_of_the_pair(blocks):
    # The versioned raw values are kept; only the derived columns are stripped. RefSeq
    # `Feature` values are versioned and Ensembl ones are not, so both are covered here.
    stripped = 0
    for consequence, raw in blocks:
        assert consequence["mane_select"] == raw[MANE_SELECT_IDX]
        assert consequence["transcript_id"] == raw[FEATURE_IDX]
        assert consequence["mane_pair_transcript_id"] == (raw[MANE_SELECT_IDX].split(".")[0])
        assert consequence["transcript_id_unversioned"] == (raw[FEATURE_IDX].split(".")[0])
        stripped += "." in raw[MANE_SELECT_IDX] or "." in raw[FEATURE_IDX]
    assert stripped, "nothing in the fixture was versioned -- the test proves nothing"


def test_the_mane_pair_joins_its_twin_in_both_directions(merged):
    # The reason `transcript_id_unversioned` exists. Stripping only the cross-reference makes
    # RefSeq->Ensembl resolve and Ensembl->RefSeq resolve nothing, silently; this fails in
    # that case because the round trip never completes from the Ensembl side.
    directions = set()
    for _, _, consequences, _ in merged:
        by_transcript = {c["transcript_id_unversioned"]: c for c in consequences}
        for consequence in consequences:
            twin = by_transcript.get(consequence["mane_pair_transcript_id"])
            if not consequence["is_mane_select"] or twin is None:
                continue
            assert twin["source"] != consequence["source"], "a MANE pair must span the two catalogues"
            assert twin["mane_pair_transcript_id"] == consequence["transcript_id_unversioned"]
            directions.add(consequence["source"])
    assert directions == {SOURCE_ENSEMBL, SOURCE_REFSEQ}, f"pair resolved only from {directions or 'neither side'}"


def test_mane_plus_clinical_blocks_carry_no_pair(blocks):
    # VEP fills `MANE_SELECT` only on the Select transcript, so Plus Clinical rows have no 1:1
    # pointer to borrow scores through. Pinned so the gap stays a known property.
    plus = [c for c, _ in blocks if c["is_mane_plus"]]
    assert plus
    assert not [c for c in plus if c["mane_pair_transcript_id"]]


def test_the_picked_block_is_one_of_the_consequences_verbatim(merged):
    # QA check 5: the headline is a block of fields describing one transcript, not one field.
    # Identity rather than equality, so no field can be spliced in from another block.
    picked_sources = set()
    for _, picked, consequences, _ in merged:
        if not consequences:
            continue
        assert any(c is picked for c in consequences)
        picked_sources.add(picked["source"])
    # §5: the pick runs once over the combined list, so RefSeq sometimes wins and the headline
    # transcript is an `NM_`. Extraction must carry that through rather than force Ensembl.
    assert SOURCE_REFSEQ in picked_sources


def test_a_record_without_csq_yields_no_consequences(merged):
    # Extraction must return empty rather than raise, leaving the variant with no pick.
    for chrom, pos in NO_CSQ_VARIANTS:
        picked, consequences = at(merged, chrom, pos)
        assert consequences == []
        assert picked is None
