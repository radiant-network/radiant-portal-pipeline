from acmg.protein import mane_proteins, missense

MANE = [
    "#NCBI_GeneID\tEnsembl_Gene\tHGNC_ID\tsymbol\tname\tRefSeq_nuc\tRefSeq_prot\tEnsembl_nuc\tEnsembl_prot\tMANE_status\n",
    "GeneID:5053\tENSG1\tHGNC:8582\tPAH\tx\tNM_000277.3\tNP_000268.1\tENST00000553106.6\tENSP1\tMANE Select\n",
    "GeneID:9\tENSG2\tHGNC:9\tB\tx\tNM_9.1\tNP_9.1\tENST9.1\tENSP2\tMANE Plus Clinical\n",
]


def hgvs(variation: str, protein: str) -> str:
    return f"PAH\t5053\t{variation}\t1\tcoding\tna\tNM_000277.3:c.1\tc.1\t{protein}\tx\tYes\tCurrent\tNo\n"


def test_mane_select_protein_to_unversioned_ensembl_transcript():
    assert mane_proteins(MANE) == {"NP_000268.1": "ENST00000553106"}


def test_missense_on_mane_select_only():
    lines = [
        hgvs("577", "NP_000268.1:p.Arg408Trp"),
        hgvs("577", "NP_000268.1:p.Arg408Trp"),  # the same row twice: one output row
        hgvs("1", "NP_000268.1:p.Arg408Ter"),  # nonsense
        hgvs("2", "NP_000268.1:p.Arg408="),  # synonymous
        hgvs("3", "NP_000268.2:p.Arg408Gln"),  # another protein version
        hgvs("4", "-"),
    ]
    rows = missense(lines, mane_proteins(MANE)).to_dicts()
    assert rows == [
        {"transcript_id": "ENST00000553106", "variation_id": "577", "position": 408, "ref_aa": "Arg", "alt_aa": "Trp"}
    ]
