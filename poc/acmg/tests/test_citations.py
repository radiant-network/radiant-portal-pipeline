from acmg import citations

TEXT = (
    "#AlleleID\tVariationID\trs\tnsv\tcitation_source\tcitation_id\torganization_ids\n"
    "1\t10\t\t\tPubMed\t300\t1\n"
    "1\t10\t\t\tPubMed\t20\t1,3\n"
    "1\t10\t\t\tPubMed\t300\t5\n"
    "1\t10\t\t\tPubMedCentral\t999\t1\n"
    "2\t11\t\t\tNCBIBookShelf\t5\t1\n"
)


def test_variations_keep_distinct_pubmed_ids_only():
    df = citations.variations(TEXT)
    assert df.to_dicts() == [{"variation_id": "10", "pmids": [20, 300]}]
    assert df.schema == citations.SCHEMA
