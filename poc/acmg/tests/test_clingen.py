from acmg.clingen import genes

DOSAGE = (
    "#ClinGen\n#Gene Symbol\tGene ID\tcytoBand\tLocation\tHI Score\n"
    "A\t1\t1p\tchr1:1-2\t3\nB\t2\t1p\tchr1:3-4\tNot yet evaluated\n"
)
VALIDITY = """"CLINGEN GENE DISEASE VALIDITY CURATIONS","","","","","",""
"+++","+++","+++","+++","+++","+++","+++"
"GENE SYMBOL","GENE ID (HGNC)","DISEASE LABEL","DISEASE ID (MONDO)","MOI","SOP","CLASSIFICATION"
"+++","+++","+++","+++","+++","+++","+++"
"A","HGNC:1","d1","MONDO:1","AD","SOP10","Refuted"
"A","HGNC:1","d2","MONDO:2","AD","SOP10","Definitive"
"C","HGNC:3","d3","MONDO:3","AD","SOP10","Disputed"
"C","HGNC:3","d4","MONDO:4","AR","SOP10","No Known Disease Relationship"
"""


def test_genes_joins_dosage_and_validity():
    rows = {r["symbol"]: (r["hi_score"], r["no_disease"]) for r in genes(DOSAGE, VALIDITY).to_dicts()}
    assert rows == {"A": (3, False), "B": (None, False), "C": (None, True)}
