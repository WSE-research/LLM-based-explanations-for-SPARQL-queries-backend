from utils.rdf import extract_entities


def test_extract_entities():
    true = ["wd:Q567"]
    res = extract_entities("SELECT * WHERE { wd:Q567 ?p ?o }")
    assert true == res