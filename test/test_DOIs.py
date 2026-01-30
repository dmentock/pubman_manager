import logging

logging.basicConfig(level=logging.DEBUG)
for _logger in (
    "pubman_manager.api_manager_crossref",
    "pubman_manager.api_manager_scopus",
    "pubman_manager.doi_parser",
):
    logging.getLogger(_logger).setLevel(logging.DEBUG)


def test_cover_feature_doi_is_filtered(run_doi_test):
    result = run_doi_test("10.1002/batt.202400015")
    assert 'Cover feature (Crossref)' in result.dois_data.loc[0, 'Field']

def test_simple_publication(run_doi_test):
    result = run_doi_test("10.1038/s41586-024-07932-w", write_excel=True)
    assert not result.dois_data.empty
    assert result.capture_pubman_creations, "Expected publication payload(s) to be prepared"
    upload = result.capture_pubman_creations[0]["requests"]
    assert upload, "No create_items request captured"
    assert upload[0][0]["metadata.identifiers.id"] == "10.1038/s41586-024-07932-w"


def test_director_affiliation(run_doi_test):
    result = run_doi_test("10.1016/j.matdes.2025.115090", write_excel=True)
    table = (result.table_overview or [])
    if table:
        row = table[0]
        print("author_affiliations")
        i = 1
        while f"Author {i}" in row and f"Affiliation {i}" in row:
            author_cell = row.get(f"Author {i}")
            affiliation_cell = row.get(f"Affiliation {i}")
            author = getattr(author_cell, "data", author_cell)
            affiliation = getattr(affiliation_cell, "data", affiliation_cell)
            if author=='Gerhard Dehm':
                assert affiliation == 'Structure and Micro-/Nanomechanics of Materials, Max Planck Institute for Sustainable Materials, Max Planck Society'
                break
            i += 1
    # print("result", result)
    # assert not result.dois_data.empty
    # assert result.capture_pubman_creations, "Expected publication payload(s) to be prepared"
    # upload = result.capture_pubman_creations[0]["requests"]
    # assert upload, "No create_items request captured"
    # assert upload[0][0]["metadata.identifiers.id"] == "10.1038/s41586-024-07932-w"

