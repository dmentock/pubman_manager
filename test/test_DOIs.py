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

def test_existing_pure_doi_is_ignored(run_doi_test, monkeypatch):
    from pubman_manager.pubman_base import PubmanBase
    from pubman_manager.api_manager_crossref import CrossrefManager

    doi = "10.1088/1367-2630/ad309e"

    original_get_metadata = CrossrefManager.get_metadata

    def _patched_metadata(self, doi_value):
        metadata = original_get_metadata(self, doi_value)
        if doi_value == doi and metadata:
            metadata = dict(metadata)
            resource = dict(metadata.get("resource") or {})
            # Force a non-iopscience link so the PuRe check runs first.
            resource["primary"] = {"URL": "https://example.org/article"}
            metadata["resource"] = resource
        return metadata

    monkeypatch.setattr(CrossrefManager, "get_metadata", _patched_metadata)

    def _fake_search(self, match_criteria, size=100000):
        identifiers = match_criteria.get("metadata.identifiers", {})
        if identifiers.get("id") == doi and identifiers.get("type") == "DOI":
            return [{"data": {"metadata": {"identifiers": [identifiers]}}}]
        return []

    monkeypatch.setattr(PubmanBase, "search_publication_by_criteria", _fake_search)
    result = run_doi_test(doi)
    assert result.table_overview == []
    assert not result.capture_pubman_creations

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

def test_mpi_affiliation_requirement(run_doi_test):
    """Sometimes a group leader participates in a publication without using their MPI affiliation."""
    result = run_doi_test("10.1007/s38313-024-1989-y", write_excel=True)
    print("table", result.table_overview )
