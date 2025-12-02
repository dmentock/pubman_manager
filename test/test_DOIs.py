import datetime

import pytest

from pubman_manager import DOIParser, PubmanCreator, PUBLICATIONS_DIR
from pubman_manager.pubman_base import PubmanBase


@pytest.mark.parametrize("doi", ["10.1016/j.matdes.2025.114720"])
def test_collect_doi(
    monkeypatch,
    external_http_cache,
    mock_pubman,
    capture_pubman_creations,
    tmp_path,          # ⬅ add pytest's tmp_path fixture
    doi,
):
    # Avoid actual network / file download in tests
    monkeypatch.setattr(DOIParser, "download_pdf", lambda self, link, doi, retries=3: True)

    pubman_api = PubmanCreator()
    doi_parser = DOIParser(pubman_api)

    dois_data = doi_parser.collect_data_for_dois([doi], [doi])
    table_overview = doi_parser.generate_table_from_dois_data(dois_data, force=True)

    # Use a temporary directory instead of PUBLICATIONS_DIR / 'new'
    date_str = datetime.datetime.now().strftime("%d.%m.%Y")
    out_dir = tmp_path / "new"
    out_dir.mkdir(parents=True, exist_ok=True)

    pub_path = out_dir / f"doi_overview_{date_str}.xlsx"

    doi_parser.write_dois_data(pub_path, table_overview)
    pubman_api.create_publications(pub_path, overwrite=True, submit_items=False)

    # Assertions unchanged
    assert capture_pubman_creations, \
        "Expected create_publications to attempt at least one payload submission."
    first_call = capture_pubman_creations[0]
    assert first_call["create_items"] is True
    assert not first_call["submit_items"]
    assert first_call["requests"], \
        "Expected at least one publication request to be queued."
