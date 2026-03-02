import json
from pathlib import Path

from pubman_manager import PROJECT_ROOT
from pubman_manager import pubman_creator
from pubman_manager.pubman_creator import PubmanCreator


def test_pubman_payload_from_excel_prints_json(tmp_path, monkeypatch):
    excel_path = PROJECT_ROOT / "test" / "resources" / "test.xlsx"
    rows = PubmanCreator.extract_prefilled_rows(excel_path, header_name="Title")
    assert rows, "Expected at least one publication row in test.xlsx"

    monkeypatch.setattr(pubman_creator, "FILES_DIR", tmp_path)
    for row in rows:
        doi = str(row.get("DOI") or "").strip()
        license_url = row.get("License url")
        if doi and license_url:
            pdf_path = tmp_path / f"{doi.replace('/', '')}.pdf"
            pdf_path.write_bytes(b"%PDF-1.4\n1 0 obj\n<< /Type /Catalog >>\nendobj\n%%EOF\n")

    creator = PubmanCreator.__new__(PubmanCreator)
    creator.identifier_paths = {}
    creator.authors_info = {}
    creator.journals = {}
    creator.ctx_id = "ctx_test"
    creator.user_id = "user_test"
    creator.auth_token = "token"
    creator.base_url = "https://pure.mpg.de/rest"

    monkeypatch.setattr(PubmanCreator, "get_journal_by_issn", lambda _self, _issn: {})
    monkeypatch.setattr(PubmanCreator, "upload_pdf", lambda _self, _path: 123)

    captured = {}

    def _capture(self, request_list, create_items=True, submit_items=False, overwrite=False):
        captured["request_list"] = request_list
        return {"created": 0, "skipped_existing": 0, "blocked_existing": 0, "total": len(request_list)}

    monkeypatch.setattr(PubmanCreator, "create_items", _capture)

    creator.create_publications(excel_path, submit_items=False, overwrite=False)
    request_list = captured.get("request_list") or []
    assert request_list, "Expected request payloads to be generated"

    payloads = [request_json for _criteria, request_json in request_list]
    print(json.dumps(payloads, indent=2, ensure_ascii=False))
