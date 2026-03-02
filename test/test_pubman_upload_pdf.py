from pathlib import Path

import requests

from pubman_manager import PubmanCreator


def test_pubman_upload_pdf_posts_binary_payload(tmp_path, monkeypatch):
    pdf_name = "10.1016jnanoen.2026.111797.pdf"
    pdf_bytes = b"%PDF-1.4\n1 0 obj\n<< /Type /Catalog >>\nendobj\n%%EOF\n"
    pdf_path = tmp_path / pdf_name
    pdf_path.write_bytes(pdf_bytes)

    creator = PubmanCreator.__new__(PubmanCreator)
    creator.auth_token = "token"
    creator.base_url = "https://pure.mpg.de/rest"

    captured = {}

    def _fake_post(url, headers=None, data=None, files=None, **_kwargs):
        captured["url"] = url
        captured["headers"] = headers or {}
        captured["data"] = data
        captured["files"] = files
        resp = requests.Response()
        resp.status_code = 201
        resp._content = b"123"
        resp.headers["Content-Type"] = "application/json"
        return resp

    monkeypatch.setattr(requests, "post", _fake_post)

    file_id = creator.upload_pdf(pdf_path)

    assert file_id == 123
    assert captured["files"] is None
    assert captured["data"] == pdf_bytes
    assert captured["headers"].get("Content-Type") == "application/pdf"
