from pathlib import Path

import pytest


def test_upload_publication_pdfs_valid(monkeypatch, tmp_path):
    from pubman_manager import main

    target_dir = tmp_path / "files"
    monkeypatch.setattr(main, "FILES_DIR", target_dir)

    source = tmp_path / "10.1002adem.202201912.pdf"
    source.write_bytes(b"%PDF-1.4\n")

    copied = main.upload_publication_pdfs([source])

    assert copied == [target_dir / source.name]
    assert (target_dir / source.name).exists()


def test_upload_publication_pdfs_invalid(monkeypatch, tmp_path):
    from pubman_manager import main

    target_dir = tmp_path / "files"
    monkeypatch.setattr(main, "FILES_DIR", target_dir)

    bad_ext = tmp_path / "10.1002adem.202201912.txt"
    bad_ext.write_text("nope", encoding="utf-8")

    bad_name = tmp_path / "not_a_doi.pdf"
    bad_name.write_bytes(b"%PDF-1.4\n")

    too_big = tmp_path / "10.1002adem.202201913.pdf"
    too_big.write_bytes(b"x" * (50 * 1024 * 1024 + 1))

    with pytest.raises(ValueError):
        main.upload_publication_pdfs([bad_ext, bad_name, too_big])
