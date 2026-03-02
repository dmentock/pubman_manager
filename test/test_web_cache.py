import yaml
from types import SimpleNamespace


def test_update_cache_uses_affiliation_counts(tmp_path, monkeypatch):
    from web import misc

    authors_info = {
        ("Ada", "Lovelace"): {
            "affiliation_counts": {
                "Analytical Engine Institute": 3,
            }
        }
    }
    cache_dir = tmp_path / "user_3523285" / "pubman_cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    with (cache_dir / "authors_info.yaml").open("w", encoding="utf-8") as fh:
        yaml.dump(authors_info, fh)

    monkeypatch.setattr(misc, "get_user_cache_dir", lambda _user_id: cache_dir)
    monkeypatch.setattr(misc, "TALKS_DIR", tmp_path)
    monkeypatch.setattr(misc, "extractor", SimpleNamespace(extract_org_data=lambda _org_id: None))
    monkeypatch.setattr(misc, "refresh_pubman_cache_for_user", lambda *_args, **_kwargs: None)

    captured = {}

    def fake_create_sheet(file_path, affiliations_by_name_pubman, column_details, n_authors, header_name, **kwargs):
        captured["file_path"] = file_path
        captured["affiliations_by_name_pubman"] = affiliations_by_name_pubman
        captured["header_name"] = header_name

    monkeypatch.setattr(misc, "create_sheet", fake_create_sheet)

    misc.update_cache("user_3523285", ["ou_test"])

    assert captured["header_name"] == "Event Name"
    assert captured["affiliations_by_name_pubman"][("Ada", "Lovelace")] == {
        "Analytical Engine Institute": 3
    }
