import pandas as pd
import yaml

from pubman_manager import main as pubman_main


def test_generate_author_overview_filters_existing_pure(tmp_path, monkeypatch):
    user_yaml = tmp_path / "user_123.yaml"
    user_yaml.write_text(
        yaml.safe_dump({"tracked_authors": ["Jane Doe"]}),
        encoding="utf-8",
    )

    class DummyCreator:
        pass

    class FakeParser:
        def __init__(self, pubman_api):
            self.pubman_api = pubman_api

        def get_dois_for_author(self, author, pubyear_start=None, processed_dois=None, split=False):
            dois = ["10.1111/aaa", "10.2222/bbb"]
            if split:
                return (dois, [])
            return dois

        def collect_data_for_dois(self, dois_crossref, dois_scopus):
            return pd.DataFrame(
                {
                    "DOI": ["10.1111/aaa", "10.2222/bbb"],
                    "Title": ["Existing in PuRe", "New DOI"],
                    "Field": ["", ""],
                    "crossref": ["x", "y"],
                }
            )

        def has_pubman_entry(self, doi, title=None):
            return doi == "10.1111/aaa"

        def process_dois(self, dois_data):
            return []

        def write_dois_data(self, path_out, dois_data):
            path_out.write_text("", encoding="utf-8")

    monkeypatch.setattr(pubman_main, "PubmanCreator", DummyCreator)
    monkeypatch.setattr(pubman_main, "DOIParser", FakeParser)

    pubman_main.generate_author_overview(user_yaml, update_user_yaml=False)

    cache_path = user_yaml.parent / "publication_collection_history.yaml"
    cache_data = yaml.safe_load(cache_path.read_text(encoding="utf-8"))
    all_cached = set()
    for entry in cache_data.values():
        all_cached.update(entry or [])

    assert "10.2222/bbb" in all_cached
    assert "10.1111/aaa" not in all_cached


def test_generate_author_overview_handles_legacy_cache(tmp_path, monkeypatch):
    user_yaml = tmp_path / "user_123.yaml"
    user_yaml.write_text(
        yaml.safe_dump({"tracked_authors": ["Jane Doe"]}),
        encoding="utf-8",
    )
    legacy_cache = user_yaml.parent / "publication_collection_history.yaml"
    legacy_cache.write_text(
        yaml.safe_dump(["10.1111/legacy", "10.2222/legacy"]),
        encoding="utf-8",
    )

    class DummyCreator:
        pass

    class FakeParser:
        def __init__(self, pubman_api):
            self.pubman_api = pubman_api

        def get_dois_for_author(self, author, pubyear_start=None, processed_dois=None, split=False):
            return ([], []) if split else []

        def collect_data_for_dois(self, dois_crossref, dois_scopus):
            return None

        def process_dois(self, dois_data):
            return []

        def write_dois_data(self, path_out, dois_data):
            path_out.write_text("", encoding="utf-8")

    monkeypatch.setattr(pubman_main, "PubmanCreator", DummyCreator)
    monkeypatch.setattr(pubman_main, "DOIParser", FakeParser)

    pubman_main.generate_author_overview(user_yaml, update_user_yaml=False)

    cache_data = pubman_main._load_doi_cache(legacy_cache)
    assert isinstance(cache_data, dict)
    assert "legacy" in cache_data
