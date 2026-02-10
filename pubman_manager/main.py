from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable, Optional, Tuple

import re
import yaml

from .pubman_extractor import PubmanExtractor
from .doi_parser import DOIParser
from .pubman_creator import PubmanCreator
from . import PUBLICATIONS_DIR, FILES_DIR, PUBMAN_CACHE_DIR
from .util import save_yaml, normalize_user_id


@dataclass(frozen=True)
class AuthorName:
    display: str
    first: str
    last: str


def load_user_config(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh)
    if isinstance(data, list):
        return {"tracked_authors": data}
    return data or {}


def save_user_config(path: Path, data: dict) -> None:
    with path.open("w", encoding="utf-8") as fh:
        yaml.safe_dump(data, fh, sort_keys=False)


def normalize_author(author_entry) -> AuthorName:
    if isinstance(author_entry, (list, tuple)):
        first_name, last_name = author_entry
        display = f"{first_name} {last_name}"
        return AuthorName(display=display, first=first_name, last=last_name)
    parts = str(author_entry).split(" ")
    first_name = parts[0]
    last_name = " ".join(parts[1:]).strip()
    return AuthorName(display=str(author_entry), first=first_name, last=last_name)


def _default_output_path(prefix: str) -> Path:
    stamp = datetime.now().strftime("%d.%m.%Y_%H_%M_%S")
    return PUBLICATIONS_DIR / "new" / f"{prefix}_{stamp}.xlsx"

def _cache_path_for_user(user_yaml_path: Path) -> Path:
    cache_dir = user_yaml_path.parent.parent / ".cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir / user_yaml_path.name

def _load_doi_cache(cache_path: Path) -> dict:
    if not cache_path.exists():
        return {}
    with cache_path.open("r", encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}

def _save_doi_cache(cache_path: Path, data: dict) -> None:
    with cache_path.open("w", encoding="utf-8") as fh:
        yaml.safe_dump(data, fh, sort_keys=False)

def generate_author_overview(
    user_yaml_path: Path,
    pubyear_start: int = 2019,
    output_path: Optional[Path] = None,
    update_user_yaml: bool = True,
    force: bool = False,
) -> Path:
    user_data = load_user_config(user_yaml_path)
    tracked_authors = user_data.get("tracked_authors", []) if isinstance(user_data, dict) else []
    cache_path = _cache_path_for_user(user_yaml_path)
    cache_data = _load_doi_cache(cache_path)
    cached_dois = set()
    if not force:
        for entry in cache_data.values():
            if isinstance(entry, list):
                cached_dois.update(entry)

    pubman_api = PubmanCreator()
    doi_parser = DOIParser(pubman_api)

    final_overview: list = []
    collected_dois: set[str] = set()
    for author_entry in tracked_authors:
        author = normalize_author(author_entry)
        processed_for_author = set(cached_dois)
        dois_crossref, dois_scopus = doi_parser.get_dois_for_author(
            f"{author.first} {author.last}",
            pubyear_start=pubyear_start,
            processed_dois=processed_for_author,
            split=True,
        )
        new_dois = set(dois_crossref + dois_scopus)
        collected_dois.update(new_dois)
        dois_data = doi_parser.collect_data_for_dois(
            dois_crossref,
            dois_scopus,
        )
        if dois_data is not None:
            table_overview = doi_parser.process_dois(dois_data)
            final_overview.extend(table_overview)

    if collected_dois:
        cache_key = datetime.now().strftime("%Y-%m-%d")
        existing = cache_data.get(cache_key, [])
        merged = list(dict.fromkeys((existing or []) + sorted(collected_dois)))
        cache_data[cache_key] = merged
        _save_doi_cache(cache_path, cache_data)
    if update_user_yaml:
        if "processed_dois_by_author" in user_data:
            user_data.pop("processed_dois_by_author", None)
        save_user_config(user_yaml_path, user_data)

    output_path = output_path or _default_output_path("full_overview")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    doi_parser.write_dois_data(output_path, final_overview)
    return output_path


def generate_doi_overview(
    dois: Iterable[str],
    output_path: Optional[Path] = None,
    force: bool = True,
) -> Path:
    pubman_api = PubmanCreator()
    doi_parser = DOIParser(pubman_api)
    dois_list = list(dois)
    dois_data = doi_parser.collect_data_for_dois(dois_list, [])
    table_overview: list = []
    if dois_data is not None:
        table_overview = doi_parser.process_dois(dois_data, force=force)
    output_path = output_path or _default_output_path("doi_overview")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    doi_parser.write_dois_data(output_path, table_overview)
    return output_path


def refresh_pubman_cache_for_user(user_id: str, org_ids: Iterable[str]) -> Path:
    org_ids = list(dict.fromkeys(org_ids))
    if not org_ids:
        raise ValueError("department_org_ids missing in user yaml.")

    pubman_api = PubmanExtractor()
    raw_departments = pubman_api.fetch_all_organizations()
    mpg_department_ids_by_name = {
        str(k): (str(v) if v is not None else "")
        for k, v in (raw_departments or {}).items()
        if k is not None
    }

    publications = []
    for org_id in org_ids:
        publications.extend(pubman_api.search_publications_by_organization(org_id, size=200000))

    cache_dir = PUBMAN_CACHE_DIR / f"user_{normalize_user_id(user_id)}"
    cache_dir.mkdir(parents=True, exist_ok=True)
    save_yaml(mpg_department_ids_by_name, cache_dir / "mpg_departments.yaml")
    save_yaml(publications, cache_dir / "publications.yaml")
    save_yaml(pubman_api.extract_authors_info(publications), cache_dir / "authors_info.yaml")
    save_yaml(pubman_api.extract_organization_mapping(publications), cache_dir / "identifier_paths.yaml")
    save_yaml(pubman_api.extract_journals(publications), cache_dir / "journals.yaml")
    return cache_dir


def refresh_pubman_cache(user_yaml_path: Path) -> Path:
    user_data = load_user_config(user_yaml_path)
    if not isinstance(user_data, dict):
        raise ValueError("User yaml must be a dict with department_org_ids.")
    org_ids = user_data.get("department_org_ids", [])
    user_id = normalize_user_id(user_yaml_path.stem.replace("user_", "", 1))
    return refresh_pubman_cache_for_user(user_id, org_ids)


def upload_publication_pdfs(
    file_paths: Iterable[Path],
    max_size_mb: int = 50,
) -> list[Path]:
    max_size_bytes = max_size_mb * 1024 * 1024
    copied: list[Path] = []
    errors: list[str] = []

    for file_path in file_paths:
        path = Path(file_path)
        if not path.exists() or not path.is_file():
            errors.append(f"Missing file: {path}")
            continue
        if path.suffix.lower() != ".pdf":
            errors.append(f"Not a PDF: {path.name}")
            continue
        size = path.stat().st_size
        if size > max_size_bytes:
            errors.append(f"File too large ({size} bytes): {path.name}")
            continue
        doi_stub = path.stem
        if "/" in doi_stub or not doi_stub.startswith("10."):
            errors.append(f"Invalid DOI filename: {path.name}")
            continue
        if not re.match(r"^10\.[0-9]{4,9}[A-Za-z0-9.()_-]+$", doi_stub):
            errors.append(f"Invalid DOI filename: {path.name}")
            continue
        dest = FILES_DIR / path.name
        dest.parent.mkdir(parents=True, exist_ok=True)
        if not dest.exists():
            dest.write_bytes(path.read_bytes())
        copied.append(dest)

    if errors:
        raise ValueError("Upload failed:\n" + "\n".join(errors))
    return copied
