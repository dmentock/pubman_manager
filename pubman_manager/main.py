from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable, Optional, Tuple

import yaml

from .doi_parser import DOIParser
from .pubman_creator import PubmanCreator
from . import PUBLICATIONS_DIR


@dataclass(frozen=True)
class AuthorName:
    display: str
    first: str
    last: str


def load_user_config(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


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


def generate_author_overview(
    user_yaml_path: Path,
    pubyear_start: int = 2019,
    output_path: Optional[Path] = None,
    update_user_yaml: bool = True,
    force: bool = False,
) -> Path:
    user_data = load_user_config(user_yaml_path)
    tracked_authors = user_data.get("tracked_authors", [])
    processed_dois_by_author = user_data.get("processed_dois_by_author", {})

    pubman_api = PubmanCreator()
    doi_parser = DOIParser(pubman_api)

    final_overview: list = []
    for author_entry in tracked_authors:
        author = normalize_author(author_entry)
        processed_for_author = set()
        if not force:
            processed_for_author = set(processed_dois_by_author.get(author.display, []))
        dois_crossref, dois_scopus = doi_parser.get_dois_for_author(
            f"{author.first} {author.last}",
            pubyear_start=pubyear_start,
            processed_dois=processed_for_author,
            split=True,
        )
        new_dois = set(dois_crossref + dois_scopus)
        dois_data = doi_parser.collect_data_for_dois(
            dois_crossref,
            dois_scopus,
        )
        processed_dois_by_author[author.display] = sorted(processed_for_author.union(new_dois))
        if dois_data is not None:
            table_overview = doi_parser.process_dois(dois_data)
            final_overview.extend(table_overview)

    user_data["processed_dois_by_author"] = processed_dois_by_author
    if update_user_yaml:
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
