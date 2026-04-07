from __future__ import annotations

import argparse
from pathlib import Path

from pubman_manager import USER_DATA_DIR, load_user_config
from pubman_manager.main import (
    generate_author_overview,
    generate_doi_overview,
    generate_talks_template,
    upload_publication_pdfs,
    refresh_pubman_cache,
    delete_publications_by_dois,
    load_dois_from_yaml,
)
from pubman_manager import PubmanCreator


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="pubman")
    subparsers = parser.add_subparsers(dest="command", required=True)

    author_parser = subparsers.add_parser("author-overview", help="Generate overview for tracked authors")
    user_group = author_parser.add_mutually_exclusive_group(required=True)
    user_group.add_argument("--user-yaml", type=Path, help="Path to user yaml config")
    user_group.add_argument("--user-id", type=str, help="User id (e.g. 3523285)")
    author_parser.add_argument("--pubyear-start", type=int, default=2019)
    author_parser.add_argument("--output", type=Path, default=None)
    author_parser.add_argument("--no-update-user-yaml", action="store_true")
    author_parser.add_argument("--force", action="store_true")
    author_parser.add_argument("--author", action="append", dest="authors", default=[])

    doi_parser = subparsers.add_parser("doi-overview", help="Generate overview for explicit DOIs")
    doi_parser.add_argument("--doi", action="append", dest="dois", required=True)
    doi_parser.add_argument("--output", type=Path, default=None)
    doi_parser.add_argument("--force", action="store_true")

    upload_parser = subparsers.add_parser("upload-pdfs", help="Upload publication PDFs into .files")
    upload_parser.add_argument("--file", action="append", dest="files", type=Path, default=[])
    upload_parser.add_argument("--dir", dest="dirs", action="append", type=Path, default=[])

    cache_parser = subparsers.add_parser("refresh-cache", help="Refresh Pubman cache data for orgs in user yaml")
    cache_group = cache_parser.add_mutually_exclusive_group(required=True)
    cache_group.add_argument("--user-yaml", type=Path, help="Path to user yaml config")
    cache_group.add_argument("--user-id", type=str, help="User id (e.g. 3523285)")

    delete_parser = subparsers.add_parser("delete-dois", help="Delete publications by DOI")
    delete_group = delete_parser.add_mutually_exclusive_group(required=True)
    delete_group.add_argument("--doi", action="append", dest="dois")
    delete_group.add_argument("--doi-yaml", type=Path, dest="doi_yaml", help="YAML list of DOIs or {dois: [...]}")
    delete_parser.add_argument("--dry-run", action="store_true")

    upload_excel_parser = subparsers.add_parser("upload-excel", help="Upload an Excel sheet to PuRe")
    upload_excel_parser.add_argument("--file", type=Path, required=True, help="Path to .xlsx file")
    upload_excel_parser.add_argument("--overwrite", action="store_true", help="Overwrite existing entries")
    upload_excel_parser.add_argument("--submit", action="store_true", help="Submit items after upload")

    generate_template_parser = subparsers.add_parser(
        "generate-talks-template",
        help="Generate a talks template .xlsx file for a user/org",
    )
    template_user_group = generate_template_parser.add_mutually_exclusive_group(required=True)
    template_user_group.add_argument("--user-yaml", type=Path, help="Path to user yaml config")
    template_user_group.add_argument("--user-id", type=str, help="User id (e.g. 3523285)")
    generate_template_parser.add_argument(
        "--org-id",
        action="append",
        dest="org_ids",
        help="Department org id (e.g. ou_1863336)",
    )
    generate_template_parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Path for the generated talks template .xlsx file",
    )

    upload_talks_parser = subparsers.add_parser("upload-talks", help="Upload a talks Excel sheet to PuRe")
    upload_talks_parser.add_argument("--file", type=Path, required=True, help="Path to talks .xlsx file")
    upload_talks_parser.add_argument("--overwrite", action="store_true", help="Overwrite existing entries")
    upload_talks_parser.add_argument("--submit", action="store_true", help="Submit items after upload")

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command == "author-overview":
        user_yaml_path = args.user_yaml
        if args.user_id:
            user_yaml_path = USER_DATA_DIR / f"user_{args.user_id}" / "metadata.yaml"
        generate_author_overview(
            user_yaml_path=user_yaml_path,
            pubyear_start=args.pubyear_start,
            output_path=args.output,
            update_user_yaml=not args.no_update_user_yaml,
            force=args.force,
            override_authors=args.authors or None,
        )
        return 0

    if args.command == "doi-overview":
        generate_doi_overview(
            dois=args.dois,
            output_path=args.output,
            force=args.force,
        )
        return 0
    if args.command == "upload-pdfs":
        files = list(args.files or [])
        for directory in args.dirs or []:
            if directory.exists():
                files.extend(directory.glob("*.pdf"))
        upload_publication_pdfs(files)
        return 0
    if args.command == "refresh-cache":
        user_yaml_path = args.user_yaml
        if args.user_id:
            user_yaml_path = USER_DATA_DIR / f"user_{args.user_id}" / "metadata.yaml"
        refresh_pubman_cache(user_yaml_path)
        return 0
    if args.command == "delete-dois":
        if args.doi_yaml:
            dois = load_dois_from_yaml(args.doi_yaml)
        else:
            dois = args.dois or []
        summary = delete_publications_by_dois(dois, dry_run=args.dry_run)
        print(summary)
        return 0
    if args.command == "upload-excel":
        if not args.file.exists():
            parser.error(f"Excel file not found: {args.file}")
        if args.file.stat().st_size == 0:
            parser.error(f"Excel file is empty: {args.file}")
        pubman_api = PubmanCreator()
        pubman_api.create_publications(
            args.file,
            overwrite=args.overwrite,
            submit_items=args.submit,
        )
        return 0
    if args.command == "generate-talks-template":
        user_yaml_path = args.user_yaml
        if args.user_id:
            user_yaml_path = USER_DATA_DIR / f"user_{args.user_id}" / "metadata.yaml"
        if not user_yaml_path.exists():
            parser.error(f"User yaml not found: {user_yaml_path}")
        org_ids = args.org_ids or []
        if not org_ids:
            user_data = load_user_config(user_yaml_path)
            if isinstance(user_data, dict):
                org_ids = user_data.get("department_org_ids", [])
        if not org_ids:
            parser.error(
                "No department org ids configured. Provide --org-id or set department_org_ids in user yaml."
            )
        user_id = args.user_id or user_yaml_path.parent.name.replace("user_", "", 1)
        output_path = generate_talks_template(
            user_id,
            org_ids,
            output_path=args.output,
        )
        print(output_path)
        return 0
    if args.command == "upload-talks":
        if not args.file.exists():
            parser.error(f"Excel file not found: {args.file}")
        if args.file.stat().st_size == 0:
            parser.error(f"Excel file is empty: {args.file}")
        pubman_api = PubmanCreator()
        pubman_api.create_talks(
            args.file,
            overwrite=args.overwrite,
            submit_items=args.submit,
        )
        return 0

    parser.error("Unknown command")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
