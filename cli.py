from __future__ import annotations

import argparse
from pathlib import Path

from pubman_manager.main import generate_author_overview, generate_doi_overview


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="pubman")
    subparsers = parser.add_subparsers(dest="command", required=True)

    author_parser = subparsers.add_parser("author-overview", help="Generate overview for tracked authors")
    author_parser.add_argument("--user-yaml", required=True, type=Path, help="Path to user yaml config")
    author_parser.add_argument("--pubyear-start", type=int, default=2019)
    author_parser.add_argument("--output", type=Path, default=None)
    author_parser.add_argument("--no-update-user-yaml", action="store_true")
    author_parser.add_argument("--force", action="store_true")

    doi_parser = subparsers.add_parser("doi-overview", help="Generate overview for explicit DOIs")
    doi_parser.add_argument("--doi", action="append", dest="dois", required=True)
    doi_parser.add_argument("--output", type=Path, default=None)
    doi_parser.add_argument("--force", action="store_true")

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command == "author-overview":
        generate_author_overview(
            user_yaml_path=args.user_yaml,
            pubyear_start=args.pubyear_start,
            output_path=args.output,
            update_user_yaml=not args.no_update_user_yaml,
            force=args.force,
        )
        return 0

    if args.command == "doi-overview":
        generate_doi_overview(
            dois=args.dois,
            output_path=args.output,
            force=args.force,
        )
        return 0

    parser.error("Unknown command")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
