import datetime
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

import pandas as pd
import pytest

from pubman_manager import DOIParser, PubmanCreator

logging.basicConfig(level=logging.DEBUG)
for _logger in (
    "pubman_manager.api_manager_crossref",
    "pubman_manager.api_manager_scopus",
    "pubman_manager.doi_parser",
):
    logging.getLogger(_logger).setLevel(logging.DEBUG)


@dataclass
class DoiTestResult:
    description: str
    doi: str
    dois_data: pd.DataFrame
    table_overview: List[dict]
    excel_path: Optional[Path]
    excel_dataframe: Optional[pd.DataFrame]
    capture_pubman_creations: List[dict]
    http_create_payloads: List[dict]


@pytest.fixture
def run_doi_test(
    monkeypatch,
    external_http_cache,
    mock_pubman,
    capture_pubman_creations,
    tmp_path,
):
    """
    Factory fixture returning a helper that runs the DOI workflow and exposes intermediate artifacts.
    """

    def _runner(
        doi: str,
        *,
        description: Optional[str] = None,
        force: bool = False,
        write_excel: bool = False,
    ) -> DoiTestResult:
        monkeypatch.setattr(DOIParser, "download_pdf", lambda self, link, doi, retries=3: True)

        pubman_api = PubmanCreator()
        doi_parser = DOIParser(pubman_api)

        dois_data = doi_parser.collect_data_for_dois([doi], [doi])
        table_overview = doi_parser.generate_table_from_dois_data(dois_data, force=force)

        excel_path = None
        excel_df = None
        if write_excel and table_overview:
            date_str = datetime.datetime.now().strftime("%d.%m.%Y")
            out_dir = tmp_path / "new"
            out_dir.mkdir(parents=True, exist_ok=True)
            excel_path = out_dir / f"doi_overview_{date_str}.xlsx"
            doi_parser.write_dois_data(excel_path, table_overview)
            pubman_api.create_publications(excel_path, overwrite=True, submit_items=False)
            excel_df = pd.read_excel(excel_path)

        return DoiTestResult(
            description=description or doi,
            doi=doi,
            dois_data=dois_data,
            table_overview=table_overview,
            excel_path=excel_path,
            excel_dataframe=excel_df,
            capture_pubman_creations=list(capture_pubman_creations),
            http_create_payloads=list(external_http_cache.create_item_payloads),
        )

    return _runner

def test_cover_feature_doi_is_filtered(run_doi_test):
    result = run_doi_test("10.1002/batt.202400015")
    assert 'Cover feature (Crossref)' in result.dois_data.loc[0, 'Field']
