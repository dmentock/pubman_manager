import tempfile
from pathlib import Path
from typing import List

import pandas as pd
from pubman_manager import PUBLICATIONS_DIR, PubmanCreator

def test_create_publications(monkeypatch, test_resources_dir):
    def mock_create_items(self, request_list, create_items = True, submit_items=False, overwrite=False):
        print("request_list", request_list)

    monkeypatch.setattr(
        PubmanCreator,
        "create_items",
        mock_create_items,
        raising=True,
    )
    pc = PubmanCreator()

    pc.create_publications(test_resources_dir / 'test_pubman_creator_cone_journal_misspelled.xlsx')
