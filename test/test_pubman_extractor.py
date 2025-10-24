import tempfile
from pathlib import Path
from typing import List

import pandas as pd
from pubman_manager import PUBLICATIONS_DIR, PubmanExtractor

def test_journal_matching():
    pe = PubmanExtractor()
    journal = pe.get_journal_by_issn('1359-6454')
    print("journal",journal)

