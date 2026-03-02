import tempfile
from pathlib import Path
from typing import List

import pandas as pd
from pubman_manager import PubmanCreator, DOIParser

def test_journal_matching():
    pc = PubmanCreator()
    dp = DOIParser(pc)

    doi = '10.1016/j.actamat.2025.121656'
    # crossref_res = dp.crossref_manager.get_overview(doi)
    # print("crossref_res",crossref_res)
    # df = pd.DataFrame.from_dict(crossref_res)
    # print("df", df)
    # table_overview = dp.process_dois(df, force=True)
    # print("table_overview",table_overview)
    # return
    res = dp.collect_data_for_dois([doi], [])
    print("res", res)
