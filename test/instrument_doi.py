from pubman_manager import DOIParser, PubmanCreator, PUBLICATIONS_DIR, USER_DATA_DIR, PROJECT_ROOT
import datetime
import yaml

import logging
logger = logging.getLogger('pubman_manager.api_manager_scopus')
logger.setLevel(logging.INFO)

pubman_api = PubmanCreator()
doi_parser = DOIParser(pubman_api)

dois = [
    '10.1016/j.proci.2024.105538'
    ]

dois_data = doi_parser.collect_data_for_dois(dois, dois, force=True)
table_overview = doi_parser.generate_table_from_dois_data(dois_data)

pub_path = PUBLICATIONS_DIR / 'new' / f'doi_overview_{datetime.datetime.now().strftime("%d.%m.%Y")}.xlsx'
doi_parser.write_dois_data(pub_path, table_overview)
