from pubman_manager import DOIParser, PubmanCreator, PUBLICATIONS_DIR, USER_DATA_DIR, PROJECT_ROOT
import datetime
import yaml

import logging
logger = logging.getLogger('pubman_manager.api_manager_scopus')
logger.setLevel(logging.INFO)

pubman_api = PubmanCreator()
doi_parser = DOIParser(pubman_api)

# author_name = 'Baptiste Gault'
# pub_path = PUBLICATIONS_DIR / 'new' / f'{author_name}_{datetime.datetime.now().strftime("%d.%m.%Y")}.xlsx'
# df_dois_overview = doi_parser.get_doi_data_for_author(author_name, pubyear_start = 2019)
# df_dois_overview
# dois_data = doi_parser.collect_data_for_dois(df_dois_overview)
# doi_parser.write_dois_data(pub_path, dois_data)



with open(USER_DATA_DIR / 'user_3523285.yaml', 'r') as f:
    user_data = yaml.safe_load(f)

tracked_authors = user_data['tracked_authors']
processed_dois_by_author = user_data.get('processed_dois_by_author', {})
# authors
# authors = ['Dierk Raabe']
for author_name in tracked_authors:
    print(1)

    pub_path = PUBLICATIONS_DIR / 'new' / f'{author_name}_{datetime.datetime.now().strftime("%d.%m.%Y")}.xlsx'
    df_dois_overview = doi_parser.get_doi_data_for_author(author_name, pubyear_start = 2019, processed_dois=processed_dois)
    print(2)
    df_dois_overview
    processed_dois = processed_dois_by_author.get(author_name, [])
    dois_data = doi_parser.collect_data_for_dois(df_dois_overview, processed_dois)
    processed_dois_by_author[author_name] = processed_dois
    print(3)
    doi_parser.write_dois_data(pub_path, dois_data)
    print(4)

user_data['processed_dois_by_author'] = processed_dois_by_author
with open(USER_DATA_DIR / 'user_3523285.yaml', 'w') as f:
    yaml.safe_dump(user_data, f)