from pubman_manager import DOIParser, PubmanCreator, PUBLICATIONS_DIR, USER_DATA_DIR, PROJECT_ROOT
import datetime
import yaml

import logging
logger = logging.getLogger('pubman_manager.api_manager_scopus')
logger.setLevel(logging.INFO)

pubman_api = PubmanCreator()
doi_parser = DOIParser(pubman_api)

with open(USER_DATA_DIR / 'user_3523285.yaml', 'r') as f:
    user_data = yaml.safe_load(f)

tracked_authors = user_data['tracked_authors']
processed_dois_by_author = user_data.get('processed_dois_by_author', {})

final_overview = []
for author_name in tracked_authors:
    print('Processing author', author_name)
    pubyear_start = 2019

    if isinstance(author_name, list) or isinstance(author_name, tuple):
        first_name, last_name = author_name
        author_name = f'{first_name} {last_name}'
    else:
        first_name, last_name = author_name.split(' ')[0], ' '.join(author_name.split(' ')[1:])
    dois_crossref = doi_parser.crossref_manager.get_dois_for_author(first_name, last_name, pubyear_start)
    dois_scopus = doi_parser.scopus_manager.get_dois_for_author(first_name, last_name, pubyear_start)

    new_dois = set(dois_crossref + dois_scopus)
    print("n new_dois",len(new_dois))

    processed_dois = set()
    dois_data = doi_parser.collect_data_for_dois(dois_crossref, dois_scopus, processed_dois=processed_dois)
    print("dois_data",dois_data)

    processed_dois_by_author.setdefault(author_name, set()).update(new_dois)
    if dois_data is not None:
        print(f"found data for {author_name}")
        table_overview = doi_parser.generate_table_from_dois_data(dois_data)
        final_overview.extend(table_overview)
    else:
        print(f"No data for {author_name}")
    user_data['processed_dois_by_author'] = processed_dois_by_author

with open(USER_DATA_DIR / 'user_3523285.yaml', 'w') as f:
    yaml.safe_dump(user_data, f)

pub_path = PUBLICATIONS_DIR / 'new' / f'full_overview_{datetime.datetime.now().strftime("%d.%m.%Y_%H_%M_%S")}.xlsx'
doi_parser.write_dois_data(pub_path, final_overview)
