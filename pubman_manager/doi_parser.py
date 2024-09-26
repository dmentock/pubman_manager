from habanero import Crossref
from unidecode import unidecode
from bs4 import BeautifulSoup
from pathlib import Path
import pandas as pd
from collections import OrderedDict, Counter
from dateutil import parser
import yaml
from fuzzywuzzy import process
import requests
import unicodedata
from typing import List, Dict, Tuple
import os
import html
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from pubman_manager import create_sheet, FILES_DIR, PUBMAN_CACHE_DIR, ENV_SCOPUS_API_KEY, SCOPUS_AFFILIATION_ID

class DOIParser:
    def __init__(self, pubman_api, scopus_api_key = None, logging_level = logging.INFO):
        self.log = logging.getLogger()
        logging.basicConfig(level=logging_level)
        self.scopus_api_key = scopus_api_key if scopus_api_key else ENV_SCOPUS_API_KEY
        self.pubman_api = pubman_api
        with open(PUBMAN_CACHE_DIR / 'authors_info.yaml', 'r', encoding='utf-8') as f:
            authors_info = yaml.safe_load(f)
        self.affiliations_by_name_pubman = OrderedDict({key: val['affiliations'] for key, val in authors_info.items() if val})
        self.crossref_metadata_map = {}
        self.scopus_metadata_map = {}

    def get_crossref_metadata(self, doi):
        if doi in self.crossref_metadata_map:
            return self.crossref_metadata_map[doi]
        cr = Crossref()
        try:
            result = cr.works(ids=doi)
            self.crossref_metadata_map[doi] = result['message']
            print(f'crossref_metadata {result["message"]}')
            self.log.debug(f'crossref_metadata {result["message"]}')
            return result['message']
        except Exception as e:
            self.log.error(f"Failed to retrieve data for DOI {doi}: {e}")

    def get_scopus_metadata(self, doi):
        if doi in self.scopus_metadata_map:
            return self.scopus_metadata_map[doi]
        url = "https://api.elsevier.com/content/abstract/doi/"
        headers = {
            'Accept': 'application/json',
            'X-ELS-APIKey': self.scopus_api_key,
        }
        response = requests.get(url + doi, headers=headers)
        response.raise_for_status()
        self.scopus_metadata_map[doi] = response.json()
        print(f'scopus_metadata {response.json()}')
        self.log.debug(f'scopus_metadata {response.json()}')
        return response.json()


    def extract_crossref_authors_affiliations(self, crossref_metadata):
        affiliations_by_name = OrderedDict()
        for author in crossref_metadata.get('author', []):
            author_name = self.process_name(self.affiliations_by_name_pubman, unidecode(author.get('given', '')) + ' ' + unidecode(author.get('family', '')))
            affiliations_by_name[author_name] = []
            for affiliation in author.get('affiliation', []):
                affiliations_by_name[author_name].append(unidecode(affiliation.get('name', '')))
        return affiliations_by_name

    def get_scopus_author_full_name(self, author_id):
        scopus_author_api_url = f"https://api.elsevier.com/content/author/author_id/{author_id}"
        headers = {
            'Accept': 'application/json',
            'X-ELS-APIKey': self.scopus_api_key
        }
        response = requests.get(scopus_author_api_url, headers=headers)
        if response.status_code == 200:
            author_data = response.json()
            author_name = author_data.get('author-retrieval-response', [{}])[0].get('author-profile', {}).get('preferred-name', {})
            full_name = f"{author_name.get('given-name', '')} {author_name.get('surname', '')}"
            return full_name
        else:
            print(f"Error: Unable to retrieve author data (status code: {response.status_code})")
            return None

    def extract_scopus_authors_affiliations(self, scopus_metadata):
        author_affiliation_map = OrderedDict()
        authors_list = scopus_metadata['abstracts-retrieval-response']['authors']['author']
        author_groups = scopus_metadata['abstracts-retrieval-response']['item']['bibrecord']['head']['author-group']
        if isinstance(author_groups, dict):
            author_groups = [author_groups]
        author_id_to_affiliations = {}
        for group in author_groups:
            affiliation_info = group.get('affiliation', {})
            affiliation_list = []
            source_text = affiliation_info.get('ce:source-text')
            if source_text:
                if len(source_text) > 2 and source_text[0].islower() and source_text[1].isupper():
                    source_text = source_text[1:]
                affiliation_list.append(source_text)
            else:
                organization_entries = affiliation_info.get('organization', [])
                if isinstance(organization_entries, dict):
                    organization_names = [organization_entries['$']]
                elif isinstance(organization_entries, list):
                    organization_names = [org['$'] for org in organization_entries]
                else:
                    raise RuntimeError("Invalid organization_entries", type(organization_entries), organization_entries)
                department_name = affiliation_info.get('affilname', '')
                city = affiliation_info.get('city', '')
                postal_code = affiliation_info.get('postal-code', '')
                country = affiliation_info.get('country', '')
                if organization_names:
                    parts = organization_names + [city, postal_code, country]
                    full_affiliation = ', '.join(filter(None, parts))  # Remove empty parts
                    affiliation_list.append(full_affiliation)
                else:
                    full_affiliation = ', '.join(filter(None, [department_name, city, postal_code, country]))
                    affiliation_list.append(full_affiliation)
            for author in group.get('author', []):
                author_id = author.get('@auid', '')
                if author_id not in author_id_to_affiliations:
                    author_id_to_affiliations[author_id] = []
                author_id_to_affiliations[author_id].extend(affiliation_list)
        for author in authors_list:
            preferred_name = author.get('preferred-name', {})
            given_name = preferred_name.get('ce:given-name', '')
            surname = preferred_name.get('ce:surname', '')
            scopus_name = f"{given_name} {surname}".strip()
            if '.' in scopus_name:
                author_id = author.get('author-url', '/').split('/')[-1]
                if author_id:
                    scopus_name = self.get_scopus_author_full_name(author_id)
            full_name = self.process_name(self.affiliations_by_name_pubman, scopus_name)
            author_id = author.get('@auid', '')
            affiliations = author_id_to_affiliations.get(author_id, ['No affiliation available'])
            unique_affiliations = list(OrderedDict.fromkeys(affiliations))
            author_affiliation_map[full_name] = unique_affiliations
        return author_affiliation_map

    def parse_date(self, date_value):
        if isinstance(date_value, str):
            parsed_date = parser.parse(date_value)
            return parsed_date.strftime("%d.%m.%Y")
        elif isinstance(date_value, list) and all(isinstance(i, int) for i in date_value):
            if len(date_value) == 3:
                year, month, day = date_value
                parsed_date = parser.parse(f"{day:02d}.{month:02d}.{year}")
                return parsed_date.strftime("%d.%m.%Y")
            elif len(date_value) == 2:
                year, month = date_value
                parsed_date = parser.parse(f"{month:02d}.{year}")
                return parsed_date.strftime("%m.%Y")
            elif len(date_value) == 1:
                return date_value[0]
        raise RuntimeError

    def process_name(self, names_affiliations, name):
        def normalize_name(n):
            """Normalize the name to ignore special characters and case."""
            return unicodedata.normalize('NFKD', n).encode('ASCII', 'ignore').decode('utf-8').lower()

        def get_initials(name_parts):
            """Get the initials from name parts."""
            return ''.join(part[0] for part in name_parts if part)

        normalized_name = normalize_name(name)
        abbrev_parts = normalized_name.replace('.', '').split()
        surname = abbrev_parts[-1]
        abbrev_first_names = abbrev_parts[:-1]
        abbrev_initials = get_initials(abbrev_first_names)

        candidates = []
        exact_matches = []

        for full_name in names_affiliations:
            normalized_full_name = normalize_name(full_name)
            full_name_parts = normalized_full_name.split()
            candidate_surname = full_name_parts[-1]
            candidate_first_names = full_name_parts[:-1]
            candidate_initials = get_initials(candidate_first_names)

            if surname.replace('-', '').replace(' ', '') == candidate_surname.replace('-', '').replace(' ', ''):
                if abbrev_first_names == candidate_first_names:
                    exact_matches.append(full_name)
                elif abbrev_initials == candidate_initials[:len(abbrev_initials)]:
                    candidates.append((full_name, len(candidate_first_names)))

        if exact_matches:
            return exact_matches[0]
        elif candidates:
            return max(candidates, key=lambda x: (len(abbrev_initials), x[1]))[0]

        # Attempt to remove middle name and try again
        if len(abbrev_first_names) > 1:  # If there is a middle name, remove it and try again
            abbrev_without_middle = [abbrev_first_names[0]] + [surname]
            new_name = ' '.join(abbrev_without_middle)
            return self.process_name(names_affiliations, new_name)
        return name

    def process_author_list(self,
                            affiliations_by_name: Dict[str, List[str]],
                            title: str) -> Dict[str, List[Tuple[str, str]]]:
        non_mpg_affiliations = Counter()
        pubman_affiliations = set()
        processed_affiliations = {}
        for author, affiliations in affiliations_by_name.items():
            processed_affiliations[author] = []
            for i, proposed_affiliation in enumerate(affiliations if affiliations else ['']):
                compare_error = ''
                if self.affiliations_by_name_pubman.get(author):
                    if not proposed_affiliation.strip():
                        affiliation, score = process.extractOne(title, self.affiliations_by_name_pubman[author])
                        color = 'orange'
                    elif 'Max-Planck' in proposed_affiliation and \
                          (pubman_mpi_pubs:=[pub for pub in self.affiliations_by_name_pubman[author] if 'Max-Planck' in pub]):
                        affiliation, score = process.extractOne(title, pubman_mpi_pubs)
                        color = 'purple'
                    else:
                        affiliation, score = process.extractOne(proposed_affiliation, self.affiliations_by_name_pubman[author])
                        if score > 80:
                            color = 'yellow'
                        else:
                            affiliation = proposed_affiliation.replace('  ', ', ').replace(') ', '), ')
                            color = 'gray'
                    compare_error = (100-score)/100
                elif proposed_affiliation.strip():
                    affiliation = proposed_affiliation.replace('  ', ', ').replace(') ', '), ')
                    color = 'gray' if 'Max-Planck' not in affiliation else 'red'
                else:
                    continue
                processed_affiliations[author].append([affiliation, color, compare_error])
                if 'Max-Planck' not in affiliation:
                    non_mpg_affiliations[affiliation] += 1
                if color != 'gray':
                    pubman_affiliations.add(affiliation)
        if non_mpg_affiliations:
            most_common_affiliation = non_mpg_affiliations.most_common(1)[0][0]
        else:
            most_common_affiliation = ''
        for author, affiliations in processed_affiliations.items():
            if not affiliations:
                processed_affiliations[author] = [[most_common_affiliation, 'red', '']]
            for i, affiliation in enumerate(affiliations):
                if affiliation[1] == 'gray':
                    similar_affiliation, score = process.extractOne(affiliation[0], pubman_affiliations)
                    if score > 90 and similar_affiliation not in affiliations:
                        processed_affiliations[author][i][0] = similar_affiliation
                        processed_affiliations[author][i][1] = 'pink'
                        processed_affiliations[author][i][2] = (100-score)/100
        return processed_affiliations

    def download_pdf(self, pdf_link, doi):
        self.log.debug(f"Attempting to download PDF for DOI: {doi}")
        self.log.debug(f"PDF link: {pdf_link}")
        if pdf_link is None:
            self.log.error(f"No valid PDF link found for DOI: {doi}")
        else:
            response = requests.get(pdf_link, stream=True)
            if response.status_code == 200:
                with open(FILES_DIR / f'{doi.replace("/", "_")}.pdf', 'wb') as f:
                    for chunk in response.iter_content(1024):
                        f.write(chunk)
                return True
            else:
                self.log.error(f"Failed to download PDF. Status code: {response.status_code}, {response.text}")
        return False

    def get_dois_for_author(self,
                            author_name,
                            pubyear_start=None,
                            pubyear_end=None,
                            extra_queries: List[str] = None):
        BASE_URL = "https://api.elsevier.com/content/search/scopus"

        query_components = [f'AF-ID({SCOPUS_AFFILIATION_ID})']
        query_components.append(f'AUTHFIRST("{author_name.split()[0][0]}")')
        query_components.append(f'AUTHOR-NAME("{author_name.split()[-1]}")')
        if pubyear_start:
            query_components.append(f'PUBYEAR > {pubyear_start - 1}')
        if pubyear_end:
            query_components.append(f'PUBYEAR < {pubyear_end + 1}')
        if extra_queries:
            for extra_query in extra_queries:
                query_components.append(extra_query)
        query = ' AND '.join(query_components)
        if not query:
            raise ValueError("At least one search parameter must be provided.")
        headers = {
            "X-ELS-APIKey": ENV_SCOPUS_API_KEY,
            "Accept": "application/json"
        }
        params = {
            "query": query,
            "field": "doi",
            "count": 200,
            "start": 0
        }
        def get_dois():
            dois = []
            start = 0
            total_results = 1
            while start < total_results:
                params['start'] = start
                response = requests.get(BASE_URL, headers=headers, params=params)
                if response.status_code == 200:
                    data = response.json()
                    total_results = int(data['search-results']['opensearch:totalResults'])
                    entries = data['search-results'].get('entry', [])
                    for entry in entries:
                        doi = entry.get('prism:doi')
                        if doi:
                            dois.append(doi)
                    start += len(entries)
                else:
                    raise RuntimeError(f"Scopus query API error {response.status_code}: {response.text}")
            return dois
        dois = get_dois()
        return self.filter_dois(dois)

    def fetch_metadata(self, doi):
        field = []
        title = ""
        publication_date = ""

        # Search publication in PuRe
        pub = self.pubman_api.search_publication_by_criteria({
            "metadata.identifiers.id": doi,
            "metadata.identifiers.type": 'DOI'
        })

        title = 'Unknown Title'
        publication_date = 'Unknonw Date'
        if pub:
            field.append("Already exists in PuRe")
            title = pub[0].get('data', {}).get('metadata', {}).get('title', 'Title not specified in PuRe')
            publication_date = pub[0].get('data', {}).get('metadata', {}).get('datePublishedInPrint', 'Date not specified in PuRe')
        else:
            crossref_metadata = self.get_crossref_metadata(doi)
            scopus_metadata = self.get_scopus_metadata(doi)

            if not scopus_metadata:
                field.append('Publication not found on Scopus')
            else:
                author_affiliation_map = self.extract_scopus_authors_affiliations(scopus_metadata)
                is_mp_publication = False
                for _, affiliations in author_affiliation_map.items():
                    for affiliation in affiliations:
                        if 'Max-Planck' in affiliation or 'Max Planck' in affiliation:
                            is_mp_publication = True
                if not is_mp_publication:
                    field.append(f'Authors {list(author_affiliation_map.keys())} have no Max-Planck affiliation')

            if crossref_metadata:
                title = crossref_metadata.get('title', [None])[0]
                publication_date = crossref_metadata.get('published-online', {}).get('date-parts', [None])[0]

        return {
            'Title': title,
            'Publication Date': publication_date,
            'DOI': doi,
            'Field': "\n".join(field)
        }

    def filter_dois(self, dois):
        pd.set_option('display.max_rows', None)
        pd.set_option('display.max_columns', None)
        results = []
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(self.fetch_metadata, doi) for doi in dois]
            for future in as_completed(futures):
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    self.log.error(f"Error fetching data for DOI: {e}")
        df = pd.DataFrame(results)
        return df
        # def highlight_rows(row):
        #     if "PuRe" in row['Field']:
        #         return ['background-color: #233333' for _ in row]  # Dark greenish-blue
        #     elif row['Field']:  # If there's something else in the Field column
        #         return ['background-color: #433333' for _ in row]  # Dark reddish-brown
        #     else:
        #         return ['background-color: #999900' for _ in row]  # Yellowish for empty Field
        # print(df.style.apply(highlight_rows, axis=1).render())

    def collect_data_for_dois(self, df_dois_overview: pd.DataFrame) -> pd.DataFrame:
        new_dois = df_dois_overview[(df_dois_overview['Field'].isnull()) | (df_dois_overview['Field'] == '')]['DOI'].values
        dois_data = []
        for doi in new_dois:
            self.log.debug("Processing Publication DOI {doi}")
            crossref_metadata = self.get_crossref_metadata(doi)
            if not crossref_metadata:
                return None
            self.log.debug(f"crossref_metadata {crossref_metadata}")
            try:
                scopus_metadata = self.get_scopus_metadata(doi)
            except requests.HTTPError:
                scopus_metadata = None
            self.log.debug(f"scopus_metadata {scopus_metadata}")

            def clean_html(raw_html):
                soup = BeautifulSoup(raw_html, "html.parser")
                return soup.get_text()
            title = html.unescape(unidecode(clean_html(crossref_metadata.get('title', [None])[0])))
            container_title = crossref_metadata.get('container-title', [None])

            journal_title = html.unescape(unidecode(container_title[0])) if container_title else None
            license_list = crossref_metadata.get('license')
            license_url = license_list[-1].get('URL', '') if license_list else None
            license_year = license_list[-1].get('start', {}).get('date-parts', [[None]])[0][0] if license_list else None
            page = crossref_metadata.get('page') if '-' in crossref_metadata.get('page', '') else ''
            article_number = crossref_metadata.get('article-number', '')

            license_type = 'closed'
            if scopus_metadata:
                affiliations_by_name = self.extract_scopus_authors_affiliations(scopus_metadata)
                if int(scopus_metadata['abstracts-retrieval-response']['coredata']['openaccess'])==1:
                    license_type = 'open'
                    pdf_found = self.download_pdf(crossref_metadata.get('link', [{}])[0].get('URL'), doi)
            else:
                affiliations_by_name = self.extract_crossref_authors_affiliations(crossref_metadata)
            cleaned_author_list = self.process_author_list(affiliations_by_name, title)

            # copyright = [assertion for assertion in crossref_metadata.get('assertion', []) if assertion.get('name') == 'copyright']
            # if copyright:
            #     copyright = copyright[0]

            prefill_publication = OrderedDict({
                "Title": [title, 35, ''],
                # "Type": [data.get('type'), 15, ''],
                "Journal Title": [journal_title, 25, ''],
                "Publisher": [html.unescape(unidecode(crossref_metadata.get('publisher', None)) or ''), 20, ''],
                "Issue": [crossref_metadata.get('issue', None), 10, ''],
                "Volume": [crossref_metadata.get('volume', None), 10, ''],
                "Page": [page, 10, ''],
                'Article Number': [article_number, 10, ''],
                "ISSN": [html.unescape(unidecode(crossref_metadata.get('ISSN', [None])[0] or '')), 15, ''],
                "Date created": [self.parse_date(crossref_metadata.get('created', {}).get('date-time', None)), 20, ''],
                # 'Date issued': [self.parse_date(.get('issued', {}).get('date-parts', [[None]])[0]), 20, ''],
                'Date published': [self.parse_date(crossref_metadata.get('published', {}).get('date-parts', [[None]])[0]), 20, ''],
                'DOI': [doi, 20, ''],
                # 'License type': [license_type, 15, ''],
                'License url': [license_url, 20, ''] if license_type=='open' else ['', 20, ''],
                'License year': [license_year, 15, ''] if license_type=='open' else ['', 15, ''],
                'Pdf found': ['' if license_type=='closed' else 'y' if pdf_found else 'n', 15, ''],
                'Link': [crossref_metadata.get('resource', {}).get('primary', {}).get('URL', ''), 20, ''],
                # 'Copyright': [copyright, 15, '']
            })
            i = 1
            for author, affiliations in cleaned_author_list.items():
                for affiliation in affiliations:
                    prefill_publication[f"Author {i}"] = [author, None, '']
                    prefill_publication[f"Affiliation {i}"] = [affiliation[0], affiliation[1], '', affiliation[2]]
                    i = i+1
            dois_data.append(prefill_publication)
        return dois_data

    def write_dois_data(self, path_out, dois_data):
        if not dois_data:
            empty_path = Path(os.path.abspath(path_out)).parent / f'{path_out.stem}_empty{path_out.suffix}'
            df = pd.DataFrame()
            df.to_excel(empty_path, index=False)
            self.log.info(f"Saved empty_path {empty_path} successfully.")
        else:
            n_authors = 45
            column_details = OrderedDict({
                key: [val[1], val[2]]
                for key, val in dois_data[0].items()
                if 'Author ' not in key and 'Affiliation ' not in key
            })
            create_sheet(path_out, self.affiliations_by_name_pubman,
                        column_details, n_authors,
                        prefill_publications = dois_data)
            self.log.info(f"Saved {path_out} successfully.")
