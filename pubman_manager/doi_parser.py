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

from pubman_manager import create_sheet, FILES_DIR, PUBMAN_CACHE_DIR, ENV_SCOPUS_API_KEY, SCOPUS_AFFILIATION_ID

class DOIParser:
    def __init__(self, pubman_api, scopus_api_key = None):
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
            return result['message']
        except Exception as e:
            print(f"Failed to retrieve data for DOI {doi}: {e}")

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
        return response.json()

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
        def normalize_name(name):
            """Normalize the name to ignore special characters and case."""
            return unicodedata.normalize('NFKD', name).encode('ASCII', 'ignore').decode('utf-8').lower()

        def strip_middle_names(full_name):
            """Remove middle names or abbreviations from the full name, keeping only first and last names."""
            name_parts = full_name.split(' ')
            if len(name_parts) > 2:
                return f"{name_parts[0]} {name_parts[-1]}"
            return full_name

        def strict_first_name_match(first_name, candidate_first_name):
            """Match first names more strictly, accounting for hyphenations and abbreviations."""
            return first_name == candidate_first_name or first_name.replace('-', '') == candidate_first_name.replace('-', '')

        def find_extended_match(surname, names_affiliations):
            """Look for a version of the name with an extension in the middle that doesn't contain a '.' character."""
            for full_name in names_affiliations:
                normalized_full_name = normalize_name(full_name)
                full_name_parts = normalized_full_name.split(' ')
                # Check if the surname matches and if there is a middle name without '.' (not an abbreviation)
                if surname == full_name_parts[-1] and len(full_name_parts) > 2 and '.' not in full_name_parts[1]:
                    return full_name
            return None

        normalized_name = normalize_name(name)
        abbrev_parts = normalized_name.split(' ')
        first_name = abbrev_parts[0]
        surname = abbrev_parts[-1]

        best_match = None
        best_score = -1

        # Iterate over the name affiliations to find the best match
        for full_name in names_affiliations:
            normalized_full_name = normalize_name(full_name)
            full_name_parts = normalized_full_name.split(' ')

            # If the surname matches and the first name (or abbreviation) matches exactly
            if surname.replace(' ', '').replace('-', '') == full_name_parts[-1].replace(' ', '').replace('-', ''):
                if strict_first_name_match(first_name, full_name_parts[0]):  # First name match or abbreviation
                    score = 1  # Initial match counts for something
                    if len(full_name_parts) == 2:  # Prefer names with exactly two parts
                        score += 1

                    if score > best_score:
                        best_match = full_name
                        best_score = score

        # If no match found, attempt to find an extended version of the name
        if not best_match:
            extended_match = find_extended_match(surname, names_affiliations)
            if extended_match:
                return extended_match

        # Normalize best match if found
        if best_match:
            return best_match

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
                print("overriding affiliation;", author, most_common_affiliation)
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
        print(f"Attempting to download PDF for DOI: {doi}")
        print(f"PDF link: {pdf_link}")
        if pdf_link is None:
            print(f"No valid PDF link found for DOI: {doi}")
        else:
            response = requests.get(pdf_link, stream=True)
            if response.status_code == 200:
                with open(FILES_DIR / f'{doi.replace("/", "_")}.pdf', 'wb') as f:
                    for chunk in response.iter_content(1024):
                        f.write(chunk)
                return True
            else:
                print(f"Failed to download PDF. Status code: {response.status_code}, {response.text}")
        return False

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
            full_name = self.process_name(self.affiliations_by_name_pubman, f"{given_name} {surname}".strip())
            author_id = author.get('@auid', '')
            affiliations = author_id_to_affiliations.get(author_id, ['No affiliation available'])
            unique_affiliations = list(OrderedDict.fromkeys(affiliations))
            author_affiliation_map[full_name] = unique_affiliations
        return author_affiliation_map

    def extract_crossref_authors_affiliations(self, crossref_metadata):
        affiliations_by_name = OrderedDict()
        for author in crossref_metadata.get('author', []):
            author_name = self.process_name(self.affiliations_by_name_pubman, unidecode(author.get('given', '')) + ' ' + unidecode(author.get('family', '')))
            affiliations_by_name[author_name] = []
            for affiliation in author.get('affiliation', []):
                affiliations_by_name[author_name].append(unidecode(affiliation.get('name', '')))
        return affiliations_by_name

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
                    print(f"Error {response.status_code}: {response.text}")
                    break
            return dois
        dois = get_dois()
        return self.filter_dois(dois)

    def filter_dois(self, dois):
        pd.set_option('display.max_rows', None)
        pd.set_option('display.max_columns', None)
        titles = []
        publication_dates = []
        fields = []
        for doi in dois:
            field = []
            pub = self.pubman_api.search_publication_by_criteria({
                "metadata.identifiers.id": doi,
                "metadata.identifiers.type": 'DOI'
            })

            if pub:
                field.append("Already exists in PuRe")
                titles.append(pub[0].get('data', {}).get('metadata', {}).get('title', 'Title not specified in PuRe'))
                publication_dates.append(pub[0].get('data', {}).get('metadata', {}).get('datePublishedInPrint', 'Date not specified in PuRe'))
            else:
                crossref_metadata = self.get_crossref_metadata(doi)
                scopus_metadata = self.get_scopus_metadata(doi)

                if not crossref_metadata:
                    field.append('Publication not found on Crossref')
                    titles.append('Unknown Title')
                    publication_dates.append('Unknown Date')
                else:
                    titles.append(crossref_metadata.get('title', 'Unknown Title'))
                    publication_dates.append(crossref_metadata.get('published-online', 'Unknown Date'))
                    main_author = crossref_metadata.get('author', {'affiliation': ['Max-Planck']})[0]
                    has_mp_affiliation = False
                    for affiliation in main_author['affiliation']:
                        print("hah",unidecode(main_author.get('given', '')) + ' ' + unidecode(main_author.get('family', '')), affiliation)
                        if 'Max-Planck' in affiliation.get('name', '') or 'Max Planck' in affiliation.get('name', ''):
                            has_mp_affiliation = True
                    if not has_mp_affiliation:
                        main_author_name = unidecode(main_author.get('given', '')) + ' ' + unidecode(main_author.get('family', ''))
                        field.append(f'Author "{main_author_name}" has no Max-Planck affiliation')

            fields.append("\n".join(field))
        df = pd.DataFrame({
            'Title': titles,
            'Publication Date': publication_dates,
            'DOI': dois,
            'Field': fields
        })
        # def highlight_rows(row):
        #     if "PuRe" in row['Field']:
        #         return ['background-color: #233333' for _ in row]  # Dark greenish-blue
        #     elif row['Field']:  # If there's something else in the Field column
        #         return ['background-color: #433333' for _ in row]  # Dark reddish-brown
        #     else:
        #         return ['background-color: #999900' for _ in row]  # Yellowish for empty Field
        # print(df.style.apply(highlight_rows, axis=1).render())
        return df

    def collect_data_for_dois(self, df_dois_overview: pd.DataFrame) -> pd.DataFrame:
        new_dois = df_dois_overview[(df_dois_overview['Field'].isnull()) | (df_dois_overview['Field'] == '')]['DOI'].values
        dois_data = []
        for doi in new_dois:
            print("Processing Publication DOI", doi)
            crossref_metadata = self.get_crossref_metadata(doi)
            if not crossref_metadata:
                return None
            try:
                scopus_metadata = self.get_scopus_metadata(doi)
            except requests.HTTPError:
                scopus_metadata = None
            print("scopus_metadata", scopus_metadata)

            def clean_html(raw_html):
                soup = BeautifulSoup(raw_html, "html.parser")
                return soup.get_text()
            title = html.unescape(unidecode(clean_html(crossref_metadata.get('title', [None])[0])))
            container_title = crossref_metadata.get('container-title', [None])

            journal_title = html.unescape(unidecode(container_title[0])) if container_title else None
            license_list = crossref_metadata.get('license')
            print("license_list",license_list)
            license_url = license_list[-1].get('URL', '') if license_list else None
            license_year = license_list[-1].get('start', {}).get('date-parts', [[None]])[0][0] if license_list else None
            page = crossref_metadata.get('page') if '-' in crossref_metadata.get('page', '') else ''
            article_number = crossref_metadata.get('article-number', '')

            license_type = 'closed'
            if scopus_metadata:
                affiliations_by_name = self.extract_scopus_authors_affiliations(scopus_metadata)
                if int(scopus_metadata['abstracts-retrieval-response']['coredata']['openaccess'])==1:
                    license_type = 'open'
                    print("hah",crossref_metadata.get('link', [{}]))
                    pdf_found = self.download_pdf(crossref_metadata.get('link', [{}])[0].get('URL'), doi)
            else:
                affiliations_by_name = self.extract_crossref_authors_affiliations(crossref_metadata)
            cleaned_author_list = self.process_author_list(affiliations_by_name, title)

            copyright = [assertion for assertion in crossref_metadata.get('assertion', []) if assertion.get('name') == 'copyright']
            if copyright:
                copyright = copyright[0]

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
                # 'Date issued': [self.parse_date(crossref_metadata.get('issued', {}).get('date-parts', [[None]])[0]), 20, ''],
                'Date published': [self.parse_date(crossref_metadata.get('published', {}).get('date-parts', [[None]])[0]), 20, ''],
                'DOI': [doi, 20, ''],
                # 'License type': [license_type, 15, ''],
                'License url': [license_url, 20, ''] if license_type=='open' else ['', 20, ''],
                'License year': [license_year, 15, ''] if license_type=='open' else ['', 15, ''],
                'Pdf found': ['' if license_type=='closed' else 'y' if pdf_found else 'n', 15, ''],
                'Link': [crossref_metadata.get('resource', {}).get('primary', {}).get('URL', ''), 20, ''],
                'Copyright': [copyright, 15, '']
            })

            i = 1
            for author, affiliations in cleaned_author_list.items():
                for affiliation in affiliations:
                    prefill_publication[f"Author {i}"] = [author, None, '']
                    prefill_publication[f"Affiliation {i}"] = [affiliation[0], affiliation[1], '', affiliation[2]]
                    i = i+1
            dois_data.append(prefill_publication)
        return pd.DataFrame(dois_data)

    def write_dois_data(self, path_out, df_dois_data):
        if df_dois_data.empty:
            # Handle empty DataFrame case
            empty_path = Path(os.path.abspath(path_out)).parent / f'{path_out.stem}_empty{path_out.suffix}'
            df = pd.DataFrame()
            df.to_excel(empty_path, index=False)
            print(f"Saved empty_path {empty_path} successfully.")
        else:
            n_authors = 45

            # Create column details based on the DataFrame columns
            column_details = OrderedDict({
                col: [df_dois_data[col].iloc[0], df_dois_data[col].iloc[1]]
                for col in df_dois_data.columns
                if 'Author ' not in col and 'Affiliation ' not in col
            })
            dois_data = df_dois_data.to_dict('records')
            create_sheet(path_out, self.affiliations_by_name_pubman,
                        column_details, n_authors,
                        prefill_publications=dois_data)
            print(f"Saved {path_out} successfully.")
