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
        self.log = logging.getLogger(__name__)
        self.log.setLevel(logging_level)
        self.scopus_api_key = scopus_api_key if scopus_api_key else ENV_SCOPUS_API_KEY
        self.pubman_api = pubman_api
        with open(PUBMAN_CACHE_DIR / 'authors_info.yaml', 'r', encoding='utf-8') as f:
            self.affiliations_by_name_pubman = yaml.load(f, Loader=yaml.FullLoader)
        mpi_affiliation_counter = Counter()
        for author, author_info in self.affiliations_by_name_pubman.items():
            for affiliation in author_info['affiliations']:
                if 'Max-Planck' in affiliation:
                    print("affiliation",affiliation)
                    mpi_affiliation_counter[affiliation]+=1
        self.mpi_affiliations = [item[0] for item in sorted(mpi_affiliation_counter.items(), key=lambda x: x[1], reverse=True)]
        self.crossref_metadata_map = {}
        self.scopus_metadata_map = {}

    def get_crossref_metadata(self, doi):
        if doi in self.crossref_metadata_map:
            return self.crossref_metadata_map[doi]
        cr = Crossref()
        try:
            result = cr.works(ids=doi)
            self.crossref_metadata_map[doi] = result['message']
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
        self.log.debug(f'scopus_metadata {response.json()}')
        return response.json()

    def extract_crossref_authors_affiliations(self, crossref_metadata):
        affiliations_by_name = OrderedDict()
        for author in crossref_metadata.get('author', []):
            author_name = self.process_name(self.affiliations_by_name_pubman.keys(), unidecode(author.get('given', ''))), unidecode(unidecode(author.get('family', '')))
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
            preferred_name = author_data.get('author-retrieval-response', [{}])[0].get('author-profile', {}).get('preferred-name', {})
            print("preferred_name.get('given-name', '')",preferred_name.get('given-name', ''))
            print("preferred_name.get('surname', '')",preferred_name.get('surname', ''))
            if '.' in (first_name:=preferred_name.get('given-name', '').split()[0]):
                name_variants = author_data.get('author-retrieval-response', [{}])[0].get('author-profile', {}).get('name-variant', [])
                if isinstance(name_variants, list):
                    for variant in name_variants:
                        if len(variant_name:=variant.get('given-name', '')) > len(first_name):
                            first_name = variant_name
                            break
            return first_name, preferred_name.get('surname', '')
        else:
            self.log.error(f"Unable to retrieve author data for author {author_id} (status code: {response.status_code}, {response.text}")
            return None

    def extract_scopus_authors_affiliations(self, scopus_metadata) -> OrderedDict[str, List[str]]:
        """
        Generates an ordered mapping from authors to their affiliations with the Scopus API
        """
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
                    full_affiliation = ', '.join(filter(None, parts))
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
            first_name = preferred_name.get('ce:given-name', '')
            surname = preferred_name.get('ce:surname', '')
            if '.' in first_name:
                author_id = author.get('author-url', '/').split('/')[-1]
                if author_id:
                    first_name, surname = self.get_scopus_author_full_name(author_id)
            full_name = self.process_name(self.affiliations_by_name_pubman.keys(), first_name, surname)
            # print("full_name",full_name)
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

    def process_name(self, pubman_names, first_name, surname):
        def normalize_name_for_comparison(name):
            name = unicodedata.normalize('NFD', name).encode('ascii', 'ignore').decode('utf-8').strip()
            return ''.join(name.lower().replace('.', '').replace('-', '').split()).strip()
        def get_initials(name):
            name_parts = name.replace('.', '').replace('-', ' ').split()
            initials = ''.join([part[0] for part in name_parts if part])
            return initials.lower().strip()

        def get_name_parts(name):
            return name.replace('.', '').replace('-', ' ').split()

        def is_abbreviated(name):
            return '.' in name

        # Normalize incoming surname for comparison
        surname_normalized = normalize_name_for_comparison(surname)
        first_name_normalized = normalize_name_for_comparison(first_name)
        first_name_initials = get_initials(first_name)
        first_name_parts = get_name_parts(first_name)

        best_match = None
        best_matching_type = float('inf')
        best_first_name_parts_count = float('inf')
        best_has_middle_name = True  # We prefer names without middle names
        best_has_abbreviation = True  # We prefer names without abbreviations

        for pubman_firstname, pubman_surname in pubman_names:
            pubman_surname_normalized = normalize_name_for_comparison(pubman_surname)

            if surname_normalized != pubman_surname_normalized:
                continue  # Surname does not match

            pubman_firstname_normalized = normalize_name_for_comparison(pubman_firstname)
            pubman_firstname_initials = get_initials(pubman_firstname)
            pubman_firstname_parts = get_name_parts(pubman_firstname)

            matching_type = None

            if first_name_normalized == pubman_firstname_normalized:
                matching_type = 0  # Exact match
            elif first_name_initials == pubman_firstname_initials:
                matching_type = 1  # Initials match
            elif pubman_firstname_normalized.startswith(first_name_normalized):
                matching_type = 2  # Incoming name is a prefix of the database name
            elif pubman_firstname_initials.startswith(first_name_initials):
                matching_type = 3  # Partial initials match
            elif first_name_parts[0] == pubman_firstname_parts[0]:
                matching_type = 4  # First name parts match
            else:
                continue  # No match

            # Decide if this match is better than the best so far
            pubman_has_middle_name = len(pubman_firstname_parts) > 1
            pubman_has_abbreviation = is_abbreviated(pubman_firstname)
            pubman_firstname_parts_count = len(pubman_firstname_parts)

            if matching_type < best_matching_type:
                best_match = (pubman_firstname, pubman_surname)
                best_matching_type = matching_type
                best_first_name_parts_count = pubman_firstname_parts_count
                best_has_middle_name = pubman_has_middle_name
                best_has_abbreviation = pubman_has_abbreviation
            elif matching_type == best_matching_type:
                # Prefer names without middle names if we find a better match
                if pubman_has_middle_name:
                    continue
                elif not pubman_has_middle_name and best_has_middle_name:
                    best_match = (pubman_firstname, pubman_surname)
                    best_first_name_parts_count = pubman_firstname_parts_count
                    best_has_middle_name = pubman_has_middle_name
                    best_has_abbreviation = pubman_has_abbreviation
                else:
                    # Compare number of first name parts (fewer is better)
                    if pubman_firstname_parts_count < best_first_name_parts_count:
                        best_match = (pubman_firstname, pubman_surname)
                        best_first_name_parts_count = pubman_firstname_parts_count
                        best_has_abbreviation = pubman_has_abbreviation
                    elif pubman_firstname_parts_count == best_first_name_parts_count:
                        # Prefer entries without abbreviations
                        if not pubman_has_abbreviation and best_has_abbreviation:
                            best_match = (pubman_firstname, pubman_surname)
                            best_has_abbreviation = pubman_has_abbreviation

        # Search for a better match that excludes the middle name
        if best_match and best_has_middle_name:
            for pubman_firstname, pubman_surname in pubman_names:
                pubman_surname_normalized = normalize_name_for_comparison(pubman_surname)
                if surname_normalized == pubman_surname_normalized:
                    # Check if a version of the first name without the middle name exists
                    pubman_firstname_parts = get_name_parts(pubman_firstname)
                    if len(pubman_firstname_parts) == 1 and pubman_firstname_parts[0] == first_name_parts[0]:
                        return pubman_firstname, pubman_surname

        if best_match:
            return best_match
        else:
            # No match found, return incoming name with first name and surname, no middle name
            first_name_no_middle = first_name.split()[0]
            return first_name_no_middle, surname

    def process_author_list(self, affiliations_by_name: Dict[str, List[str]], title: str) -> Dict[str, List[Dict[str, Any]]]:
        """
        Go over list of authors and affiliations from Scopus, compare to PuRe entries,
        and possibly adopt PuRe if differences are small.
        """
        print("process_author_list", affiliations_by_name)

        from collections import Counter
        from fuzzywuzzy import process  # Assuming you have this import elsewhere

        def is_mpi_affiliation(affiliation: str) -> bool:
            """Check if the affiliation belongs to the Max-Planck Institute."""
            return any(keyword in affiliation for keyword in ['Max-Planck', 'Max Planck', 'Max Plank'])

        def process_scopus_affiliation(affiliation: str) -> str:
            """Format Scopus affiliation string."""
            return affiliation.replace('  ', ', ').replace(') ', '), ')

        def handle_mpi_affiliation(author, proposed_affiliation, pubman_author_affiliations):
            """Handle MPI affiliation processing."""
            pubman_mpi_groups = [aff for aff in pubman_author_affiliations if is_mpi_affiliation(aff)]
            print(f"Pubman MPI groups for {author}: {pubman_mpi_groups}")

            if len(pubman_mpi_groups) == 1:
                mpi_groups[pubman_mpi_groups[0]] += 1
                return {
                    'affiliation': pubman_mpi_groups[0],
                    'color': 'yellow',
                    'compare_error': 0,
                    'comment': 'Found MPI match in database.'
                }
            elif len(pubman_mpi_groups) > 1:
                ambiguous_mpi_affiliations[author] = pubman_mpi_groups
                return {
                    'affiliation': 'AMBIGUOUS MPI',
                }
            return {
                'affiliation': 'MISSING MPI',
            }

        def handle_non_mpi_affiliation(proposed_affiliation, pubman_author_affiliations):
            """Handle non-MPI affiliation using fuzzy matching."""
            affiliation, score = process.extractOne(proposed_affiliation, pubman_author_affiliations)
            compare_error = (100 - score) / 100
            if score > 80:
                return {
                    'affiliation': affiliation,
                    'color': 'green',
                    'compare_error': compare_error,
                    'comment': 'High confidence match from database.'
                }
            return {
                'affiliation': process_scopus_affiliation(proposed_affiliation),
                'color': 'gray',
                'compare_error': 0,
                'comment': f'Author or similar affiliation not found in database -> using affiliation from publisher (err={compare_error}).'
            }
        mpi_groups = Counter()
        processed_affiliations = {}
        ambiguous_mpi_affiliations = {}

        print("self.mpi_affiliations", self.mpi_affiliations)

        # Process each author and their affiliations
        for author, affiliations in affiliations_by_name.items():
            processed_affiliations[author] = []

            for proposed_affiliation in affiliations if affiliations else []:
                print(f"Processing author: {author}")
                pubman_author_affiliations = self.affiliations_by_name_pubman.get(author, {}).get('affiliations', [])
                if pubman_author_affiliations:
                    print(f"Proposed affiliation for {author}: {proposed_affiliation}, MPI check:", is_mpi_affiliation(proposed_affiliation))

                    if is_mpi_affiliation(proposed_affiliation):
                        affiliation_info = handle_mpi_affiliation(author, proposed_affiliation, pubman_author_affiliations)
                    else:
                        affiliation_info = handle_non_mpi_affiliation(proposed_affiliation, pubman_author_affiliations)
                elif proposed_affiliation.strip():
                    print(f"No PuRe affiliation for {author}, but provided by Scopus: {proposed_affiliation}")
                    if is_mpi_affiliation(proposed_affiliation):
                        affiliation_info = {
                            'affiliation': 'MISSING MPI',
                        }
                    else:
                        affiliation_info = {
                            'affiliation': process_scopus_affiliation(proposed_affiliation),
                            'color': 'gray',
                            'compare_error': 0,
                            'comment': 'No PuRe affiliation for author, adopting Scopus affiliation.'
                        }
                else:
                    raise RuntimeError(f'Affiliation for {author} not found in PuRe or Scopus')

                processed_affiliations[author].append(affiliation_info)

                if affiliation_info['color'] != 'gray' and is_mpi_affiliation(affiliation_info['affiliation']):
                    mpi_groups[affiliation_info['affiliation']] += 1

        print("MPI groups:", mpi_groups)
        most_common_mpi_group = mpi_groups.most_common(1)[0][0] if mpi_groups else self.mpi_affiliations[0]
        print("Most common MPI group:", most_common_mpi_group)

        # Resolve ambiguous MPI affiliations and assign missing MPI groups
        for author, affiliations in processed_affiliations.items():
            for i, affiliation_info in enumerate(affiliations):
                affiliation = affiliation_info['affiliation']
                print(f"Author: {author}, Current affiliation: {affiliation}")

                if affiliation == 'AMBIGUOUS MPI':
                    print(f"Resolving ambiguous MPI affiliation for {author}. Groups: {ambiguous_mpi_affiliations[author]}")
                    similar_affiliation, score = process.extractOne(most_common_mpi_group, ambiguous_mpi_affiliations[author])
                    compare_error = (100 - score) / 100
                    processed_affiliations[author][i] = {
                        'affiliation': similar_affiliation,
                        'color': 'yellow' if score > 95 else 'red',
                        'compare_error': compare_error,
                        'comment': 'Resolved ambiguous MPI affiliation using most common group.'
                    }
                elif affiliation == 'MISSING MPI':
                    processed_affiliations[author][i] = {
                        'affiliation': most_common_mpi_group,
                        'color': 'yellow',
                        'compare_error': 0,
                        'comment': 'Assigned most common MPI group due to missing MPI affiliation.'
                    }

        return processed_affiliations

    def download_pdf(self, pdf_link, doi):
        """
        Download PDF for given DOI with Scopus API
        """

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
        """
        Use Scopus API to generate a list of DOIs from an author name with the Affiliation ID from the .env file
        """

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
        """
        Helper function to parallelize `filter_dois` method
        """
        field = []
        title = ""
        publication_date = ""

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
                print("author_affiliation_map",author_affiliation_map)
                is_mp_publication = False
                for _, affiliations in author_affiliation_map.items():
                    # print(_)
                    for affiliation in affiliations:
                        # print(affiliation)
                        # Scopus sometimes has a Max Plank typo
                        if 'Max-Planck' in affiliation.replace(' ', '') or 'Max Planck' in affiliation or 'Max Plank' in affiliation:
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

    def filter_dois(self, dois: List[str]) -> pd. DataFrame:
        """
        Takes list of DOIs, checks if it already exists on PuRe as well as the availability on Crossref and Scopus, returns overview dataframe
        """
        pd.set_option('display.max_rows', None)
        pd.set_option('display.max_columns', None)
        results = []
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(self.fetch_metadata, doi) for doi in dois]
            for future in as_completed(futures):
                result = future.result()
                results.append(result)
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

    def collect_data_for_dois(self, df_dois_overview: pd.DataFrame) -> List[OrderedDict[str, Tuple[str, int, str]]]:
        """
        Takes overview dataframe, collects all data for DOIs which are not yet on PuRe and have Scopus and Crossref entries.
        Generates dataframe which can be passed to the excel_generator.create_sheet method to prefill the sheet with data.

        Result
        ------

        Each entry in the result list is a dict that corresponds to a publication.
        The dict maps column data to a tuple, e.g. `"Title": [title, 35, '']`
        Where "title" is the value for this column, "35" is the width of the column on the excel, and the last entry is an optional Tooltip to be displayed
        """
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
            # if scopus_metadata:
            affiliations_by_name = self.extract_scopus_authors_affiliations(scopus_metadata)
            if int(scopus_metadata['abstracts-retrieval-response']['coredata']['openaccess'])==1:
                license_type = 'open'
                pdf_found = self.download_pdf(crossref_metadata.get('link', [{}])[0].get('URL'), doi)
            # else:
            #     affiliations_by_name = self.extract_crossref_authors_affiliations(crossref_metadata)
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
            for (first_name, last_name), affiliations_info in cleaned_author_list.items():
                for affiliation_info in affiliations_info:
                    prefill_publication[f"Author {i}"] = [first_name + ' ' + last_name, None, '']
                    prefill_publication[f"Affiliation {i}"] = [affiliation_info['affiliation'], affiliation[1], '', affiliation[2]]
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
            create_sheet(path_out, {author: author_info.get('affiliations', []) for author, author_info in self.affiliations_by_name_pubman.items()},
                        column_details, n_authors,
                        prefill_publications = dois_data)
            self.log.info(f"Saved {path_out} successfully.")
