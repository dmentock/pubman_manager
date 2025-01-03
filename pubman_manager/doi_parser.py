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
from typing import List, Dict, Tuple, Any
import os
import html
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import re
from urllib.parse import urlencode

from pubman_manager import create_sheet, Cell, FILES_DIR, PUBMAN_CACHE_DIR, ENV_SCOPUS_API_KEY, SCOPUS_AFFILIATION_ID

class DOIParser:
    def __init__(self, pubman_api, scopus_api_key = None, logging_level = logging.INFO):
        self.log = logging.getLogger(__name__)
        self.log.setLevel(logging_level)
        self.scopus_api_key = scopus_api_key if scopus_api_key else ENV_SCOPUS_API_KEY
        self.pubman_api = pubman_api
        with open(PUBMAN_CACHE_DIR / pubman_api.org_id / 'authors_info.yaml', 'r', encoding='utf-8') as f:
            self.affiliations_by_name_pubman = yaml.load(f, Loader=yaml.FullLoader)
        mpi_affiliation_counter = Counter()
        for author, author_info in self.affiliations_by_name_pubman.items():
            for affiliation in author_info['affiliations']:
                if 'Max-Planck' in affiliation:
                    mpi_affiliation_counter[affiliation]+=1
        self.mpi_affiliations = [item[0] for item in sorted(mpi_affiliation_counter.items(), key=lambda x: x[1], reverse=True)]
        self.crossref_metadata_map = {}
        self.scopus_metadata_map = {}
        self.af_id_ = None

    @property
    def af_id(self):
        if self.af_id_:
            return self.af_id_
        BASE_URL = "https://api.elsevier.com/content/search/affiliation"
        params = {
            "query": f"AFFIL({self.pubman_api.org_name})"
        }
        encoded_params = urlencode(params)
        headers = {
            "X-ELS-APIKey": ENV_SCOPUS_API_KEY,
            "Accept": "application/json"
        }
        response = requests.get(f"{BASE_URL}?{encoded_params}", headers=headers)
        if response.status_code == 200:
            data = response.json()
            if "search-results" in data and "entry" in data["search-results"]:
                for affiliation in data["search-results"]["entry"]:
                    dc_identifier = affiliation.get('dc:identifier', 'Unknown')
                    af_id = dc_identifier.split(":")[-1] if dc_identifier.startswith("AFFILIATION_ID:") else None
                    self.af_id_ = af_id
                    return af_id
            else:
                raise RuntimeError(f"Scopus API request to get AF-ID from '{self.pubman_api.org_name}' unsuccessful, unexpected datastructure: {data}")
        else:
            raise RuntimeError(f"Scopus API request to get AF-ID from '{self.pubman_api.org_name}' unsuccessful: {response.status_code} - {response.text}")

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
        try:
            response = requests.get(url + doi, headers=headers)
            response.raise_for_status()
            self.scopus_metadata_map[doi] = response.json()
            self.log.debug(f'scopus_metadata {response.json()}')
            return response.json()
        except requests.HTTPError:
            return None

    def extract_crossref_authors_affiliations(self, crossref_metadata):
        affiliations_by_name = OrderedDict()
        for author in crossref_metadata.get('author', []):
            author_name = self.process_name(self.affiliations_by_name_pubman.keys(), author.get('given', ''), author.get('family', ''))
            affiliations_by_name[author_name] = []
            print("author", author_name, author)
            for affiliation in author.get('affiliation', []):
                print("affiliation", affiliation)
                affiliations_by_name[author_name].append(unidecode(affiliation.get('name', '')))
        return affiliations_by_name

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
            author_id = author.get('@auid', '')
            affiliations = author_id_to_affiliations.get(author_id, ['No affiliation available'])
            unique_affiliations = list(OrderedDict.fromkeys(affiliations))
            author_affiliation_map[full_name] = unique_affiliations
        return author_affiliation_map

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
            if '.' in (first_name:=preferred_name.get('given-name', '').split()[0]):
                name_variants = author_data.get('author-retrieval-response', [{}])[0].get('author-profile', {}).get('name-variant', [])
                if isinstance(name_variants, list):
                    for variant in name_variants:
                        if len(variant_name:=variant.get('given-name', '')) > len(first_name):
                            first_name = variant_name
                            break
            return first_name, preferred_name.get('surname', '')
        else:
            raise RuntimeError(f"Unable to retrieve author data for author {author_id} (status code: {response.status_code}, {response.text}")

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

    def process_name(self, pubman_names, first_name, surname) -> Tuple[str]:
        """Match Scopus author names with PuRe author names, preserving formatting from the database."""

        def normalize_name_for_comparison(name):
            # Remove accents and convert to ASCII
            name = unicodedata.normalize('NFD', name).encode('ascii', 'ignore').decode('utf-8')
            # Replace hyphens with spaces
            name = name.replace('-', ' ')
            # Handle camelCase names by inserting spaces before uppercase letters
            name = re.sub('([a-z])([A-Z])', r'\1 \2', name)
            # Remove dots
            name = name.replace('.', '')
            # Convert to lowercase
            name = name.lower()
            # Remove spaces to get a continuous string for comparison
            name_normalized = ''.join(name.split())
            return name_normalized

        # Normalize incoming surname and first name for comparison
        surname_normalized = normalize_name_for_comparison(surname)
        first_name_normalized = normalize_name_for_comparison(first_name)

        for pubman_firstname, pubman_surname in pubman_names:
            # Normalize names from the database for comparison
            pubman_surname_normalized = normalize_name_for_comparison(pubman_surname)
            pubman_firstname_normalized = normalize_name_for_comparison(pubman_firstname)

            # Compare surnames and first names after normalization
            if (surname_normalized == pubman_surname_normalized and
                    first_name_normalized == pubman_firstname_normalized):
                # Match found; return the original names from the database
                return pubman_firstname, pubman_surname

        # No match found; return the input name without middle names or initials
        # Extract the first name without middle names or initials
        first_name_no_middle = first_name.split()[0]
        return first_name_no_middle, surname



    def is_mpi_affiliation(self, affiliation: str) -> bool:
        """Check if the affiliation belongs to the Max-Planck Institute."""
        return any(keyword in affiliation for keyword in ['Max Planck', 'Max Plank']) or \
               any(keyword in affiliation.replace(' ', '') for keyword in ['Max-Planck'])


    def process_author_list(self, affiliations_by_name: Dict[str, List[str]]) -> Dict[str, List[Dict[str, Any]]]:
        """
        Go over list of authors and affiliations from Scopus/Crossref, compare to PuRe entries,
        and possibly adopt PuRe if differences are small.
        """

        self.log.debug(f'Entering process_author_list')
        def process_affiliation(affiliation: str) -> str:
            """Format affiliation string."""
            return affiliation.replace('  ', ', ').replace(') ', '), ')

        def handle_mpi_affiliation(author, proposed_affiliation, pubman_author_affiliations):
            """Handle MPI affiliation processing."""
            pubman_mpi_groups = [aff for aff in pubman_author_affiliations if self.is_mpi_affiliation(aff)]
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
                    'comment': f'High confidence match from database. err={compare_error}'
                }
            return {
                'affiliation': process_affiliation(proposed_affiliation),
                'color': 'gray',
                'compare_error': 0,
                'comment': f'Author or similar affiliation not found in database -> using affiliation from publisher (err={compare_error}).'
            }
        mpi_groups = Counter()
        processed_affiliations = {}
        ambiguous_mpi_affiliations = {}
        for author, affiliations in affiliations_by_name.items():
            processed_affiliations[author] = []
            for proposed_affiliation in affiliations if affiliations else []:
                pubman_author_affiliations = self.affiliations_by_name_pubman.get(author, {}).get('affiliations', [])
                if pubman_author_affiliations:
                    self.log.debug(f"Proposed affiliation for {author}: {proposed_affiliation}, MPI check: {self.is_mpi_affiliation(proposed_affiliation)}")

                    if self.is_mpi_affiliation(proposed_affiliation):
                        self.log.debug(f"Handling MPI affiliation")
                        affiliation_info = handle_mpi_affiliation(author, proposed_affiliation, pubman_author_affiliations)
                    else:
                        self.log.debug(f"Handling non-MPI affiliation")
                        affiliation_info = handle_non_mpi_affiliation(proposed_affiliation, pubman_author_affiliations)
                elif proposed_affiliation.strip():
                    self.log.debug(f"No PuRe affiliation for {author}, but provided: {proposed_affiliation}")
                    if self.is_mpi_affiliation(proposed_affiliation):
                        affiliation_info = {
                            'affiliation': 'MISSING MPI',
                        }
                    else:
                        affiliation_info = {
                            'affiliation': process_affiliation(proposed_affiliation),
                            'color': 'gray',
                            'compare_error': 0,
                            'comment': 'No PuRe affiliation for author, adopting external affiliation.'
                        }
                else:
                    raise RuntimeError(f'Affiliation for {author} not found in PuRe or Scopus/Crossref')

                processed_affiliations[author].append(affiliation_info)

                if self.is_mpi_affiliation(affiliation_info['affiliation']):
                    mpi_groups[affiliation_info['affiliation']] += 1

        self.log.debug(f"MPI groups: {mpi_groups}")
        most_common_mpi_group = mpi_groups.most_common(1)[0][0] if mpi_groups else self.mpi_affiliations[0]
        self.log.debug(f"Most common MPI group: {most_common_mpi_group}")

        # Resolve ambiguous MPI affiliations and assign missing MPI groups
        for author, affiliations in processed_affiliations.items():
            self.log.debug(f'Postprocessing author {author}')
            if not affiliations:
                pubman_author_affiliations = self.affiliations_by_name_pubman.get(author, {}).get('affiliations', [])
                if pubman_author_affiliations:
                    self.log.debug(f'No external affiliations found, using PuRe instead: {pubman_author_affiliations}')
                    processed_affiliations[author].append({
                        'affiliation': list(pubman_author_affiliations)[0],
                        'color': 'red',
                        'compare_error': 0,
                        'comment': 'No external affiliations found, using PuRe instead'
                    })
                else:
                    self.log.debug(f'No external affiliations or PuRe affiliations found, leaving empty')
                    processed_affiliations[author].append({
                        'affiliation': '',
                        'color': 'red',
                        'compare_error': 0,
                        'comment': 'No external affiliations or PuRe affiliations found, leaving empty'
                    })
            else:
                for i, affiliation_info in enumerate(affiliations):
                    affiliation = affiliation_info['affiliation']
                    self.log.debug(f"Author: {author}, Current affiliation: {affiliation}")
                    if affiliation == 'AMBIGUOUS MPI':
                        self.log.debug(f"Resolving ambiguous MPI affiliation for {author}. Groups: {ambiguous_mpi_affiliations[author]}")
                        similar_affiliation, score = process.extractOne(most_common_mpi_group, ambiguous_mpi_affiliations[author])
                        compare_error = (100 - score) / 100
                        processed_affiliations[author][i] = {
                            'affiliation': similar_affiliation,
                            'color': 'yellow' if score > 95 else 'red',
                            'compare_error': compare_error,
                            'comment': f'Resolved ambiguous MPI affiliation using most common group. err={compare_error}'
                        }
                    elif affiliation == 'MISSING MPI':
                        processed_affiliations[author][i] = {
                            'affiliation': most_common_mpi_group,
                            'color': 'yellow',
                            'compare_error': 0,
                            'comment': 'Assigned most common MPI group due to missing MPI affiliation.'
                        }

        return processed_affiliations


    def download_pdf(self, pdf_link, doi, retries=3):
        """
        Download PDF for given DOI with Scopus API, retrying up to `retries` times if a failure occurs.

        Args:
            pdf_link (str): The link to the PDF.
            doi (str): The DOI of the article.
            retries (int): Number of retry attempts.
            delay (int): Delay (in seconds) between retries.

        Returns:
            bool: True if the PDF was successfully downloaded, False otherwise.
        """
        pdf_path = FILES_DIR / f'{doi.replace("/", "_")}.pdf'
        if pdf_path.exists():
            self.log.debug(f'Pdf path {pdf_path} already exists, skipping...')
            return True
        else:
            self.log.debug(f"Attempting to download PDF for DOI: {doi}")
            self.log.debug(f"PDF link: {pdf_link}")
            if pdf_link is None:
                self.log.error(f"No valid PDF link found for DOI: {doi}")
                return False

            attempt = 0
            while attempt < retries:
                try:
                    response = requests.get(pdf_link, stream=True)
                    if response.status_code == 200:
                        with open(pdf_path, 'wb') as f:
                            for chunk in response.iter_content(1024):
                                f.write(chunk)
                        self.log.info(f"Successfully downloaded PDF for DOI: {doi}")
                        return True
                    else:
                        cleaned_html = BeautifulSoup(response.text, "html.parser").text
                        self.log.error(f"Failed to download PDF. Status code: {response.status_code}, {cleaned_html}")
                        break  # Stop retrying if the server returns a valid response but not a 200.
                except requests.exceptions.RequestException as e:
                    self.log.warning(f"Error downloading PDF on attempt {attempt + 1}: {e}")
                    attempt += 1

            self.log.error(f"Failed to download PDF after {retries} attempts for DOI: {doi}")
            return False

    def get_dois_for_author(self,
                            author_name,
                            pubyear_start=None,
                            pubyear_end=None,
                            extra_queries: List[str] = None) -> pd.DataFrame:
        """
        Use Scopus and Crossref APIs to generate a list of DOIs for an author.
        """
        BASE_SCOPUS_URL = "https://api.elsevier.com/content/search/scopus"
        BASE_CROSSREF_URL = "https://api.crossref.org/works"

        # Build the Scopus query
        query_components = [f'AF-ID({self.af_id})']
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

        # Define headers and params for Scopus
        scopus_headers = {
            "X-ELS-APIKey": ENV_SCOPUS_API_KEY,
            "Accept": "application/json"
        }
        scopus_params = {
            "query": query,
            "field": "doi",
            "count": 200,
            "start": 0
        }

        def get_dois_scopus():
            dois = []
            start = 0
            total_results = 1
            while start < total_results:
                scopus_params['start'] = start
                response = requests.get(BASE_SCOPUS_URL, headers=scopus_headers, params=scopus_params)
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

        def get_dois_crossref():
            dois = []
            crossref_params = {
                "query.author": author_name,
                "filter": [],
                "rows": 1000
            }
            if pubyear_start:
                crossref_params["filter"].append(f"from-pub-date:{pubyear_start}")
            if pubyear_end:
                crossref_params["filter"].append(f"until-pub-date:{pubyear_end}")
            crossref_params["filter"] = ",".join(crossref_params["filter"])

            response = requests.get(BASE_CROSSREF_URL, params=crossref_params)
            if response.status_code == 200:
                data = response.json()
                items = data.get('message', {}).get('items', [])
                for item in items:
                    doi = item.get('DOI')
                    if doi:
                        dois.append(doi)
            else:
                raise RuntimeError(f"Crossref query API error {response.status_code}: {response.text}")
            return dois

        # Fetch DOIs from both sources
        scopus_dois = get_dois_scopus()
        crossref_dois = get_dois_crossref()

        return self.filter_dois(crossref_dois, scopus_dois)


    def filter_dois(self, dois_crossref: List[str], dois_scopus: List[str]) -> pd.DataFrame:
        """
        Takes lists of DOIs from Crossref and Scopus, checks their availability, and returns an overview DataFrame.

        Args:
            dois_crossref (List[str]): List of DOIs to check on Crossref.
            dois_scopus (List[str]): List of DOIs to check on Scopus.

        Returns:
            pd.DataFrame: DataFrame with 'DOI', 'crossref', and 'scopus' columns.
        """
        pd.set_option('display.max_rows', None)
        pd.set_option('display.max_columns', None)

        # Dictionary to store results for each DOI
        results: Dict[str, Dict[str, bool]] = {}

        # Helper function to process metadata
        def process_metadata(source, doi):
            if source == 'crossref':
                metadata = self.fetch_crossref_metadata(doi)
                return doi, {'crossref': bool(metadata)}
            elif source == 'scopus':
                metadata = self.fetch_scopus_metadata(doi)
                return doi, {'scopus': bool(metadata)}

        # Fetch Crossref metadata
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = {executor.submit(process_metadata, 'crossref', doi): doi for doi in dois_crossref}
            for future in as_completed(futures):
                doi, crossref_result = future.result()
                results.setdefault(doi, {'crossref': False, 'scopus': False}).update(crossref_result)

        # Fetch Scopus metadata
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = {executor.submit(process_metadata, 'scopus', doi): doi for doi in dois_scopus}
            for future in as_completed(futures):
                doi, scopus_result = future.result()
                results.setdefault(doi, {'crossref': False, 'scopus': False}).update(scopus_result)

        # Convert results to DataFrame
        df = pd.DataFrame.from_dict(results, orient='index').reset_index()
        df.rename(columns={'index': 'DOI'}, inplace=True)
        return df

    def fetch_crossref_metadata(self, doi):
        """Fetch metadata from Crossref for the given DOI."""
        crossref_metadata = self.get_crossref_metadata(doi)
        title = crossref_metadata.get('title', [None])[0] if crossref_metadata else 'Unknown Title'
        publication_date = crossref_metadata.get('published-online', {}).get('date-parts', [None])[0] if crossref_metadata else 'Unknown Date'
        return {
            'Title': title,
            'Publication Date': publication_date,
            'DOI': doi,
            'Source': 'Crossref'
        }

    def fetch_scopus_metadata(self, doi):
        """Fetch metadata from Scopus for the given DOI."""
        scopus_metadata = self.get_scopus_metadata(doi)
        field = []
        title = 'Unknown Title'
        publication_date = 'Unknown Date'

        if scopus_metadata:
            # Example of processing Scopus metadata (uncomment and customize as needed)
            author_affiliation_map = self.extract_scopus_authors_affiliations(scopus_metadata)
            is_mp_publication = False
            for _, affiliations in author_affiliation_map.items():
                for affiliation in affiliations:
                    if self.is_mpi_affiliation(affiliation):
                        is_mp_publication = True
                        break
                if is_mp_publication:
                    break
            if not is_mp_publication:
                field.append(f'Authors {list(author_affiliation_map.keys())} have no Max-Planck affiliation')
        else:
            field.append('Publication not found on Scopus')

        return {
            'Title': title,
            'Publication Date': publication_date,
            'DOI': doi,
            'Field': "\n".join(field),
            'Source': 'Scopus'
        }

    def collect_data_for_dois(self, df_dois_overview: pd.DataFrame, force=False) -> List[OrderedDict[str, Tuple[str, int, str]]]:
        """
        Takes overview dataframe, collects all data for DOIs which are not yet on PuRe and have Scopus and Crossref entries.
        Generates dataframe which can be passed to the excel_generator.create_sheet method to prefill the sheet with data.

        Result
        ------

        Each entry in the result list is a dict that corresponds to a publication.
        The dict maps column data to a tuple, e.g. `"Title": (title, 35)`
        Where "title" is the value for this column, "35" is the width of the column on the excel, and the last entry is an optional Tooltip to be displayed
        """
        print("df_dois_overview",df_dois_overview)
        if force:
            new_dois = df_dois_overview['DOI'].values
        else:
            new_dois = df_dois_overview[(df_dois_overview['Field'].isnull()) | (df_dois_overview['Field'] == '')]['DOI'].values
        dois_data = []
        for index, row in df_dois_overview[(df_dois_overview['Field'].isnull()) | (df_dois_overview['Field'] == '')].iterrows():
            doi = row['DOI']
            self.log.debug("Processing Publication DOI {doi}")
            crossref_metadata = self.get_crossref_metadata(doi)
            if not crossref_metadata:
                return None
            self.log.debug(f"crossref_metadata {crossref_metadata}")
            scopus_metadata = self.get_scopus_metadata(doi)
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
                date_issued_scopus = scopus_metadata['abstracts-retrieval-response']['item']['bibrecord']['head']['source']['publicationdate']
                date_issued = (f"{date_issued_scopus.get('day', '').zfill(2)}." if date_issued_scopus.get('day') else "") + \
                              (f"{date_issued_scopus.get('month', '').zfill(2)}." if date_issued_scopus.get('month') else "") + \
                              (date_issued_scopus.get('year', '') ).rstrip('.')
            else:
                if crossref_metadata.get('license'):
                    pdf_found = False
                self.log.info(f'Scopus not available for {doi}, using crossref affiliations...')
                affiliations_by_name = self.extract_crossref_authors_affiliations(crossref_metadata)
                print(f"zzzzzzzzzz")
                print(f"affiliations_by_name {affiliations_by_name}")
                date_issued_crossref = crossref_metadata['issued']['date-parts']
                date_issued = (f"{date_issued_crossref[0][2]}." if len(date_issued_crossref[0])==3 else "") + \
                              (f"{date_issued_crossref[0][1]}." if len(date_issued_crossref[0])>=2 else "") + \
                              (f"{date_issued_crossref[0][0]}")
                print("date_issuedct",date_issued)
            cleaned_author_list = self.process_author_list(affiliations_by_name)
            missing_pdf = True if license_type!='closed' and not pdf_found else False
            prefill_publication = OrderedDict({
                "Title": Cell(title, 35),
                "Journal Title": Cell(journal_title, 25),
                "Publisher": Cell(html.unescape(unidecode(crossref_metadata.get('publisher', None)) or ''), 20),
                "Issue": Cell(crossref_metadata.get('issue', None), 10),
                "Volume": Cell(crossref_metadata.get('volume', None), 10),
                "Page": Cell(page, 10),
                'Article Number': Cell(article_number, 10),
                "ISSN": Cell(html.unescape(unidecode(crossref_metadata.get('ISSN', [None])[0] or '')), 15),
                "Date published online": Cell(self.parse_date(crossref_metadata.get('created', {}).get('date-time', None)), 20),
                'Date issued': Cell(date_issued, 20),
                'DOI': Cell(doi, 20),
                'License url': Cell(license_url if license_type=='open' else '', 20),
                'License year': Cell(license_year if license_type=='open' else '', 15),
                'Pdf found': Cell('' if license_type=='closed' else 'y' if pdf_found else 'n', 15,
                                  color='red' if missing_pdf else '',
                                  comment = 'Please upload the file and license info when submitting in PuRe'
                                  if missing_pdf else ''),
                'Link': Cell(crossref_metadata.get('resource', {}).get('primary', {}).get('URL', ''), 20),
            })
            i = 1
            for (first_name, last_name), affiliations_info in cleaned_author_list.items():
                for affiliation_info in affiliations_info:
                    prefill_publication[f"Author {i}"] = Cell(first_name + ' ' + last_name)
                    prefill_publication[f"Affiliation {i}"] = Cell(affiliation_info['affiliation'],
                                                                   color = affiliation_info['color'],
                                                                   compare_error = affiliation_info['compare_error'],
                                                                   comment = affiliation_info['comment'])
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
                header: [cell.width, cell.comment]
                for header, cell in dois_data[0].items()
                if 'Author ' not in header and 'Affiliation ' not in header
            })
            create_sheet(path_out, {author: author_info.get('affiliations', []) for author, author_info in self.affiliations_by_name_pubman.items()},
                        column_details, n_authors,
                        prefill_publications = dois_data)
            self.log.info(f"Saved {path_out} successfully.")
