from bs4 import BeautifulSoup
import pandas as pd
from collections import OrderedDict
import requests
from typing import List, Dict, Tuple, Any
import logging
from urllib.parse import urlencode

from pubman_manager import FILES_DIR, ENV_SCOPUS_API_KEY, is_mpi_affiliation

class ScopusManager:
    def __init__(self, org_name, api_key = None, logging_level = logging.INFO):
        self.log = logging.getLogger(__name__)
        self.log.setLevel(logging_level)
        self.api_key = api_key if api_key else ENV_SCOPUS_API_KEY
        self.org_name = org_name
        self.metadata_map = {}
        self.af_id_ = None

    @property
    def af_id(self):
        if self.af_id_:
            return self.af_id_
        BASE_URL = "https://api.elsevier.com/content/search/affiliation"
        params = {
            "query": f"AFFIL({self.org_name})"
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

    def get_metadata(self, doi):
        if doi not in self.metadata_map:
            url = "https://api.elsevier.com/content/abstract/doi/"
            headers = {
                'Accept': 'application/json',
                'X-ELS-APIKey': self.api_key,
            }
            try:
                response = requests.get(url + doi, headers=headers)
                response.raise_for_status()
                self.metadata_map[doi] = response.json()
                self.log.debug(f'scopus_metadata {response.json()}')
                return response.json()
            except requests.HTTPError as e:
                self.log.error(f"Failed to retrieve Scopus data for DOI {doi}: {e}")
                return None
        return self.metadata_map[doi]

    def get_overview(self, doi):
        """Fetch overview from Scopus for the given DOI."""
        scopus_metadata = self.get_metadata(doi)
        field = []
        title = 'Unknown Title'
        publication_date = 'Unknown Date'

        if scopus_metadata:
            author_affiliation_map = self.extract_authors_affiliations(scopus_metadata)
            is_mp_publication = False
            for affiliations in author_affiliation_map.values():
                for affiliation in affiliations:
                    if is_mpi_affiliation(affiliation):
                        is_mp_publication = True
                        break
                if is_mp_publication:
                    break
            if not is_mp_publication:
                field.append(f'Authors {list(author_affiliation_map.keys())} have no Max-Planck affiliation')
        else:
            field.append('Publication not found on Scopus')
            return doi, {}

        return doi, {
            'Title': title,
            'Publication Date': publication_date,
            'Field': "\n".join(field),
            'scopus': True
        }

    def get_author_full_name(self, author_id):
        author_api_url = f"https://api.elsevier.com/content/author/author_id/{author_id}"
        headers = {
            'Accept': 'application/json',
            'X-ELS-APIKey': self.api_key
        }
        response = requests.get(author_api_url, headers=headers)

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
            raise RuntimeError(f"Unable to retrieve Scopus author data for author {author_id} (status code: {response.status_code}, {response.text}")

    def extract_authors_affiliations(self, scopus_metadata) -> OrderedDict[str, List[str]]:
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
                    first_name, surname = self.get_author_full_name(author_id)
            author_id = author.get('@auid', '')
            affiliations = author_id_to_affiliations.get(author_id, ['No affiliation available'])
            unique_affiliations = list(OrderedDict.fromkeys(affiliations))
            author_affiliation_map[(first_name, surname)] = unique_affiliations
        return author_affiliation_map

    def get_dois_for_author(self,
                            author_name,
                            pubyear_start=None,
                            pubyear_end=None,
                            extra_queries: List[str] = None) -> pd.DataFrame:
        """
        Use Scopus API to generate a list of DOIs for an author.
        """
        BASE_SCOPUS_URL = "https://api.elsevier.com/content/search/scopus"

        # Build the Scopus query
        query_components = [f'AF-ID({self.af_id})']
        first_name, last_name = author_name.split(' ')[0], ' '.join(author_name.split(' ')[1:])

        query_components.append(f'AUTHFIRST("{first_name}")')
        query_components.append(f'AUTHOR-NAME("{last_name}")')
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

        dois = []
        start = 0
        total_results = 1
        while start < total_results:
            params['start'] = start
            response = requests.get(BASE_SCOPUS_URL, headers=headers, params=params)
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

