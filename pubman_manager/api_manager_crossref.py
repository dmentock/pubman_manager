from habanero import Crossref
from unidecode import unidecode
import pandas as pd
from collections import OrderedDict
import requests
from typing import List, Dict, Tuple, Any
import logging

class CrossrefManager:
    def __init__(self, scopus_api_key = None):
        self.metadata_map = {}

    def get_metadata(self, doi):
        if doi not in self.metadata_map:
            cr = Crossref()
            try:
                result = cr.works(ids=doi)
                self.metadata_map[doi] = result['message']
            except Exception as e:
                self.log.error(f"Failed to retrieve Crossref data for DOI {doi}: {e}")
                return None
        return self.metadata_map[doi]

    def get_overview(self, doi):
        crossref_metadata = self.get_metadata(doi)
        if not crossref_metadata:
            return doi, {}
        title = crossref_metadata.get('title', [None])[0] if crossref_metadata else 'Unknown Title'
        publication_date = crossref_metadata.get('published-online', {}).get('date-parts', [None])[0] if crossref_metadata else 'Unknown Date'
        return doi, {
            'Title': title,
            'Publication Date': publication_date,
            'crossref': f"https://doi.org/{doi}"
        }

    def extract_authors_affiliations(self, crossref_metadata):
        affiliations_by_name = OrderedDict()
        for author in crossref_metadata.get('author', []):
            first_name, last_name = author.get('given', ''), author.get('family', '')
            affiliations_by_name[(first_name, last_name)] = []
            print("author", first_name, last_name, author)
            for affiliation in author.get('affiliation', []):
                print("affiliation", affiliation)
                affiliations_by_name[(first_name, last_name)].append(unidecode(affiliation.get('name', '')))
        return affiliations_by_name

    def get_dois_for_author(self,
                            author_name,
                            pubyear_start=None,
                            pubyear_end=None,
                            extra_queries: List[str] = None) -> pd.DataFrame:
        """
        Use Crossref API to generate a list of DOIs for an author.
        """
        BASE_CROSSREF_URL = "https://api.crossref.org/works"

        dois = []
        params = {
            "query.author": author_name,
            "filter": [],
            "rows": 1000
        }
        if pubyear_start:
            params["filter"].append(f"from-pub-date:{pubyear_start}")
        if pubyear_end:
            params["filter"].append(f"until-pub-date:{pubyear_end}")
        # params["filter"].append("has-affiliation:true")
        params["filter"] = ",".join(params["filter"])

        response = requests.get(BASE_CROSSREF_URL, params=params)
        if response.status_code == 200:
            data = response.json()
            items = data.get('message', {}).get('items', [])
            for item in items:
                doi = item['DOI']
                for author_data in item.get('author', []):
                    if author_name == f"{author_data.get('given', '').strip()} {author_data.get('family', '').strip()}".strip():
                        dois.append(doi)
        else:
            raise RuntimeError(f"Crossref query API error {response.status_code}: {response.text}")
        return dois
