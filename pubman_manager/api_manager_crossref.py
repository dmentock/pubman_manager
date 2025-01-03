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

class CrossrefManager:
    def __init__(self, scopus_api_key = None, logging_level = logging.INFO):
        self.log = logging.getLogger(__name__)
        self.log.setLevel(logging_level)
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

    def get_overview(source, doi):
        crossref_metadata = self.get_metadata(doi)
        title = crossref_metadata.get('title', [None])[0] if crossref_metadata else 'Unknown Title'
        publication_date = crossref_metadata.get('published-online', {}).get('date-parts', [None])[0] if crossref_metadata else 'Unknown Date'
        return {
            'Title': title,
            'Publication Date': publication_date,
            'crossref': True
        }

    def extract_authors_affiliations(self, crossref_metadata):
        affiliations_by_name = OrderedDict()
        for author in crossref_metadata.get('author', []):
            author_name = self.process_name(self.affiliations_by_name_pubman.keys(), author.get('given', ''), author.get('family', ''))
            affiliations_by_name[author_name] = []
            print("author", author_name, author)
            for affiliation in author.get('affiliation', []):
                print("affiliation", affiliation)
                affiliations_by_name[author_name].append(unidecode(affiliation.get('name', '')))
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
