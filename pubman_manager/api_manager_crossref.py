from habanero import Crossref
from unidecode import unidecode
import pandas as pd
from collections import OrderedDict
import requests
import time
from typing import List, Dict, Tuple, Any
import logging

from pubman_manager import is_mpi_affiliation
from pubman_manager.util import date_to_cell

logger = logging.getLogger(__name__)

class CrossrefManager:
    def __init__(self):
        self.metadata_map = {}

    def get_metadata(self, doi):
        if doi not in self.metadata_map:
            cr = Crossref()
            try:
                result = cr.works(ids=doi)
                self.metadata_map[doi] = result['message']
                logger.debug(f'crossref {self.metadata_map[doi]}')
            except Exception as e:
                logger.error(f"Failed to retrieve Crossref data for DOI {doi}: {e}")
                return None
        return self.metadata_map[doi]

    def get_overview(self, doi):
        crossref_metadata = self.get_metadata(doi)
        if not crossref_metadata:
            return {}

        title = crossref_metadata.get('title', [None])[0] if crossref_metadata else 'Unknown Title'
        publication_date = crossref_metadata.get('published-online', {}).get('date-parts', [None])[0] if crossref_metadata else None

        overview = {
            'Title': title,
            'Publication Date': date_to_cell(publication_date),
            'crossref': f"https://doi.org/{doi}"
        }

        field = []
        if (isbn:=crossref_metadata.get('ISBN')):
            logger.info(f'Skipping Book DOI: {doi}')
            field.append(f'Has ISBN: {isbn}')

        else:
            author_affiliation_map = self.extract_authors_affiliations(crossref_metadata)
            is_mp_publication = False
            has_any_affiliations = False
            for affiliations in author_affiliation_map.values():
                for affiliation in affiliations:
                    if not has_any_affiliations:
                        has_any_affiliations = True
                    if is_mpi_affiliation(affiliation):
                        is_mp_publication = True
                        break
                if is_mp_publication:
                    break
            if has_any_affiliations and not is_mp_publication:
                field.append(f'Authors have no Max-Planck affiliation (Crossref)')
        overview['Field'] = field
        return overview

    def extract_authors_affiliations(self, crossref_metadata):
        affiliations_by_name = OrderedDict()
        for author in crossref_metadata.get('author', []):
            first_name, last_name = author.get('given', ''), author.get('family', '')
            affiliations_by_name[(first_name, last_name)] = []
            for affiliation in author.get('affiliation', []):
                affiliations_by_name[(first_name, last_name)].append(unidecode(affiliation.get('name', '')))
        return affiliations_by_name

    def get_dois_for_author(self,
                            first_name,
                            last_name,
                            pubyear_start=None,
                            pubyear_end=None,
                            extra_queries: List[str] = None) -> List[str]:
        """
        Use Crossref API to generate a list of DOIs for an author.
        """
        BASE_CROSSREF_URL = "https://api.crossref.org/works"

        author_name = f'{first_name} {last_name}'
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
        attempt = 0
        while True:
            response = requests.get(BASE_CROSSREF_URL, params=params)
            if response.status_code == 200:
                data = response.json()
                items = data.get('message', {}).get('items', [])
                for item in items:
                    doi = item['DOI']
                    if item.get('subtype') == 'preprint':
                        logger.debug(f"Skipping preprint {doi} {item.get('published', {})}")
                        continue
                    if 'proceeding' in item.get('type', ''):
                        logger.debug(f"Skipping proceeding article {doi} {item.get('type', '')}")
                        continue
                    if 'ssrn' in doi.lower() or 'egusphere' in doi.lower():
                        logger.debug(f"Skipping ssrn or egusphere {doi}")
                        continue
                    for author_data in item.get('author', []):
                        if author_name == f"{author_data.get('given', '').strip()} {author_data.get('family', '').strip()}".strip():
                            dois.append(doi)
            else:
                if attempt>3:
                    raise RuntimeError(f"Crossref query API error {response.status_code}: {response.text}")
                else:
                    attempt+=1
                    time.sleep(5)
            return dois
