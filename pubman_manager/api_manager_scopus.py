import time
import pandas as pd
from collections import OrderedDict
import requests
from typing import List, Dict, Tuple, Any
import logging
from urllib.parse import urlencode

from pubman_manager import FILES_DIR, ENV_SCOPUS_API_KEY, is_mpi_affiliation
from pubman_manager.util import date_to_cell

logger = logging.getLogger(__name__)

BASE_URL =             "https://api.elsevier.com/content"
BASE_AUTHOR_URL =      f"{BASE_URL}/search/author"
BASE_AFFILIATION_URL = f"{BASE_URL}/search/affiliation"
BASE_SEARCH_URL = "https://api.elsevier.com/content/search/scopus"

class ScopusManager:
    def __init__(self, org_name, api_key = None):
        self.api_key = api_key if api_key else ENV_SCOPUS_API_KEY
        self.org_name = org_name
        self.metadata_map = {}
        self.af_id_ = None
        self.author_id_map = {}
        self.last_request = time.time()
        self.rate_limit = 2

    @property
    def af_id(self):
        if self.af_id_:
            return self.af_id_
        params = {
            "query": f"AFFIL({self.org_name})"
        }
        encoded_params = urlencode(params)
        headers = {
            "X-ELS-APIKey": self.api_key,
            "Accept": "application/json"
        }
        response = requests.get(f"{BASE_AFFILIATION_URL}?{encoded_params}", headers=headers)
        if response.status_code == 200:
            data = response.json()
            if "search-results" in data and "entry" in data["search-results"]:
                for affiliation in data["search-results"]["entry"]:
                    dc_identifier = affiliation.get('dc:identifier', 'Unknown')
                    af_id = dc_identifier.split(":")[-1] if dc_identifier.startswith("AFFILIATION_ID:") else None
                    self.af_id_ = af_id
                    return af_id
            else:
                raise RuntimeError(f"Scopus API request to get AF-ID unsuccessful, unexpected datastructure: {data}")
        else:
            raise RuntimeError(f"Scopus API request to get AF-ID unsuccessful: {response.status_code} - {response.text}")

    def get_metadata(self, doi):
        if doi not in self.metadata_map:
            url = "https://api.elsevier.com/content/abstract/doi/"
            headers = {
                'Accept': 'application/json',
                'X-ELS-APIKey': self.api_key,
            }
            try:
                time.sleep(max(self.rate_limit - (time.time()-self.last_request), 0))
                response = requests.get(url + doi, headers=headers)
                self.last_request = time.time()
                response.raise_for_status()
                self.metadata_map[doi] = response.json()
            except requests.HTTPError as e:
                logger.error(f"Failed to retrieve Scopus data for DOI {doi}: {e}")
                self.metadata_map[doi] = {}
        return self.metadata_map[doi]

    def get_overview(self, doi):
        """Fetch overview from Scopus for the given DOI."""
        scopus_metadata = self.get_metadata(doi)
        overview = {}
        if scopus_metadata:
            # Fetch title and publication date from scopus_metadata
            title = scopus_metadata['abstracts-retrieval-response']['coredata'].get('dc:title', 'Unknown Title')
            if title:
                overview['Title']  = title
            publication_date = scopus_metadata['abstracts-retrieval-response']['coredata'].get('prism:coverDate')
            overview['Publication Date'] = date_to_cell(publication_date)
            author_affiliation_map = self.extract_authors_affiliations(scopus_metadata)
            is_mp_publication = False
            has_any_affiliations = False
            for (first_name, last_name), affiliations in author_affiliation_map.items():
                for affiliation in affiliations:
                    print("(first_name, last_name)",(first_name, last_name), affiliation)
                    if not has_any_affiliations:
                        # print("oop")
                        has_any_affiliations = True
                    if is_mpi_affiliation(affiliation):
                        print("ISMPI")
                        is_mp_publication = True
                        break
                if is_mp_publication:
                    print("breaking")
                    break
            print("has_any_affiliations", has_any_affiliations)
            print("is_mp_publication", is_mp_publication)
            if has_any_affiliations and not is_mp_publication:
                overview['Field'] = [f'Authors have no Max-Planck affiliation (Scopus)']
            scopus_id = scopus_metadata['abstracts-retrieval-response']['coredata']['prism:url'].split('/')[-1]
            overview['scopus'] = f"https://www.scopus.com/inward/record.uri?partnerID=HzOxMe3b&scp={scopus_id}&origin=inward"
        return overview

    def get_author_full_name(self, author_id):
        author_api_url = f"https://api.elsevier.com/content/author/author_id/{author_id}"
        headers = {
            'Accept': 'application/json',
            'X-ELS-APIKey': self.api_key
        }
        while True:
            time.sleep(max(self.rate_limit - (time.time()-self.last_request),0))
            try:
                response = requests.get(author_api_url, headers=headers)
                self.last_request = time.time()
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
            except Exception as e:
                import traceback
                logger.error(f"Failed to get response from {author_api_url}: {e}\n\n{traceback.format_exc()}")
            else:
                if response.status_code == 429 or "QUOTA_EXCEEDED" in response.headers.get("X-ELS-Status", ""):
                    logger.warning(f"Quota exceeded for Scopus author {author_id} (status code: {response.status_code})")
                    reset_hdr = response.headers.get("X-RateLimit-Reset")
                    if reset_hdr:
                        try:
                            wait = int(reset_hdr) - int(time.time()) + 1  # +1s buffer
                            logger.info(f"Rate limit resets in {wait} seconds. Sleeping...")
                            time.sleep(max(wait, 1))
                        except Exception as e:
                            logger.warning(f"Failed to parse X-RateLimit-Reset: {reset_hdr} ({e})")
                    else:
                        logger.info("No X-RateLimit-Reset header found. Sleeping default 30s.")
                else:
                    logger.error(f"Failed to retrieve Scopus author data for author {author_id} "
                                f"(status code: {response.status_code}, {response.text})")
            time.sleep(30)

    def extract_authors_affiliations(self, scopus_metadata) -> "OrderedDict[Tuple[str, str], List[str]]":
        """
        Build an ordered mapping (first_name, surname) -> list of affiliation strings.
        Safe against missing fields and odd Scopus shapes.
        """

        def _get(d: Dict[str, Any], *keys, default=None):
            """Safe nested get: _get(x, 'a','b','c') == x['a']['b']['c'] if present."""
            cur = d
            for k in keys:
                if not isinstance(cur, dict) or k not in cur:
                    return default
                cur = cur[k]
            return cur

        def _as_list(v):
            if v is None:
                return []
            return v if isinstance(v, list) else [v]

        def _text(v):
            """Normalize text: accept str or dicts with '$' / '#text' / 'text'."""
            if v is None:
                return None
            if isinstance(v, str):
                s = v.strip()
                return s or None
            if isinstance(v, dict):
                for k in ('$', '#text', 'text'):
                    if k in v and isinstance(v[k], str):
                        s = v[k].strip()
                        return s or None
            return None

        def _format_affiliation(aff: Dict[str, Any]) -> str:
            orgs = []
            for item in _as_list(aff.get('organization')):
                t = _text(item)
                if t:
                    orgs.append(t)
            orgs = list(dict.fromkeys(orgs))

            dept   = _text(aff.get('affilname'))
            street = _text(aff.get('address-part')) or _text(aff.get('street')) or _text(aff.get('address'))
            city   = _text(aff.get('city'))
            region = _text(aff.get('state')) or _text(aff.get('region')) or _text(aff.get('state-prov'))
            postal = _text(aff.get('postal-code')) or _text(aff.get('postcode'))
            country= _text(aff.get('country'))

            parts: List[str] = []
            parts.extend(x for x in orgs if x)
            if dept:
                parts.append(dept)
            for x in (street, city, region, postal, country):
                if x:
                    parts.append(x)

            if parts:
                return ', '.join(parts)

            src = _text(aff.get('ce:source-text'))
            return src or ""

        author_affiliation_map: "OrderedDict[Tuple[str, str], List[str]]" = OrderedDict()

        abstracts = scopus_metadata.get('abstracts-retrieval-response', {})
        if not abstracts:
            logger.warning('No info found in Scopus')
            return OrderedDict()

        auid_to_affs: Dict[str, List[str]] = {}
        author_groups = _get(abstracts, 'item', 'bibrecord', 'head', 'author-group', default=[])
        author_groups = _as_list(author_groups)

        for group in author_groups:
            aff_info = group.get('affiliation') or {}
            aff_str = _format_affiliation(aff_info)
            if not aff_str:
                continue

            for a in _as_list(group.get('author')):
                auid = _text(a.get('@auid')) or ""
                if not auid:
                    continue
                auid_to_affs.setdefault(auid, []).append(aff_str)

        # Authors listing (order preserved)
        authors_block = abstracts.get('authors', {})
        authors = _as_list(authors_block.get('author'))

        for a in authors:
            pref = a.get('preferred-name', {}) if isinstance(a, dict) else {}
            first = _text(pref.get('ce:given-name')) or ""
            last  = _text(pref.get('ce:surname')) or ""

            # If Scopus returns just an initial, try to resolve full name (your existing hook)
            if '.' in first:
                # Prefer @auid (author-id) over parsing author-url
                auid = _text(a.get('@auid')) or ""
                if not auid:
                    url = _text(a.get('author-url')) or ""
                    auid = url.rsplit('/', 1)[-1] if '/' in url else ""
                try:
                    if auid:
                        first, last = self.get_author_full_name(auid)
                except Exception:  # be defensive; don't fail the whole mapping
                    pass

            auid = _text(a.get('@auid')) or ""
            affs = auid_to_affs.get(auid, [])

            # Deduplicate while keeping order
            unique_affs = list(dict.fromkeys(x for x in affs if x.strip()))
            if not unique_affs:
                unique_affs = ["No affiliation available"]

            author_affiliation_map[(first, last)] = unique_affs

        return author_affiliation_map
    def get_author_id(self, first_name: str, last_name: str) -> str:
        """
        Retrieve the Scopus Author ID for the specified author.
        """
        author_name = f'{first_name} {last_name}'
        if author_name not in self.author_id_map:
            # TODO: Make generic or relax criteria
            query = f'AUTHLASTNAME("{last_name}") AND AUTHFIRST("{first_name}") AND (AFFIL("Max-Planck-Institut für Eisenforschung GmbH") OR AFFIL("Max Planck Institute for Sustainable Materials"))'
            headers = {
                "X-ELS-APIKey": self.api_key,
                "Accept": "application/json"
            }
            params = {
                "query": query,
                "count": 1
            }
            time.sleep(max(self.rate_limit - (time.time()-self.last_request), 0))
            response = requests.get(BASE_AUTHOR_URL, headers=headers, params=params)
            self.last_request = time.time()
            if response.status_code == 200:
                data = response.json()
                entries = data['search-results'].get('entry', [])
                if entries and entries[0].get('dc:identifier'):
                    self.author_id_map[author_name] = entries[0].get('dc:identifier').split(':')[-1]
                else:
                    raise ValueError("Author not found in Scopus.")
            else:
                raise RuntimeError(f"Scopus Author query API error {response.status_code}: {response.text}")
        return self.author_id_map[author_name]

    def get_dois_for_author(self,
                            first_name,
                            last_name,
                            pubyear_start=None,
                            pubyear_end=None,
                            extra_queries: List[str] = None) -> List[str]:
        """
        Use Scopus API to generate a list of DOIs for an author using their Author ID.
        """
        author_id = self.get_author_id(first_name, last_name)
        query_components = [f'AU-ID({author_id})']
        if pubyear_start:
            query_components.append(f'PUBYEAR > {pubyear_start - 1}')
        if pubyear_end:
            query_components.append(f'PUBYEAR < {pubyear_end + 1}')
        if extra_queries:
            query_components.extend(extra_queries)

        query = ' AND '.join(query_components)

        headers = {
            "X-ELS-APIKey": self.api_key,
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
            response = requests.get(BASE_SEARCH_URL, headers=headers, params=params)

            if response.status_code == 200:
                data = response.json()
                logger.info(f'Scopus data: {data}')

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
