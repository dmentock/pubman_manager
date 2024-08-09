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

from pubman_manager import create_sheet, FILES_DIR, PUBMAN_CACHE_DIR, PUBLICATIONS_DIR

class DOIParser:
    def __init__(self, scopus_api_key, pubman_api):
        self.scopus_api_key = scopus_api_key
        self.pubman_api = pubman_api
        with open(PUBMAN_CACHE_DIR / 'authors_info.yaml', 'r', encoding='utf-8') as f:
            authors_info = yaml.safe_load(f)
        self.affiliations_by_name_pubman = OrderedDict({key: val['affiliations'] for key, val in authors_info.items() if val})

    def get_crossref_metadata(self, doi):
        cr = Crossref()
        try:
            result = cr.works(ids=doi)
            return result['message']
        except Exception as e:
            print(f"Failed to retrieve data for DOI {doi}: {e}")

    def get_scopus_metadata(self, doi, api_key):
        url = "https://api.elsevier.com/content/abstract/doi/"
        headers = {
            'Accept': 'application/json',
            'X-ELS-APIKey': api_key,
        }
        response = requests.get(url + doi, headers=headers)
        response.raise_for_status()
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
        print('process_name',name)
        normalized_name = normalize_name(name)
        abbrev_parts = normalized_name.split(' ')
        initials = [part[0] for part in abbrev_parts if len(part) == 1 or '.' in part]
        first_name = abbrev_parts[0]
        surname = abbrev_parts[-1]
        best_match = None
        best_score = -1

        # First, try to find an exact match of the full name without considering abbreviations
        for full_name in names_affiliations:
            normalized_full_name = normalize_name(full_name)
            if name.lower() == normalized_full_name and '.' not in name:
                return full_name
        print('full_name')

        # Try to find an exact match of compound surname
        for full_name in names_affiliations:
            normalized_full_name = normalize_name(full_name)
            if normalized_name.replace(' ', '').replace('-', '') == normalized_full_name.replace(' ', '').replace('-', ''):
                return full_name

        # Then, try to find a full name match considering initials and surname
        for full_name in names_affiliations:
            normalized_full_name = normalize_name(full_name)
            full_name_parts = normalized_full_name.split(' ')

            # Check if the surname matches (consider hyphens and spaces)
            if surname.replace(' ', '').replace('-', '') == full_name_parts[-1].replace(' ', '').replace('-', '') and \
                (('.' in first_name and first_name[0] == full_name_parts[0][0]) or first_name==full_name_parts[0]):
                # Calculate the match score based on initials

                score = sum(any(fn.startswith(init) for fn in full_name_parts) for init in initials)
                print("first_name",first_name)
                print("full_name",full_name)
                print("first_name[0] == full_name_parts[0][0]",first_name[0] == full_name_parts[0][0])
                print("first_name==full_name_parts[0]",first_name==full_name_parts[0])
                print("score1",score)
                # Check if all initials and surname are present
                if all(any(fn.startswith(init) for fn in full_name_parts) for init in initials):
                    score += len(initials)
                print("score2",score)

                # Prefer matches with more matching initials
                if score > best_score or (score == best_score and len(full_name) > len(best_match)):
                    best_match = full_name
                    best_score = score
        print("nonon",best_match)
        # If no full name match is found, consider abbreviations
        if not best_match:
            for full_name in names_affiliations:
                normalized_full_name = normalize_name(full_name)
                if ' '.join(abbrev_parts).replace(' ', '').replace('-', '') == normalized_full_name.replace(' ', '').replace('-', ''):
                    best_match = full_name
                    best_score = len(abbrev_parts)
                    break
        print("nonon2",best_match)

        # Ensure the best match is not an abbreviation
        if best_match and '.' not in best_match:
            return best_match
        else:
            # Try to find a full match excluding abbreviations
            for full_name in names_affiliations:
                if normalize_name(full_name) == normalized_name.replace('.', ''):
                    return full_name
        print("nonon3")

        return best_match if best_match else name

    def process_author_list(self,
                            affiliations_by_name: Dict[str, List[str]],
                            title: str) -> Dict[str, List[Tuple[str, str]]]:
        non_mpg_affiliations = Counter()
        pubman_affiliations = set()
        processed_affiliations = {}
        for author, affiliations in affiliations_by_name.items():
            print("author, affiliations", author, affiliations)
            processed_affiliations[author] = []
            for i, proposed_affiliation in enumerate(affiliations if affiliations else ['']):
                print("proposed_affiliation", proposed_affiliation)
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
                    print("WHATcompare_error",compare_error)
                elif proposed_affiliation.strip():
                    affiliation = proposed_affiliation.replace('  ', ', ').replace(') ', '), ')
                    color = 'gray' if 'Max-Planck' not in affiliation else 'purple'
                else:
                    continue
                print("okeycompare_error",compare_error)
                processed_affiliations[author].append([affiliation, color, compare_error])
                if 'Max-Planck' not in affiliation:
                    non_mpg_affiliations[affiliation] += 1
                if color != 'gray':
                    pubman_affiliations.add(affiliation)
        print("yye",processed_affiliations)

        if non_mpg_affiliations:
            most_common_affiliation = non_mpg_affiliations.most_common(1)[0][0]
        else:
            most_common_affiliation = ''
        for author, affiliations in processed_affiliations.items():
            if not affiliations:
                print("overriding affiliation;", author, most_common_affiliation)
                processed_affiliations[author] = [[most_common_affiliation, 'red', '']]
            print("affiliationss",author, affiliations)
            for i, affiliation in enumerate(affiliations):
                if affiliation[1] == 'gray':
                    similar_affiliation, score = process.extractOne(affiliation[0], pubman_affiliations)
                    if score > 90 and similar_affiliation not in affiliations:
                        print("OVER")
                        processed_affiliations[author][i][0] = similar_affiliation
                        processed_affiliations[author][i][1] = 'pink'
                        processed_affiliations[author][i][2] = (100-score)/100
        print("eee",processed_affiliations)
        return processed_affiliations

    def download_scopus_pdf(self, doi):
        print('DOI:', doi)
        url = f"https://api.elsevier.com/content/article/doi/{doi}?httpAccept=application/pdf"
        headers = {
            'X-ELS-APIKey': self.scopus_api_key,
            'Accept': 'application/pdf'
        }
        response = requests.get(url, headers=headers, allow_redirects=False)
        if response.status_code in [303, 307]:
            redirect_url = response.headers.get('Location')
            if redirect_url:
                print(f"Redirecting to: {redirect_url}")
                response = requests.get(redirect_url, headers=headers, stream=True)
            else:
                print("Redirect URL not found. Cannot download PDF.")
                return
        if response.status_code == 200 and 'application/pdf' in response.headers.get('Content-Type', ''):
            file_path = FILES_DIR / f'{doi.replace("/", "_")}.pdf'
            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            return file_path
        else:
            if response.status_code == 401:
                print("Authentication Error: Check your API key and tokens.")
            elif response.status_code == 403:
                print("Authorization/Entitlements Error: You may not have the necessary rights.")
            elif response.status_code == 404:
                print("Resource Not Found: The DOI may be incorrect or unavailable.")
            else:
                print(f"Failed to download PDF. Status code: {response.status_code}, Message: {response.text}")

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
                affiliation_list.append(source_text)
            else:
                organization_entries = affiliation_info.get('organization', [])
                if isinstance(organization_entries, dict):
                    organization_names = [organization_entries['$']]
                elif isinstance(organization_entries, list):
                    organization_names = [org['$'] for org in organization_entries]
                else:
                    print("organization_entries", type(organization_entries), organization_entries)
                    raise
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

    def collect_data_for_doi(self, doi):
        print("Processing Publication DOI", doi)
        crossref_metadata = self.get_crossref_metadata(doi)
        print("metadata", crossref_metadata)
        if not crossref_metadata:
            return None
        try:
            scopus_metadata = self.get_scopus_metadata(doi, self.scopus_api_key)
        except requests.HTTPError:
            scopus_metadata = None
        print("scopus_metadata", scopus_metadata)

        def clean_html(raw_html):
            soup = BeautifulSoup(raw_html, "html.parser")
            return soup.get_text()
        title = unidecode(clean_html(crossref_metadata.get('title', [None])[0]))
        container_title = crossref_metadata.get('container-title', [None])

        # Use the first title if the list is not empty or None
        journal_title = unidecode(container_title[0]) if container_title else None
        license_list = crossref_metadata.get('license')
        license = license_list[0]['URL'] if license_list else None
        prefill_publication = OrderedDict({
            "Title": [title, 35, ''],
            # "Type": [data.get('type'), 15, ''],
            "Journal Title": [journal_title, 25, ''],
            "Publisher": [unidecode(crossref_metadata.get('publisher', None) or ''), 20, ''],
            "Issue": [crossref_metadata.get('issue', None), 10, ''],
            "Volume": [crossref_metadata.get('volume', None), 10, ''],
            "Page": [crossref_metadata.get('page', None), 10, ''],
            "ISSN": [unidecode(crossref_metadata.get('ISSN', [None])[0] or ''), 15, ''],
            "Date created": [self.parse_date(crossref_metadata.get('created', {}).get('date-time', None)), 20, ''],
            'Date issued': [self.parse_date(crossref_metadata.get('issued', {}).get('date-parts', [[None]])[0]), 20, ''],
            'Date published': [self.parse_date(crossref_metadata.get('published', {}).get('date-parts', [[None]])[0]), 20, ''],
            'DOI': [doi, 20, ''],
            'license': [license, 20, ''],
            'link': [crossref_metadata.get('resource', {}).get('primary', {}).get('URL', ''), 20, ''],
        })

        if scopus_metadata:
            affiliations_by_name = self.extract_scopus_authors_affiliations(scopus_metadata)
        else:
            affiliations_by_name = self.extract_crossref_authors_affiliations(crossref_metadata)
        print("affiliations_by_name",affiliations_by_name)
        cleaned_author_list = self.process_author_list(affiliations_by_name, title)
        print("cleaned_author_list",cleaned_author_list)
        i = 1
        for author, affiliations in cleaned_author_list.items():
            for affiliation in affiliations:
                prefill_publication[f"Author {i}"] = [author, None, '']
                prefill_publication[f"Affiliation {i}"] = [affiliation[0], affiliation[1], '', affiliation[2]]
                i = i+1
        return prefill_publication

    def collect_data_for_dois(self, doi_list):
        dois_data = []
        for doi in doi_list:
            pub = self.pubman_api.search_publication_by_criteria({
                "metadata.identifiers.id": doi,
                "metadata.identifiers.type": 'DOI'
            })
            if pub:
                print("Publication {doi} already exists in PuRe, skipping...")
            else:
                if (doi_data:=self.collect_data_for_doi(doi)):
                    dois_data.append(doi_data)
                else:
                    print('Publication not found on Crossref, skipping...')
        return dois_data

    def write_dois_data(self, dois_data, path, overwrite = False):
        if not dois_data:
            empty_path = Path(os.path.abspath(path)).parent / f'{path.stem}_empty{path.suffix}'
            df = pd.DataFrame()
            df.to_excel(empty_path, index=False)
            print(f"Saved empty_path {empty_path} successfully.")
        else:
            n_authors = 45
            column_details = OrderedDict({
                key: [val[1], val[2]]
                for key, val in dois_data[0].items()
                if 'Author ' not in key and 'Affiliation ' not in key
            })
            create_sheet(path, self.affiliations_by_name_pubman,
                        column_details, n_authors,
                        prefill_publications = dois_data)
            print(f"Saved {path} successfully.")

    def iterate_over_sheets_in_folder(self, doi_sheets_dir):
        for publication_sheet in Path(doi_sheets_dir).iterdir():
            path = Path(PUBLICATIONS_DIR / f'{Path(publication_sheet.stem)}.xlsx')
            # path = Path(f'./Publication Templates/test.xlsx')
            empty_path = Path(PUBLICATIONS_DIR / f'{Path(publication_sheet.stem)}_empty.xlsx')
            if path.exists():
                print(f'Skipping "{path}" because the file already exists...')
                continue
            if empty_path.exists():
                print(f'Skipping empty file "{path}" because the file already exists...')
                continue
            print(f'Reading {publication_sheet}')
            df = pd.read_csv(
                publication_sheet,
                encoding='ISO-8859-1',
                engine='python',
                on_bad_lines='skip'
            )
            print("df.columns",df.columns)
            if 'DOI' not in df.columns:
                df = pd.read_csv(publication_sheet, delimiter='\t', encoding='ISO-8859-1')
                if 'DOI' not in df.columns:
                    raise RuntimeError(f"DOI col not found in the CSV file.")

            dfo = df['DOI'].dropna()
            # for doi in ['10.1093/micmic/ozad067.876']:
            dois_data = self.collect_data_for_dois(list(dfo))
            print("dois_data",dois_data)
            import sys
            sys.exit()
            self.write_dois_data(dois_data)


