import time
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

from pubman_manager import create_sheet, Cell, PUBMAN_CACHE_DIR, ScopusManager, CrossrefManager, FILES_DIR, is_mpi_affiliation

logger = logging.getLogger(__name__)

class DOIParser:
    def __init__(self, pubman_api, scopus_api_key = None):
        self.crossref_manager = CrossrefManager()
        self.scopus_manager = ScopusManager(org_name = pubman_api.org_name, api_key=scopus_api_key)

        self.pubman_api = pubman_api
        with open(PUBMAN_CACHE_DIR / pubman_api.org_id / 'authors_info.yaml', 'r', encoding='utf-8') as f:
            self.affiliations_by_name_pubman = yaml.load(f, Loader=yaml.FullLoader)
        mpi_affiliation_counter = Counter()
        for author, author_info in self.affiliations_by_name_pubman.items():
            for affiliation in author_info['affiliations']:
                if 'Max-Planck' in affiliation:
                    mpi_affiliation_counter[affiliation]+=1
        self.mpi_affiliations = [item[0] for item in sorted(mpi_affiliation_counter.items(), key=lambda x: x[1], reverse=True)]
        self.af_id_ = None

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

    def compare_author_name_to_pure_db(self, pubman_names, first_name, surname) -> Tuple[str]:
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
        if first_name:
            first_name_no_middle = first_name.split()[0]
        else:
            print("NOFIRSTNAME", first_name, surname)
            first_name_no_middle = ''
        return first_name_no_middle, surname

    def compare_author_list_to_pure_db(self, affiliations_by_name: Dict[str, List[str]]) -> Dict[str, List[Dict[str, Any]]]:
        """
        Go over list of authors and affiliations from Scopus/Crossref, compare to PuRe entries,
        and possibly adopt PuRe if differences are small.
        """

        logger.debug(f'Entering compare_author_list_to_pure_db')
        def process_affiliation(affiliation: str) -> str:
            """Format affiliation string."""
            return affiliation.replace('  ', ', ').replace(') ', '), ')

        def handle_mpi_affiliation(author, proposed_affiliation, pubman_author_affiliations):
            """Handle MPI affiliation processing."""
            pubman_mpi_groups = [aff for aff in pubman_author_affiliations if is_mpi_affiliation(aff)]
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
        for (first_name, last_name), affiliations in affiliations_by_name.items():
            author = self.compare_author_name_to_pure_db(self.affiliations_by_name_pubman.keys(), first_name, last_name)
            processed_affiliations[author] = []
            for proposed_affiliation in affiliations if affiliations else []:
                pubman_author_affiliations = self.affiliations_by_name_pubman.get(author, {}).get('affiliations', [])
                if pubman_author_affiliations:
                    logger.debug(f"Proposed affiliation for {author}: {proposed_affiliation}, MPI check: {is_mpi_affiliation(proposed_affiliation)}")

                    if is_mpi_affiliation(proposed_affiliation):
                        logger.debug(f"Handling MPI affiliation")
                        affiliation_info = handle_mpi_affiliation(author, proposed_affiliation, pubman_author_affiliations)
                    else:
                        logger.debug(f"Handling non-MPI affiliation")
                        affiliation_info = handle_non_mpi_affiliation(proposed_affiliation, pubman_author_affiliations)
                elif proposed_affiliation.strip():
                    logger.debug(f"No PuRe affiliation for {author}, but provided: {proposed_affiliation}")
                    if is_mpi_affiliation(proposed_affiliation):
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

                if is_mpi_affiliation(affiliation_info['affiliation']):
                    mpi_groups[affiliation_info['affiliation']] += 1

        logger.debug(f"MPI groups: {mpi_groups}")
        most_common_mpi_group = mpi_groups.most_common(1)[0][0] if mpi_groups else self.mpi_affiliations[0]
        logger.debug(f"Most common MPI group: {most_common_mpi_group}")

        # Resolve ambiguous MPI affiliations and assign missing MPI groups
        for author, affiliations in processed_affiliations.items():
            logger.debug(f'Postprocessing author {author}')
            if not affiliations:
                pubman_author_affiliations = self.affiliations_by_name_pubman.get(author, {}).get('affiliations', [])
                if pubman_author_affiliations:
                    logger.debug(f'No external affiliations found, using PuRe instead: {pubman_author_affiliations}')
                    processed_affiliations[author].append({
                        'affiliation': list(pubman_author_affiliations)[0],
                        'color': 'red',
                        'compare_error': 0,
                        'comment': 'No external affiliations found, using PuRe instead'
                    })
                else:
                    logger.debug(f'No external affiliations or PuRe affiliations found, leaving empty')
                    processed_affiliations[author].append({
                        'affiliation': '',
                        'color': 'red',
                        'compare_error': 0,
                        'comment': 'No external affiliations or PuRe affiliations found, leaving empty'
                    })
            else:
                for i, affiliation_info in enumerate(affiliations):
                    affiliation = affiliation_info['affiliation']
                    logger.debug(f"Author: {author}, Current affiliation: {affiliation}")
                    if affiliation == 'AMBIGUOUS MPI':
                        logger.debug(f"Resolving ambiguous MPI affiliation for {author}. Groups: {ambiguous_mpi_affiliations[author]}")
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
            logger.debug(f'Pdf path {pdf_path} already exists, skipping...')
            return True
        else:
            logger.debug(f"Attempting to download PDF for DOI: {doi}")
            logger.debug(f"PDF link: {pdf_link}")
            if pdf_link is None:
                logger.error(f"No valid PDF link found for DOI: {doi}")
                return False
            attempt = 0
            while attempt < retries:
                try:
                    response = requests.get(pdf_link, stream=True)
                    if response.status_code == 200:
                        with open(pdf_path, 'wb') as f:
                            for chunk in response.iter_content(1024):
                                f.write(chunk)
                        logger.info(f"Successfully downloaded PDF for DOI: {doi}")
                        return True
                    else:
                        cleaned_html = BeautifulSoup(response.text, "html.parser").text
                        logger.error(f"Failed to download PDF. Status code: {response.status_code}, {cleaned_html}")
                        break  # Stop retrying if the server returns a valid response but not a 200.
                except requests.exceptions.RequestException as e:
                    logger.warning(f"Error downloading PDF on attempt {attempt + 1}: {e}")
                    attempt += 1

            logger.error(f"Failed to download PDF after {retries} attempts for DOI: {doi}")
            return False

    def has_pubman_entry(self, doi):
        pub =self.pubman_api.search_publication_by_criteria({
                    "metadata.identifiers.id": doi,
                    "metadata.identifiers.type": 'DOI'
                })
        print("pub HASDOI",doi,pub)
        return bool(pub)

    def get_dois_for_author(self, author: str, pubyear_start=None, pubyear_end=None) -> pd.DataFrame:
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

        results: Dict[str, Dict[str, bool]] = {}

        dois_crossref = self.crossref_manager.get_dois_for_author(author, pubyear_start=pubyear_start, pubyear_end=pubyear_end)
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = {executor.submit(self.crossref_manager.get_overview, doi): doi for doi in dois_crossref}
            for future in as_completed(futures):
                doi, crossref_result = future.result()
                results.setdefault(doi, {'crossref': False, 'scopus': False}).update(crossref_result)

        dois_scopus = self.scopus_manager.get_dois_for_author(author, pubyear_start=pubyear_start, pubyear_end=pubyear_end)
        for doi in dois_scopus:
            doi, scopus_result = self.scopus_manager.get_overview(doi)
            if existing_entry:=results.get(doi):
                existing_field = existing_entry.get('Field', '')
                scopus_result['Field'] = existing_field + '\n' + scopus_result.get('Field', '')
            results.setdefault(doi, {'crossref': False, 'scopus': False}).update(scopus_result)

        for doi in results.keys():
            if self.has_pubman_entry(doi):
                logger.info(f'Foud Publication for {doi} in PuRe.')
                results[doi]['Field'] = (results[doi].setdefault('Field', '') + "\nAlready exists in PuRe").strip()
            if results[doi].get('Publication Date'):
                date = self.parse_date(results[doi]['Publication Date'])
                results[doi]['Publication Date'] = pd.to_datetime(
                    date, format='%d.%m.%Y', errors='coerce'
                )
            else:
                results[doi]['Publication Date'] = pd.NaT

        df = pd.DataFrame.from_dict(results, orient='index').reset_index()
        df.rename(columns={'index': 'DOI'}, inplace=True)
        df = df.sort_values('Publication Date')
        return df

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

        dois_data = []
        for index, row in df_dois_overview.iterrows():
            print("row['Field']",row['Field'])
            if str(row['Field']).strip():
                logger.info(f'Skipping {row["DOI"]}, reason: {row["Field"]}')
                continue
            doi = row['DOI']
            logger.debug(f"Processing Publication DOI {doi}")
            print(row['Title'], row['crossref'], row['scopus'])

            def clean_html(raw_html):
                soup = BeautifulSoup(raw_html, "html.parser")
                return soup.get_text()

            if not row['crossref']:
                logger.warning(f'Publication {row["DOI"]} has no crossref entry, ignoring for now...')
                continue

            crossref_metadata = self.crossref_manager.get_metadata(doi)
            title = html.unescape(unidecode(clean_html(crossref_metadata.get('title', [None])[0])))
            container_title = crossref_metadata.get('container-title', [None])
            journal_title = html.unescape(unidecode(container_title[0])) if container_title else None
            license_list = crossref_metadata.get('license')
            license_url = license_list[-1].get('URL', '') if license_list else None
            license_year = license_list[-1].get('start', {}).get('date-parts', [[None]])[0][0] if license_list else None
            page = crossref_metadata.get('page') if '-' in crossref_metadata.get('page', '') else ''
            article_number = crossref_metadata.get('article-number', '')

            license_type = 'open'
            pdf_found = self.download_pdf(crossref_metadata.get('link', [{}])[0].get('URL'), doi)
            if row['scopus']:
                scopus_metadata = self.scopus_manager.get_metadata(doi)
                affiliations_by_name = self.scopus_manager.extract_authors_affiliations(scopus_metadata)
                if int(scopus_metadata['abstracts-retrieval-response']['coredata']['openaccess'])!=1:
                    license_type = 'closed'
                date_issued_scopus = scopus_metadata['abstracts-retrieval-response']['item']['bibrecord']['head']['source']['publicationdate']
                date_issued = (f"{date_issued_scopus.get('day', '').zfill(2)}." if date_issued_scopus.get('day') else "") + \
                              (f"{date_issued_scopus.get('month', '').zfill(2)}." if date_issued_scopus.get('month') else "") + \
                              (date_issued_scopus.get('year', '') ).rstrip('.')
            else:
                logger.info(f'Scopus not available for {doi}, using crossref affiliations...')
                affiliations_by_name = self.crossref_manager.extract_authors_affiliations(crossref_metadata)
                date_issued_crossref = crossref_metadata['issued']['date-parts']
                date_issued = (f"{date_issued_crossref[0][2]}." if len(date_issued_crossref[0])==3 else "") + \
                              (f"{date_issued_crossref[0][1]}." if len(date_issued_crossref[0])>=2 else "") + \
                              (f"{date_issued_crossref[0][0]}")

            missing_pdf = True if license_type!='closed' and not pdf_found else False

            cleaned_author_list = self.compare_author_list_to_pure_db(affiliations_by_name)
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
                'Using Scopus': Cell(str(row['scopus']), 15)
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
            logger.info(f"Saved empty_path {empty_path} successfully.")
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
            logger.info(f"Saved {path_out} successfully.")
