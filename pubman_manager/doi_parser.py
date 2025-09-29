from fuzzywuzzy import process as fuzz
from unidecode import unidecode
from bs4 import BeautifulSoup
from pathlib import Path
import pandas as pd
from collections import OrderedDict, Counter
import yaml
import requests
import unicodedata
import os
import html
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import re
from enum import Enum
from dataclasses import dataclass

from typing import List, Dict, Tuple, Iterable, Optional

from pubman_manager import create_sheet, Cell, PUBMAN_CACHE_DIR, ScopusManager, CrossrefManager, FILES_DIR, is_mpi_affiliation
from pubman_manager.util import date_to_cell

logger = logging.getLogger(__name__)

AFFILIATION_MATCH_THRESHOLD = 90


class DecisionColor(Enum):
    def __new__(cls, comment: str):
        obj = object.__new__(cls)
        obj._value_ = len(cls.__members__) + 1
        obj.comment = comment
        return obj

    GREEN   = "PuRe match"                     # any PuRe-based outcome (fuzzy>=90 or fallback to PuRe)
    GRAY    = "Using publisher affiliation"    # external/publisher adopted
    PURPLE  = "MPI affiliation detected"       # any MPI case (match/ambiguous/missing/resolved)
    RED = "No affiliation information"     # nothing available


@dataclass
class AffiliationResult:
    affiliation: str
    color: DecisionColor
    compare_error: float = 0.0

    @property
    def comment(self) -> str:
        return self.color.comment + (f' {self.compare_error}' if self.compare_error else '')


def normalize_affiliation(text: str) -> str:
    return (text or "").replace("  ", ", ").replace(") ", "), ").strip()

def find_best_fuzzy_match(proposed: str, candidates: Iterable[str]) -> Tuple[Optional[str], float]:
    candidates = list(candidates)
    if not proposed or not candidates:
        return None, 1.0
    match, score = fuzz.extractOne(proposed, candidates)  # score in [0..100]
    compare_error = (100 - score) / 100.0
    return (match if score >= AFFILIATION_MATCH_THRESHOLD else None), compare_error

class DOIParser:
    def __init__(self, pubman_api, scopus_api_key = None):
        self.crossref_manager = CrossrefManager()
        self.scopus_manager = ScopusManager(org_name = pubman_api.org_name, api_key=scopus_api_key)

        self.pubman_api = pubman_api
        with open(PUBMAN_CACHE_DIR / 'authors_info.yaml', 'r', encoding='utf-8') as f:
            self.affiliations_by_name_pubman = yaml.load(f, Loader=yaml.FullLoader)
        mpi_affiliation_counter = Counter()
        for author, author_info in self.affiliations_by_name_pubman.items():
            for affiliation in author_info['affiliations']:
                if 'Max-Planck' in affiliation:
                    mpi_affiliation_counter[affiliation]+=1
        self.mpi_affiliations = [item[0] for item in sorted(mpi_affiliation_counter.items(), key=lambda x: x[1], reverse=True)]
        self.af_id_ = None

    def compare_author_name_to_pure_db(
        self,
        pure_author_names: Iterable[Tuple[str, str]],
        first_name: str,
        surname: str,
    ) -> Tuple[str, str]:
        """
        Compare name to all authors in PuRe DB to make sure middle names or different writing styles match.

        Returns corrected name if correction was needed.
        """
        def normalize_name_for_comparison(name: str) -> str:
            ascii_name = unicodedata.normalize("NFD", name).encode("ascii", "ignore").decode("utf-8")
            spaced_hyphens = ascii_name.replace("-", " ")
            camel_split = re.sub(r"([a-z])([A-Z])", r"\1 \2", spaced_hyphens)
            stripped = camel_split.replace(".", "").lower()
            return "".join(stripped.split())

        surname_key = normalize_name_for_comparison(surname)
        first_name_key = normalize_name_for_comparison(first_name)

        for pure_first, pure_last in pure_author_names:
            pure_last_key = normalize_name_for_comparison(pure_last)
            pure_first_key = normalize_name_for_comparison(pure_first)
            if surname_key == pure_last_key and first_name_key == pure_first_key:
                return pure_first, pure_last

        first_name_without_middles = first_name.split()[0] if first_name else ""
        return first_name_without_middles, surname

    def compare_author_list_to_pure_db(
        self,
        affiliations_by_author_name: Dict[Tuple[str, str], List[str]],
    ) -> Dict[Tuple[str, str], List[AffiliationResult]]:
        """
        Compare external (Scopus/Crossref) affiliations to PuRe data.

        Colors encode status:
        GREEN  = PuRe-based (fuzzy>=90 or fallback-to-PuRe)
        GRAY   = publisher/external
        PURPLE = MPI-Affiliation
        RED    = no information, leave blank
        """

        external_to_pure_affiliation_cache: Dict[str, str] = {}
        mpi_group_frequencies: Counter = Counter()
        pending_mpi_indices_by_author: Dict[Tuple[str, str], List[int]] = {}

        results_by_author: Dict[Tuple[str, str], List[AffiliationResult]] = {}

        for (first_name, last_name), proposed_affiliations in affiliations_by_author_name.items():
            resolved_author: Tuple[str, str] = self.compare_author_name_to_pure_db(
                self.affiliations_by_name_pubman.keys(), first_name, last_name
            )
            author_results: List[AffiliationResult] = []
            pure_affiliations: List[str] = list(self.affiliations_by_name_pubman.get(resolved_author, {}).get("affiliations", []))
            external_pure_affiliations: List[str] = [affiliation for affiliation in pure_affiliations if not is_mpi_affiliation(affiliation)]

            for proposed_affiliation in proposed_affiliations:
                if pure_affiliations:
                    if is_mpi_affiliation(proposed_affiliation):
                        mpi_affiliations_for_author = [a for a in pure_affiliations if is_mpi_affiliation(a)]
                        if len(mpi_affiliations_for_author) == 1:
                            mpi_group = mpi_affiliations_for_author[0]
                            author_results.append(AffiliationResult(affiliation=mpi_group, color=DecisionColor.PURPLE))
                            mpi_group_frequencies[mpi_group] += 1
                        else:
                            author_results.append(AffiliationResult(affiliation="", color=DecisionColor.PURPLE))
                            pending_mpi_indices_by_author.setdefault(resolved_author, []).append(len(author_results) - 1)
                    else:
                        if proposed_affiliation in external_to_pure_affiliation_cache:
                            mapped = external_to_pure_affiliation_cache[proposed_affiliation]
                            author_results.append(AffiliationResult(affiliation=mapped, color=DecisionColor.GREEN))
                        else:
                            best_match, compare_error = find_best_fuzzy_match(proposed_affiliation, external_pure_affiliations)
                            if best_match:
                                external_to_pure_affiliation_cache[proposed_affiliation] = best_match
                                author_results.append(
                                    AffiliationResult(affiliation=best_match, color=DecisionColor.GREEN, compare_error=compare_error)
                                )
                            else:
                                author_results.append(
                                    AffiliationResult(affiliation=normalize_affiliation(proposed_affiliation), color=DecisionColor.GRAY)
                                )
                else:
                    if is_mpi_affiliation(proposed_affiliation):
                        author_results.append(AffiliationResult(affiliation="", color=DecisionColor.PURPLE))
                        pending_mpi_indices_by_author.setdefault(resolved_author, []).append(len(author_results) - 1)
                    else:
                        author_results.append(
                            AffiliationResult(affiliation=normalize_affiliation(proposed_affiliation), color=DecisionColor.GRAY)
                        )

            if not author_results:
                if pure_affiliations:
                    author_results.append(AffiliationResult(affiliation=pure_affiliations[0], color=DecisionColor.GREEN))
                else:
                    author_results.append(AffiliationResult(affiliation="", color=DecisionColor.RED))

            results_by_author[resolved_author] = author_results

        most_common_mpi_group: str = (
            mpi_group_frequencies.most_common(1)[0][0] if mpi_group_frequencies else self.mpi_affiliations[0]
        )

        for author_key, indices in pending_mpi_indices_by_author.items():
            for idx in indices:
                results_by_author[author_key][idx].affiliation = most_common_mpi_group  # dataclass is mutable

        return results_by_author

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
                        # cleaned_html = BeautifulSoup(response.text, "html.parser").text
                        logger.error(f"Failed to download PDF. Status code: {response.status_code}")
                        break  # Stop retrying if the server returns a valid response but not a 200.
                except requests.exceptions.RequestException as e:
                    logger.warning(f"Error downloading PDF on attempt {attempt + 1}: {e}")
                    attempt += 1

            logger.error(f"Failed to download PDF after {retries} attempts for DOI: {doi}")
            return False

    def has_pubman_entry(self, doi, title=None):
        pub = self.pubman_api.search_publication_by_criteria({
                    "metadata.identifiers.id": doi,
                    "metadata.identifiers.type": 'DOI'
                })
        if not pub and title:
            logger.info(f'Unable to find DOI match in PuRe database, trying to find title instead: "{title}"')
            if len(title) < 50:
                pub = self.pubman_api.search_publication_by_criteria({"metadata.title": title})
            else:
                title_words = title.split(' ')
                pub_first_half = self.pubman_api.search_publication_by_criteria({"metadata.title": ' '.join(title_words[:int(len(title_words)//1.5)])})
                pub_latter_half = self.pubman_api.search_publication_by_criteria({"metadata.title": ' '.join(title_words[int(len(title_words)//1.5):])})
                if pub_first_half or pub_latter_half:
                    logger.info(f'Found Title match in Database, ignoring new entry')
                else:
                    pub = pub_first_half if pub_first_half else pub_latter_half
        return bool(pub)

    def get_dois_for_author(self, author: str, pubyear_start=None, pubyear_end=None) -> List[str]:
        dois_crossref = self.crossref_manager.get_dois_for_author(author, pubyear_start, pubyear_end)
        dois_scopus = self.scopus_manager.get_dois_for_author(author, pubyear_start, pubyear_end)
        return list(set(dois_crossref).union(set(dois_scopus)))

    def collect_data_for_dois(self, dois_crossref: List[str], dois_scopus: List[str], processed_dois=None) -> pd.DataFrame:
        results = {}
        dois = set(list(dois_crossref) + list(dois_scopus))
        processed_dois = set(processed_dois) if processed_dois else set()
        dois = set(dois)
        dois_to_process = list(dois - processed_dois)
        dois_to_skip = list(dois & processed_dois)

        for doi in dois_to_process:
            if self.has_pubman_entry(doi):
                logger.info(f'Found Publication for {doi} in PuRe, skipping...')
                dois_to_skip.append(doi)

        if dois_to_skip:
            logger.info(f'Skipping already processed dois: {len(dois_to_skip)}')

        results = {}
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = {executor.submit(self.crossref_manager.get_overview, doi): doi
                    for doi in [d for d in dois_to_process if d in dois_crossref and d not in dois_to_skip]}
            for future in as_completed(futures):
                doi = futures[future]
                crossref_result = future.result()
                results[doi] = crossref_result
        for doi in [doi for doi in dois_to_process if not doi in dois_to_skip]:
            scopus_result = self.scopus_manager.get_overview(doi)
            if scopus_result:
                if doi not in results:
                    results[doi] = scopus_result
                else: # if we already have a crossref entry, we merge the results
                    results[doi]['scopus'] = scopus_result['scopus']
                    if scopus_result.get('Field'):
                        results[doi]['Field'] = results[doi].get('Field', []) + scopus_result['Field']

        if not results:
            return None
        for doi in results:
            if results[doi].get('Field'):
                results[doi]['Field'] = '\n'.join(results[doi]['Field'])
        df = pd.DataFrame.from_dict(results, orient='index').reset_index()
        df.rename(columns={'index': 'DOI'}, inplace=True)
        return df

    def generate_table_from_dois_data(self,
                                      dois_data: pd.DataFrame,
                                      force=False) -> List[OrderedDict[str, Tuple[str, int, str]]]:
        """
        Takes overview dataframe, collects all data for DOIs which are not yet on PuRe and have Scopus and Crossref entries.
        Generates dataframe which can be passed to the excel_generator.create_sheet method to prefill the sheet with data.

        Result
        ------

        Each entry in the result list is a dict that corresponds to a publication.
        The dict maps column data to a tuple, e.g. `"Title": (title, 35)`
        Where "title" is the value for this column, "35" is the width of the column on the excel, and the last entry is an optional Tooltip to be displayed

        """
        table_overview = []
        for index, row in dois_data.iterrows():
            if row['Field'] and not force:
                logger.info(f'Skipping {row["DOI"]}, reason: {row["Field"]}')
                continue

            doi = row['DOI']

            logger.debug(f"Processing Publication DOI {doi}")

            def clean_html(raw_html):
                soup = BeautifulSoup(raw_html, "html.parser")
                return soup.get_text()

            if not row['crossref']:
                logger.warning(f'Publication {row["DOI"]} has no crossref entry, ignoring for now...')
                continue

            crossref_metadata = self.crossref_manager.get_metadata(doi)
            container_title = crossref_metadata.get('container-title', [None])
            journal_title = html.unescape(unidecode(container_title[0])) if container_title else None
            unused_journals = ['Meeting Abstract', 'iopscience']
            skip = False
            for name in unused_journals:
                if journal_title and name in journal_title:
                    logger.warning(f'Skipping unused journal {name}: {journal_title}')
                    skip = True
                    break
            if skip:
                continue

            link = crossref_metadata.get('resource', {}).get('primary', {}).get('URL', '')
            unused_sites = ['researchhub', 'iopscience']
            skip = False
            for site in unused_sites:
                if site in link:
                    logger.warning(f'Skipping link from {site}: {link}')
                    skip = True
                    break
            if skip:
                continue

            title = html.unescape(unidecode(clean_html(crossref_metadata.get('title', [None])[0])))
            license_list = crossref_metadata.get('license')
            license_url = license_list[-1].get('URL', '') if license_list else None
            license_year = license_list[-1].get('start', {}).get('date-parts', [[None]])[0][0] if license_list else None
            page = crossref_metadata.get('page') if '-' in crossref_metadata.get('page', '') else ''
            article_number = crossref_metadata.get('article-number', '')

            license_type = 'open'
            pdf_found = self.download_pdf(crossref_metadata.get('link', [{}])[0].get('URL'), doi)

            if row.get('scopus') and self.scopus_manager.get_metadata(doi).get('abstracts-retrieval-response'):
                scopus_metadata = self.scopus_manager.get_metadata(doi)
                affiliations_by_name = self.scopus_manager.extract_authors_affiliations(scopus_metadata)
                if not affiliations_by_name:
                    affiliations_by_name = self.crossref_manager.extract_authors_affiliations(crossref_metadata)
                open_access = scopus_metadata['abstracts-retrieval-response']['coredata']['openaccess']
                if open_access is not None and int(open_access)!=1:
                    license_type = 'closed'
                date_issued_scopus = scopus_metadata['abstracts-retrieval-response']['item']['bibrecord']['head']['source']['publicationdate']
                date_issued = (f"{date_issued_scopus.get('day', '').zfill(2)}." if date_issued_scopus.get('day') else "") + \
                              (f"{date_issued_scopus.get('month', '').zfill(2)}." if date_issued_scopus.get('month') else "") + \
                              (date_issued_scopus.get('year', '') ).rstrip('.')
            else:
                logger.info(f'Scopus not available for {doi}, using crossref affiliations...')
                affiliations_by_name = self.crossref_manager.extract_authors_affiliations(crossref_metadata)
                date_issued_crossref = crossref_metadata.get('published-print', crossref_metadata.get('issued', {}))['date-parts']
                date_issued = (f"{date_issued_crossref[0][2]}." if len(date_issued_crossref[0])==3 else "") + \
                              (f"{date_issued_crossref[0][1]}." if len(date_issued_crossref[0])>=2 else "") + \
                              (f"{date_issued_crossref[0][0]}")


            from datetime import date
            from calendar import monthrange

            def is_older_than_six_months(s: str, today: date | None = None) -> bool:
                today = today or date.today()
                total = today.year * 12 + (today.month - 1) - 6
                cy, m0 = divmod(total, 12)
                cm = m0 + 1
                cutoff = date(cy, cm, min(today.day, monthrange(cy, cm)[1]))
                t = s.strip().replace("/", "-").replace(".", "-")
                parts = t.split("-")
                def end_of_month(y, m): return date(y, m, monthrange(y, m)[1])
                if len(parts) == 1 and parts[0].isdigit() and len(parts[0]) == 4:
                    y = int(parts[0]); end = date(y, 12, 31)
                elif len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit():
                    a, b = parts
                    if len(a) == 4:   # YYYY-MM
                        y, m = int(a), int(b)
                    elif len(b) == 4: # MM-YYYY
                        y, m = int(b), int(a)
                    else:
                        raise ValueError(f"Ambiguous 2-part date: {s!r}")
                    if not (1 <= m <= 12): raise ValueError(f"Invalid month: {m}")
                    end = end_of_month(y, m)
                elif len(parts) == 3 and all(p.isdigit() for p in parts):
                    a, b, c = parts
                    if len(a) == 4:            # YYYY-MM-DD
                        y, m, d = int(a), int(b), int(c)
                    elif len(c) == 4:          # DD-MM-YYYY (day-first)
                        y, m, d = int(c), int(b), int(a)
                    else:
                        raise ValueError(f"Ambiguous 3-part date: {s!r}")
                    if not (1 <= m <= 12): raise ValueError(f"Invalid month: {m}")
                    if not (1 <= d <= monthrange(y, m)[1]): raise ValueError(f"Invalid day: {d}")
                    end = date(y, m, d)
                else:
                    raise ValueError(f"Unrecognized date format: {s!r}")
                return end < cutoff

            if not page and not article_number and not is_older_than_six_months(date_issued):
                logger.info(f'Skipping {row["DOI"]}, no page or article number specified and newer than 6 months, probably still a preprint')
                continue
            cleaned_author_list = self.compare_author_list_to_pure_db(affiliations_by_name)
            is_mpi_publication = False
            for (first_name, last_name), affiliations_info in cleaned_author_list.items():
                for affiliation_info in affiliations_info:
                    if is_mpi_affiliation(affiliation_info.affiliation):
                        is_mpi_publication = True
            if not is_mpi_publication:
                logger.error('Publication has no author from Max Planck Institute, skipping...')
                continue

            missing_pdf = True if license_type!='closed' and not pdf_found else False
            prefill_publication = OrderedDict({
                "Title": Cell(title, 35),
                "Journal Title": Cell(journal_title, 25),
                "Publisher": Cell(html.unescape(unidecode(crossref_metadata.get('publisher', None)) or ''), 20),
                "Issue": Cell(crossref_metadata.get('issue', None), 10),
                "Volume": Cell(crossref_metadata.get('volume', None), 10),
                "Page": Cell(page, 10, color='RED' if not article_number and not page else ''),
                'Article Number': Cell(article_number, 10, color='RED' if not article_number and not page else '', force_text=True),
                "ISSN": Cell(html.unescape(unidecode(crossref_metadata.get('ISSN', [None])[0] or '')), 15),
                "Date published online": Cell(date_to_cell(crossref_metadata.get('created', {}).get('date-time', None)), 20, force_text=True),
                'Date issued': Cell(date_issued, 20, force_text=True),
                'DOI': Cell(doi, 20, force_text=True),
                'License url': Cell(license_url if license_type=='open' else '', 20),
                'License year': Cell(license_year if license_type=='open' else '', 15),
                'Pdf found': Cell('' if license_type=='closed' else 'y' if pdf_found else 'n', 15,
                                  color='RED' if missing_pdf else '',
                                  comment = 'Please upload the file and license info when submitting in PuRe'
                                  if missing_pdf else ''),
                'Crossref Link': Cell(link, 20),
                'Scopus Link': Cell(row.get('scopus', ''), 15)
            })
            i = 1
            for (first_name, last_name), affiliations_info in cleaned_author_list.items():
                for affiliation_info in affiliations_info:
                    prefill_publication[f"Author {i}"] = Cell(first_name + ' ' + last_name)
                    prefill_publication[f"Affiliation {i}"] = Cell(affiliation_info.affiliation,
                                                                   color = affiliation_info.color.name,
                                                                   compare_error = affiliation_info.compare_error,
                                                                   comment = affiliation_info.comment)
                    i = i+1
            table_overview.append(prefill_publication)
        return table_overview

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
