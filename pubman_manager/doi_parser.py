# from fuzzywuzzy import process as fuzz
from fuzzywuzzy import fuzz, process


from unidecode import unidecode
from bs4 import BeautifulSoup
from pathlib import Path
import pandas as pd
from collections import OrderedDict, Counter
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
from pubman_manager.util import date_to_cell, load_yaml

logger = logging.getLogger(__name__)

AFFILIATION_MATCH_THRESHOLD = 90


class DecisionColor(Enum):
    def __new__(cls, comment: str):
        obj = object.__new__(cls)
        obj._value_ = len(cls.__members__) + 1
        obj.comment = comment
        return obj

    GREEN  = "PuRe match"                     # any PuRe-based outcome (fuzzy>=90 or fallback to PuRe)
    ORANGE = "No info from external APIs, adopting frequent PuRe result" # any PuRe-based outcome (fuzzy>=90 or fallback to PuRe)
    GRAY   = "Using publisher affiliation"    # external/publisher adopted
    PURPLE = "MPI affiliation detected"       # any MPI case (match/ambiguous/missing/resolved)
    RED    = "No affiliation information"     # nothing available


@dataclass
class AffiliationResult:
    affiliation: str
    color: DecisionColor
    compare_error: float = 0.0

    @property
    def comment(self) -> str:
        return self.color.comment + (f' {self.compare_error}' if self.compare_error else '')


def normalize_affiliation(text: str) -> str:
    if text.startswith('Current affiliation: '):
        text = text.replace('Current affiliation: ', '')
    return (text or "").replace("  ", ", ").replace(") ", "), ").strip()

def find_best_fuzzy_match(proposed: str, candidates: Iterable[str]) -> Tuple[Optional[str], float]:
    candidates = list(candidates)
    if not proposed or not candidates:
        return None, 1.0
    match, score = process.extractOne(proposed, candidates)  # score in [0..100]
    compare_error = (100 - score) / 100.0
    return (match if score >= AFFILIATION_MATCH_THRESHOLD else None), compare_error

class DOIParser:
    def __init__(self, pubman_api, scopus_api_key = None):
        self.crossref_manager = CrossrefManager()
        self.scopus_manager = ScopusManager(org_name = pubman_api.org_name, api_key=scopus_api_key)

        self.pubman_api = pubman_api
        raw_authors_info = load_yaml(PUBMAN_CACHE_DIR / 'authors_info.yaml')
        self.authors_affiliation_counters = {
            author: Counter(info["affiliation_counts"])
            for author, info in raw_authors_info.items()
        }
        mpi_affiliation_counter = Counter()
        for author, counter in self.authors_affiliation_counters.items():
            for affiliation, count in counter.items():
                if 'Max-Planck' in affiliation:
                    mpi_affiliation_counter[affiliation] += count
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
        fuzz_threshold: int = 97, # percentile value
    ) -> Dict[Tuple[str, str], List[AffiliationResult]]:
        """
        Compare external (Scopus/Crossref) affiliations to PuRe data.

        Colors encode status:
        GREEN  = PuRe-based (fuzzy>=90)
        ORANGE  = Adopt PuRe when no Info was given
        GRAY   = publisher/external
        PURPLE = MPI-Affiliation
        RED    = no information, leave blank

        Parameters
        ----------
        affiliations_by_author_name : dict
            Publication affiliation, mapping from (first_name, last_name) to list of affiliations (str) from publication.
        fuzz_threshold : int
            Fuzzy matching threshold (0-100) to match affiliation from PuRe to publication affiliation.

        Returns
        -------
        Dict[Tuple[str, str], List[AffiliationResult]]
            Mapping from (first_name, last_name) to list of PuRe-matched AffiliationResult

        """

        cache: Dict[str, str] = {}
        results_by_author: Dict[Tuple[str, str], List[AffiliationResult]] = {}

        for (first_name, last_name), publication_affiliations in affiliations_by_author_name.items():
            resolved_author: Tuple[str, str] = self.compare_author_name_to_pure_db(
                self.authors_affiliation_counters.keys(), first_name, last_name
            )
            author_results: List[AffiliationResult] = []
            pure_affiliations: List[str] = sorted(self.authors_affiliation_counters.get(resolved_author, {}).keys(),
                                                  key=lambda x: self.authors_affiliation_counters.get(resolved_author, {}).get(x, 0),
                                                  reverse=True) if resolved_author in self.authors_affiliation_counters else []

            for publication_affiliation in publication_affiliations:
                is_mpi = is_mpi_affiliation(publication_affiliation)

                if is_mpi:
                    mpi_affiliations_for_author = [a for a in pure_affiliations if is_mpi_affiliation(a)]
                    if mpi_affiliations_for_author:
                        # Pick the most frequent MPI affiliation for this author
                        author_results.append(AffiliationResult(mpi_affiliations_for_author[0], DecisionColor.PURPLE))
                    else:
                        # No PuRe MPI data, leave blank
                        author_results.append(AffiliationResult("", DecisionColor.PURPLE))
                    continue

                # Non-MPI
                if pure_affiliations:
                    if publication_affiliation in cache:
                        author_results.append(AffiliationResult(cache[publication_affiliation], DecisionColor.GREEN))
                    else:
                        external_pure_affiliations: List[str] = [affiliation for affiliation in pure_affiliations if not is_mpi_affiliation(affiliation)]
                        best_match, compare_error = find_best_fuzzy_match(publication_affiliation, external_pure_affiliations)
                        if best_match:
                            cache[publication_affiliation] = best_match
                            author_results.append(AffiliationResult(best_match, DecisionColor.GREEN, compare_error=compare_error))
                        else:
                            author_results.append(AffiliationResult(normalize_affiliation(publication_affiliation), DecisionColor.GRAY))
                else:
                    author_results.append(AffiliationResult(normalize_affiliation(publication_affiliation), DecisionColor.GRAY))

            if not author_results:
                author_results.append(AffiliationResult(affiliation=pure_affiliations[0] if pure_affiliations else "",
                                                        color=DecisionColor.RED))
            results_by_author[resolved_author] = author_results

        # Post-processing: Ensure consistency across authors
        assigned_mpi = [res.affiliation for author_results in results_by_author.values() for res in author_results if res.color == DecisionColor.PURPLE and res.affiliation]
        if assigned_mpi:
            most_common_aff = Counter(assigned_mpi).most_common(1)[0][0]
            for author, author_results in results_by_author.items():
                pure_affiliations = sorted(self.authors_affiliation_counters.get(author, {}).keys(), key=lambda x: self.authors_affiliation_counters.get(author, {}).get(x, 0), reverse=True) if author in self.authors_affiliation_counters else []
                for res in author_results:
                    # If an author's most common mpi group is not the same as the most common group from this publication,
                    # but the author has been part of this publication-specific group in the past, override common author group with common publication group
                    if res.color == DecisionColor.PURPLE and res.affiliation and res.affiliation != most_common_aff and \
                        most_common_aff in [a for a in pure_affiliations if is_mpi_affiliation(a)]:
                        res.affiliation = most_common_aff

        # Assume that very similar affiliations overlap (see fuzz_threshold)
        canon: list[str] = []
        for _author, res_list in results_by_author.items():
            for res in res_list:
                s = (res.affiliation or "").strip()
                if not s:
                    continue
                if not canon:
                    canon.append(s)
                    continue
                match = process.extractOne(s, canon, scorer=fuzz.token_set_ratio)
                if match and match[1] >= fuzz_threshold:
                    res.affiliation = match[0]
                else:
                    canon.append(s)
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
        pdf_path = FILES_DIR / f'{doi.replace("/", "")}.pdf'
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
            "metadata.identifiers": {
                "id": doi,
                "type": "DOI",
            }
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

    def get_dois_for_author(
        self,
        author: str,
        pubyear_start=None,
        pubyear_end=None,
        processed_dois: Optional[Iterable[str]] = None,
        split: bool = False,
    ) -> List[str] | Tuple[List[str], List[str]]:
        if isinstance(author, (tuple, list)):
            first_name = author[0] if author else ""
            last_name = " ".join(author[1:]).strip() if len(author) > 1 else ""
        else:
            parts = str(author).split()
            first_name = parts[0] if parts else ""
            last_name = " ".join(parts[1:]).strip()
        dois_crossref = self.crossref_manager.get_dois_for_author(first_name, last_name, pubyear_start, pubyear_end)
        dois_scopus = self.scopus_manager.get_dois_for_author(first_name, last_name, pubyear_start, pubyear_end)
        if processed_dois:
            processed_set = set(processed_dois)
            dois_crossref = [d for d in dois_crossref if d not in processed_set]
            dois_scopus = [d for d in dois_scopus if d not in processed_set]
        if split:
            return dois_crossref, dois_scopus
        return list(set(dois_crossref).union(set(dois_scopus)))

    def collect_data_for_dois(self, dois_crossref: List[str], dois_scopus: List[str]) -> pd.DataFrame:
        results = {}
        dois_to_process = list(set(list(dois_crossref) + list(dois_scopus)))
        for doi in dois_to_process:
            crossref_result = self.crossref_manager.get_overview(doi)
            results[doi] = crossref_result
        # with ThreadPoolExecutor(max_workers=2) as executor:
        #     futures = {executor.submit(self.crossref_manager.get_overview, doi): doi
        #             for doi in [d for d in dois_to_process if d in dois_crossref and d not in dois_to_skip]}
        #     for future in as_completed(futures):
        #         doi = futures[future]
        #         crossref_result = future.result()
        #         print("crossref_result",crossref_result)
        #         results[doi] = crossref_result
        for doi in dois_to_process:
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
        df = df.drop_duplicates(subset='DOI', keep='first').reset_index(drop=True)
        return df

    def process_dois(
        self,
        dois_data: pd.DataFrame,
        force: bool = False,
    ) -> List[OrderedDict[str, Tuple[str, int, str]]]:
        """
        Filter and enrich DOI data (MPI checks + author/affiliation matching).

        Returns a list of processed publication dicts that can be converted to table rows.
        """
        processed: List[OrderedDict[str, Tuple[str, int, str]]] = []
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
            if self.has_pubman_entry(doi, title=title):
                logger.info(f'Skipping {doi}, already exists in PuRe')
                continue
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
            has_affiliation_info = False
            for (first_name, last_name), affiliations_info in cleaned_author_list.items():
                for affiliation_info in affiliations_info:
                    if affiliation_info.affiliation:
                        has_affiliation_info = True
                    if is_mpi_affiliation(affiliation_info.affiliation):
                        is_mpi_publication = True
            if not is_mpi_publication and has_affiliation_info:
                logger.error('Publication has no author from Max Planck Institute, skipping...')
                continue

            missing_pdf = True if license_type!='closed' and not pdf_found else False
            authors_affiliations = []
            for (first_name, last_name), affiliations_info in cleaned_author_list.items():
                for affiliation_info in affiliations_info:
                    authors_affiliations.append((f"{first_name} {last_name}", affiliation_info))

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
                                  comment='Please upload the file and license info when submitting in PuRe'
                                  if missing_pdf else ''),
                'Crossref Link': Cell(link, 20),
                'Scopus Link': Cell(row.get('scopus', ''), 15),
            })
            i = 1
            for author_name, affiliation_info in authors_affiliations:
                prefill_publication[f"Author {i}"] = Cell(author_name)
                prefill_publication[f"Affiliation {i}"] = Cell(
                    affiliation_info.affiliation,
                    color=affiliation_info.color.name,
                    compare_error=affiliation_info.compare_error,
                    comment=affiliation_info.comment,
                )
                i += 1
            processed.append(prefill_publication)
        return processed


    def write_dois_data(self, path_out, dois_data):
        if not dois_data:
            empty_path = Path(os.path.abspath(path_out)).parent / f'{path_out.stem}_empty{path_out.suffix}'
            df = pd.DataFrame()
            df.to_excel(empty_path, index=False)
            logger.info(f"Saved empty_path {empty_path} successfully.")
        else:
            n_authors = 45
            deduped_prefills = []
            seen_doi_values = set()
            for entry in dois_data:
                doi_cell = entry.get('DOI')
                doi_value = getattr(doi_cell, 'data', None)
                normalized = doi_value.strip().lower() if isinstance(doi_value, str) else doi_value
                if normalized and normalized in seen_doi_values:
                    continue
                if normalized:
                    seen_doi_values.add(normalized)
                deduped_prefills.append(entry)

            if not deduped_prefills:
                logger.info("All DOIs were duplicates; no sheet written.")
                return

            column_details = OrderedDict({
                header: [cell.width, cell.comment]
                for header, cell in deduped_prefills[0].items()
                if 'Author ' not in header and 'Affiliation ' not in header
            })
            create_sheet(path_out, self.authors_affiliation_counters,
                        column_details, n_authors,'Title',
                        prefill_publications=deduped_prefills)
            logger.info(f"Saved {path_out} successfully.")
