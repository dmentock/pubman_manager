from pubman_manager import PubmanBase, PUBMAN_CACHE_DIR
import requests
import json
from fuzzywuzzy import fuzz, process
from collections import Counter, defaultdict
from pathlib import Path
from pubman_manager.util import save_yaml, load_yaml, normalize_user_id

class PubmanExtractor(PubmanBase):

    def extract_org_data(self, org_id, cache_dir: Path | None = None):
        if cache_dir is None:
            cache_dir = PUBMAN_CACHE_DIR / f"user_{normalize_user_id(self.user_id)}"
        cache_dir.mkdir(parents=True, exist_ok=True)
        publications = []
        org_publications = self.search_publications_by_organization(org_id, size=200000)
        publications.extend(org_publications)
        save_yaml(publications, cache_dir / "publications.yaml")
        authors_info = self.extract_authors_info(publications)
        save_yaml(authors_info, cache_dir / "authors_info.yaml")
        save_yaml(self.extract_organization_mapping(publications), cache_dir / "identifier_paths.yaml")
        journals = self.extract_journals(publications)
        save_yaml(journals, cache_dir / "journals.yaml")

    def extract_organization_mapping(self, data):
        organizations = {}
        for publication in data:
            creators = publication.get('data', {}).get('metadata', {}).get('creators', [])
            for creator in creators:
                orgs = creator.get('person', {}).get('organizations', [])
                for org in orgs:
                    org_name = org.get('name')
                    if org_name and org_name not in organizations and org.get('identifier') not in ['ou_persistent22', 'persistent22'] and org.get('identifierPath'):
                        organizations[org_name] = org.get('identifierPath')
        return organizations

    def _canonicalize_and_rank_affiliations(self, affs, *, threshold=95, replace_old_new=None):
        """
        Cluster near-duplicate strings (token_set_ratio >= threshold) and
        count occurrences. Returns a list of canonical strings sorted by
        descending frequency; ties keep first-seen order.

        Parameters
        ----------
        affs : List[str]
            Raw affiliations aggregated across all publications for one author.
        threshold : int
            FuzzyWuzzy similarity threshold (0..100). 95 ≈ 0.05 distance.
        """
        canon = []              # first-seen representatives
        counts = Counter()      # rep -> frequency
        old, new = (replace_old_new or (None, None))

        for s in affs:
            s = s.strip().replace('\n', ' ').replace('  ', ' ')
            if old:
                s = s.replace(old, new)

            if not canon:
                canon.append(s)
                counts[s] += 1
                continue

            match = process.extractOne(s, canon, scorer=fuzz.token_set_ratio)
            if match and match[1] >= threshold:
                rep = match[0]
            else:
                rep = s
                canon.append(rep)

            counts[rep] += 1

        # sort by frequency desc; tie-breaker = first-seen order (index in canon)
        order_index = {c: i for i, c in enumerate(canon)}
        ranked = sorted(counts.keys(), key=lambda x: (-counts[x], order_index[x]))
        return counts

    def process_affiliations(self, affiliation_list):
        """
        Return a reduced list of affiliations for a single record.

        We collapse near-duplicates within this one publication using
        fuzzywuzzy token_set_ratio >= 85 (~ Levenshtein distance <= 0.15).
        """
        reduced = []
        for affiliation in affiliation_list:
            if '_' in affiliation or 'x0' in affiliation:
                continue
            s = affiliation.strip().replace('\n', ' ').replace('  ', ' ')
            found = False
            for r in reduced:
                if fuzz.token_set_ratio(s, r) >= 85:
                    found = True
                    break
            if not found:
                reduced.append(s.strip())
        return reduced

    def extract_authors_info(self, publications):
        # no need for smart_deduplicate; we will cluster at the end
        authors_info = {}
        raw_affiliations_by_author = defaultdict(list)
        for record in publications:
            metadata = record.get('data', {}).get('metadata', {})
            creators = metadata.get('creators', [])
            for creator in creators:
                person = creator.get('person', {})
                given_name = person.get('givenName', '')
                family_name = person.get('familyName', '')

                if not (given_name and family_name):
                    continue

                organizations = person.get('organizations', [])
                affiliation_list = self.process_affiliations([org['name'] for org in organizations])

                full_name = (given_name, family_name)
                if full_name not in authors_info:
                    authors_info[full_name] = {}

                raw_affiliations_by_author[full_name].extend(affiliation_list)

                if (identifier := person.get('identifier')) and 'identifier' not in authors_info[full_name]:
                    authors_info[full_name]['identifier'] = identifier

        # remove ambiguous abbreviated-name duplicates
        to_remove = []
        for author in authors_info:
            if '.' in author[0]:
                if len(author[0].split()[0]) <= 2:
                    for author_ in authors_info:
                        if len(author[0]) > 2 and author_[1] == author[1] and author[0][0] == author_[0][0]:
                            to_remove.append(author)
                elif (author[0].split()[0], author[1]) in authors_info:
                    to_remove.append(author)
        for author in set(to_remove):
            del authors_info[author]

        # prefer full names over abbreviated variants that share the same surname
        full_names = {name for name in authors_info.keys() if '.' not in name[0].split()[0]}
        abbreviated_names = {name for name in authors_info.keys() if '.' in name[0].split()[0]}
        for abbreviated_name in abbreviated_names:
            for full_name in full_names:
                if abbreviated_name[1] != full_name[1]:
                    continue
                ab_first = abbreviated_name[0].split()[0]
                full_first = full_name[0].split()[0]
                if ('.' not in ab_first and ab_first == full_first) or ('.' in ab_first and ab_first[0] == full_first[0]):
                    authors_info.pop(abbreviated_name, None)
                    break

        #  cluster near-duplicates and sort by frequency
        for author in authors_info:
            raw_list = list(raw_affiliations_by_author.get(author, []))
            unified_counts = self._canonicalize_and_rank_affiliations(
                raw_list,
                threshold=95,                     # adjust to taste (97 for stricter)
            )
            authors_info[author]['affiliation_counts'] = dict(unified_counts)

        director_path = Path(__file__).resolve().parents[1] / "director_affiliations.yaml"
        entries = load_yaml(director_path)
        for entry in entries:
            first = entry["first_name"]
            last = entry["last_name"]
            affiliation = entry["affiliation"]
            key = (first, last)
            authors_info.setdefault(key, {})
            authors_info[key]["affiliation_counts"] = {affiliation: 1}
        return authors_info

    def extract_journals(self, publications):
        journals = {}
        for record in publications:
            metadata = record.get('data', {}).get('metadata', {})
            sources = metadata.get('sources', [])
            for source in sources:
                ids = {i.get('type'): i.get('id') for i in source.get('identifiers', [])}
                if {'CONE', 'ISSN'} <= ids.keys() and ids['ISSN'] not in journals:
                    journals[ids['ISSN']] = {
                        'alternativeTitles': source.get('alternativeTitles'),
                        'genre': source.get('genre'),
                        'publishingInfo': source.get('publishingInfo'),
                        'cone': ids['CONE'],
                        'title': source['title'],
                    }
        return journals

    def search_publications_by_organization(self, organization_id, size=50):
        query = {
            "query": {
                "nested": {
                    "path": "metadata.creators.person.organizations",
                    "query": {
                        "bool": {
                            "must": [
                                {
                                    "match_phrase": {
                                        "metadata.creators.person.organizations.identifierPath": organization_id
                                    }
                                }
                            ]
                        }
                    }
                },
            },
            "sort": [
                {
                    "metadata.datePublishedInPrint": {
                        "order": "desc"
                    }
                }
            ],
            "size": size  # Adjust the size as needed
        }
        headers = {
            "Authorization": self.auth_token,
            "Content-Type": "application/json"
        }
        response = requests.post(
            f"{self.base_url}/items/search?scroll=true",
            headers=headers,
            data=json.dumps(query)
        )
        if response.status_code != 200:
            raise Exception("Failed to search for publications", response.status_code)
        results = response.json()
        items = results.get('records', [])
        if isinstance(items, dict):
            items = items.get('hits', {}).get('hits', []) or []
        elif not isinstance(items, list):
            items = list(items)
        scroll_id = results.get('scrollId')
        while scroll_id:
            scroll_response = self.fetch_scroll_results(scroll_id)
            if not scroll_response:
                break
            items.extend(scroll_response.get('hits', {}).get('hits', []))
            scroll_id = scroll_response.get('_scroll_id')
        return items

    def fetch_all_organizations(self, size=10000):
        query = {
            "query": {
                "match_all": {}
            },
            "size": size,
            "_source": ["metadata.creators.person.organizations"]
        }
        headers = {
            "Authorization": self.auth_token,
            "Content-Type": "application/json"
        }
        response = requests.post(
            f"{self.base_url}/items/search",
            headers=headers,
            data=json.dumps(query)
        )

        if response.status_code != 200:
            raise Exception(f"Failed to fetch organizations: {response.status_code}")
        results = response.json().get('records', [])
        organizations = {}
        for record in results:
            data = record.get('data', {})
            metadata = data.get('metadata', {})
            for creator in metadata.get("creators", []):
                person = creator.get("person", {})
                for org in person.get("organizations", []):
                    org_id = org.get("identifier")
                    org_name = org.get("name")
                    if org_id and org_name:
                        organizations[org_name.strip()] = org_id.strip()

        return organizations
