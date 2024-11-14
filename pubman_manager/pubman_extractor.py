from pubman_manager import PubmanBase, PUBMAN_CACHE_DIR
import yaml
import requests
import json
from fuzzywuzzy import fuzz

class PubmanExtractor(PubmanBase):

    def extract_org_data(self, org_id):
        (PUBMAN_CACHE_DIR / org_id).mkdir(parents=True, exist_ok=True)
        publications = []
        org_publications = self.search_publications_by_organization(org_id, size=200000)
        publications.extend(org_publications)
        with open(PUBMAN_CACHE_DIR / org_id / "publications.yaml", "w") as f:
            yaml.dump(publications, f)
        authors_info = self.extract_authors_info(publications)
        authors_info[('Dierk', 'Raabe')]['affiliations'] = ['Microstructure Physics and Alloy Design, Max-Planck-Institut für Eisenforschung GmbH, Max Planck Society']
        with open(PUBMAN_CACHE_DIR / org_id / "authors_info.yaml", "w") as f:
            yaml.dump(authors_info, f)
        with open(PUBMAN_CACHE_DIR / org_id / 'identifier_paths.yaml', 'w', encoding='utf-8') as f:
            yaml.dump(self.extract_organization_mapping(publications), f)

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

    def process_affiliations(self, affiliation_list):
        """
        Returns a reduced list of affiliations, based on Levenshtein distance <= 0.15 using fuzzywuzzy.
        """
        reduced_affiliations = []
        for affiliation in affiliation_list:
            if '_' in affiliation or 'x0' in affiliation:
                continue
            affiliation = affiliation.strip()
            found_similar = False
            for reduced_affiliation in reduced_affiliations:
                ratio = fuzz.ratio(affiliation, reduced_affiliation)
                if ratio >= 85:
                    found_similar = True
                    break

            if not found_similar:
                reduced_affiliations.append(affiliation.replace('\n',' ').replace('  ', ' '))
        return reduced_affiliations

    def extract_authors_info(self, publications):
        authors_info = {}
        for record in publications:
            metadata = record.get('data', {}).get('metadata', {})
            creators = metadata.get('creators', [])
            for creator in creators:
                person = creator.get('person', {})
                given_name = person.get('givenName', '')
                family_name = person.get('familyName', '')
                # Entries like 'Materials Science International Team, MSIT®' have no first name, ignore
                if given_name and family_name:
                    organizations = person.get('organizations', [])
                    affiliation_list = self.process_affiliations([org['name'] for org in organizations])
                    if (full_name:=(given_name, family_name)) not in authors_info:
                        authors_info[full_name] = {}
                    if 'affiliations' in authors_info[full_name]:
                        authors_info[full_name]['affiliations'].update(affiliation_list)
                    else:
                        authors_info[full_name]['affiliations'] = set(affiliation_list)
                    if (identifier := person.get('identifier')) and 'identifier' not in authors_info[full_name]:
                        authors_info[full_name]['identifier'] = identifier

        to_remove = []
        for author in authors_info:
            if '.' in author[0]:
                if len(author[0].split()[0])<=2:
                    for author_ in authors_info:
                        if len(author[0]) > 2 and author_[1] == author[1] and author[0][0] == author_[0][0]:
                            to_remove.append(author)
                elif (author[0].split()[0], author[1]) in authors_info:
                    to_remove.append(author)
        for author in set(to_remove):
            del authors_info[author]

        full_names = set()
        abbreviated_names = set()
        for name in authors_info.keys():
            if '.' not in name[0].split()[0]:
                full_names.add(name)
            else:
                abbreviated_names.add(name)
        for abbreviated_name in abbreviated_names:
            for full_name in full_names:
                if abbreviated_name[1] == full_name[1]:
                    if (not '.' in abbreviated_name[0].split()[0] and abbreviated_name[0].split()[0] == full_name[0].split()[0]) or \
                       ('.' in abbreviated_name[0].split()[0] and abbreviated_name[0].split()[0][0] == full_name[0].split()[0][0]):
                      authors_info.pop(abbreviated_name)
                      break
        return authors_info

    def extract_journal_names(self, publications):
        journals = {}
        for record in publications:
            metadata = record.get('data', {}).get('metadata', {})
            sources = metadata.get('sources', [])
            for source in sources:
                if source.get('title') and source['title'] not in journals:
                    journals[source.get('title')] = {
                        'alternativeTitles': source.get('alternativeTitles'),
                        'genre': source.get('genre'),
                        'publishingInfo': source.get('publishingInfo'),
                        'identifiers': {identifier['type']: identifier['id'] for identifier in source.get('identifiers', []) if 'type' in identifier}
                    }
                break
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
        items = results.get('records', {})
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
