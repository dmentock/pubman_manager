from pubman_manager import PubmanBase
import yaml
import requests
import json

class PubmanExtractor(PubmanBase):
    def extract_organization_mapping(self, yaml_file):
        with open(yaml_file, 'r', encoding='utf-8') as file:
            data = yaml.safe_load(file)
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

    def extract_authors_info(self, publications):
        authors_info = {}
        for record in publications:
            metadata = record.get('data', {}).get('metadata', {})
            creators = metadata.get('creators', [])
            for creator in creators:
                person = creator.get('person', {})
                given_name = person.get('givenName', '')
                family_name = person.get('familyName', '')
                full_name = f"{given_name} {family_name}".strip()
                organizations = person.get('organizations', [])
                affiliation_list = [org['name'] for org in organizations]

                if full_name not in authors_info:
                    authors_info[full_name] = {}
                if 'affiliations' in authors_info[full_name]:
                    authors_info[full_name]['affiliations'].update(affiliation_list)
                else:
                    authors_info[full_name]['affiliations'] = set(affiliation_list)
                if (identifier := person.get('identifier')) and 'identifier' not in authors_info[full_name]:
                    authors_info[full_name]['identifier'] = identifier

        for author in authors_info:
            if 'affiliations' in authors_info[author]:
                authors_info[author]['affiliations'] = list(authors_info[author]['affiliations'])

        full_names = set()
        abbreviated_names = []

        for name in authors_info.keys():
            if '.' not in name.split()[0]:
                full_names.add(name)
            else:
                abbreviated_names.append(name)
        def has_full_name_equivalent(abbreviated, full_names):
            abbrev_parts = abbreviated.split()
            abbrev_initials = [part[0] for part in abbrev_parts if '.' in part]

            for full_name in full_names:
                full_name_parts = full_name.split()
                if len(full_name_parts) >= len(abbrev_initials) and all(full_name_parts[i][0] == abbrev_initials[i] for i in range(len(abbrev_initials))):
                    return True
            return False
        return {name: affiliations for name, affiliations in authors_info.items()
                 if not ('.' in name.split()[0] and has_full_name_equivalent(name, full_names))}


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

        # Extracting the list of records
        results = response.json().get('records', [])

        organizations = {}
        for record in results:
            # Access the 'data' key where the publication metadata is stored
            data = record.get('data', {})
            metadata = data.get('metadata', {})

            # Loop through the creators to extract organization information
            for creator in metadata.get("creators", []):
                person = creator.get("person", {})
                for org in person.get("organizations", []):
                    org_id = org.get("identifier")
                    org_name = org.get("name")
                    if org_id and org_name:
                        organizations[org_name.strip()] = org_id.strip()

        return organizations
