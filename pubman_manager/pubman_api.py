import requests
import json
import yaml
import pandas as pd
import dateutil
from collections import OrderedDict
from pathlib import Path
from dateutil.parser import ParserError

class PubManAPI:
    def __init__(self, username, password, base_url = "https://pure.mpg.de/rest"):
        self.base_url = base_url
        self.username = username
        self.password = password
        self.org_id = 'ou_1863381' # Org ID for all MPIE publications, TODO: fetch based on Institute name
        self.user_id = "user_1944725"  # User id for user PuRe user "Mentock", TODO: fetch automatically in the future based on username
        self.ctx_id = "ctx_2019354" # CTX ID for all MPIE publications, TODO: fetch based on org_id
        with open(Path(__file__).parent.parent / 'identifier_paths.yaml', 'r') as f:
            self.identifier_paths = yaml.safe_load(f)
        try:
            self.auth_token = self.login()
        except:
            print("login failed")

    def login(self):
        login_response = requests.post(
            f"{self.base_url}/login",
            headers={"Content-Type": "text/plain"},
            data=f"{self.username}:{self.password}"
        )
        if login_response.status_code == 200:
            self.auth_token = login_response.headers.get("Token")
            return self.auth_token
        else:
            raise Exception("Failed to log in")

    def logout(self):
        logout_response = requests.get(
            f"{self.base_url}/logout",
            headers={"Authorization": self.auth_token}
        )
        if logout_response.status_code != 200:
            raise Exception("Failed to log out")

    def get_item(self, publication_id):
        response = requests.get(
            f"{self.base_url}/items/{publication_id}",
            headers={"Authorization": self.auth_token}
        )
        return response.json()

    def get_item_history(self, publication_id):
        response = requests.get(
            f"{self.base_url}/items/{publication_id}/history",
            headers={"Authorization": self.auth_token}
        )
        return response.json()

    def get_component_content(self, publication_id, file_id):
        response = requests.get(
            f"{self.base_url}/items/{publication_id}/component/{file_id}/content",
            headers={"Authorization": self.auth_token}
        )
        return response.content

    def get_component_metadata(self, publication_id, file_id):
        response = requests.get(
            f"{self.base_url}/items/{publication_id}/component/{file_id}/metadata",
            headers={"Authorization": self.auth_token}
        )
        return response.json()

    def search_items(self, query, format="json", citation=None, cslConeId=None, scroll=False):
        params = {
            "format": format,
            "citation": citation,
            "cslConeId": cslConeId,
            "scroll": str(scroll).lower()
        }
        headers = {
            "Authorization": self.auth_token,
            "Content-Type": "application/json"
        }
        response = requests.post(
            f"{self.base_url}/items/search",
            headers=headers,
            params=params,
            data=json.dumps(query)
        )
        return response.json()

    def search_items_scroll(self, scrollId, format="json", citation=None, cslConeId=None):
        params = {
            "format": format,
            "citation": citation,
            "cslConeId": cslConeId,
            "scrollId": scrollId
        }
        headers = {
            "Authorization": self.auth_token
        }
        response = requests.get(
            f"{self.base_url}/items/search/scroll",
            headers=headers,
            params=params
        )
        return response.json()

    def stage_file(self, component_name, file_path):
        with open(file_path, 'rb') as f:
            file_data = f.read()
        headers = {
            "Authorization": self.auth_token
        }
        response = requests.post(
            f"{self.base_url}/staging/{component_name}",
            headers=headers,
            data=file_data
        )
        return response.json()

    def create_item(self, item_data):
        headers = {
            "Authorization": self.auth_token,
            "Content-Type": "application/json"
        }
        response = requests.post(
            f"{self.base_url}/items",
            headers=headers,
            data=json.dumps(item_data)
        )
        return response.json()

    def update_item(self, item_id, item_data):
        headers = {
            "Authorization": self.auth_token,
            "Content-Type": "application/json"
        }
        response = requests.put(
            f"{self.base_url}/items/{item_id}",
            headers=headers,
            data=json.dumps(item_data)
        )
        return response.json()

    def delete_item(self, item_id, last_modification_date):
        headers = {
            "Authorization": self.auth_token,
            "Content-Type": "application/json"
        }
        response = requests.delete(
            f"{self.base_url}/items/{item_id}",
            headers=headers,
            data=json.dumps({"lastModificationDate": last_modification_date})
        )
        if response.status_code != 200:
            raise Exception("Failed to delete item")

    def submit_item(self, item_id, last_modification_date, comment):
        headers = {
            "Authorization": self.auth_token,
            "Content-Type": "application/json"
        }
        response = requests.put(
            f"{self.base_url}/items/{item_id}/submit",
            headers=headers,
            data=json.dumps({"lastModificationDate": last_modification_date, "comment": comment})
        )
        return response.json()

    def release_item(self, item_id, last_modification_date, comment):
        headers = {
            "Authorization": self.auth_token,
            "Content-Type": "application/json"
        }
        response = requests.put(
            f"{self.base_url}/items/{item_id}/release",
            headers=headers,
            data=json.dumps({"lastModificationDate": last_modification_date, "comment": comment})
        )
        return response.json()

    def withdraw_item(self, item_id, last_modification_date, comment):
        headers = {
            "Authorization": self.auth_token,
            "Content-Type": "application/json"
        }
        response = requests.put(
            f"{self.base_url}/items/{item_id}/withdraw",
            headers=headers,
            data=json.dumps({"lastModificationDate": last_modification_date, "comment": comment})
        )
        return response.json()

    def revise_item(self, item_id, last_modification_date, comment):
        headers = {
            "Authorization": self.auth_token,
            "Content-Type": "application/json"
        }
        response = requests.put(
            f"{self.base_url}/items/{item_id}/revise",
            headers=headers,
            data=json.dumps({"lastModificationDate": last_modification_date, "comment": comment})
        )
        return response.json()

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

    def fetch_scroll_results(self, scroll_id):
        headers = {
            "Authorization": self.auth_token,
            "Content-Type": "application/json"
        }
        response = requests.get(
            f"{self.base_url}/items/search/scroll?scrollId={scroll_id}",
            headers=headers
        )
        if response.status_code == 200:
            return response.json()
        return None

    def get_organization_mapping(self, yaml_file):
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

    def extract_author_affiliations(self, publications):
        author_affiliations = {}
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

                if full_name in author_affiliations:
                    author_affiliations[full_name].update(affiliation_list)
                else:
                    author_affiliations[full_name] = set(affiliation_list)
        for author in author_affiliations:
            author_affiliations[author] = list(author_affiliations[author])

        return author_affiliations

    def create_event_publication_request(self, event_name, start_date, end_date, talk_date, location, invited, title, authors_affiliations):
        metadata_creators = []
        for author, affiliations in authors_affiliations.items():
            given_name, family_name = author.split(' ', 1)
            affiliation_list = []
            for affiliation in affiliations:
                if affiliation in self.identifier_paths.keys():
                    affiliation_list.append({"name": affiliation, "identifier": self.identifier_paths[affiliation][0], "identifierPath" : [ "" ]})
                else:
                    affiliation_list.append({"name": affiliation, "identifier": 'ou_persistent22', "identifierPath" : [ "" ]})
            metadata_creators.append({
                "person": {
                    "givenName": given_name,
                    "familyName": family_name,
                    "organizations": affiliation_list
                },
                "role": "AUTHOR",
                "type": "PERSON"
            })

        return {
            "context": {
                "objectId": self.ctx_id,
                "name" : "",
                "lastModificationDate" : "",
                "creationDate" : "",
                "creator" : {
                  "objectId" : ""
                },
            },
            "creator": {
                "objectId": self.user_id
            },
            "modifier": {
                "objectId": self.user_id
            },
            "localTags": [],
            "metadata": {
                "title": title,
                "creators": metadata_creators,
                "dateCreated": talk_date.strftime("%Y-%m-%d") if talk_date else None,
                "datePublishedInPrint": "",
                "datePublishedOnline": "",
                "genre": "TALK_AT_EVENT",
                "event": {
                    "endDate": end_date.strftime("%Y-%m-%d"),
                    "place": location,
                    "startDate": start_date.strftime("%Y-%m-%d"),
                    "title": event_name,
                    "invited": invited == 'y'
                },
                "languages": ["eng"]
            },
            "files": []
        }

    def process_excel_and_create_publications(self, file_path):
        def find_header_row(df):
            for i, row in df.iterrows():
                if row[0] == 1:
                    return i - 2
        def find_end_row(df, start_row):
            for i in range(start_row, len(df)):
                if pd.isna(df.iloc[i, 1]) or df.iloc[i, 1].strip() == '':
                    return i - 1
            return len(df) - 1

        def extract_authors_affiliations(row):
            authors_affiliations = OrderedDict()
            for i in range(1, 50): # TODO: make scalable
                author_name_key = f'Name {i}'
                affiliation_key = f'Affiliation {i}'
                if author_name_key in row and affiliation_key in row:
                    if pd.notna(row[author_name_key]) and pd.notna(row[affiliation_key]):
                        if row[author_name_key] not in authors_affiliations:
                            authors_affiliations[row[author_name_key]] = [row[affiliation_key]]
                        else:
                            authors_affiliations[row[author_name_key]].append(row[affiliation_key])
            return authors_affiliations

        def safe_date_parse(date_str):
            try:
                return dateutil.parser.parse(date_str)
            except (ParserError, ValueError):
                return None

        df_full = pd.read_excel(file_path, engine='openpyxl', header=None)
        header_row = find_header_row(df_full)
        start_row = header_row + 2
        end_row = find_end_row(df_full, start_row)
        df_data = pd.read_excel(file_path, engine='openpyxl', header=header_row)
        df = df_data.iloc[start_row - header_row - 1:end_row - header_row]

        request_list = []
        for index, row in df.iterrows():
            print(f"generating requests for \"{row.get('Talk Title')}\"")
            authors_affiliations = extract_authors_affiliations(row)
            request_list.append(self.create_event_publication_request(
                event_name=row.get('Event Name'),
                start_date=safe_date_parse(str(row.get('Conference start date\n(dd.mm.YYYY)'))),
                end_date=safe_date_parse(str(row.get('Conference end date\n(dd.mm.YYYY)'))),
                talk_date=safe_date_parse(str(row.get('Talk date\n(dd.mm.YYYY)'))),
                location=row.get('Conference Location'),
                invited=row.get('Invited (y/n)'),
                title=row.get('Talk Title'),
                authors_affiliations=authors_affiliations
            ))

        for request_json in request_list:
            print(f"executing request: {request_json}")
            headers = {
                "Authorization": self.auth_token,
                "Content-Type": "application/json"
            }
            response = requests.post(
                f"{self.base_url}/items",
                headers=headers,
                data=json.dumps(request_json)
            )
        if response.status_code in [200, 201]:
            return response.json()
        else:
            raise Exception("Failed to create item", response.status_code, response.text)
