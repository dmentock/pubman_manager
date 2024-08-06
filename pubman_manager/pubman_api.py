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
        with open(Path(__file__).parent.parent / 'authors_info.yaml', 'r') as f:
            self.authors_info = yaml.safe_load(f)
        with open(Path(__file__).parent.parent / 'journals.yaml', 'r') as f:
            self.journals = yaml.safe_load(f)
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
        author_info = {}
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

                if full_name not in author_info:
                    author_info[full_name] = {}
                if 'affiliations' in author_info[full_name]:
                    author_info[full_name]['affiliations'].update(affiliation_list)
                else:
                    author_info[full_name]['affiliations'] = set(affiliation_list)
                if (identifier := person.get('identifier')) and 'identifier' not in author_info[full_name]:
                    author_info[full_name]['identifier'] = identifier
        for author in author_info:
            if 'affiliations' in author_info[author]:
                author_info[author]['affiliations'] = list(author_info[author]['affiliations'])
        return author_info

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

    def search_publication_by_criteria(self, match_criteria, size=100000):
        must_clauses = []
        for key, value in match_criteria.items():
            if isinstance(value, dict):
                for sub_key, sub_value in value.items():
                    must_clauses.append({
                        "nested": {
                            "path": key,
                            "query": {
                                "bool": {
                                    "must": [
                                        {"match_phrase": {f"{key}.{sub_key}": sub_value}}
                                    ]
                                }
                            }
                        }
                    })
            else:
                must_clauses.append({
                    "match_phrase": {
                        key: value
                    }
                })

        query = {
            "query": {
                "bool": {
                    "must": must_clauses
                }
            },
            "size": size  # Adjust the size as needed
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
        if response.status_code in [200, 201]:
            results = response.json()
            return results.get('records')
        else:
            raise Exception("Failed to search for item", response.status_code, response.text)


    def create_item(self, request_json):
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

    def submit_item(self, item_id, last_modification_date):
        headers = {
            "Authorization": self.auth_token,
            "Content-Type": "application/json"
        }
        submit_data = {
            "comment": "Item Submitted via API",
            "lastModificationDate": last_modification_date
        }
        response = requests.put(
            f"{self.base_url}/items/{item_id}/submit",
            headers=headers,
            data=json.dumps(submit_data)
        )
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception("Failed to submit item", response.status_code, response.text)

    def parse_excel_table(self, file_path):
        def find_header_row(df):
            for i, row in df.iterrows():
                if row[0] == 1:
                    return i - 2
        def find_end_row(df, start_row):
            for i in range(start_row, len(df)):
                if pd.isna(df.iloc[i, 1]) or df.iloc[i, 1].strip() == '':
                    return i - 1
            return len(df) - 1
        df_full = pd.read_excel(file_path, engine='openpyxl', header=None)
        header_row = find_header_row(df_full)
        start_row = header_row + 2
        end_row = find_end_row(df_full, start_row)
        df_data = pd.read_excel(file_path, engine='openpyxl', header=header_row)
        df = df_data.iloc[start_row - header_row - 1:end_row - header_row]
        return df

    def get_authors_info(self, row):
        authors_info = OrderedDict()
        for i in range(1, 50):  # TODO: make scalable
            author_name_key = f'Author {i}'
            affiliation_key = f'Affiliation {i}'
            if author_name_key in row and affiliation_key in row:
                if pd.notna(row[author_name_key]) and pd.notna(row[affiliation_key]):
                    if row[author_name_key] not in authors_info:
                        if identifier:=self.authors_info.get(row[author_name_key],{}).get('identifier'):
                            authors_info[row[author_name_key]] = {'identifier': identifier}
                        else:
                            authors_info[row[author_name_key]] = {}
                    if 'affiliations' not in authors_info[row[author_name_key]]:
                        authors_info[row[author_name_key]]['affiliations'] = [row[affiliation_key]]
                    else:
                        authors_info[row[author_name_key]]['affiliations'].append(row[affiliation_key])
        return authors_info

    def safe_date_parse(self, date_str):
        try:
            return dateutil.parser.parse(date_str)
        except (ParserError, ValueError):
            return None

    def create_talks(self, file_path, create_items=True, submit_items=True, overwrite=False):
        df = self.parse_excel_table(file_path)
        request_list = []

        for index, row in df.iterrows():
            print(f"Generating requests for \"{row.get('Talk Title')}\"")
            authors_info = self.get_authors_info(row)

            metadata_creators = []
            for author, info in authors_info.items():
                given_name, family_name = author.split(' ', 1)
                affiliation_list = []
                for affiliation in info['affiliations']:
                    if affiliation in self.identifier_paths.keys():
                        affiliation_list.append({"name": affiliation, "identifier": self.identifier_paths[affiliation][0], "identifierPath" : [ "" ]})
                    else:
                        affiliation_list.append({"name": affiliation, "identifier": 'ou_persistent22', "identifierPath" : [ "" ]})
                identifier = info.get('identifier')
                metadata_creators.append({
                    "person": {
                        "givenName": given_name,
                        "familyName": family_name,
                        "organizations": affiliation_list,
                        "identifier": identifier
                    },
                    "role": "AUTHOR",
                    "type": "PERSON"
                })

            request = {
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
                    "title": row.get('Talk Title'),
                    "creators": metadata_creators,
                    "dateCreated": self.safe_date_parse(str(row.get('Talk date\n(dd.mm.YYYY)'))).strftime("%Y-%m-%d") if row.get('Talk date\n(dd.mm.YYYY)') else None,
                    "datePublishedInPrint": "",
                    "datePublishedOnline": "",
                    "genre": "TALK_AT_EVENT",
                    "event": {
                        "endDate": self.safe_date_parse(str(row.get('Conference end date\n(dd.mm.YYYY)'))).strftime("%Y-%m-%d") if row.get('Conference end date\n(dd.mm.YYYY)') and row.get('Conference end date\n(dd.mm.YYYY)') != row.get('Conference start date\n(dd.mm.YYYY)') else None,
                        "place": row.get('Conference Location'),
                        "startDate": self.safe_date_parse(str(row.get('Conference start date\n(dd.mm.YYYY)'))).strftime("%Y-%m-%d"),
                        "title": row.get('Event Name'),
                        'invitationStatus': 'INVITED' if row.get('Invited (y/n)').strip().lower() == 'y' else None
                    },
                    "languages": ["eng"]
                },
                "files": []
            }

            request_list.append(({
                    "metadata.title": row.get('Talk Title'),
                    "metadata.event.title": row.get('Event Name')
                }, request)
            )

        if create_items:
            self.create_items(request_list, submit_items=submit_items, overwrite=overwrite)

    def create_publications(self, file_path, create_items=True, submit_items=True, overwrite=False):
        df = self.parse_excel_table(file_path)
        request_list = []

        for index, row in df.iterrows():
            print(f"Generating requests for \"{row.get('Title')}\"")
            authors_info = self.get_authors_info(row)

            metadata_creators = []
            for author, info in authors_info.items():
                given_name, family_name = author.split(' ', 1)
                affiliation_list = []
                for affiliation in info['affiliations']:
                    if affiliation in self.identifier_paths.keys():
                        affiliation_list.append({
                            "name": affiliation,
                            "identifier": self.identifier_paths[affiliation][0],
                            "identifierPath": [""]
                        })
                    else:
                        affiliation_list.append({
                            "name": affiliation,
                            "identifier": 'ou_persistent22',
                            "identifierPath": [""]
                        })
                identifier = info.get('identifier')
                metadata_creators.append({
                    "person": {
                        "givenName": given_name,
                        "familyName": family_name,
                        "organizations": affiliation_list,
                        "identifier": identifier
                    },
                    "role": "AUTHOR",
                    "type": "PERSON"
                })

            # Extracting other relevant information
            date_created_ = self.safe_date_parse(str(row.get('Date created'))) if row.get('Date created') else None
            date_created = date_created_.strftime("%Y-%m-%d") if date_created_ else None
            date_issued_ = self.safe_date_parse(str(row.get('Date issued'))) if row.get('Date issued') else None
            date_issued = date_issued_.strftime("%Y-%m-%d") if date_issued_ else date_created.strftime("%Y") if date_created else None
            date_published = self.safe_date_parse(str(row.get('Date published'))).strftime("%Y-%m-%d") if row.get('Date published') else None
            identifiers = self.journals.get(row.get('Journal Title', ''), {}).get('identifiers', {})
            if 'ISSN' not in identifiers and row.get('ISSN'):
                identifiers['ISSN'] = identifiers and row.get('ISSN')
            identifiers_list = [{'type': key, 'id': id} for key, id in
                                identifiers.items()]
            start_page, end_page, n_pages = None, None, None
            if row.get('Page') and str(row.get('Page')) != 'nan':
                start_page = row.get('Page').split('-')[0].strip()
                end_page = row.get('Page').split('-')[-1].strip()
                n_pages = int(row.get('Page').split('-')[-1].strip()) - \
                          int(row.get('Page').split('-')[0].strip()) + 1
            sources = [{
                'alternativeTitles': self.journals.get(row.get('Journal Title', ''), {}).get('alternativeTitles', []),
                "genre": self.journals.get(row.get('Journal Title', ''), {}).get('genre', 'JOURNAL'),
                "title": row.get('Journal Title'),
                "publishingInfo":  self.journals.get(row.get('Journal Title', ''), {}).get('publishingInfo', {'publisher': row.get('Publisher')}),
                "volume": int(row.get('Volume')),
                "issue": int(row.get('Issue')),
                "startPage": start_page,
                "endPage": end_page,
                "totalNumberOfPages": n_pages,
                "identifiers": identifiers_list,
            }]
            # Building the request dictionary
            request = {
                "context": {
                    "objectId": self.ctx_id,
                    "name": "",
                    "lastModificationDate": "",
                    "creationDate": "",
                    "creator": {
                        "objectId": ""
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
                    "title": row.get('Title'),
                    "creators": metadata_creators,
                    "dateCreated": date_created,
                    "dateIssued": date_issued,
                    "datePublishedOnline": date_published,
                    "genre": 'ARTICLE',
                    "identifiers": [
                        {"id": row.get('DOI'), "type": "DOI"},
                    ],
                    "languages": ["eng"],
                    "sources": sources,
                    'reviewMethod': 'PEER'
                },
                "files": []
            }

            request_list.append(({
                    "metadata.title": row.get('Title'),
                    # TODO
                    # "metadata.sources[0].title": row.get('Journal Title')
                }, request)
            )

        if create_items:
            self.create_items(request_list, submit_items=submit_items, overwrite=overwrite)


    def create_items(self, request_list, submit_items=True, overwrite=False):
        item_ids = []
        for criteria, request_json in request_list:
            print(f"Creating entry: {criteria}")
            existing_publication = self.search_publication_by_criteria(criteria)
            if existing_publication:
                if overwrite:
                    print(f"Overwriting existing publication: '{criteria}'")
                    for pub in existing_publication:
                        self.delete_item(pub['data']['objectId'], pub['data']['lastModificationDate'])
                else:
                    print(f"Publication already exists, skipping creation: '{criteria}'")
                    created_item = existing_publication[0]['data']
                    item_ids.append((created_item['objectId'], created_item['lastModificationDate'], created_item['versionState']))
                    continue

            print(f"Creating new publication: '{criteria}'")
            print("request_json",request_json)
            created_item = self.create_item(request_json)
            item_ids.append((created_item['objectId'], created_item['lastModificationDate'], created_item['versionState']))

        if submit_items:
            for item_id, modification_date, version_state in item_ids:
                if version_state not in ['PENDING', 'IN_REVISION']:
                    print(f"Entry already has the state '{version_state}', skipping...")
                else:
                    submitted_item = self.submit_item(item_id, modification_date)
                    print(f"Submitted item: {submitted_item}")