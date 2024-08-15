from pubman_manager import PubmanBase, PUBMAN_CACHE_DIR, FILES_DIR
import pandas as pd
from collections import OrderedDict
from dateutil.parser import ParserError

from dateutil import parser
from datetime import datetime
import yaml
from pathlib import Path
import requests
import json

class PubmanCreator(PubmanBase):
    def __init__(self, username, password, base_url = "https://pure.mpg.de/rest"):
        super().__init__(username, password, base_url)
        with open(PUBMAN_CACHE_DIR / 'identifier_paths.yaml', 'r') as f:
            self.identifier_paths = yaml.safe_load(f)
        with open(PUBMAN_CACHE_DIR / 'authors_info.yaml', 'r') as f:
            self.authors_info = yaml.safe_load(f)
        with open(PUBMAN_CACHE_DIR / 'journals.yaml', 'r') as f:
            self.journals = yaml.safe_load(f)
        try:
            self.auth_token = self.login()
        except:
            print("login failed")

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
            parsed_date = parser.parse(date_str, fuzzy=True)
            parts = date_str.split('-') if '-' in date_str else date_str.split('.')
            if len(parts) == 1:
                return parsed_date.replace(month=1, day=1)
            if len(parts) == 2:
                return parsed_date.replace(day=1)
            return parsed_date

        except (parser.ParserError, ValueError):
            return None

    def format_date(self, parsed_date, original_date_str):
        parts = original_date_str.split('-') if '-' in original_date_str else original_date_str.split('.')
        if len(parts) == 1:
            return parsed_date.strftime("%Y")  # Just year
        elif len(parts) == 2:
            return parsed_date.strftime("%Y-%m")  # Year and month
        else:
            return parsed_date.strftime("%Y-%m-%d")  # Full date


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

    def fetch_sample_documents(self, size=5):
        """Fetch a small sample of documents to explore the structure."""
        search_url = f"{self.base_url}/items/search"

        query = {
            "query": {
                "match_all": {}  # This will fetch all documents
            },
            "size": size  # Fetch only a few documents
        }

        headers = {
            "Authorization": self.auth_token,
            "Content-Type": "application/json"
        }

        response = requests.post(search_url, headers=headers, json=query)

        if response.status_code == 200:
            results = response.json().get('records', [])
            for result in results:
                print(json.dumps(result, indent=2))  # Pretty-print the JSON structure
        else:
            raise Exception(f"Failed to fetch sample documents: {response.status_code} {response.text}")
    def fetch_sample_document(self):
        search_url = f"{self.base_url}/items/search"

        query = {
            "query": {
                "match_all": {}
            },
            "size": 1  # Fetch a single document
        }

        headers = {
            "Authorization": self.auth_token,
            "Content-Type": "application/json"
        }

        response = requests.post(search_url, headers=headers, json=query)

        if response.status_code == 200:
            document = response.json().get('records', [])[0]  # Fetch the first document
            print(json.dumps(document, indent=2))  # Pretty-print the document for inspection
            return document
        else:
            raise Exception(f"Failed to fetch sample document: {response.status_code} {response.text}")

    def upload_pdf(self, pdf_path):
        """Upload the PDF if it doesn't already exist in the repository."""
        pdf_path = Path(pdf_path)
        file_title = pdf_path.name

        # Check if the file with the same title already exists
        # existing_file_id = self.search_file_by_title(file_title)
        # if existing_file_id:
        #     print(f"File with title '{file_title}' already exists. Returning existing file ID: {existing_file_id}")
        #     return existing_file_id

        # If the file does not exist, upload it
        staging_url = f"{self.base_url}/staging/{pdf_path.name}"

        headers = {
            "Authorization": self.auth_token,
        }

        with open(pdf_path, 'rb') as file_data:
            response = requests.post(staging_url, headers=headers, files={"data": file_data})

        if response.status_code in [200, 201]:
            file_id = response.json()
            print(f"File uploaded successfully. File ID: {file_id}")
            return file_id
        else:
            raise Exception(f"Failed to upload file: {response.status_code} {response.text}")

    def create_publications(self, file_path, create_items=True, submit_items=True, overwrite=False):
        df = self.parse_excel_table(file_path)
        request_list = []

        for index, row in df.iterrows():
            title = row.get('Title')
            if not title:
                print(f"Warning, missing entry for row {index}")
                continue
            print(f"Generating requests for \"{title}\"")
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
            doi = str(row.get("DOI"))

            date_created_sheet = row.get('Date created')
            date_created_parsed = self.safe_date_parse(str(date_created_sheet)) if date_created_sheet else None
            date_created = self.format_date(date_created_parsed, date_created_sheet) if date_created_parsed else None

            date_issued_sheet = str(row.get('Date issued'))
            date_issued_parsed = self.safe_date_parse(str(date_issued_sheet)) if date_issued_sheet else None
            date_issued = self.format_date(date_issued_parsed, date_issued_sheet) if date_issued_parsed else \
                          self.format_date(date_created_parsed, '') if date_created_parsed else None

            date_published_sheet = str(row.get('Date published'))
            date_published_parsed = self.safe_date_parse(str(date_published_sheet)) if date_published_sheet else None
            date_published = self.format_date(date_published_parsed, date_published_sheet) if date_published_parsed else None

            identifiers = self.journals.get(row.get('Journal Title', ''), {}).get('identifiers', {})
            if 'ISSN' not in identifiers and row.get('ISSN'):
                identifiers['ISSN'] = row.get('ISSN')
            identifiers_list = [{'type': key, 'id': id} for key, id in
                                identifiers.items()]
            start_page, end_page, n_pages = None, None, None
            if isinstance(row.get('Page'), str) and '-' in row.get('Page', '') and str(row.get('Page')) != 'nan':
                start_page = row.get('Page').split('-')[0].strip()
                end_page = row.get('Page').split('-')[-1].strip()
                n_pages = int(row.get('Page').split('-')[-1].strip()) - \
                          int(row.get('Page').split('-')[0].strip()) + 1
            sources = [{
                'alternativeTitles': self.journals.get(row.get('Journal Title', ''), {}).get('alternativeTitles', []),
                "genre": self.journals.get(row.get('Journal Title', ''), {}).get('genre', 'JOURNAL'),
                "title": row.get('Journal Title'),
                "publishingInfo":  self.journals.get(row.get('Journal Title', ''), {}).get('publishingInfo', {'publisher': row.get('Publisher')}),
                "volume": int(row.get('Volume')) if row.get('Volume') and str(row.get('Volume')) != 'nan' else '',
                "issue": int(row.get('Issue')) if row.get('Issue') and str(row.get('Issue')) != 'nan' else '',
                "startPage": start_page,
                "endPage": end_page,
                "totalNumberOfPages": n_pages,
                "identifiers": identifiers_list,
            }]
            files = []
            pdf_path = Path(FILES_DIR / f'{doi.replace("/", "_")}.pdf')
            print("pdf_path",pdf_path)
            if pdf_path.exists():
                print("PDFTHERE")
                file_id = self.upload_pdf(pdf_path)
                file =  {
                    "objectId": '',
                    "name": pdf_path.name,
                    "lastModificationDate" : "",
                    "creationDate" : "",
                    "creator" : {
                      "objectId" : ""
                    },
                    "pid" : "",
                    'content': file_id,
                    "visibility": "PUBLIC",  # Set according to your needs
                    "contentCategory": "publisher-version",
                    "checksum" : "",
                    "checksumAlgorithm" : "MD5",
                    "mimeType" : "",
                    "size" : 0,
                    'storage': 'INTERNAL_MANAGED',
                    "metadata" : {
                      "title" : pdf_path.name,
                      "contentCategory" : 'pre-print' if 'arxiv' in row.get('License url', '') else 'publisher-version',
                      "description" : "File downloaded from scopus",
                      "formats" : [ {
                        "value" : "",
                        "type" : ""
                      } ],
                      "size" : 0,
                      "license" : row.get('License url', ''),
                      'copyrightDate': str(int(row.get('License year', ''))),
                    },
                }
                files.append(file)
            # Building the request dictionary
            request = {
                "context": {
                    "objectId": self.ctx_id,
                    "name": "",
                    "lastModificationDate": "",
                    "creationDate": "",
                    "creator": {
                        "objectId": self.user_id
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
                    "datePublishedInPrint": date_issued,
                    "datePublishedOnline": date_published,
                    "genre": 'ARTICLE',
                    "identifiers": [
                        {"id": doi, "type": "DOI"},
                    ],
                    "languages": ["eng"],
                    "sources": sources,
                    'reviewMethod': 'PEER'
                },
                "files": files
            }

            request_list.append(({
                    "metadata.identifiers.id": doi,
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