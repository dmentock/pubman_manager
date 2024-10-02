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
import re
import math

class PubmanCreator(PubmanBase):
    def __init__(self, username=None, password=None, base_url = "https://pure.mpg.de/rest"):
        super().__init__(username=username, password=password, base_url=base_url)
        with open(PUBMAN_CACHE_DIR / 'identifier_paths.yaml', 'r') as f:
            self.identifier_paths = yaml.load(f, Loader=yaml.FullLoader)
        with open(PUBMAN_CACHE_DIR / 'authors_info.yaml', 'r') as f:
            self.authors_info = yaml.load(f, Loader=yaml.FullLoader)
        with open(PUBMAN_CACHE_DIR / 'journals.yaml', 'r') as f:
            self.journals = yaml.load(f, Loader=yaml.FullLoader)

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

    def get_row_authors_info(self, row):
        row_authors = OrderedDict()
        authors_info_merged_names = {f'{first_name} {last_name}': affiliations for (first_name, last_name), affiliations in self.authors_info.items()}
        for i in range(1, 50):  # TODO: make scalable
            author_name_key = f'Author {i}'
            affiliation_key = f'Affiliation {i}'
            if author_name_key in row and affiliation_key in row:
                if pd.notna(row[author_name_key]) and pd.notna(row[affiliation_key]):
                    if row[author_name_key] not in row_authors:
                        if identifier:=authors_info_merged_names.get(row[author_name_key],{}).get('identifier'):
                            row_authors[row[author_name_key]] = {'identifier': identifier}
                        else:
                            row_authors[row[author_name_key]] = {}
                    if 'affiliations' not in row_authors[row[author_name_key]]:
                        row_authors[row[author_name_key]]['affiliations'] = [row[affiliation_key]]
                    else:
                        row_authors[row[author_name_key]]['affiliations'].append(row[affiliation_key])
        return row_authors

    def safe_date_parse(self, date_str):
        try:
            if '-' in date_str:
                parsed_date = parser.parse(date_str, fuzzy=True)
            else:
                parsed_date = parser.parse(date_str, fuzzy=True, dayfirst=True)
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
            return parsed_date.strftime("%Y")
        elif len(parts) == 2:
            return parsed_date.strftime("%Y-%m")
        else:
            return parsed_date.strftime("%Y-%m-%d")

    def clean_affiliation(self, affiliation):
        affiliation = re.sub(r'_x[0-9A-Fa-f]+_', '', affiliation)
        pattern = r'[^\u0020-\u007E\u00A1-\u00FF\u0100-\u017F\u0180-\u024F\u0370-\u03FF\u0400-\u04FF\u0530-\u058F\u0590-\u05FF\u0600-\u06FF\u0750-\u077F\u0900-\u097F\u0980-\u09FF\u0A00-\u0A7F\u0A80-\u0AFF\u0B00-\u0B7F\u0B80-\u0BFF\u0C00-\u0C7F\u0C80-\u0CFF\u0D00-\u0D7F\u0E00-\u0E7F\u0E80-\u0EFF\u0F00-\u0FFF\u1000-\u109F\u1100-\u11FF\u1200-\u137F\u13A0-\u13FF\u1400-\u167F\u1680-\u169F\u1700-\u171F\u1720-\u173F\u1740-\u175F\u1760-\u177F\u1780-\u17FF\u1800-\u18AF\u1900-\u194F\u1950-\u197F\u1980-\u19DF\u1A00-\u1A1F\u1A20-\u1AFF\u1B00-\u1B7F\u1B80-\u1BBF\u1C00-\u1C4F\u1C50-\u1C7F\u1C80-\u1CDF\u1D00-\u1D7F\u1D80-\u1DBF\u1DC0-\u1DFF\u1E00-\u1EFF\u1F00-\u1FFF\u2000-\u206F\u2070-\u209F\u20A0-\u20CF\u20D0-\u20FF\u2100-\u214F\u2150-\u218F\u2190-\u21FF\u2200-\u22FF\u2300-\u23FF\u2400-\u243F\u2440-\u245F\u2460-\u24FF\u2500-\u257F\u2580-\u259F\u25A0-\u25FF\u2600-\u26FF\u2700-\u27BF\u27C0-\u27EF\u27F0-\u27FF\u2800-\u28FF\u2900-\u297F\u2980-\u29FF\u2A00-\u2AFF\u2B00-\u2BFF\u2C00-\u2C5F\u2C60-\u2C7F\u2C80-\u2CFF\u2D00-\u2D2F\u2D30-\u2D7F\u2D80-\u2DDF\u2E00-\u2E7F\u2E80-\u2EFF\u2F00-\u2FDF\u2FF0-\u2FFF\u3000-\u303F\u3040-\u309F\u30A0-\u30FF\u3100-\u312F\u3130-\u318F\u3190-\u319F\u31A0-\u31BF\u31C0-\u31EF\u31F0-\u31FF\u3200-\u32FF\u3300-\u33FF\u3400-\u4DBF\u4DC0-\u4DFF\u4E00-\u9FFF\uA000-\uA48F\uA490-\uA4CF\uA500-\uA61F\uA620-\uA6FF\uA700-\uA71F\uA720-\uA7FF\uA800-\uA82F\uA830-\uA83F\uA840-\uA87F\uA880-\uA8DF\uA8E0-\uA8FF\uA900-\uA92F\uA930-\uA95F\uA960-\uA97F\uA980-\uA9DF\uA9E0-\uA9FF\uAA00-\uAA5F\uAA60-\uAA7F\uAA80-\uAADF\uAAE0-\uAAFF\uAB00-\uAB2F\uAB30-\uAB6F\uAB70-\uABBF\uABC0-\uABFF\uAC00-\uD7AF\uD7B0-\uD7FF\uD800-\uDB7F\uDB80-\uDBFF\uDC00-\uDFFF\uE000-\uF8FF\uF900-\uFAFF\uFB00-\uFB4F\uFB50-\uFDFF\uFE00-\uFE0F\uFE10-\uFE1F\uFE20-\uFE2F\uFE30-\uFE4F\uFE50-\uFE6F\uFE70-\uFEFF\uFF00-\uFFEF\uFFF0-\uFFFF]'

        return re.sub(pattern, '', affiliation)

    def create_talks(self, file_path, create_items=True, submit_items=False, overwrite=False):
        df = self.parse_excel_table(file_path)
        request_list = []

        for index, row in df.iterrows():
            self.log.info(f"Generating requests for \"{row.get('Talk Title')}\"")
            row_authors_info = self.get_row_authors_info(row)
            metadata_creators = []
            for author, info in row_authors_info.items():
                given_name, family_name = author.split(' ', 1)
                affiliation_list = []
                for affiliation in info['affiliations']:
                    if affiliation in self.identifier_paths.keys():
                        affiliation_list.append({"name": self.clean_affiliation(affiliation), "identifier": self.identifier_paths[affiliation][0], "identifierPath" : [ "" ]})
                    else:
                        affiliation_list.append({"name": self.clean_affiliation(affiliation), "identifier": 'ou_persistent22', "identifierPath" : [ "" ]})
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

            if row.get('Type (Talk/Poster)', '').lower().strip() == 'talk':
                genre = "TALK_AT_EVENT"
            elif row.get('Type (Talk/Poster)', '').lower().strip() == 'poster':
                genre = "POSTER"
            else:
                raise RuntimeError(f'''Invalid Type (Talk/Poster): "{row.get('Type (Talk/Poster)')}"''')
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
                    "genre": genre,
                    "event": {
                        "endDate": self.safe_date_parse(str(row.get('Conference end date\n(dd.mm.YYYY)'))).strftime("%Y-%m-%d") if row.get('Conference end date\n(dd.mm.YYYY)') and row.get('Conference end date\n(dd.mm.YYYY)') != row.get('Conference start date\n(dd.mm.YYYY)') else None,
                        "place": row.get('Conference Location\n(City, Country)'),
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
        self.create_items(request_list, create_items=create_items, submit_items=submit_items, overwrite=overwrite)

    def upload_pdf(self, pdf_path):
        """Upload the PDF if it doesn't already exist in the repository."""
        pdf_path = Path(pdf_path)
        file_title = pdf_path.name

        # TODO: find way to check if the file with the same title already exists
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
            self.log.debug(f"File uploaded successfully. File ID: {file_id}")
            return file_id
        else:
            raise Exception(f"Failed to upload file: {response.status_code} {response.text}")

    def create_publications(self, file_path, create_items=True, submit_items=True, overwrite=False):
        df = self.parse_excel_table(file_path)
        request_list = []

        for index, row in df.iterrows():
            title = row.get('Title')
            if not title:
                raise RuntimeError(f"Missing entry for row {index}")
            row_authors_info = self.get_row_authors_info(row)

            metadata_creators = []
            for author, info in row_authors_info.items():
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
                        "givenName": given_name.strip(),
                        "familyName": family_name.strip(),
                        "organizations": affiliation_list,
                        "identifier": identifier
                    },
                    "role": "AUTHOR",
                    "type": "PERSON"
                })
            doi = str(row.get("DOI"))

            date_issued_sheet = str(row.get('Date issued'))
            date_issued_parsed = self.safe_date_parse(str(date_issued_sheet)) if date_issued_sheet else None
            date_issued = self.format_date(date_issued_parsed, date_issued_sheet) if date_issued_parsed else None

            date_published_sheet = str(row.get('Date published online'))
            date_published_parsed = self.safe_date_parse(str(date_published_sheet)) if date_published_sheet else None
            date_published_online = self.format_date(date_published_parsed, date_published_sheet) if date_published_parsed else None

            # print("date_published_online",date_published_online_sheet, date_published_online)
            # print("date_issued",date_issued_sheet, date_issued_parsed, date_issued)
            # print("date_published",date_published_sheet, date_published_parsed, date_published)
            identifiers = self.journals.get(row.get('Journal Title', ''), {}).get('identifiers', {})
            issn = row.get('ISSN')
            if 'ISSN' not in identifiers and issn:
                if isinstance(issn, float) and math.isnan(issn):
                    issn = ''
                identifiers['ISSN'] = issn

            identifiers_list = [{'type': key, 'id': id} for key, id in
                                identifiers.items()]

            issue = str(row.get('Issue')) if row.get('Issue') and str(row.get('Issue')) != 'nan' else ''
            sources = [{
                'alternativeTitles': self.journals.get(row.get('Journal Title', ''), {}).get('alternativeTitles', []),
                "genre": self.journals.get(row.get('Journal Title', ''), {}).get('genre', 'JOURNAL'),
                "title": row.get('Journal Title'),
                "publishingInfo":  self.journals.get(row.get('Journal Title', ''), {}).get('publishingInfo', {'publisher': row.get('Publisher')}),
                "volume": str(int(row.get('Volume'))) if row.get('Volume') and str(row.get('Volume')) != 'nan' else '',
                "issue": issue,
                "identifiers": identifiers_list,
            }]
            if isinstance(row.get('Page'), str) and '-' in row.get('Page', '') and str(row.get('Page')) != 'nan':
                sources[0]['startPage'] = row.get('Page').split('-')[0].strip()
                sources[0]['endPage'] = row.get('Page').split('-')[-1].strip()
                sources[0]['totalNumberOfPages'] = int(row.get('Page').split('-')[-1].strip()) - \
                          int(row.get('Page').split('-')[0].strip()) + 1
            article_number = row.get('Article Number')
            if article_number and not math.isnan(article_number):
                sources[0]['sequenceNumber'] = int(article_number)

            files = []
            pdf_path = Path(FILES_DIR / f'{doi.replace("/", "_")}.pdf')
            if row.get('License url') and not pd.isna(row.get('License url')):
                if not pdf_path.exists():
                    raise RuntimeError(f'PDF for DOI {doi} not found in {pdf_path}')
                else:
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
                      "visibility": "PUBLIC",
                      "contentCategory": "publisher-version",
                      "checksum" : "",
                      "checksumAlgorithm" : "MD5",
                      "mimeType" : "",
                      "size" : 0,
                      'storage': 'INTERNAL_MANAGED',
                      "metadata" : {
                        "title" : pdf_path.name,
                        "description" : "File downloaded from scopus",
                        "formats" : [ {
                          "value" : "",
                          "type" : ""
                        } ],
                        "size" : 0,
                      },
                  }
                  license_url = row.get('License url', '')
                  license_year = row.get('License year', '')
                  if (license_url and not isinstance(license_url, float)) or (isinstance(license_url, float) and not math.isnan(license_url)):
                      file['metadata']['license'] = license_url
                  else:
                      license_url = ''
                  if license_year and license_year!=license_year:
                      try:
                          license_year = str(int(license_year))
                          file['metadata']['copyrightDate'] = license_year
                      except:
                          pass
                  file['metadata']['contentCategory'] = 'pre-print' if 'arxiv' in license_url else 'publisher-version'
                  files.append(file)

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
                    "dateCreated": date_published_online,
                    "datePublishedInPrint": date_issued,
                    "datePublishedOnline": date_published_online ,
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

    def create_items(self, request_list, create_items = True, submit_items=False, overwrite=False):
        item_ids = []
        for criteria, request_json in request_list:
            created_item = None
            title = {request_json['metadata']['title']}
            existing_publication = self.search_publication_by_criteria(criteria)
            if existing_publication:
                if overwrite:
                    self.log.info(f"Overwriting existing publication: '{title}' with criteria '{criteria}':")
                    for pub in existing_publication:
                        deleted = self.delete_item(pub['data']['objectId'], pub['data']['lastModificationDate'])
                        if not deleted:
                            self.log.info(f"Publication \"{title}\" cannot be deleted, skipping...")
                else:
                    self.log.info(f"Publication already exists, skipping creation: '{criteria}'")
                    created_item = existing_publication[0]['data']
                    item_ids.append((created_item['objectId'], created_item['lastModificationDate'], created_item['versionState']))
                    continue
            if create_items:
                self.log.info(f"Creating new publication: '{criteria}'")
                created_item = self.create_item(request_json)
            if created_item:
                item_ids.append((created_item['objectId'], created_item['lastModificationDate'], created_item['versionState']))

        if submit_items:
            for item_id, modification_date, version_state in item_ids:
                if version_state not in ['PENDING', 'IN_REVISION']:
                    self.log.info(f"Entry already has the state '{version_state}', skipping...")
                else:
                    submitted_item = self.submit_item(item_id, modification_date)
                    self.log.info(f"Submitted item: {submitted_item}")