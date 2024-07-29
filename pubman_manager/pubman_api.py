import requests
import json
import yaml
import pandas as pd
import dateutil

class PubManAPI:
    def __init__(self, username, password, base_url = "https://pure.mpg.de/rest"):
        self.base_url = base_url
        self.username = username
        self.password = password
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
            self.auth_token = login_response.headers.get("Authorization")
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

        # Initial search request with scrolling enabled
        response = requests.post(
            f"{self.base_url}/items/search?scroll=true",
            headers=headers,
            data=json.dumps(query)
        )

        if response.status_code != 200:
            raise Exception("Failed to search for publications", response.status_code)

        results = response.json()
        print("results",results)

        items = results.get('records', {})
        print("items",items)
        scroll_id = results.get('scrollId')
        print("scroll_id",scroll_id)

        # Continue fetching with scroll ID
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
                    # Add new affiliations to the existing list
                    author_affiliations[full_name].update(affiliation_list)
                else:
                    # Initialize the set of affiliations
                    author_affiliations[full_name] = set(affiliation_list)

        # Convert sets back to lists for the final output
        for author in author_affiliations:
            author_affiliations[author] = list(author_affiliations[author])

        return author_affiliations

    def create_event_publication(self, event_name, start_date, end_date, talk_date, location, invited, title, authors_affiliations):
        print(f"Event Name: {event_name}")
        print(f"Conference Start Date: {start_date}")
        print(f"Conference End Date: {end_date}")
        print(f"Talk Date: {talk_date}")
        print(f"Location: {location}")
        print(f"Invited: {invited}")
        print(f"Title: {title}")
        print(f"Authors and Affiliations: {authors_affiliations}")
        metadata_creators = []
        for author, affiliation in authors_affiliations.items():
            given_name, family_name = author.split(' ', 1)
            metadata_creators.append({
                "person": {
                    "givenName": given_name,
                    "familyName": family_name,
                    "organizations": [{"name": affiliation, "identifierPath": []}]
                },
                "role": "AUTHOR",
                "type": "PERSON"
            })

        item_data = {
            "localTags": [],
            "metadata": {
                "title": title,
                "creators": metadata_creators,
                "dateCreated": talk_date.strftime("%Y-%m-%d"),
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

        headers = {
            "Authorization": self.auth_token,
            "Content-Type": "application/json"
        }
        print("item_data",item_data )
        response = requests.post(
            f"{self.base_url}/items",
            headers=headers,
            data=json.dumps(item_data)
        )

        if response.status_code == 200:
            return response.json()
        else:
            raise Exception("Failed to create item", response.status_code, response.text)

    def process_excel_and_create_publications(self, file_path):
        def find_header_row(df):
            for i, row in df.iterrows():
                if row[0] == 1:
                    return i - 2  # Assuming header is 2 rows above the first entry

        def find_end_row(df, start_row):
            for i in range(start_row, len(df)):
                if pd.isna(df.iloc[i, 1]) or df.iloc[i, 1].strip() == '':  # Stop at the first row where column B is NaN
                    return i - 1
            return len(df) - 1

        def extract_authors_affiliations(row):
            authors_affiliations = {}
            for i in range(1, 21):  # Assume up to 20 authors for scalability
                author_name_key = f'Name {i}'
                affiliation_key = f'Affiliation {i}'
                if author_name_key in row and affiliation_key in row:
                    if pd.notna(row[author_name_key]) and pd.notna(row[affiliation_key]):
                        authors_affiliations[row[author_name_key]] = row[affiliation_key]
            return authors_affiliations

        def process_dataframe(df):
            for index, row in df.iterrows():
                authors_affiliations = extract_authors_affiliations(row)
                # try:
                self.create_event_publication(
                    event_name=row.get('Event Name'),
                    start_date=dateutil.parser.parse(row.get('Conference start date\n(dd.mm.YYYY)')),
                    end_date=dateutil.parser.parse(row.get('Conference end date\n(dd.mm.YYYY)')),
                    talk_date=dateutil.parser.parse(row.get('Talk date\n(dd.mm.YYYY)')),
                    location=row.get('Conference Location'),
                    invited=row.get('Invited (y/n)'),
                    title=row.get('Talk Title'),
                    authors_affiliations=authors_affiliations
                )
                # except Exception as e:
                #     print(f"Failed to create publication for {row.get('Talk Title')}: {e}")
                quit()
        # Load the Excel file without skipping rows
        df_full = pd.read_excel(file_path, engine='openpyxl', header=None)

        # Find the header row and end row dynamically
        header_row = find_header_row(df_full)
        start_row = header_row + 2
        end_row = find_end_row(df_full, start_row)

        # Load the data starting from the header row
        df_data = pd.read_excel(file_path, engine='openpyxl', header=header_row)

        # Filter the data to the identified meaningful range
        df_cleaned = df_data.iloc[start_row - header_row - 1:end_row - header_row]

        # Process the cleaned dataframe
        process_dataframe(df_cleaned)