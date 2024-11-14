import requests
import json
import logging
import pandas as pd
import jwt

from collections import OrderedDict
from pathlib import Path

from pubman_manager import PUBMAN_CACHE_DIR, ENV_USERNAME, ENV_PASSWORD

class PubmanBase:
    def __init__(self, base_url = "https://pure.mpg.de/rest", auth_token = None, user_id=None):
        logging.basicConfig(level=logging.INFO)
        self.log = logging.getLogger()
        self.base_url = base_url

        self.org_id = 'ou_1863381' # PuRe Org ID for all MPIE publications, TODO: fetch based on Institute name
        # self.user_id = "user_1944725"  # PuRe User id for user PuRe user "Mentock", TODO: fetch automatically based on username
        self.ctx_id = "ctx_2019354" # PuRe CTX ID for all MPIE publications, TODO: fetch based on org_id
        if auth_token and user_id:
            self.auth_token = auth_token
            self.user_id = user_id
        else:
            self.log.info('No auth_token provided, using ENV_USERNAME and ENV_PASSWORD')
            self.auth_token, self.user_id = self.login(ENV_USERNAME, ENV_PASSWORD)


    @staticmethod
    def login(username, password):
        login_response = requests.post(
            f"https://pure.mpg.de/rest/login",
            headers={"Content-Type": "text/plain"},
            data=f"{username}:{password}"
        )
        if login_response.status_code == 200:
            print("login_response.headers",login_response.headers)
            auth_token = login_response.headers.get("Token")
            decoded_token = jwt.decode(auth_token, options={"verify_signature": False})
            return auth_token, decoded_token['id']
        else:
            raise Exception("Failed to log in to PuRe")

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
        # if response.status_code != 200:
        #     self.log.info(f'deleting item {item_id} failed: {response.text}')

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

        print("headers",headers)
        print("query",query)
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
