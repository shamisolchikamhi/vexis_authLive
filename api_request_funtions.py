import json
import ast
import pandas as pd
import numpy as np
import http.client
import time
import requests

class ApiGet:
    # using http client
    def __init__(self, http, api_key):
        self.http = http
        self.api_key = api_key

    def api_connection_auth(self):

        headers = {
            'Authorization': f'Bearer {self.api_key}',
            'Content-Type': 'application/json'
        }

        return headers

    def fetch_data(self, end_point):
        conn = http.client.HTTPSConnection(self.http)
        conn.request("GET", end_point, headers=self.api_connection_auth())
        res = conn.getresponse()
        data = res.read()

        try:
            return json.loads(data.decode("utf-8"))
        except json.JSONDecodeError:
            return {"error": "Failed to parse JSON response"}

    def fetch_data_id(self, end_point, ids):
        results = []

        for id in ids:
            end_point_id = end_point.format(id)
            data = self.fetch_data(end_point=end_point_id)
            if isinstance(data, dict) and data.get("error") == "Failed to parse JSON response":
                time.sleep(10)
                data = self.fetch_data(end_point=end_point_id)
            if isinstance(data, dict) and data.get("error") == "Failed to parse JSON response":
                pass
            else:
                results.append(data)
            time.sleep(1)

        return results

    def process_reponse_df(self, json_response):
        if isinstance(json_response, str):
            try:
                # Try normal JSON parsing
                json_response = json.loads(json_response)
            except json.JSONDecodeError:
                try:
                    # Handle improperly formatted JSON with single quotes
                    json_response = ast.literal_eval(json_response)
                except (ValueError, SyntaxError):
                    raise ValueError("Invalid JSON string provided.")

        # Original method: handles dictionaries and simple lists of dictionaries
        if isinstance(json_response, dict):
            return pd.DataFrame([json_response])

        if all(isinstance(sublist, list) for sublist in json_response):
            flat_list = [item for sublist in json_response for item in sublist]
            # Ensure that nested dictionaries are preserved
            return pd.DataFrame([item if isinstance(item, dict) else {} for item in flat_list])

        if isinstance(json_response, list):
            # Check if each item is a dictionary, otherwise convert to an empty dictionary
            return pd.DataFrame([item if isinstance(item, dict) else {} for item in json_response])

        raise ValueError("JSON response must be a dictionary or a list of dictionaries.")

    def fix_data_types(self, df):
        for col in df.columns:
            # Convert column to numeric where possible
            df[col] = pd.to_numeric(df[col], errors='ignore')

            # If column is numeric, check if it contains only whole numbers
            if pd.api.types.is_numeric_dtype(df[col]):
                # Replace infinite values with NaN
                df[col] = df[col].replace([np.inf, -np.inf], np.nan)

                # If all non-null values are whole numbers, convert to int
                if all(isinstance(x, (int, float)) and (float(x).is_integer() if isinstance(x, float) else True)
                       for x in df[col].dropna()):
                    df[col] = df[col].fillna(0).astype(int)

        return df

class ApiGetRequest:
    def __init__(self, domain, api_key, platform):
        self.domain = domain
        self.api_key = api_key
        self.platform = platform

    def api_connection_auth(self):
        data = {
            "api_key": self.api_key
        }

        return data

    def fetch_data(self, end_point, data_details = dict()):
        url = f"https://{self.domain}/{self.platform}/{end_point}"
        data = self.api_connection_auth()
        data.update(data_details)
        response = requests.post(url, data=data)

        return response.json()

    def fetch_data_id(self, end_point, data_details_key, ids, registrants):
        results = []
        for id in ids:
            data_details = {data_details_key: id}
            if registrants:
                data_details.update({"date_range": 0})
            data = self.fetch_data(end_point=end_point, data_details=data_details)
            results.append(data)
            time.sleep(1)

        return results

    def process_reponse_df(self, json_response, dict_key):
        if dict_key == 'registrants':
            df = pd.DataFrame([item[dict_key] if isinstance(item, dict) else {} for item in json_response])
            all_data = [item for sublist in df['data'] if isinstance(sublist, list) for item in sublist]
            df = pd.DataFrame(all_data)
            if len(df) >0:
                df = df.astype(str)
                df[['id', 'lead_id']] = df[['id', 'lead_id']].astype(int)
                df['signup_date'] = pd.to_datetime(df['signup_date'], format="%a, %d %b %Y, %I:%M %p", errors='coerce')
                df['event'] = pd.to_datetime(df['event'], format="%a, %d %b %Y, %I:%M %p", errors='coerce')
                df['date_live'] = pd.to_datetime(df['date_live'], format="%a, %d %b %Y, %I:%M %p", errors='coerce')
                df['gdpr_status_date'] = pd.to_datetime(df['gdpr_status_date'], format="%a, %d %b %Y, %I:%M %p",
                                                        errors='coerce')
                df['time_live'] = pd.to_timedelta(df['time_live']).dt.total_seconds()/60
                df['entered_live'] = pd.to_timedelta(df['entered_live']).dt.total_seconds()/60
                df['time_replay'] = pd.to_timedelta(df['time_replay']).dt.total_seconds()/60
            return df

        if isinstance(json_response, dict):
            df  = pd.DataFrame(json_response[dict_key])
            return df

        if isinstance(json_response, list):
            df = pd.DataFrame([item[dict_key] if isinstance(item, dict) else {} for item in json_response])
            return df
