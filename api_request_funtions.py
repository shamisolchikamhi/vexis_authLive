import json
import ast
import pandas as pd
import http.client

class ApiGet:
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

    def process_reponse_df(self, json_response):
        if isinstance(json_response, str):
            try:
                # Try normal JSON parsing
                json_response = json.loads(json_response)
            except json.JSONDecodeError:
                try:
                    # Handle improperly formatted JSON with single quotes using
                    json_response = ast.literal_eval(json_response)
                except (ValueError, SyntaxError):
                    raise ValueError("Invalid JSON string provided.")

        if isinstance(json_response, dict):
            # Convert a dictionary to a DataFrame
            return pd.DataFrame([json_response])

        if isinstance(json_response, list):
            # Convert a list of dictionaries to a DataFrame
            return pd.DataFrame(json_response)

        raise ValueError("JSON response must be a dictionary or a list of dictionaries.")

        return df

    def fix_data_types(self, df):
        for col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='ignore')

            # If column is numeric, check if it contains only whole numbers
            if pd.api.types.is_numeric_dtype(df[col]):
                if all(isinstance(x, (int, float)) and (float(x).is_integer() if isinstance(x, float) else True) for x
                       in df[col].dropna()):
                    df[col] = df[col].astype(int)

        return df

