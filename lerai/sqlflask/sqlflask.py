import requests
import json
import urllib.parse
import pandas as pd
import logging
import os

class SQLFlask():
    """
    This is a helper class to use SQLFlask AppBattery service. SQLFlask allows users to make requests to common 
    services at Akamai, such as NetarchDB and Mako DB."""

    def __init__(self):

        self.available_services = ['query2', 'mako', 'mrom', 'netarch']

    def set_cert_paths(self, cert_path: str, key_path: str):
        """
        This function allows users to set custom paths for the certificate and key used to authenticate to SQLFlask. 
        If not used, the request call will fail!
        """

        self.cert_path = cert_path
        self.key_path = key_path

    def set_sqlflask_url(self, url: str):
        """
        This function allows users to set a custom URL for SQLFlask. If not used, the query will fail.

        Usage example:
        sqlflask = SQLFlask()
        sqlflask.set_sqlflask_url("https://custom-sqlflask-url.com")
        """

        self.sqlflask_url = url

    def get_request(self, url):

        cert = (
            self.cert_path,
            self.key_path
        )

        request = requests.get(url, cert=cert, verify=False)
        return request

    def run_query(self, service: str, query: str) -> dict:
        """
        Runs the query for the given service and query. It loads the json returned in the response from SQLFlask and returns it.
        """
        
        encoded_query = urllib.parse.quote(query)
        query_request = self.get_request(self.sqlflask_url + f"/?agg={service}&sql={encoded_query}&timeout=100")
        result = json.loads(query_request.text)

        return result

    def get_dataframe(self, service:str, query: str):
        """
        Runs the query for the given service and query, laods the response to a Pandas dataframe.

        Usage example:
        sqlflask = SQLFlask()
        sqlflask.set_cert_paths("/path/to/cert.crt", "/path/to/key.key")
        df = sqlflask.get_dataframe("mako", "select * from footprint_capacity_region where rownum <= 10")

        """

        result = self.run_query(service, query)
        dataframe = pd.DataFrame(result['data'], columns=result['columns'])
        return dataframe