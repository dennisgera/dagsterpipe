from typing import Dict

import requests
from dagster import ConfigurableResource
from pydantic import PrivateAttr

class AlphaVantageAPI(ConfigurableResource):
    api_access_key: str
    base_url: str = 'https://www.alphavantage.co'
    _session: requests.Session = PrivateAttr()

    def setup_for_execution(self, context) -> None:
        self._session = requests.Session()

    def teardown_after_execution(self, context) -> None:
        self._session.close()
    
    def get_time_series_daily(self, ticker: str) -> Dict:
        url = f"{self.base_url}/query"
        params = {
            "function": "TIME_SERIES_DAILY",
            "outputsize": "full",
            "symbol": ticker,
            "apikey": self.api_access_key
        }
        response = self._session.get(url, params=params)
        response.raise_for_status()
        return response.json()

    