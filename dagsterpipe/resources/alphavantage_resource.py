from typing import Dict

import requests
from dagster import ConfigurableResource
from pydantic import PrivateAttr
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

retry_config = {
    "reraise": True,
    "stop": stop_after_attempt(5),
    "retry": retry_if_exception_type(requests.HTTPError),
    "wait": wait_exponential(multiplier=2, min=5, max=60),
}

class AlphaVantageAPI(ConfigurableResource):
    api_access_key: str
    base_url: str = 'https://www.alphavantage.co'
    _session: requests.Session = PrivateAttr()

    def setup_for_execution(self, context) -> None:
        self._session = requests.Session()

    def teardown_after_execution(self, context) -> None:
        self._session.close()

    @retry(**retry_config)
    def make_request(self, method: str, url: str, params: Dict) -> requests.Response:
        response = self._session.request(method, url, params=params)
        response.raise_for_status()
        return response
    
    def get_time_series_daily(self, ticker: str) -> dict:
        url = f"{self.base_url}/query"
        params = {
            "function": "TIME_SERIES_DAILY",
            "outputsize": "full",
            "symbol": ticker,
            "apikey": self.api_access_key
        }
        response = self.make_request("GET", url, params)
        return response.json()

    