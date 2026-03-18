import requests
import logging
import time
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type

# Configure global logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("scraper.log"),
        logging.StreamHandler()
    ]
)

class BaseAPIScraper:
    """
    Base class for all supermarket API scrapers.
    Implements retry logic, rate limiting, logging, and session management.
    """
    def __init__(self, base_url, store_name):
        self.base_url = base_url
        self.store_name = store_name
        self.session = requests.Session()
        self.logger = logging.getLogger(self.store_name)
        
        # Standard headers to mimic a real browser
        self.default_headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
        }

    @retry(
        wait=wait_exponential(multiplier=1, min=2, max=15), # Exponential backoff: 2s, 4s, 8s, 15s
        stop=stop_after_attempt(5), # Try 5 times before giving up
        retry=retry_if_exception_type((requests.exceptions.RequestException, ValueError))
    )
    def fetch_api(self, endpoint="", params=None, headers=None, method='GET', json_payload=None, **kwargs):
        """
        Fetches an API endpoint returning JSON, with built-in retries and rate limiting.
        """
        req_headers = {**self.default_headers, **(headers or {})}
        url = self.base_url + endpoint if endpoint else self.base_url
        
        self.logger.info(f"Fetching: {url} | Params: {params}")
        
        # Basic rate limiting (1 second between requests to be polite)
        time.sleep(1) 
        
        if method.upper() == 'GET':
            response = self.session.get(url, params=params, headers=req_headers, timeout=20, **kwargs)
        else:
            response = self.session.post(url, json=json_payload, headers=req_headers, timeout=20, **kwargs)
            
        response.raise_for_status() # Raise exception for 4xx/5xx errors
        
        try:
            return response.json()
        except ValueError:
            self.logger.error("Failed to parse JSON response. Ensure the endpoint returns JSON.")
            raise
