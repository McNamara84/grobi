"""DataCite API Client for fetching DOIs and metadata."""

import logging
from typing import List, Tuple
import requests
from requests.auth import HTTPBasicAuth


logger = logging.getLogger(__name__)


class DataCiteAPIError(Exception):
    """Base exception for DataCite API errors."""
    pass


class AuthenticationError(DataCiteAPIError):
    """Raised when authentication fails."""
    pass


class NetworkError(DataCiteAPIError):
    """Raised when network connection fails."""
    pass


class DataCiteClient:
    """Client for interacting with the DataCite REST API v2."""
    
    PRODUCTION_ENDPOINT = "https://api.datacite.org"
    TEST_ENDPOINT = "https://api.test.datacite.org"
    PAGE_SIZE = 100  # Maximum page size supported by DataCite API
    TIMEOUT = 30  # Request timeout in seconds
    
    def __init__(self, username: str, password: str, use_test_api: bool = False):
        """
        Initialize DataCite API client.
        
        Args:
            username: DataCite username (client-id)
            password: DataCite password
            use_test_api: If True, use test API endpoint instead of production
        """
        self.username = username
        self.password = password
        self.base_url = self.TEST_ENDPOINT if use_test_api else self.PRODUCTION_ENDPOINT
        self.auth = HTTPBasicAuth(username, password)
        
        logger.info(f"DataCite client initialized for {'TEST' if use_test_api else 'PRODUCTION'} API")
    
    def fetch_all_dois(self) -> List[Tuple[str, str]]:
        """
        Fetch all DOIs registered by this client from DataCite API.
        
        Returns:
            List of tuples containing (DOI, Landing Page URL)
            
        Raises:
            AuthenticationError: If credentials are invalid
            NetworkError: If connection to API fails
            DataCiteAPIError: For other API errors
        """
        all_dois = []
        page_number = 1
        
        logger.info(f"Starting to fetch DOIs for client: {self.username}")
        
        while True:
            try:
                dois, has_more = self._fetch_page(page_number)
                all_dois.extend(dois)
                
                logger.info(f"Fetched page {page_number}: {len(dois)} DOIs (Total: {len(all_dois)})")
                
                if not has_more:
                    break
                    
                page_number += 1
                
            except requests.exceptions.Timeout:
                error_msg = "Die Anfrage hat zu lange gedauert. Bitte versuchen Sie es erneut."
                logger.error(f"Timeout on page {page_number}")
                raise DataCiteAPIError(error_msg)
            
            except requests.exceptions.ConnectionError as e:
                error_msg = "Verbindung zur DataCite API fehlgeschlagen. Bitte überprüfen Sie Ihre Internetverbindung."
                logger.error(f"Connection error: {e}")
                raise NetworkError(error_msg)
            
            except requests.exceptions.RequestException as e:
                error_msg = f"Netzwerkfehler bei der Kommunikation mit DataCite: {str(e)}"
                logger.error(f"Request exception: {e}")
                raise NetworkError(error_msg)
        
        logger.info(f"Successfully fetched {len(all_dois)} DOIs in total")
        return all_dois
    
    def _fetch_page(self, page_number: int) -> Tuple[List[Tuple[str, str]], bool]:
        """
        Fetch a single page of DOIs from the API.
        
        Args:
            page_number: Page number to fetch (1-indexed)
            
        Returns:
            Tuple of (list of DOI tuples, has_more_pages boolean)
            
        Raises:
            AuthenticationError: If credentials are invalid
            DataCiteAPIError: For other API errors
        """
        url = f"{self.base_url}/dois"
        params = {
            "client-id": self.username,
            "page[size]": self.PAGE_SIZE,
            "page[number]": page_number
        }
        
        logger.debug(f"Requesting: {url} with params: {params}")
        
        response = requests.get(
            url,
            auth=self.auth,
            params=params,
            timeout=self.TIMEOUT,
            headers={"Accept": "application/vnd.api+json"}
        )
        
        # Handle authentication errors
        if response.status_code == 401:
            error_msg = "Anmeldung fehlgeschlagen. Bitte überprüfen Sie Benutzername und Passwort."
            logger.error(f"Authentication failed for user: {self.username}")
            raise AuthenticationError(error_msg)
        
        # Handle rate limiting
        if response.status_code == 429:
            error_msg = "Zu viele Anfragen. Bitte warten Sie einen Moment und versuchen Sie es erneut."
            logger.error("Rate limit exceeded")
            raise DataCiteAPIError(error_msg)
        
        # Handle other HTTP errors
        if response.status_code != 200:
            error_msg = f"DataCite API Fehler (HTTP {response.status_code}): {response.text}"
            logger.error(f"API error: {response.status_code} - {response.text}")
            raise DataCiteAPIError(error_msg)
        
        # Parse JSON response
        try:
            data = response.json()
        except ValueError as e:
            error_msg = "Ungültige Antwort von der DataCite API (kein gültiges JSON)."
            logger.error(f"Invalid JSON response: {e}")
            raise DataCiteAPIError(error_msg)
        
        # Extract DOIs and URLs
        dois = []
        if "data" in data and isinstance(data["data"], list):
            for item in data["data"]:
                try:
                    doi = item.get("id")
                    url = item.get("attributes", {}).get("url")
                    
                    if doi and url:
                        dois.append((doi, url))
                    else:
                        logger.warning(f"Incomplete data for DOI entry: {item.get('id', 'unknown')}")
                        
                except (KeyError, AttributeError) as e:
                    logger.warning(f"Error parsing DOI entry: {e}")
                    continue
        
        # Check if there are more pages
        has_more = False
        if "links" in data and "next" in data["links"]:
            has_more = True
        
        # Alternative: Check meta information
        if "meta" in data:
            meta = data["meta"]
            page = meta.get("page", page_number)
            total_pages = meta.get("totalPages", 1)
            
            if page < total_pages:
                has_more = True
        
        return dois, has_more
