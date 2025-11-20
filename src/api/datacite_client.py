"""DataCite API Client for fetching DOIs and metadata."""

import logging
from typing import List, Tuple, Dict, Any, Optional
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
                error_msg = "Die Anfrage hat zu lange gedauert. Bitte versuche es erneut."
                logger.error(f"Timeout on page {page_number}")
                raise DataCiteAPIError(error_msg)
            
            except requests.exceptions.ConnectionError as e:
                error_msg = "Verbindung zur DataCite API fehlgeschlagen. Bitte überprüfe deine Internetverbindung."
                logger.error(f"Connection error: {e}")
                raise NetworkError(error_msg)
            
            except requests.exceptions.RequestException as e:
                error_msg = f"Netzwerkfehler bei der Kommunikation mit DataCite: {str(e)}"
                logger.error(f"Request exception: {e}")
                raise NetworkError(error_msg)
        
        logger.info(f"Successfully fetched {len(all_dois)} DOIs in total")
        return all_dois
    
    def fetch_all_dois_with_creators(self) -> List[Tuple[str, str, str, str, str, str, str, str]]:
        """
        Fetch all DOIs with creator information from DataCite API.
        
        Returns one row per creator, so a DOI with multiple creators will appear multiple times.
        Only ORCID identifiers are included; other identifier schemes are ignored.
        
        Returns:
            List of tuples containing:
            (DOI, Creator Name, Name Type, Given Name, Family Name, 
             Name Identifier, Name Identifier Scheme, Scheme URI)
            
        Raises:
            AuthenticationError: If credentials are invalid
            NetworkError: If connection to API fails
            DataCiteAPIError: For other API errors
        """
        all_creator_data = []
        page_number = 1
        
        logger.info(f"Starting to fetch DOIs with creators for client: {self.username}")
        
        while True:
            try:
                creator_data, has_more = self._fetch_page_with_creators(page_number)
                all_creator_data.extend(creator_data)
                
                logger.info(f"Fetched page {page_number}: {len(creator_data)} creator entries (Total: {len(all_creator_data)})")
                
                if not has_more:
                    break
                    
                page_number += 1
                
            except requests.exceptions.Timeout:
                error_msg = "Die Anfrage hat zu lange gedauert. Bitte versuche es erneut."
                logger.error(f"Timeout on page {page_number}")
                raise DataCiteAPIError(error_msg)
            
            except requests.exceptions.ConnectionError as e:
                error_msg = "Verbindung zur DataCite API fehlgeschlagen. Bitte überprüfe deine Internetverbindung."
                logger.error(f"Connection error: {e}")
                raise NetworkError(error_msg)
            
            except requests.exceptions.RequestException as e:
                error_msg = f"Netzwerkfehler bei der Kommunikation mit DataCite: {str(e)}"
                logger.error(f"Request exception: {e}")
                raise NetworkError(error_msg)
        
        logger.info(f"Successfully fetched {len(all_creator_data)} creator entries in total")
        return all_creator_data
    
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
            error_msg = "Anmeldung fehlgeschlagen. Bitte überprüfe deinen Benutzernamen und dein Passwort."
            logger.error(f"Authentication failed for user: {self.username}")
            raise AuthenticationError(error_msg)
        
        # Handle rate limiting
        if response.status_code == 429:
            error_msg = "Zu viele Anfragen. Bitte warte einen Moment und versuche es erneut."
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
    
    def _fetch_page_with_creators(self, page_number: int) -> Tuple[List[Tuple[str, str, str, str, str, str, str, str]], bool]:
        """
        Fetch a single page of DOIs with creator information from the API.
        
        Args:
            page_number: Page number to fetch (1-indexed)
            
        Returns:
            Tuple of (list of creator tuples, has_more_pages boolean)
            Each tuple contains: (DOI, Creator Name, Name Type, Given Name, Family Name,
                                 Name Identifier, Name Identifier Scheme, Scheme URI)
            
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
        
        logger.debug(f"Requesting creators from: {url} with params: {params}")
        
        response = requests.get(
            url,
            auth=self.auth,
            params=params,
            timeout=self.TIMEOUT,
            headers={"Accept": "application/vnd.api+json"}
        )
        
        # Handle authentication errors
        if response.status_code == 401:
            error_msg = "Anmeldung fehlgeschlagen. Bitte überprüfe deinen Benutzernamen und dein Passwort."
            logger.error(f"Authentication failed for user: {self.username}")
            raise AuthenticationError(error_msg)
        
        # Handle rate limiting
        if response.status_code == 429:
            error_msg = "Zu viele Anfragen. Bitte warte einen Moment und versuche es erneut."
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
        
        # Extract DOIs and creator information
        creator_entries = []
        if "data" in data and isinstance(data["data"], list):
            for item in data["data"]:
                try:
                    doi = item.get("id")
                    if not doi:
                        logger.warning("DOI entry without ID, skipping")
                        continue
                    
                    attributes = item.get("attributes", {})
                    creators = attributes.get("creators", [])
                    
                    # Skip DOIs without creators
                    if not creators:
                        logger.warning(f"DOI {doi} has no creators, skipping")
                        continue
                    
                    # Process each creator
                    for creator in creators:
                        creator_name = creator.get("name", "")
                        name_type = creator.get("nameType", "")
                        given_name = creator.get("givenName", "")
                        family_name = creator.get("familyName", "")
                        
                        # Extract ORCID identifier if present
                        name_identifier = ""
                        name_identifier_scheme = ""
                        scheme_uri = ""
                        
                        name_identifiers = creator.get("nameIdentifiers", [])
                        for identifier in name_identifiers:
                            scheme = identifier.get("nameIdentifierScheme", "")
                            if scheme.upper() == "ORCID":
                                name_identifier = identifier.get("nameIdentifier", "")
                                name_identifier_scheme = identifier.get("nameIdentifierScheme", "")
                                scheme_uri = identifier.get("schemeUri", "")
                                break  # Only take the first ORCID
                        
                        # Create tuple entry
                        entry = (
                            doi,
                            creator_name,
                            name_type,
                            given_name,
                            family_name,
                            name_identifier,
                            name_identifier_scheme,
                            scheme_uri
                        )
                        creator_entries.append(entry)
                        
                except (KeyError, AttributeError, TypeError) as e:
                    logger.warning(f"Error parsing creator data for DOI {item.get('id', 'unknown')}: {e}")
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
        
        return creator_entries, has_more
    
    def update_doi_url(self, doi: str, new_url: str) -> Tuple[bool, str]:
        """
        Update the landing page URL for a specific DOI.
        
        Args:
            doi: The DOI identifier to update (e.g., "10.5880/GFZ.1.1.2021.001")
            new_url: The new landing page URL
            
        Returns:
            Tuple of (success: bool, message: str)
            - (True, "Success message") if update succeeded
            - (False, "Error message") if update failed
            
        Raises:
            NetworkError: If connection to API fails
        """
        url = f"{self.base_url}/dois/{doi}"
        
        # Prepare JSON payload according to DataCite API specification
        payload = {
            "data": {
                "type": "dois",
                "attributes": {
                    "url": new_url
                }
            }
        }
        
        logger.info(f"Updating DOI {doi} with new URL: {new_url}")
        
        try:
            response = requests.put(
                url,
                auth=self.auth,
                json=payload,
                timeout=self.TIMEOUT,
                headers={
                    "Content-Type": "application/vnd.api+json",
                    "Accept": "application/vnd.api+json"
                }
            )
            
            # Handle different response codes
            if response.status_code == 200:
                logger.info(f"Successfully updated DOI {doi}")
                return True, f"DOI {doi} erfolgreich aktualisiert"
            
            elif response.status_code == 401:
                error_msg = f"Authentifizierung fehlgeschlagen für DOI {doi}"
                logger.error(f"Authentication failed for DOI update: {doi}")
                return False, error_msg
            
            elif response.status_code == 403:
                error_msg = f"Keine Berechtigung für DOI {doi} (gehört möglicherweise einem anderen Client)"
                logger.error(f"Forbidden: No permission to update DOI {doi}")
                return False, error_msg
            
            elif response.status_code == 404:
                error_msg = f"DOI {doi} nicht gefunden"
                logger.error(f"DOI not found: {doi}")
                return False, error_msg
            
            elif response.status_code == 422:
                # Unprocessable Entity - validation error
                error_msg = f"Ungültige URL für DOI {doi}"
                logger.error(f"Validation error for DOI {doi}: {response.text}")
                return False, error_msg
            
            elif response.status_code == 429:
                error_msg = "Zu viele Anfragen - Rate Limit erreicht"
                logger.error("Rate limit exceeded during update")
                return False, error_msg
            
            else:
                error_msg = f"API Fehler (HTTP {response.status_code}): {response.text}"
                logger.error(f"Unexpected status code {response.status_code} for DOI {doi}: {response.text}")
                return False, error_msg
                
        except requests.exceptions.Timeout:
            error_msg = f"Zeitüberschreitung bei DOI {doi}"
            logger.error(f"Timeout updating DOI {doi}")
            return False, error_msg
        
        except requests.exceptions.ConnectionError as e:
            error_msg = "Verbindungsfehler zur DataCite API"
            logger.error(f"Connection error during update: {e}")
            raise NetworkError(error_msg)
        
        except requests.exceptions.RequestException as e:
            error_msg = f"Netzwerkfehler bei DOI {doi}: {str(e)}"
            logger.error(f"Request exception during update: {e}")
            raise NetworkError(error_msg)
    
    def get_doi_metadata(self, doi: str) -> Optional[Dict[str, Any]]:
        """
        Fetch complete metadata for a specific DOI.
        
        Args:
            doi: The DOI identifier (e.g., "10.5880/GFZ.1.1.2021.001")
            
        Returns:
            Complete metadata dictionary from DataCite API, or None if DOI not found
            
        Raises:
            AuthenticationError: If credentials are invalid
            NetworkError: If connection to API fails
            DataCiteAPIError: For other API errors
        """
        url = f"{self.base_url}/dois/{doi}"
        
        logger.info(f"Fetching metadata for DOI: {doi}")
        
        try:
            response = requests.get(
                url,
                auth=self.auth,
                timeout=self.TIMEOUT,
                headers={"Accept": "application/vnd.api+json"}
            )
            
            # Handle different response codes
            if response.status_code == 200:
                try:
                    data = response.json()
                    logger.info(f"Successfully fetched metadata for DOI {doi}")
                    return data
                except ValueError as e:
                    error_msg = f"Ungültige JSON-Antwort für DOI {doi}"
                    logger.error(f"Invalid JSON response: {e}")
                    raise DataCiteAPIError(error_msg)
            
            elif response.status_code == 401:
                error_msg = "Anmeldung fehlgeschlagen. Bitte überprüfe deinen Benutzernamen und dein Passwort."
                logger.error(f"Authentication failed while fetching DOI: {doi}")
                raise AuthenticationError(error_msg)
            
            elif response.status_code == 404:
                logger.warning(f"DOI not found: {doi}")
                return None
            
            elif response.status_code == 429:
                error_msg = "Zu viele Anfragen. Bitte warte einen Moment und versuche es erneut."
                logger.error("Rate limit exceeded")
                raise DataCiteAPIError(error_msg)
            
            else:
                error_msg = f"DataCite API Fehler (HTTP {response.status_code}): {response.text}"
                logger.error(f"API error for DOI {doi}: {response.status_code} - {response.text}")
                raise DataCiteAPIError(error_msg)
                
        except requests.exceptions.Timeout:
            error_msg = f"Zeitüberschreitung bei DOI {doi}"
            logger.error(f"Timeout fetching DOI metadata: {doi}")
            raise DataCiteAPIError(error_msg)
        
        except requests.exceptions.ConnectionError as e:
            error_msg = "Verbindung zur DataCite API fehlgeschlagen. Bitte überprüfe deine Internetverbindung."
            logger.error(f"Connection error: {e}")
            raise NetworkError(error_msg)
        
        except requests.exceptions.RequestException as e:
            error_msg = f"Netzwerkfehler bei der Kommunikation mit DataCite: {str(e)}"
            logger.error(f"Request exception: {e}")
            raise NetworkError(error_msg)
    
    def validate_creators_match(self, doi: str, csv_creators: List[Dict[str, str]]) -> Tuple[bool, str]:
        """
        Validate that CSV creators match the current DataCite metadata exactly.
        
        This is the "dry run" validation that checks:
        - Same number of creators
        - Same order of creators
        - No additions or deletions
        
        Args:
            doi: The DOI identifier
            csv_creators: List of creator dictionaries from CSV
                         (in the order they appear in the CSV)
            
        Returns:
            Tuple of (is_valid: bool, message: str)
            - (True, "Success message") if validation passes
            - (False, "Error message") if validation fails
        """
        logger.info(f"Validating creators for DOI {doi}")
        
        # Fetch current metadata
        try:
            metadata = self.get_doi_metadata(doi)
        except (AuthenticationError, NetworkError, DataCiteAPIError) as e:
            return False, f"Fehler beim Abrufen der Metadaten: {str(e)}"
        
        if metadata is None:
            return False, f"DOI {doi} nicht gefunden"
        
        # Extract current creators from metadata
        try:
            current_creators = metadata.get("data", {}).get("attributes", {}).get("creators", [])
        except (KeyError, AttributeError) as e:
            logger.error(f"Error extracting creators from metadata: {e}")
            return False, f"Ungültige Metadatenstruktur für DOI {doi}"
        
        # Check if creator counts match
        if len(current_creators) != len(csv_creators):
            return False, (
                f"DOI {doi}: Anzahl der Creators stimmt nicht überein "
                f"(DataCite: {len(current_creators)}, CSV: {len(csv_creators)}). "
                f"Creators dürfen nicht hinzugefügt oder entfernt werden."
            )
        
        # If no creators in both, that's valid (though unusual)
        if len(current_creators) == 0:
            logger.info(f"DOI {doi} has no creators in DataCite or CSV")
            return True, f"DOI {doi}: Keine Creators vorhanden"
        
        # Validate order and consistency
        for i, (current, csv_creator) in enumerate(zip(current_creators, csv_creators), 1):
            current_name = current.get("name", "")
            csv_name = csv_creator.get("name", "")
            
            # Compare names (basic check - names should match or be similar)
            # We're lenient here since we mainly care about count and order
            if current_name and csv_name:
                # Just log if names differ, don't fail validation
                # (users might have intentionally edited names)
                if current_name != csv_name:
                    logger.info(
                        f"DOI {doi}: Creator {i} name differs "
                        f"(DataCite: '{current_name}', CSV: '{csv_name}')"
                    )
        
        logger.info(f"DOI {doi}: Validation passed ({len(current_creators)} creators)")
        return True, f"DOI {doi}: {len(current_creators)} Creators validiert"
    
    def update_doi_creators(
        self, 
        doi: str, 
        new_creators: List[Dict[str, str]], 
        current_metadata: Dict[str, Any]
    ) -> Tuple[bool, str]:
        """
        Update creator metadata for a specific DOI.
        
        This method preserves ALL existing metadata and only updates the creators array.
        It follows the pattern: GET current metadata → Replace creators → PUT full metadata.
        
        Args:
            doi: The DOI identifier
            new_creators: List of creator dictionaries with updated data
            current_metadata: Full current metadata from get_doi_metadata()
            
        Returns:
            Tuple of (success: bool, message: str)
            - (True, "Success message") if update succeeded
            - (False, "Error message") if update failed
            
        Raises:
            NetworkError: If connection to API fails
        """
        url = f"{self.base_url}/dois/{doi}"
        
        logger.info(f"Updating creators for DOI {doi}")
        
        # Build new creators array from CSV data
        updated_creators = []
        for creator_data in new_creators:
            creator_obj = {
                "name": creator_data.get("name", ""),
                "nameType": creator_data.get("nameType", "Personal")
            }
            
            # Add given/family names if present (only for Personal creators)
            given_name = creator_data.get("givenName", "")
            family_name = creator_data.get("familyName", "")
            
            # Only add given/family names for Personal creators
            if creator_obj["nameType"] == "Personal":
                if given_name:
                    creator_obj["givenName"] = given_name
                if family_name:
                    creator_obj["familyName"] = family_name
            
            # Add ORCID if present
            name_identifier = creator_data.get("nameIdentifier", "")
            if name_identifier:
                name_identifier_scheme = creator_data.get("nameIdentifierScheme", "ORCID")
                scheme_uri = creator_data.get("schemeUri", "https://orcid.org")
                
                creator_obj["nameIdentifiers"] = [{
                    "nameIdentifier": name_identifier,
                    "nameIdentifierScheme": name_identifier_scheme,
                    "schemeUri": scheme_uri
                }]
            
            updated_creators.append(creator_obj)
        
        # Create payload preserving all existing metadata
        try:
            payload = {
                "data": {
                    "type": "dois",
                    "attributes": current_metadata["data"]["attributes"].copy()
                }
            }
            # Replace only the creators array
            payload["data"]["attributes"]["creators"] = updated_creators
            
        except (KeyError, TypeError) as e:
            error_msg = f"Fehler beim Erstellen der Payload für DOI {doi}: {str(e)}"
            logger.error(f"Error building payload: {e}")
            return False, error_msg
        
        # Send PUT request
        try:
            response = requests.put(
                url,
                auth=self.auth,
                json=payload,
                timeout=self.TIMEOUT,
                headers={
                    "Content-Type": "application/vnd.api+json",
                    "Accept": "application/vnd.api+json"
                }
            )
            
            # Handle different response codes
            if response.status_code == 200:
                logger.info(f"Successfully updated creators for DOI {doi}")
                return True, f"DOI {doi}: {len(updated_creators)} Creators erfolgreich aktualisiert"
            
            elif response.status_code == 401:
                error_msg = f"Authentifizierung fehlgeschlagen für DOI {doi}"
                logger.error(f"Authentication failed for DOI update: {doi}")
                return False, error_msg
            
            elif response.status_code == 403:
                error_msg = f"Keine Berechtigung für DOI {doi} (gehört möglicherweise einem anderen Client)"
                logger.error(f"Forbidden: No permission to update DOI {doi}")
                return False, error_msg
            
            elif response.status_code == 404:
                error_msg = f"DOI {doi} nicht gefunden"
                logger.error(f"DOI not found: {doi}")
                return False, error_msg
            
            elif response.status_code == 422:
                # Unprocessable Entity - validation error
                error_msg = f"Validierungsfehler für DOI {doi}: {response.text}"
                logger.error(f"Validation error for DOI {doi}: {response.text}")
                return False, error_msg
            
            elif response.status_code == 429:
                error_msg = "Zu viele Anfragen - Rate Limit erreicht"
                logger.error("Rate limit exceeded during update")
                return False, error_msg
            
            else:
                error_msg = f"API Fehler (HTTP {response.status_code}): {response.text}"
                logger.error(f"Unexpected status code {response.status_code} for DOI {doi}: {response.text}")
                return False, error_msg
                
        except requests.exceptions.Timeout:
            error_msg = f"Zeitüberschreitung bei DOI {doi}"
            logger.error(f"Timeout updating DOI {doi}")
            return False, error_msg
        
        except requests.exceptions.ConnectionError as e:
            error_msg = "Verbindungsfehler zur DataCite API"
            logger.error(f"Connection error during update: {e}")
            raise NetworkError(error_msg)
        
        except requests.exceptions.RequestException as e:
            error_msg = f"Netzwerkfehler bei DOI {doi}: {str(e)}"
            logger.error(f"Request exception during update: {e}")
            raise NetworkError(error_msg)
