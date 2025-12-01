"""DataCite API Client for fetching DOIs and metadata."""

import logging
from typing import List, Tuple, Dict, Any, Optional
import requests
from requests.auth import HTTPBasicAuth

from src.utils.publisher_parser import parse_publisher_from_metadata


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
            logger.error(f"Timeout fetching DOI metadata: {doi}")
            return None  # Return None like 404 - let caller handle this gracefully
        
        except requests.exceptions.ConnectionError as e:
            error_msg = "Verbindung zur DataCite API fehlgeschlagen. Bitte überprüfe deine Internetverbindung."
            logger.error(f"Connection error: {e}")
            raise NetworkError(error_msg)
        
        except requests.exceptions.RequestException as e:
            error_msg = f"Netzwerkfehler bei der Kommunikation mit DataCite: {str(e)}"
            logger.error(f"Request exception: {e}")
            raise NetworkError(error_msg)
    
    def validate_creators_match(self, doi: str, csv_creators: List[Dict[str, Any]]) -> Tuple[bool, str]:
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
            return False, f"DOI {doi} nicht gefunden oder nicht erreichbar"
        
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
        new_creators: List[Dict[str, Any]], 
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

    # =========================================================================
    # Contributor Methods (DataCite Schema 4.6)
    # =========================================================================
    
    # Valid DataCite ContributorTypes
    VALID_CONTRIBUTOR_TYPES = [
        "ContactPerson", "DataCollector", "DataCurator", "DataManager",
        "Distributor", "Editor", "HostingInstitution", "Producer",
        "ProjectLeader", "ProjectManager", "ProjectMember",
        "RegistrationAgency", "RegistrationAuthority", "RelatedPerson",
        "Researcher", "ResearchGroup", "RightsHolder", "Sponsor",
        "Supervisor", "Translator", "WorkPackageLeader", "Other"
    ]
    
    def fetch_all_dois_with_contributors(self) -> List[Tuple[str, str, str, str, str, str, str, str, str, str, str, str, str, str]]:
        """
        Fetch all DOIs with contributor information from DataCite API.
        
        Returns one row per contributor, so a DOI with multiple contributors will appear multiple times.
        Each contributor may have multiple contributorTypes, which are returned as comma-separated list.
        Only ORCID/ROR/ISNI identifiers are included.
        
        The returned tuple includes placeholders for database-only fields (Affiliation, Affiliation Identifier,
        Email, Website, Position) which are empty when fetched from DataCite.
        These can be enriched with database data using enrich_contributors_with_db_data().
        
        Returns:
            List of 14-tuples containing:
            (DOI, Contributor Name, Name Type, Given Name, Family Name, 
             Name Identifier, Name Identifier Scheme, Scheme URI, Contributor Types,
             Affiliation, Affiliation Identifier, Email, Website, Position)
            
        Raises:
            AuthenticationError: If credentials are invalid
            NetworkError: If connection to API fails
            DataCiteAPIError: For other API errors
        """
        all_contributor_data = []
        page_number = 1
        
        logger.info(f"Starting to fetch DOIs with contributors for client: {self.username}")
        
        while True:
            try:
                contributor_data, has_more = self._fetch_page_with_contributors(page_number)
                all_contributor_data.extend(contributor_data)
                
                logger.info(f"Fetched page {page_number}: {len(contributor_data)} contributor entries (Total: {len(all_contributor_data)})")
                
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
        
        logger.info(f"Successfully fetched {len(all_contributor_data)} contributor entries in total")
        return all_contributor_data
    
    def _fetch_page_with_contributors(self, page_number: int) -> Tuple[List[Tuple[str, str, str, str, str, str, str, str, str, str, str, str, str, str]], bool]:
        """
        Fetch a single page of DOIs with contributor information from the API.
        
        Args:
            page_number: Page number to fetch (1-indexed)
            
        Returns:
            Tuple of (list of contributor tuples, has_more_pages boolean)
            Each tuple contains 14 fields:
            (DOI, Contributor Name, Name Type, Given Name, Family Name,
             Name Identifier, Name Identifier Scheme, Scheme URI, Contributor Types,
             Affiliation, Affiliation Identifier, Email, Website, Position)
            Note: Affiliation, Affiliation Identifier, Email, Website, Position are empty
                  as they come from the database, not DataCite.
            
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
        
        logger.debug(f"Requesting contributors from: {url} with params: {params}")
        
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
        
        # Extract DOIs and contributor information
        contributor_entries = []
        seen_contributors = set()  # Track (DOI, name, contributorType) to avoid duplicates
        if "data" in data and isinstance(data["data"], list):
            for item in data["data"]:
                try:
                    doi = item.get("id")
                    if not doi:
                        logger.warning("DOI entry without ID, skipping")
                        continue
                    
                    attributes = item.get("attributes", {})
                    contributors = attributes.get("contributors", [])
                    
                    # Skip DOIs without contributors
                    if not contributors:
                        continue
                    
                    # Process each contributor
                    for contributor in contributors:
                        contributor_name = contributor.get("name", "")
                        name_type = contributor.get("nameType", "")
                        given_name = contributor.get("givenName", "")
                        family_name = contributor.get("familyName", "")
                        contributor_type = contributor.get("contributorType", "")
                        
                        # Default contributorType to "Other" if not provided
                        if not contributor_type:
                            contributor_type = "Other"
                            logger.debug(f"Using default contributorType 'Other' for '{contributor_name}'")
                        
                        # ContributorTypes that are ALWAYS organizations (never persons)
                        # Note: "Sponsor", "Funder", "ResearchGroup" are NOT here because they can be persons too!
                        # (e.g., an individual person can be a sponsor or funder)
                        ORGANIZATIONAL_CONTRIBUTOR_TYPES = {
                            "HostingInstitution",
                            "DistributionCenter",
                            "RegistrationAgency",
                            "RegistrationAuthority",
                        }
                        
                        # ContributorTypes that are ALWAYS persons (never organizations)
                        # Note: "Producer" is intentionally NOT here - it can be a person OR organization
                        PERSONAL_CONTRIBUTOR_TYPES = {
                            "ContactPerson",
                            "DataCollector",
                            "DataCurator",
                            "DataManager",
                            "Editor",
                            "ProjectLeader",
                            "ProjectManager",
                            "ProjectMember",
                            "Researcher",
                            "RightsHolder",
                            "Supervisor",
                            "WorkPackageLeader",
                        }
                        
                        # Keywords that indicate an organizational name (case-insensitive)
                        # Used to detect when DataCite incorrectly splits an org name into given/family name
                        # NOTE: Only include keywords that are unlikely to appear as substrings in person names
                        # Removed short keywords like "ag", "inc", "lab", "rat" that match parts of names
                        # (e.g., "Wagner" contains "ag", "Vincze" contains "inc")
                        ORGANIZATION_KEYWORDS = {
                            "university", "universität", "institute", "institut", "center", "centre",
                            "zentrum", "laboratory", "laboratorium", "college", "school",
                            "faculty", "fakultät", "department", "abteilung", "division",
                            "agency", "agentur", "authority", "behörde", "ministry", "ministerium",
                            "office", "bureau", "service", "dienst", "commission", "kommission",
                            "council", "board", "gremium", "committee", "ausschuss",
                            "foundation", "stiftung", "association", "verband", "verein",
                            "society", "gesellschaft", "organization", "organisation",
                            "corporation", "company", "firma", "gmbh", "ltd",
                            "group", "gruppe", "network", "netzwerk", "consortium",
                            "museum", "library", "bibliothek", "archive", "archiv",
                            "hospital", "klinik", "krankenhaus", "clinic",
                            "transregio", "computing",
                        }
                        
                        def _is_organization_name(name: str) -> bool:
                            """Check if a name contains organizational keywords as whole words."""
                            if not name:
                                return False
                            import re
                            name_lower = name.lower()
                            # Use word boundary matching to avoid false positives
                            # e.g., "Wagner" should not match "ag", "Vincze" should not match "inc"
                            for keyword in ORGANIZATION_KEYWORDS:
                                # Match keyword as a whole word (surrounded by non-word chars or start/end)
                                pattern = r'\b' + re.escape(keyword) + r'\b'
                                if re.search(pattern, name_lower):
                                    return True
                            return False
                        
                        def _looks_like_person_name(name: str) -> bool:
                            """Check if a name looks like a person's name based on format heuristics."""
                            if not name:
                                return False
                            # Format "Nachname, Vorname" - very likely a person
                            if "," in name:
                                parts = [p.strip() for p in name.split(",")]
                                if len(parts) == 2 and all(len(p) > 0 for p in parts):
                                    return True
                            # Format "Vorname Nachname" or "Vorname M. Nachname" - 2-3 words without org keywords
                            words = name.split()
                            if 2 <= len(words) <= 4 and not _is_organization_name(name):
                                # Check if words look like name parts (start with uppercase, reasonable length)
                                return all(len(w) >= 1 and w[0].isupper() for w in words)
                            return False
                        
                        # Extract name identifier FIRST - needed for nameType determination
                        # ORCID is ONLY given to persons, so having an ORCID proves it's a person
                        name_identifier = ""
                        name_identifier_scheme = ""
                        scheme_uri = ""
                        has_orcid = False
                        
                        name_identifiers = contributor.get("nameIdentifiers", [])
                        for identifier in name_identifiers:
                            scheme = identifier.get("nameIdentifierScheme", "")
                            if scheme.upper() == "ORCID":
                                has_orcid = True
                                name_identifier = identifier.get("nameIdentifier", "")
                                name_identifier_scheme = identifier.get("nameIdentifierScheme", "")
                                scheme_uri = identifier.get("schemeUri", "")
                                break
                        
                        # If no ORCID, check for ROR or ISNI
                        if not name_identifier:
                            for identifier in name_identifiers:
                                scheme = identifier.get("nameIdentifierScheme", "")
                                if scheme.upper() in ["ROR", "ISNI"]:
                                    name_identifier = identifier.get("nameIdentifier", "")
                                    name_identifier_scheme = identifier.get("nameIdentifierScheme", "")
                                    scheme_uri = identifier.get("schemeUri", "")
                                    break
                        
                        # Check if the contributor name looks like an organization
                        # This catches cases where DataCite incorrectly splits org names into given/family
                        name_looks_like_org = _is_organization_name(contributor_name)
                        
                        # Determine nameType with clear priority:
                        # 1. ORCID present → ALWAYS Personal (ORCID is only for persons, this is a hard fact)
                        # 2. API provides nameType → TRUST IT (don't override DataCite's data)
                        # 3. Organizational contributor types (and no API nameType) → Organizational
                        # 4. Personal contributor types (and no API nameType, name doesn't look like org) → Personal
                        # 5. Name looks like organization (and no API nameType) → Organizational
                        # 6. Fallback: infer from givenName/familyName presence
                        
                        if has_orcid:
                            # ORCID is ONLY given to persons - this overrides everything including API data
                            # because ORCID is a hard fact that cannot be wrong
                            if name_type != "Personal":
                                if name_type:
                                    logger.warning(f"Overriding nameType '{name_type}' to 'Personal' for '{contributor_name}' (has ORCID)")
                                name_type = "Personal"
                        elif name_type:
                            # API provides nameType - TRUST IT, don't override
                            # This respects DataCite's data even if it seems wrong
                            # (e.g., "Universidad..." marked as Personal is a DataCite error, not ours to fix)
                            pass
                        elif contributor_type in ORGANIZATIONAL_CONTRIBUTOR_TYPES:
                            # These roles are ALWAYS organizations - set nameType
                            name_type = "Organizational"
                            logger.debug(f"Setting nameType 'Organizational' for '{contributor_name}' (contributorType={contributor_type})")
                            # Clear given/family name for organizations (they shouldn't have them)
                            given_name = ""
                            family_name = ""
                        elif contributor_type in PERSONAL_CONTRIBUTOR_TYPES and not name_looks_like_org:
                            # These roles are ALWAYS persons - set nameType
                            # BUT only if the name doesn't look like an organization
                            name_type = "Personal"
                            logger.debug(f"Setting nameType 'Personal' for '{contributor_name}' (contributorType={contributor_type})")
                        elif name_looks_like_org:
                            # Name contains organizational keywords - treat as organization
                            name_type = "Organizational"
                            logger.debug(f"Setting nameType 'Organizational' for '{contributor_name}' (name contains org keywords)")
                            # Clear given/family name for organizations (they shouldn't have them)
                            given_name = ""
                            family_name = ""
                        else:
                            # For ambiguous contributorTypes (e.g., "Other", "RelatedPerson"), 
                            # No API nameType and no clear signals - infer from multiple signals
                            if given_name or family_name:
                                # Has structured name parts - definitely a person
                                name_type = "Personal"
                                logger.debug(f"Inferred nameType 'Personal' for '{contributor_name}' (has given/family name)")
                            elif _looks_like_person_name(contributor_name):
                                # Name format looks like a person (e.g., "Blöcher Guido" or "Doe, John")
                                name_type = "Personal"
                                logger.debug(f"Inferred nameType 'Personal' for '{contributor_name}' (name format looks like person)")
                            else:
                                # Default to Organizational for other cases
                                name_type = "Organizational"
                                logger.debug(f"Inferred nameType 'Organizational' for '{contributor_name}' (no person indicators)")
                        
                        # Create a unique key for this contributor to detect duplicates
                        # Key: (DOI, contributor name, contributor type)
                        contributor_key = (doi.lower(), contributor_name.lower(), contributor_type.lower())
                        if contributor_key in seen_contributors:
                            logger.debug(f"Skipping duplicate contributor: {contributor_name} ({contributor_type}) for DOI {doi}")
                            continue
                        seen_contributors.add(contributor_key)
                        
                        # Create tuple entry with 14 fields
                        # DB-only fields (Affiliation, Affiliation Identifier, Email, Website, Position) are empty
                        entry = (
                            doi,
                            contributor_name,
                            name_type,
                            given_name,
                            family_name,
                            name_identifier,
                            name_identifier_scheme,
                            scheme_uri,
                            contributor_type,  # Single type from DataCite
                            "",  # Affiliation (DB-only, empty from DataCite)
                            "",  # Affiliation Identifier (DB-only, empty from DataCite)
                            "",  # Email (DB-only, empty from DataCite)
                            "",  # Website (DB-only, empty from DataCite)
                            ""   # Position (DB-only, empty from DataCite)
                        )
                        contributor_entries.append(entry)
                        
                except (KeyError, AttributeError, TypeError) as e:
                    logger.warning(f"Error parsing contributor data for DOI {item.get('id', 'unknown')}: {e}")
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
        
        return contributor_entries, has_more
    
    @staticmethod
    def enrich_contributors_with_db_data(
        contributor_data: List[Tuple[str, str, str, str, str, str, str, str, str, str, str, str, str, str]],
        db_client
    ) -> List[Tuple[str, str, str, str, str, str, str, str, str, str, str, str, str, str]]:
        """
        Enrich contributor data from DataCite API with database information.
        
        For each contributor that is a ContactPerson, fetch Email, Website, and Position
        from the SUMARIOPMD database. ContactInfo is linked to resourceagents regardless
        of their role, so we fetch all contactinfo and match by name.
        
        Args:
            contributor_data: List of 14-tuples from fetch_all_dois_with_contributors()
            db_client: Instance of SumariopmdClient (connected)
            
        Returns:
            List of 14-tuples with Email, Website, Position enriched from database
            for ContactPerson contributors.
        """
        import logging
        logger = logging.getLogger(__name__)
        
        enriched_data = []
        
        # Group contributors by DOI for efficient DB lookups
        from collections import defaultdict
        dois_to_process = defaultdict(list)
        for idx, row in enumerate(contributor_data):
            doi = row[0]
            dois_to_process[doi].append((idx, row))
        
        logger.info(f"Enriching {len(contributor_data)} contributors from {len(dois_to_process)} DOIs with DB data")
        
        # Process each DOI
        for doi, contributors in dois_to_process.items():
            try:
                # Get resource_id for this DOI
                resource_id = db_client.get_resource_id_for_doi(doi)
                
                if resource_id is None:
                    # DOI not found in DB, keep original data
                    for idx, row in contributors:
                        enriched_data.append((idx, row))
                    continue
                
                # Fetch ALL contactinfo from DB for this resource (regardless of role)
                # ContactInfo is linked to resourceagent, not to a specific role
                db_contactinfo = db_client.fetch_all_contactinfo_for_resource(resource_id)
                
                # Create lookup map with multiple key formats for flexible matching
                # DB may have: lastname+firstname separate OR only "name" in "Lastname, Firstname" format
                contactinfo_lookup = {}
                for db_entry in db_contactinfo:
                    lastname = db_entry.get("lastname", "") or ""
                    firstname = db_entry.get("firstname", "") or ""
                    name = db_entry.get("name", "") or ""
                    
                    contactinfo_data = {
                        "email": db_entry.get("email", "") or "",
                        "website": db_entry.get("website", "") or "",
                        "position": db_entry.get("position", "") or ""
                    }
                    
                    # Key 1: If we have separate lastname/firstname, use those
                    if lastname:
                        key = (lastname.lower().strip(), firstname.lower().strip())
                        contactinfo_lookup[key] = contactinfo_data
                    
                    # Key 2: If we have a "name" field, try to parse "Lastname, Firstname" format
                    if name:
                        # Also store with full name as key (for organizational names)
                        contactinfo_lookup[(name.lower().strip(), "")] = contactinfo_data
                        
                        # Try to parse "Lastname, Firstname" format
                        if ", " in name:
                            parts = name.split(", ", 1)
                            if len(parts) == 2:
                                parsed_lastname = parts[0].lower().strip()
                                parsed_firstname = parts[1].lower().strip()
                                contactinfo_lookup[(parsed_lastname, parsed_firstname)] = contactinfo_data
                
                # Enrich each contributor
                for idx, row in contributors:
                    family_name = row[4] or ""  # Family Name
                    given_name = row[3] or ""   # Given Name
                    contributor_name = row[1] or ""  # Contributor Name
                    contributor_types = row[8] or ""  # Contributor Types
                    
                    # Try to find matching contactinfo with multiple key formats
                    contactinfo = None
                    
                    # Try 1: exact match with family_name + given_name
                    if family_name:
                        key = (family_name.lower().strip(), given_name.lower().strip())
                        contactinfo = contactinfo_lookup.get(key)
                    
                    # Try 2: match with contributor_name (full name format)
                    if not contactinfo and contributor_name:
                        key = (contributor_name.lower().strip(), "")
                        contactinfo = contactinfo_lookup.get(key)
                        
                        # Try 3: parse contributor_name if it's "Lastname, Firstname" format
                        if not contactinfo and ", " in contributor_name:
                            parts = contributor_name.split(", ", 1)
                            if len(parts) == 2:
                                key = (parts[0].lower().strip(), parts[1].lower().strip())
                                contactinfo = contactinfo_lookup.get(key)
                    
                    # Only add contactinfo if this is a ContactPerson and we found a match
                    if "ContactPerson" in contributor_types and contactinfo:
                        email = contactinfo.get("email", "") or ""
                        website = contactinfo.get("website", "") or ""
                        position = contactinfo.get("position", "") or ""
                        
                        # Create enriched tuple
                        enriched_row = (
                            row[0], row[1], row[2], row[3], row[4],
                            row[5], row[6], row[7], row[8], row[9], row[10],
                            email, website, position
                        )
                        enriched_data.append((idx, enriched_row))
                    else:
                        enriched_data.append((idx, row))
                        
            except Exception as e:
                logger.warning(f"Error enriching DOI {doi} with DB data: {e}")
                # Keep original data on error
                for idx, row in contributors:
                    enriched_data.append((idx, row))
        
        # Sort by original index to maintain order
        enriched_data.sort(key=lambda x: x[0])
        
        # Return just the tuples
        return [row for idx, row in enriched_data]

    def validate_contributors_match(self, doi: str, csv_contributors: List[Dict[str, Any]]) -> Tuple[bool, str]:
        """
        Validate that CSV contributors match the current DataCite metadata.
        
        This is the "dry run" validation that checks:
        - Same number of contributors
        - Contributors exist in DataCite
        
        Args:
            doi: The DOI identifier
            csv_contributors: List of contributor dictionaries from CSV
                             (in the order they appear in the CSV)
            
        Returns:
            Tuple of (is_valid: bool, message: str)
            - (True, "Success message") if validation passes
            - (False, "Error message") if validation fails
        """
        logger.info(f"Validating contributors for DOI {doi}")
        
        # Fetch current metadata
        try:
            metadata = self.get_doi_metadata(doi)
        except (AuthenticationError, NetworkError, DataCiteAPIError) as e:
            return False, f"Fehler beim Abrufen der Metadaten: {str(e)}"
        
        if metadata is None:
            return False, f"DOI {doi} nicht gefunden oder nicht erreichbar"
        
        # Extract current contributors from metadata
        try:
            current_contributors = metadata.get("data", {}).get("attributes", {}).get("contributors", [])
        except (KeyError, AttributeError) as e:
            logger.error(f"Error extracting contributors from metadata: {e}")
            return False, f"Ungültige Metadatenstruktur für DOI {doi}"
        
        # Check if contributor counts match
        if len(current_contributors) != len(csv_contributors):
            return False, (
                f"DOI {doi}: Anzahl der Contributors stimmt nicht überein "
                f"(DataCite: {len(current_contributors)}, CSV: {len(csv_contributors)}). "
                f"Contributors dürfen nicht hinzugefügt oder entfernt werden."
            )
        
        # If no contributors in both, that's valid (though unusual)
        if len(current_contributors) == 0:
            logger.info(f"DOI {doi} has no contributors in DataCite or CSV")
            return True, f"DOI {doi}: Keine Contributors vorhanden"
        
        logger.info(f"DOI {doi}: Validation passed ({len(current_contributors)} contributors)")
        return True, f"DOI {doi}: {len(current_contributors)} Contributors validiert"
    
    def update_doi_contributors(
        self, 
        doi: str, 
        new_contributors: List[Dict[str, Any]], 
        current_metadata: Dict[str, Any]
    ) -> Tuple[bool, str]:
        """
        Update contributor metadata for a specific DOI.
        
        This method preserves ALL existing metadata and only updates the contributors array.
        It follows the pattern: GET current metadata → Replace contributors → PUT full metadata.
        
        Note: Email/Website/Position are NOT sent to DataCite (only stored in local DB).
        Affiliations are preserved from current metadata and not modified.
        
        Args:
            doi: The DOI identifier
            new_contributors: List of contributor dictionaries with updated data
            current_metadata: Full current metadata from get_doi_metadata()
            
        Returns:
            Tuple of (success: bool, message: str)
            - (True, "Success message") if update succeeded
            - (False, "Error message") if update failed
            
        Raises:
            NetworkError: If connection to API fails
        """
        url = f"{self.base_url}/dois/{doi}"
        
        logger.info(f"Updating contributors for DOI {doi}")
        
        # Get current contributors to preserve affiliations
        try:
            current_contributors = current_metadata.get("data", {}).get("attributes", {}).get("contributors", [])
        except (KeyError, AttributeError):
            current_contributors = []
        
        # Build new contributors array from CSV data
        updated_contributors = []
        for i, contributor_data in enumerate(new_contributors):
            contributor_obj = {
                "name": contributor_data.get("name", ""),
                "nameType": contributor_data.get("nameType", "Personal")
            }
            
            # Add contributorType (required for contributors)
            # Handle comma-separated contributor types - use first one for DataCite
            contributor_types = contributor_data.get("contributorTypes", "")
            if contributor_types:
                # Take first type for DataCite (DataCite only supports one type per contributor entry)
                first_type = contributor_types.split(",")[0].strip()
                if first_type in self.VALID_CONTRIBUTOR_TYPES:
                    contributor_obj["contributorType"] = first_type
                else:
                    contributor_obj["contributorType"] = "Other"
            else:
                contributor_obj["contributorType"] = "Other"
            
            # Add given/family names if present (only for Personal contributors)
            given_name = contributor_data.get("givenName", "")
            family_name = contributor_data.get("familyName", "")
            
            if contributor_obj["nameType"] == "Personal":
                if given_name:
                    contributor_obj["givenName"] = given_name
                if family_name:
                    contributor_obj["familyName"] = family_name
            
            # Add name identifier if present
            name_identifier = contributor_data.get("nameIdentifier", "")
            if name_identifier:
                name_identifier_scheme = contributor_data.get("nameIdentifierScheme", "ORCID")
                scheme_uri = contributor_data.get("schemeUri", "https://orcid.org")
                
                contributor_obj["nameIdentifiers"] = [{
                    "nameIdentifier": name_identifier,
                    "nameIdentifierScheme": name_identifier_scheme,
                    "schemeUri": scheme_uri
                }]
            
            # Preserve affiliations from current metadata if available
            if i < len(current_contributors) and "affiliation" in current_contributors[i]:
                contributor_obj["affiliation"] = current_contributors[i]["affiliation"]
            
            updated_contributors.append(contributor_obj)
        
        # Create payload preserving all existing metadata
        try:
            payload = {
                "data": {
                    "type": "dois",
                    "attributes": current_metadata["data"]["attributes"].copy()
                }
            }
            # Replace only the contributors array
            payload["data"]["attributes"]["contributors"] = updated_contributors
            
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
                logger.info(f"Successfully updated contributors for DOI {doi}")
                return True, f"DOI {doi}: {len(updated_contributors)} Contributors erfolgreich aktualisiert"
            
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
    # =========================================================================
    # Publisher Methods (DataCite Schema 4.6)
    # =========================================================================
    
    def fetch_all_dois_with_publisher(self) -> List[Tuple[str, str, str, str, str, str]]:
        """
        Fetch all DOIs with publisher information from DataCite API.
        
        Returns one row per DOI (each DOI has exactly one publisher).
        
        Returns:
            List of tuples containing:
            (DOI, Publisher Name, Publisher Identifier, Publisher Identifier Scheme, 
             Scheme URI, Language)
            
        Raises:
            AuthenticationError: If credentials are invalid
            NetworkError: If connection to API fails
            DataCiteAPIError: For other API errors
        """
        all_publisher_data = []
        page_number = 1
        
        logger.info(f"Starting to fetch DOIs with publisher for client: {self.username}")
        
        while True:
            try:
                publisher_data, has_more = self._fetch_page_with_publisher(page_number)
                all_publisher_data.extend(publisher_data)
                
                logger.info(f"Fetched page {page_number}: {len(publisher_data)} publisher entries (Total: {len(all_publisher_data)})")
                
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
        
        logger.info(f"Successfully fetched {len(all_publisher_data)} publisher entries in total")
        return all_publisher_data
    
    def _fetch_page_with_publisher(self, page_number: int) -> Tuple[List[Tuple[str, str, str, str, str, str]], bool]:
        """
        Fetch a single page of DOIs with publisher information from the API.
        
        Args:
            page_number: Page number to fetch (1-indexed)
            
        Returns:
            Tuple of (list of publisher tuples, has_more_pages boolean)
            Each tuple contains: (DOI, Publisher Name, Publisher Identifier,
                                 Publisher Identifier Scheme, Scheme URI, Language)
            
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
        
        logger.debug(f"Requesting publisher data from: {url} with params: {params}")
        
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
        
        # Extract DOIs and publisher information
        publisher_entries = []
        if "data" in data and isinstance(data["data"], list):
            for item in data["data"]:
                try:
                    doi = item.get("id")
                    if not doi:
                        logger.warning("DOI entry without ID, skipping")
                        continue
                    
                    attributes = item.get("attributes", {})
                    publisher_raw = attributes.get("publisher", "")
                    
                    # Parse publisher using shared utility function
                    parsed = parse_publisher_from_metadata(publisher_raw)
                    publisher_name = parsed["name"]
                    publisher_identifier = parsed["publisherIdentifier"]
                    publisher_identifier_scheme = parsed["publisherIdentifierScheme"]
                    scheme_uri = parsed["schemeUri"]
                    lang = parsed["lang"]
                    
                    # Skip DOIs without publisher
                    if not publisher_name:
                        logger.warning(f"DOI {doi} has no publisher, skipping")
                        continue
                    
                    # Create tuple entry
                    entry = (
                        doi,
                        publisher_name,
                        publisher_identifier,
                        publisher_identifier_scheme,
                        scheme_uri,
                        lang
                    )
                    publisher_entries.append(entry)
                    
                except (KeyError, AttributeError, TypeError) as e:
                    logger.warning(f"Error parsing publisher data for DOI {item.get('id', 'unknown')}: {e}")
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
        
        return publisher_entries, has_more
    
    def update_doi_publisher(
        self, 
        doi: str, 
        publisher_data: Dict[str, str],
        current_metadata: Dict[str, Any]
    ) -> Tuple[bool, str]:
        """
        Update publisher metadata for a specific DOI.
        
        This method preserves ALL existing metadata and only updates the publisher.
        It follows the pattern: GET current metadata → Replace publisher → PUT full metadata.
        
        Args:
            doi: The DOI identifier
            publisher_data: Dictionary with publisher fields:
                - name: Publisher name (required)
                - publisherIdentifier: Identifier (e.g., ROR ID)
                - publisherIdentifierScheme: Scheme name (e.g., "ROR")
                - schemeUri: Scheme URI (e.g., "https://ror.org/")
                - lang: Language code (e.g., "en")
            current_metadata: Full current metadata from get_doi_metadata()
            
        Returns:
            Tuple of (success: bool, message: str)
            - (True, "Success message") if update succeeded
            - (False, "Error message") if update failed
            
        Raises:
            NetworkError: If connection to API fails
        """
        url = f"{self.base_url}/dois/{doi}"
        
        logger.info(f"Updating publisher for DOI {doi}")
        
        # Build publisher object for DataCite API
        publisher_name = publisher_data.get("name", "")
        if not publisher_name:
            error_msg = f"Publisher-Name fehlt für DOI {doi}"
            logger.error(error_msg)
            return False, error_msg
        
        # Check if we have extended publisher fields
        publisher_identifier = publisher_data.get("publisherIdentifier", "")
        publisher_identifier_scheme = publisher_data.get("publisherIdentifierScheme", "")
        scheme_uri = publisher_data.get("schemeUri", "")
        lang = publisher_data.get("lang", "")
        
        # If we have any extended fields, use object format; otherwise use string
        if publisher_identifier or publisher_identifier_scheme or scheme_uri or lang:
            # Extended publisher format (DataCite Schema 4.6)
            updated_publisher = {"name": publisher_name}
            if publisher_identifier:
                updated_publisher["publisherIdentifier"] = publisher_identifier
            if publisher_identifier_scheme:
                updated_publisher["publisherIdentifierScheme"] = publisher_identifier_scheme
            if scheme_uri:
                updated_publisher["schemeUri"] = scheme_uri
            if lang:
                updated_publisher["lang"] = lang
        else:
            # Simple string format (legacy compatibility)
            updated_publisher = publisher_name
        
        # Create payload preserving all existing metadata
        try:
            payload = {
                "data": {
                    "type": "dois",
                    "attributes": current_metadata["data"]["attributes"].copy()
                }
            }
            # Replace only the publisher
            payload["data"]["attributes"]["publisher"] = updated_publisher
            
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
                logger.info(f"Successfully updated publisher for DOI {doi}")
                return True, f"DOI {doi}: Publisher erfolgreich aktualisiert"
            
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
