"""DataCite API Client for fetching DOIs and metadata."""





import copy


import logging


from typing import List, Tuple, Dict, Any, Optional


from urllib.parse import urlparse, urlunparse, quote, unquote


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


        Uses cursor-based pagination to retrieve all records without limitation.


        


        Returns:


            List of tuples containing (DOI, Landing Page URL)


            


        Raises:


            AuthenticationError: If credentials are invalid


            NetworkError: If connection to API fails


            DataCiteAPIError: For other API errors


        """


        all_dois = []


        next_url = None  # Start with None to use initial cursor


        page_count = 0


        


        logger.info(f"Starting to fetch DOIs for client: {self.username} (using cursor pagination)")


        


        while True:


            try:


                page_count += 1


                dois, next_url = self._fetch_page(next_url)


                all_dois.extend(dois)


                


                logger.info(f"Fetched page {page_count}: {len(dois)} DOIs (Total: {len(all_dois)})")


                


                if not next_url:


                    break


                


            except requests.exceptions.Timeout:


                error_msg = "Die Anfrage hat zu lange gedauert. Bitte versuche es erneut."


                logger.error(f"Timeout on page {page_count}")


                raise DataCiteAPIError(error_msg)


            


            except requests.exceptions.ConnectionError as e:


                error_msg = "Verbindung zur DataCite API fehlgeschlagen. Bitte Ã¼berprÃ¼fe deine Internetverbindung."


                logger.error(f"Connection error: {e}")


                raise NetworkError(error_msg)


            


            except requests.exceptions.RequestException as e:


                error_msg = f"Netzwerkfehler bei der Kommunikation mit DataCite: {str(e)}"


                logger.error(f"Request exception: {e}")


                raise NetworkError(error_msg)


        


        logger.info(f"Successfully fetched {len(all_dois)} DOIs in total")


        return all_dois


    


    @staticmethod


    def normalize_url(url: str) -> str:


        """


        Normalize and properly encode a URL for DataCite API.


        


        URLs must be properly encoded according to RFC 3986. This function ensures that:


        - Special characters in query parameters are percent-encoded (e.g., : â %3A)


        - The URL structure (scheme, netloc, path, query, fragment) is preserved


        - URLs are normalized by decoding and re-encoding to ensure consistent formatting


        


        Args:


            url: The URL to normalize


            


        Returns:


            Properly encoded URL string


            


        Examples:


            >>> normalize_url("http://example.com/path?id=test:123")


            'http://example.com/path?id=test%3A123'


            


            >>> normalize_url("http://example.com/path?id=test%3A123")  # Already encoded


            'http://example.com/path?id=test%3A123'


            


        Note:


            This method uses a decode-then-encode strategy to normalize all URLs consistently.


            This means that any percent-encoded sequences will be decoded and re-encoded in a


            standardized way. Double-encoded sequences (e.g., "%2520") will be normalized to


            their single-encoded form ("%20"). This is intentional behavior to ensure DataCite


            receives properly formatted URLs, as double-encoding typically causes issues.


            


            **Important**: If your URL legitimately contains double-encoded sequences (e.g., a


            query parameter value that is itself a percent-encoded string like "%2520"), this


            normalization will decode it. Always provide unencoded or single-encoded URLs as input.


        """


        try:


            # Parse the URL into components


            parsed = urlparse(url)


            


            # Decode first, then encode to avoid double-encoding


            # This handles URLs that are already partially or fully encoded


            decoded_query = unquote(parsed.query) if parsed.query else ''


            decoded_path = unquote(parsed.path) if parsed.path else ''


            


            # Now encode properly:


            # For query: keep '=', '&', and '+' unencoded


            # '=' and '&' are query separators, '+' represents spaces in query strings


            # but encode special characters like ':' to '%3A'


            encoded_query = quote(decoded_query, safe='=&+') if decoded_query else ''


            


            # For path: keep '/' unencoded (it's a path separator)


            encoded_path = quote(decoded_path, safe='/') if decoded_path else ''


            


            # Reconstruct the URL with encoded components


            normalized = urlunparse((


                parsed.scheme,


                parsed.netloc,


                encoded_path,


                parsed.params,


                encoded_query,


                parsed.fragment


            ))


            


            return normalized


            


        except Exception as e:


            logger.warning(f"Could not normalize URL '{url}': {e}. Using original URL.")


            return url


    


    @staticmethod


    def _format_missing_fields_list(fields: List[str]) -> str:


        """


        Format a list of missing field names into a human-readable German string.


        


        Args:


            fields: List of field names


            


        Returns:


            Formatted string with proper German grammar:


            - 1 field: "title"


            - 2 fields: "title und creators"


            - 3+ fields: "title, creators und publisher"


        """


        if len(fields) == 1:


            return fields[0]


        elif len(fields) == 2:


            return ' und '.join(fields)


        else:


            return ', '.join(fields[:-1]) + ' und ' + fields[-1]


    


    @staticmethod


    def _format_missing_fields_with_verb(fields: List[str]) -> tuple[str, str]:


        """


        Format missing fields and determine the correct German verb form.


        


        Args:


            fields: List of missing field names


            


        Returns:


            Tuple of (formatted_fields_string, verb) where verb is "fehlt" or "fehlen"


        """


        fields_str = DataCiteClient._format_missing_fields_list(fields)


        verb = "fehlt" if len(fields) == 1 else "fehlen"


        return fields_str, verb


    


    @staticmethod


    def _filter_non_autofillable_fields(missing_fields: List[str]) -> List[str]:


        """


        Filter list of missing fields to only include non-auto-fillable fields.


        


        Auto-fillable fields:


        - resourceTypeGeneral: Can be filled with 'Dataset'


        - publisher: Can be filled with 'GFZ Data Services'


        


        Non-auto-fillable fields:


        - title: Must be provided manually


        - creators: Must be provided manually


        


        Args:


            missing_fields: List of all missing mandatory field names


            


        Returns:


            List containing only 'title' and/or 'creators' if they are missing


        """


        return [f for f in missing_fields if f in ['title', 'creators']]


    


    @staticmethod


    def _check_missing_mandatory_fields(attributes: Dict[str, Any]) -> List[str]:


        """


        Check which mandatory DataCite fields are missing from metadata.


        


        Checks for the presence of:


        - titles: Required, cannot be auto-filled


        - creators: Required, cannot be auto-filled


        - resourceTypeGeneral: Can be auto-filled with 'Dataset'


        - publisher: Can be auto-filled with 'GFZ Data Services'


        


        Args:


            attributes: The 'attributes' section of DataCite metadata


            


        Returns:


            List of missing field names


        """


        missing_fields = []


        


        titles = attributes.get('titles', [])


        if not titles:


            missing_fields.append('title')


        


        creators = attributes.get('creators', [])


        if not creators:


            missing_fields.append('creators')


        


        types = attributes.get('types', {})


        resource_type_general = types.get('resourceTypeGeneral')


        if not resource_type_general:


            missing_fields.append('resourceTypeGeneral')


        


        publisher = attributes.get('publisher')


        if not publisher:


            missing_fields.append('publisher')


        


        return missing_fields


    


    def fetch_all_dois_with_creators(self) -> List[Tuple[str, str, str, str, str, str, str, str]]:


        """


        Fetch all DOIs with creator information from DataCite API.


        Uses cursor-based pagination to retrieve all records without limitation.


        


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


        next_url = None  # Start with None to use initial cursor


        page_count = 0


        


        logger.info(f"Starting to fetch DOIs with creators for client: {self.username} (using cursor pagination)")


        


        while True:


            try:


                page_count += 1


                creator_data, next_url = self._fetch_page_with_creators(next_url)


                all_creator_data.extend(creator_data)


                


                logger.info(f"Fetched page {page_count}: {len(creator_data)} creator entries (Total: {len(all_creator_data)})")


                


                if not next_url:


                    break


                


            except requests.exceptions.Timeout:


                error_msg = "Die Anfrage hat zu lange gedauert. Bitte versuche es erneut."


                logger.error(f"Timeout on page {page_count}")


                raise DataCiteAPIError(error_msg)


            


            except requests.exceptions.ConnectionError as e:


                error_msg = "Verbindung zur DataCite API fehlgeschlagen. Bitte Ã¼berprÃ¼fe deine Internetverbindung."


                logger.error(f"Connection error: {e}")


                raise NetworkError(error_msg)


            


            except requests.exceptions.RequestException as e:


                error_msg = f"Netzwerkfehler bei der Kommunikation mit DataCite: {str(e)}"


                logger.error(f"Request exception: {e}")


                raise NetworkError(error_msg)


        


        logger.info(f"Successfully fetched {len(all_creator_data)} creator entries in total")


        return all_creator_data


    


    def _fetch_page(self, next_url: Optional[str] = None) -> Tuple[List[Tuple[str, str]], Optional[str]]:


        """


        Fetch a single page of DOIs from the API using cursor-based pagination.


        


        Args:


            next_url: Full URL for next page (from previous response), or None for first page


            


        Returns:


            Tuple of (list of DOI tuples, next_url for pagination or None if no more pages)


            


        Raises:


            AuthenticationError: If credentials are invalid


            DataCiteAPIError: For other API errors


        """


        if next_url:


            # Use the complete next URL from the API response


            url = next_url


            params = None


            logger.debug(f"Requesting next page: {url}")


        else:


            # First page: use cursor=1


            # Note: DataCite API explicitly requires page[cursor]=1 for the first page


            # See: https://support.datacite.org/docs/pagination (Method 2: Cursor)


            url = f"{self.base_url}/dois"


            params = {


                "client-id": self.username,


                "page[size]": self.PAGE_SIZE,


                "page[cursor]": 1


            }


            logger.debug(f"Requesting first page: {url} with params: {params}")


        


        response = requests.get(


            url,


            auth=self.auth,


            params=params,


            timeout=self.TIMEOUT,


            headers={"Accept": "application/vnd.api+json"}


        )


        


        # Handle authentication errors


        if response.status_code == 401:


            error_msg = "Anmeldung fehlgeschlagen. Bitte Ã¼berprÃ¼fe deinen Benutzernamen und dein Passwort."


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


            error_msg = "UngÃ¼ltige Antwort von der DataCite API (kein gÃ¼ltiges JSON)."


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


        


        # Extract next page URL from response


        next_page_url = None


        if "links" in data and "next" in data["links"]:


            next_page_url = data["links"]["next"]


            logger.debug(f"Next page URL: {next_page_url}")


        


        return dois, next_page_url


    


    def _fetch_page_with_creators(self, next_url: Optional[str] = None) -> Tuple[List[Tuple[str, str, str, str, str, str, str, str]], Optional[str]]:


        """


        Fetch a single page of DOIs with creator information from the API using cursor-based pagination.


        


        Args:


            next_url: Full URL for next page (from previous response), or None for first page


            


        Returns:


            Tuple of (list of creator tuples, next_url for pagination or None if no more pages)


            Each tuple contains: (DOI, Creator Name, Name Type, Given Name, Family Name,


                                 Name Identifier, Name Identifier Scheme, Scheme URI)


            


        Raises:


            AuthenticationError: If credentials are invalid


            DataCiteAPIError: For other API errors


        """


        if next_url:


            # Use the complete next URL from the API response


            url = next_url


            params = None


            logger.debug(f"Requesting next page with creators: {url}")


        else:


            # First page: use cursor=1


            # Note: DataCite API explicitly requires page[cursor]=1 for the first page


            # See: https://support.datacite.org/docs/pagination (Method 2: Cursor)


            url = f"{self.base_url}/dois"


            params = {


                "client-id": self.username,


                "page[size]": self.PAGE_SIZE,


                "page[cursor]": 1


            }


            logger.debug(f"Requesting first page with creators: {url} with params: {params}")


        


        response = requests.get(


            url,


            auth=self.auth,


            params=params,


            timeout=self.TIMEOUT,


            headers={"Accept": "application/vnd.api+json"}


        )


        


        # Handle authentication errors


        if response.status_code == 401:


            error_msg = "Anmeldung fehlgeschlagen. Bitte Ã¼berprÃ¼fe deinen Benutzernamen und dein Passwort."


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


            error_msg = "UngÃ¼ltige Antwort von der DataCite API (kein gÃ¼ltiges JSON)."


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


        


        # Extract next page URL from response


        next_page_url = None


        if "links" in data and "next" in data["links"]:


            next_page_url = data["links"]["next"]


            logger.debug(f"Next page URL: {next_page_url}")


        


        return creator_entries, next_page_url


    


    def update_doi_url(self, doi: str, new_url: str) -> Tuple[bool, str]:


        """


        Update the landing page URL for a specific DOI.


        


        URLs are automatically normalized and percent-encoded according to RFC 3986


        before being sent to DataCite API (e.g., colons in query parameters are 


        encoded as %3A).


        


        If the DOI uses deprecated Schema 3, it will be automatically upgraded to


        Schema 4 (kernel-4) during the URL update. The upgrade ensures that required


        Schema 4 metadata (like resourceTypeGeneral) is present.


        


        Args:


            doi: The DOI identifier to update (e.g., "10.5880/GFZ.1.1.2021.001")


            new_url: The new landing page URL (will be normalized automatically)


            


        Returns:


            Tuple of (success: bool, message: str)


            - (True, "Success message") if update succeeded


            - (False, "Error message") if update failed


            


        Raises:


            NetworkError: If connection to API fails


        """


        # Normalize and encode the URL for DataCite API


        normalized_url = self.normalize_url(new_url)


        


        # Log if URL was modified during normalization


        if normalized_url != new_url:


            logger.debug(f"URL normalized: '{new_url}' â '{normalized_url}'")


        


        # Try update with automatic schema upgrade if needed


        return self._update_doi_with_schema_upgrade(doi, normalized_url)


    


    def _update_doi_with_schema_upgrade(self, doi: str, normalized_url: str) -> Tuple[bool, str]:


        """


        Update DOI URL with automatic schema upgrade if Schema 3 is detected.


        


        This method first tries a simple URL update. If it fails with a schema


        deprecation error, it fetches the current metadata, upgrades to Schema 4,


        and retries the update.


        


        Args:


            doi: The DOI identifier


            normalized_url: The normalized landing page URL


            


        Returns:


            Tuple of (success: bool, message: str)


            


        Note:


            May raise NetworkError indirectly through internal API calls if


            connection to DataCite API fails (timeout or connection errors).


        """


        url = f"{self.base_url}/dois/{doi}"


        


        # Prepare simple URL-only update payload


        simple_payload = {


            "data": {


                "type": "dois",


                "attributes": {


                    "url": normalized_url


                }


            }


        }


        


        logger.info(f"Updating DOI {doi} with normalized URL: {normalized_url}")


        


        try:


            # First attempt: simple URL update


            response = requests.put(


                url,


                auth=self.auth,


                json=simple_payload,


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


                error_msg = f"Authentifizierung fehlgeschlagen fÃ¼r DOI {doi}"


                logger.error(f"Authentication failed for DOI update: {doi}")


                return False, error_msg


            


            elif response.status_code == 403:


                error_msg = f"Keine Berechtigung fÃ¼r DOI {doi} (gehÃ¶rt mÃ¶glicherweise einem anderen Client)"


                logger.error(f"Forbidden: No permission to update DOI {doi}")


                return False, error_msg


            


            elif response.status_code == 404:


                error_msg = f"DOI {doi} nicht gefunden"


                logger.error(f"DOI not found: {doi}")


                return False, error_msg


            


            elif response.status_code == 422:


                # Unprocessable Entity - validation error


                # Check if this is a schema-related error that can be fixed with upgrade


                try:


                    error_data = response.json()


                    if 'errors' in error_data and error_data['errors']:


                        error_details = error_data['errors'][0].get('title', '')


                        


                        # Check for schema deprecation error (kernel-3 no longer supported)


                        if 'schema' in error_details.lower() and 'no longer supported' in error_details.lower():


                            logger.warning(f"DOI {doi} uses deprecated Schema 3, attempting automatic upgrade to Schema 4")


                            return self._retry_update_with_schema_upgrade(doi, normalized_url, error_details)


                        


                        # Check for missing schema version error (no matching global declaration)


                        elif 'no matching global declaration' in error_details.lower():


                            logger.warning(f"DOI {doi} has missing schemaVersion, attempting automatic upgrade to Schema 4")


                            return self._retry_update_with_schema_upgrade(doi, normalized_url, error_details)


                        


                        # Check for "Can't be blank" error - fetch metadata to determine which field is missing


                        elif "can't be blank" in error_details.lower():


                            logger.warning(f"DOI {doi} has blank mandatory fields, fetching metadata to identify missing fields")


                            return self._handle_blank_fields_error(doi, error_details)


                        


                        else:


                            # Other validation errors (e.g., invalid URL format, missing mandatory fields)


                            error_msg = f"Validierungsfehler fÃ¼r DOI {doi}: {error_details}"


                            logger.error(f"Validation error for DOI {doi}: {error_details}")


                            return False, error_msg


                    else:


                        # Extract first line of response as error details for consistency


                        error_details = response.text.split('\n')[0] if response.text else 'Unknown error'


                        error_msg = f"Validierungsfehler fÃ¼r DOI {doi}: {error_details}"


                        logger.error(f"Validation error for DOI {doi}: {error_details}")


                        return False, error_msg


                except ValueError as e:  # json.JSONDecodeError is a subclass of ValueError


                    error_msg = f"Validierungsfehler fÃ¼r DOI {doi}: UngÃ¼ltige JSON-Antwort vom Server: {response.text}"


                    logger.error(f"Invalid JSON in validation error response for DOI {doi}: {e}. Response: {response.text}")


                    return False, error_msg


                except Exception as e:


                    error_msg = f"Validierungsfehler fÃ¼r DOI {doi}: {response.text}"


                    logger.error(f"Error parsing validation error: {e}")


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


            error_msg = f"ZeitÃ¼berschreitung bei DOI {doi}"


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


    


    def _retry_update_with_schema_upgrade(self, doi: str, normalized_url: str, original_error: str) -> Tuple[bool, str]:


        """


        Retry DOI update after upgrading metadata from Schema 3 to Schema 4.


        


        This method fetches the current metadata, upgrades it to Schema 4 by adding


        schemaVersion and ensuring resourceTypeGeneral is present, then retries the update.


        


        Args:


            doi: The DOI identifier


            normalized_url: The normalized landing page URL


            original_error: The original schema deprecation error message


            


        Returns:


            Tuple of (success: bool, message: str)


        """


        try:


            # Fetch current metadata


            logger.info(f"Fetching current metadata for DOI {doi} to perform schema upgrade")


            metadata = self.get_doi_metadata(doi)


            


            if not metadata:


                error_msg = f"Konnte Metadaten fÃ¼r DOI {doi} nicht abrufen fÃ¼r Schema-Upgrade"


                logger.error(error_msg)


                return False, error_msg


            


            # Upgrade metadata to Schema 4


            upgraded_attributes = self._upgrade_schema_to_v4(metadata, normalized_url)


            


            if not upgraded_attributes:


                # Check which mandatory fields are missing using helper method


                attrs = metadata.get('data', {}).get('attributes', {})


                all_missing = self._check_missing_mandatory_fields(attrs)


                non_autofillable = self._filter_non_autofillable_fields(all_missing)


                


                if non_autofillable:


                    fields_str, verb = self._format_missing_fields_with_verb(non_autofillable)


                    error_msg = (


                        f"DOI {doi} kann nicht automatisch zu Schema 4 aktualisiert werden: "


                        f"{fields_str} {verb} in den Metadaten. Diese Pflichtfelder kÃ¶nnen nicht automatisch "


                        f"befÃ¼llt werden. Bitte ergÃ¤nze sie manuell Ã¼ber das DataCite Fabrica Interface "


                        f"(https://doi.datacite.org/dois/{doi})."


                    )


                else:


                    error_msg = (


                        f"DOI {doi} kann nicht automatisch zu Schema 4 aktualisiert werden. "


                        f"Bitte prÃ¼fe die Metadaten manuell Ã¼ber das DataCite Fabrica Interface "


                        f"(https://doi.datacite.org/dois/{doi})."


                    )


                


                logger.error(error_msg)


                return False, error_msg


            


            # Prepare upgraded payload


            upgraded_payload = {


                "data": {


                    "type": "dois",


                    "attributes": upgraded_attributes


                }


            }


            


            url = f"{self.base_url}/dois/{doi}"


            logger.info(f"Retrying DOI {doi} update with Schema 4 metadata")


            


            # Retry with upgraded metadata


            try:


                response = requests.put(


                    url,


                    auth=self.auth,


                    json=upgraded_payload,


                    timeout=self.TIMEOUT,


                    headers={


                        "Content-Type": "application/vnd.api+json",


                        "Accept": "application/vnd.api+json"


                    }


                )


            except requests.exceptions.Timeout:


                error_msg = "Die Anfrage hat zu lange gedauert. Bitte versuche es erneut."


                logger.error(f"Timeout during schema upgrade retry for DOI {doi}")


                raise NetworkError(error_msg)


            except requests.exceptions.ConnectionError as e:


                error_msg = "Verbindung zur DataCite API fehlgeschlagen. Bitte Ã¼berprÃ¼fe deine Internetverbindung."


                logger.error(f"Connection error during schema upgrade retry: {e}")


                raise NetworkError(error_msg)


            


            if response.status_code == 200:


                success_msg = f"DOI {doi} erfolgreich aktualisiert (Schema automatisch auf kernel-4 aktualisiert)"


                logger.info(success_msg)


                return True, success_msg


            else:


                error_msg = f"Schema-Upgrade fÃ¼r DOI {doi} fehlgeschlagen (HTTP {response.status_code}): {response.text}"


                logger.error(error_msg)


                return False, error_msg


                


        except NetworkError:


            # Re-raise NetworkError to preserve specific network error context


            raise


        except Exception as e:


            error_msg = f"Fehler beim Schema-Upgrade fÃ¼r DOI {doi}: {str(e)}"


            logger.error(error_msg)


            return False, error_msg


    


    def _handle_blank_fields_error(self, doi: str, original_error: str) -> Tuple[bool, str]:


        """


        Handle 'Can't be blank' validation errors by fetching metadata 


        and identifying which mandatory fields are missing.


        


        Args:


            doi: The DOI identifier


            original_error: The original "Can't be blank" error message


            


        Returns:


            Tuple of (success: bool, message: str) with detailed error message


        """


        try:


            # Fetch current metadata to identify missing fields


            url = f"{self.base_url}/dois/{doi}"


            try:


                response = requests.get(


                    url,


                    auth=self.auth,


                    timeout=self.TIMEOUT,


                    headers={"Accept": "application/vnd.api+json"}


                )


            except requests.exceptions.Timeout:


                error_msg = f"Validierungsfehler fÃ¼r DOI {doi}: {original_error} (Timeout beim Metadaten-Abruf)"


                logger.error(f"Timeout while fetching metadata for blank field analysis")


                raise NetworkError(error_msg)


            except requests.exceptions.ConnectionError as e:


                error_msg = f"Validierungsfehler fÃ¼r DOI {doi}: {original_error} (Verbindungsfehler beim Metadaten-Abruf)"


                logger.error(f"Connection error while fetching metadata: {e}")


                raise NetworkError(error_msg)


            


            if response.status_code != 200:


                error_msg = f"Validierungsfehler fÃ¼r DOI {doi}: {original_error} (Metadaten konnten nicht abgerufen werden)"


                logger.error(f"Failed to fetch metadata for blank field analysis: HTTP {response.status_code}")


                return False, error_msg


            


            metadata = response.json()


            attributes = metadata.get('data', {}).get('attributes', {})


            


            # Check which mandatory fields are missing using helper method


            all_missing = self._check_missing_mandatory_fields(attributes)


            


            # Filter for non-auto-fillable fields only (title and creators)


            # resourceTypeGeneral and publisher can be auto-filled, so we don't report them here


            non_autofillable = self._filter_non_autofillable_fields(all_missing)


            


            if non_autofillable:


                fields_str, verb = self._format_missing_fields_with_verb(non_autofillable)


                error_msg = (


                    f"DOI {doi} kann nicht aktualisiert werden: {fields_str} {verb} in den Metadaten. "


                    f"Bitte ergÃ¤nze diese Pflichtfelder manuell Ã¼ber das DataCite Fabrica Interface (https://doi.datacite.org/dois/{doi})."


                )


                logger.error(f"Missing mandatory fields for DOI {doi}: {fields_str}")


                return False, error_msg


            else:


                # Fields are present but DataCite still says "Can't be blank" - unusual case


                error_msg = f"Validierungsfehler fÃ¼r DOI {doi}: {original_error}"


                logger.error(f"Can't be blank error but all checked fields are present for DOI {doi}")


                return False, error_msg


                


        except NetworkError:


            # Re-raise NetworkError to preserve specific network error context


            raise


        except Exception as e:


            error_msg = f"Validierungsfehler fÃ¼r DOI {doi}: {original_error} (Fehler bei Metadatenanalyse: {str(e)})"


            logger.error(f"Error analyzing blank fields for DOI {doi}: {e}")


            return False, error_msg


    


    def _upgrade_schema_to_v4(self, metadata: Dict[str, Any], new_url: str) -> Optional[Dict[str, Any]]:


        """


        Upgrade DOI metadata from Schema 3 to Schema 4.


        


        According to DataCite documentation, Schema 4 requires:


        1. schemaVersion set to "http://datacite.org/schema/kernel-4"


        2. resourceTypeGeneral must be present (mandatory in Schema 4)


        3. Contributors with contributorType "Funder" should be moved to fundingReferences


        


        Additionally handles missing mandatory fields:


        - Auto-fills resourceTypeGeneral with "Dataset" if missing


        - Auto-fills publisher with "GFZ Data Services" if missing


        - Requires title and creators to be present (cannot auto-fill)


        


        Args:


            metadata: Current DOI metadata from DataCite API


            new_url: New landing page URL to include in update


            


        Returns:


            Upgraded attributes dict ready for PUT request, or None if upgrade not possible


        """


        try:


            attributes = metadata.get('data', {}).get('attributes', {})


            


            # Check mandatory fields that cannot be auto-filled using helper method


            all_missing = self._check_missing_mandatory_fields(attributes)


            non_autofillable = self._filter_non_autofillable_fields(all_missing)


            


            if non_autofillable:


                fields_str = self._format_missing_fields_list(non_autofillable)


                logger.error(f"Cannot upgrade to Schema 4: {fields_str} missing (mandatory fields that cannot be auto-filled)")


                return None


            


            # Handle resourceTypeGeneral (can be auto-filled)


            types = copy.deepcopy(attributes.get('types', {}))


            resource_type_general = types.get('resourceTypeGeneral')


            


            if not resource_type_general:


                logger.warning("resourceTypeGeneral missing - auto-filling with 'Dataset'")


                types['resourceTypeGeneral'] = 'Dataset'


            


            # Handle publisher (can be auto-filled)


            publisher = attributes.get('publisher')


            if not publisher:


                logger.warning("publisher missing - auto-filling with 'GFZ Data Services'")


                publisher = 'GFZ Data Services'


            


            # Build upgraded attributes


            # Preserve mandatory fields from original metadata


            upgraded = {


                'url': new_url,


                'schemaVersion': 'http://datacite.org/schema/kernel-4',


                'titles': attributes.get('titles', []),


                'creators': attributes.get('creators', []),


                'types': types,


                'publisher': publisher


            }


            


            # Handle Funder contributors (deprecated in Schema 4)


            contributors = attributes.get('contributors', [])


            funding_references = copy.deepcopy(attributes.get('fundingReferences', []))


            


            non_funder_contributors = []


            funders_to_migrate = []


            


            for contributor in contributors:


                if contributor.get('contributorType') == 'Funder':


                    funders_to_migrate.append(contributor)


                else:


                    non_funder_contributors.append(contributor)


            


            # Migrate Funder contributors to fundingReferences


            if funders_to_migrate:


                logger.info(f"Migrating {len(funders_to_migrate)} Funder contributor(s) to fundingReferences")


                for funder in funders_to_migrate:


                    funding_ref = {'funderName': funder.get('name', '')}


                    


                    # Copy name identifier if present


                    name_identifiers = funder.get('nameIdentifiers', [])


                    if name_identifiers:


                        identifier = name_identifiers[0]


                        funding_ref['funderIdentifier'] = identifier.get('nameIdentifier', '')


                        funding_ref['funderIdentifierType'] = identifier.get('nameIdentifierScheme', '')


                    


                    funding_references.append(funding_ref)


            


            # Include contributors (without Funders) if any remain


            if non_funder_contributors:


                upgraded['contributors'] = non_funder_contributors


            


            # Include fundingReferences if any exist


            if funding_references:


                upgraded['fundingReferences'] = funding_references


            


            logger.info(f"Successfully prepared Schema 4 upgrade with schemaVersion=kernel-4")


            return upgraded


            


        except Exception as e:


            logger.error(f"Error preparing Schema 4 upgrade: {e}")


            return None


    


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


                    error_msg = f"UngÃ¼ltige JSON-Antwort fÃ¼r DOI {doi}"


                    logger.error(f"Invalid JSON response: {e}")


                    raise DataCiteAPIError(error_msg)


            


            elif response.status_code == 401:


                error_msg = "Anmeldung fehlgeschlagen. Bitte Ã¼berprÃ¼fe deinen Benutzernamen und dein Passwort."


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


            error_msg = "Verbindung zur DataCite API fehlgeschlagen. Bitte Ã¼berprÃ¼fe deine Internetverbindung."


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


            return False, f"UngÃ¼ltige Metadatenstruktur fÃ¼r DOI {doi}"


        


        # Check if creator counts match


        if len(current_creators) != len(csv_creators):


            return False, (


                f"DOI {doi}: Anzahl der Creators stimmt nicht Ã¼berein "


                f"(DataCite: {len(current_creators)}, CSV: {len(csv_creators)}). "


                f"Creators dÃ¼rfen nicht hinzugefÃ¼gt oder entfernt werden."


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


        It follows the pattern: GET current metadata â Replace creators â PUT full metadata.


        


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


            error_msg = f"Fehler beim Erstellen der Payload fÃ¼r DOI {doi}: {str(e)}"


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


                error_msg = f"Authentifizierung fehlgeschlagen fÃ¼r DOI {doi}"


                logger.error(f"Authentication failed for DOI update: {doi}")


                return False, error_msg


            


            elif response.status_code == 403:


                error_msg = f"Keine Berechtigung fÃ¼r DOI {doi} (gehÃ¶rt mÃ¶glicherweise einem anderen Client)"


                logger.error(f"Forbidden: No permission to update DOI {doi}")


                return False, error_msg


            


            elif response.status_code == 404:


                error_msg = f"DOI {doi} nicht gefunden"


                logger.error(f"DOI not found: {doi}")


                return False, error_msg


            


            elif response.status_code == 422:


                # Unprocessable Entity - validation error


                error_msg = f"Validierungsfehler fÃ¼r DOI {doi}: {response.text}"


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


            error_msg = f"ZeitÃ¼berschreitung bei DOI {doi}"


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


        Uses cursor-based pagination to retrieve all records without limitation.


        


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


        next_url = None  # Start with None to use initial cursor


        page_count = 0


        


        logger.info(f"Starting to fetch DOIs with contributors for client: {self.username} (using cursor pagination)")


        


        while True:


            try:


                page_count += 1


                contributor_data, next_url = self._fetch_page_with_contributors(next_url)


                all_contributor_data.extend(contributor_data)


                


                logger.info(f"Fetched page {page_count}: {len(contributor_data)} contributor entries (Total: {len(all_contributor_data)})")


                


                if not next_url:


                    break


                


            except requests.exceptions.Timeout:


                error_msg = "Die Anfrage hat zu lange gedauert. Bitte versuche es erneut."


                logger.error(f"Timeout on page {page_count}")


                raise DataCiteAPIError(error_msg)


            


            except requests.exceptions.ConnectionError as e:


                error_msg = "Verbindung zur DataCite API fehlgeschlagen. Bitte Ã¼berprÃ¼fe deine Internetverbindung."


                logger.error(f"Connection error: {e}")


                raise NetworkError(error_msg)


            


            except requests.exceptions.RequestException as e:


                error_msg = f"Netzwerkfehler bei der Kommunikation mit DataCite: {str(e)}"


                logger.error(f"Request exception: {e}")


                raise NetworkError(error_msg)


        


        logger.info(f"Successfully fetched {len(all_contributor_data)} contributor entries in total")


        return all_contributor_data


    


    def _fetch_page_with_contributors(self, next_url: Optional[str] = None) -> Tuple[List[Tuple[str, str, str, str, str, str, str, str, str, str, str, str, str, str]], Optional[str]]:


        """


        Fetch a single page of DOIs with contributor information from the API using cursor-based pagination.


        


        Args:


            next_url: Full URL for next page (from previous response), or None for first page


            


        Returns:


            Tuple of (list of contributor tuples, next_url for pagination or None if no more pages)


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


        if next_url:


            # Use the complete next URL from the API response


            url = next_url


            params = None


            logger.debug(f"Requesting next page with contributors: {url}")


        else:


            # First page: use cursor=1


            # Note: DataCite API explicitly requires page[cursor]=1 for the first page


            # See: https://support.datacite.org/docs/pagination (Method 2: Cursor)


            url = f"{self.base_url}/dois"


            params = {


                "client-id": self.username,


                "page[size]": self.PAGE_SIZE,


                "page[cursor]": 1


            }


            logger.debug(f"Requesting first page with contributors: {url} with params: {params}")


        


        response = requests.get(


            url,


            auth=self.auth,


            params=params,


            timeout=self.TIMEOUT,


            headers={"Accept": "application/vnd.api+json"}


        )


        


        # Handle authentication errors


        if response.status_code == 401:


            error_msg = "Anmeldung fehlgeschlagen. Bitte Ã¼berprÃ¼fe deinen Benutzernamen und dein Passwort."


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


            error_msg = "UngÃ¼ltige Antwort von der DataCite API (kein gÃ¼ltiges JSON)."


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


                        # Two sets of keywords:


                        # 1. SUBSTRING_ORG_KEYWORDS: Long, unique keywords that can be matched as substrings


                        #    (safe for German compound words like "GeoForschungsZentrum")


                        # 2. WORD_BOUNDARY_ORG_KEYWORDS: Shorter keywords that need word boundary matching


                        #    (to avoid false positives like "Wagner" matching "ag")


                        


                        # Keywords long enough to safely match as substrings in compound words


                        SUBSTRING_ORG_KEYWORDS = {


                            # German compound-safe (6+ chars, unlikely in names)


                            "universitÃ¤t", "universitÃ©", "universidad", "universidade", "universitÃ ", "university",


                            "universite",  # ASCII transcription without accent (e.g., "Universite Grenoble Alpes")


                            "institute", "institut", "instituto", "istituto",


                            "zentrum", "center", "centre", "centro",


                            "forschung",  # research (German) - catches "GeoForschungsZentrum"


                            "laboratory", "laboratorium", "laboratorio", "laboratoire",


                            "department", "abteilung", "departamento",


                            "ministry", "ministerium", "ministÃ¨re", "ministerio",


                            "foundation", "stiftung", "fondation", "fundaciÃ³n",


                            "gesellschaft", "association", "verband", "verein",


                            "organisation", "organization",


                            "corporation", "company",


                            "consortium", "konsortium",


                            "bibliothek", "library",


                            "krankenhaus", "hospital",


                            "geosurvey",  # catches "Iceland GeoSurvey"


                            "helmholtz",  # German research org


                            "fraunhofer",  # Fraunhofer Society


                            # German government/geo agencies


                            "landesamt",  # Landesamt fÃ¼r Geologie und Bergbau


                            "regierungsprÃ¤sidium",  # RegierungsprÃ¤sidium Freiburg


                            "geological survey",  # Geological Survey of Baden-WÃ¼rttemberg


                            "geodynamics",  # European Center for Geodynamics


                            "geophysik", "geophysics",  # Fachbereich Geophysik


                            "geowissenschaften", "geosciences",  # Institut fÃ¼r Geowissenschaften


                            "erdbebenstation",  # Erdbebenstation Bensberg


                            "fachbereich",  # Fachbereich (academic department)


                            # Observatories, research facilities


                            "observatory",  # Goma Volcano Observatory, INGV-Osservatorio


                            "osservatorio",  # Italian observatories


                            "observatoire",  # French observatories


                            "observatorium",  # German observatories


                            "synchrotron",  # ESRF, PETRA III, etc.


                            "rÃ¶ntgenstrahlungsquelle",  # X-ray source PETRA III


                            "meteorolog",  # Meteorological services (catches Meteorologie, Meteorology, etc.)


                            "klimatolog",  # Climatology services


                            "hochschule",  # German universities of applied sciences


                            "zentralanstalt",  # ZAMG - Zentralanstalt fÃ¼r Meteorologie und Geodynamik


                            "gÃ©othermie", "geothermie",  # Geothermal companies (Ã©s-GÃ©othermie)


                            "geomanagement",  # ECW Geomanagement BV


                            "transregio",  # CRC/Transregio 32


                        }


                        


                        # Shorter keywords that need word boundary matching


                        WORD_BOUNDARY_ORG_KEYWORDS = {


                            "college", "school", "faculty", "fakultÃ¤t",


                            "agency", "agentur", "authority", "behÃ¶rde",


                            "office", "bureau", "service", "dienst",


                            "commission", "kommission", "council", "board",


                            "gremium", "committee", "ausschuss",


                            "museum", "archive", "archiv",


                            "klinik", "clinic", "division",


                            "firma", "gmbh", "ltd",


                            "group", "gruppe", "network", "netzwerk",


                            "survey",  # catches geological surveys


                            "pool",  # Geophysical Instrument Pool


                            # Well-known research institution abbreviations (need word boundary)


                            "eth",  # ETH ZÃ¼rich


                            "mit",  # Massachusetts Institute of Technology


                            "cnrs",  # Centre national de la recherche scientifique


                            "nasa",  # National Aeronautics and Space Administration


                            "noaa",  # National Oceanic and Atmospheric Administration


                            "usgs",  # United States Geological Survey


                            "csic",  # Spanish National Research Council


                            "csiro",  # Commonwealth Scientific and Industrial Research Organisation


                            "rwth",  # RWTH Aachen


                            "ipgp",  # Institut de Physique du Globe de Paris


                            "gipp",  # Geophysical Instrument Pool Potsdam


                            "gfz",  # GFZ German Research Centre for Geosciences


                            "ucl",  # University College London


                            # French research keywords


                            "isterre",  # Institut des Sciences de la Terre


                            "globe",  # Institut de physique du globe


                            # Nordic/Scandinavian institutions


                            "norsar",  # Norwegian Seismic Array


                            "nve",  # Norges vassdrags- og energidirektorat (Norwegian Water Resources and Energy Directorate)


                            "dmi",  # Danish Meteorological Institute


                            "smhi",  # Swedish Meteorological and Hydrological Institute


                            # Other research institutions


                            "arditi",  # AgÃªncia Regional para o Desenvolvimento da InvestigaÃ§Ã£o, Tecnologia e InovaÃ§Ã£o


                            "fccn",  # FundaÃ§Ã£o para ComputaÃ§Ã£o CientÃ­fica Nacional


                            "fct",  # FundaÃ§Ã£o para a CiÃªncia e Tecnologia


                            # Funding/grant keywords


                            "fellowship",  # Marie Curie Fellowship, etc.


                            "grant",  # Research grants


                            "fund",  # Various funds


                            "award",  # Awards


                            # Team/staff/group identifiers (organizational entities)


                            "staff",  # ISG Staff, Support Staff


                            "team",  # WSM Team, DIGIS Team, Science Team


                            "authorities",  # Ebro Water Authorities


                            "isg",  # International Service of Geodynamics


                            "platform",  # Spanish Geothermal Technology Platform


                            # Additional institution abbreviations from validation


                            "awi",  # Alfred Wegener Institut


                            "bmkg",  # Badan Meteorologi, Klimatologi, dan Geofisika (Indonesian)


                            "ingv",  # Istituto Nazionale di Geofisica e Vulcanologia


                            "ipma",  # Instituto PortuguÃªs do Mar e da Atmosfera


                            "hbo",  # Hochschule Bochum


                            "zamg",  # Zentralanstalt fÃ¼r Meteorologie und Geodynamik


                            "eseo",  # European Student Earth Orbiter


                            "afad",  # Disaster and Emergency Management Presidency (Turkey)


                            "desy",  # Deutsches Elektronen-Synchrotron


                            "enbw",  # Energie Baden-WÃ¼rttemberg AG


                            "ecw",  # ECW Geomanagement BV


                            "esg",  # Ã©s-GÃ©othermie


                            "cnr",  # Consiglio Nazionale delle Ricerche (Italian)


                            "esrf",  # European Synchrotron Radiation Facility


                            "gvo",  # Goma Volcano Observatory


                            "petra",  # PETRA III synchrotron


                            "imaa",  # Institute of Methodologies for Environmental Analysis


                            "crc",  # Collaborative Research Centre


                            "radar",  # RADAR4KIT, radar facilities


                            # Project/Program identifiers


                            "project",  # X0-Deep Fault Drilling Project, MEET Project, etc.


                            "programme",  # EU programmes


                            "program",  # US spelling


                            "sfb",  # Sonderforschungsbereich (German collaborative research centre)


                            "minas",  # 5E-MINAS project


                            # Institute acronyms (from validation)


                            "caiag",  # Central-Asian Institute of Applied Geosciences


                            "geopribor",  # Russian geophysical instrument manufacturer


                            "dekorp",  # Deutsches Kontinentales Reflexionsseismisches Programm


                            "ilge",  # Infrastructure for Large-scale Ground-based E-science


                            "tna",  # Transnational Access (EU programs)


                        }


                        


                        def _is_organization_name(name: str) -> bool:


                            """Check if a name contains organizational keywords, URLs, or email addresses.


                            


                            IMPORTANT: This function should NOT match person names with affiliations!


                            Examples that should return False (persons):


                            - "Bindi, Dino (GFZ)" - person with affiliation


                            - "Simone Cesca, cesca@gfz.de" - person with email


                            - "Jari KortstrÃ¶m, jari.kortstrom@helsinki.fi" - person with email


                            


                            Examples that should return True (organizations):


                            - "geofon@gfz.de" - email as contact (no person name)


                            - "Deutsches GeoForschungsZentrum GFZ" - organization


                            - "University of Potsdam, Germany" - organization with location


                            """


                            if not name:


                                return False


                            import re


                            name_lower = name.lower()


                            


                            # First, detect "Person Name, email" or "Person Name (Affiliation)" patterns


                            # These are persons with contact info, NOT organizations


                            if ',' in name:


                                parts = name.split(',', 1)


                                first_part = parts[0].strip()


                                second_part = parts[1].strip() if len(parts) > 1 else ""


                                


                                # Check if first part looks like a person name (1-3 words, starts with uppercase)


                                first_words = first_part.split()


                                if 1 <= len(first_words) <= 3:


                                    all_name_like = all(


                                        len(w) >= 1 and w[0].isupper() and 


                                        not any(kw in w.lower() for kw in ['university', 'institut', 'center', 'centre'])


                                        for w in first_words


                                    )


                                    if all_name_like:


                                        # Second part is email? -> Person with contact


                                        if '@' in second_part:


                                            return False


                                        # Second part is short (likely first name)? -> Person


                                        second_words = second_part.split()


                                        if len(second_words) == 1 and len(second_part) <= 20:


                                            # Likely "Lastname, Firstname" pattern


                                            return False


                            


                            # Check for "Name (Affiliation)" pattern - person with institutional affiliation


                            if '(' in name and ')' in name:


                                before_paren = name.split('(')[0].strip()


                                # If what's before parenthesis looks like "Lastname, Firstname" -> person


                                if ',' in before_paren:


                                    parts = before_paren.split(',')


                                    if len(parts) == 2:


                                        p1, p2 = parts[0].strip(), parts[1].strip()


                                        # Both parts are short name-like strings


                                        if (1 <= len(p1.split()) <= 2 and 1 <= len(p2.split()) <= 2 and


                                            len(p1) <= 30 and len(p2) <= 20):


                                            return False


                            


                            # URLs are always organizational (websites, project pages, etc.)


                            if name_lower.startswith(('http://', 'https://', 'www.')):


                                return True


                            


                            # Email addresses ONLY when they ARE the name (not attached to person name)


                            # "geofon@gfz.de" -> Org, but "Simone Cesca, cesca@gfz.de" -> Person (handled above)


                            if '@' in name and '.' in name:


                                # Check if the ENTIRE name is basically just an email


                                if re.match(r'^[\w.-]+@[\w.-]+\.[a-z]{2,}$', name_lower.strip()):


                                    return True


                            


                            # First check substring keywords (safe for compound words)


                            for keyword in SUBSTRING_ORG_KEYWORDS:


                                if keyword in name_lower:


                                    return True


                            


                            # Then check word-boundary keywords (need exact word match)


                            for keyword in WORD_BOUNDARY_ORG_KEYWORDS:


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


                        # ROR is ONLY given to organizations, so having a ROR proves it's an organization


                        name_identifier = ""


                        name_identifier_scheme = ""


                        scheme_uri = ""


                        has_orcid = False


                        has_ror = False


                        


                        name_identifiers = contributor.get("nameIdentifiers", [])


                        for identifier in name_identifiers:


                            scheme = identifier.get("nameIdentifierScheme", "")


                            if scheme.upper() == "ORCID":


                                has_orcid = True


                                name_identifier = identifier.get("nameIdentifier", "")


                                name_identifier_scheme = identifier.get("nameIdentifierScheme", "")


                                scheme_uri = identifier.get("schemeUri", "")


                                break


                            elif scheme.upper() == "ROR":


                                has_ror = True


                                name_identifier = identifier.get("nameIdentifier", "")


                                name_identifier_scheme = identifier.get("nameIdentifierScheme", "")


                                scheme_uri = identifier.get("schemeUri", "")


                                # Don't break - continue checking for ORCID which has higher priority


                        


                        # If no ORCID or ROR, check for ISNI


                        if not name_identifier:


                            for identifier in name_identifiers:


                                scheme = identifier.get("nameIdentifierScheme", "")


                                if scheme.upper() == "ISNI":


                                    name_identifier = identifier.get("nameIdentifier", "")


                                    name_identifier_scheme = identifier.get("nameIdentifierScheme", "")


                                    scheme_uri = identifier.get("schemeUri", "")


                                    break


                        


                        # Check if the contributor name looks like an organization


                        # This catches cases where DataCite incorrectly splits org names into given/family


                        name_looks_like_org = _is_organization_name(contributor_name)


                        


                        # Determine nameType with clear priority:


                        # 1. ORCID present â ALWAYS Personal (ORCID is only for persons, this is a hard fact)


                        # 2. ROR present â ALWAYS Organizational (ROR is only for organizations, this is a hard fact)


                        # 3. Name contains org keywords â Organizational (override API nameType)


                        # 4. API provides nameType â TRUST IT (don't override DataCite's data)


                        # 5. Organizational contributor types (and no API nameType) â Organizational


                        # 6. Personal contributor types (and no API nameType, name doesn't look like org) â Personal


                        # 7. Fallback: infer from givenName/familyName presence


                        


                        if has_orcid:


                            # ORCID is ONLY given to persons - this overrides everything including API data


                            # because ORCID is a hard fact that cannot be wrong


                            if name_type != "Personal":


                                if name_type:


                                    logger.warning(f"Overriding nameType '{name_type}' to 'Personal' for '{contributor_name}' (has ORCID)")


                                name_type = "Personal"


                        elif has_ror:


                            # ROR is ONLY given to organizations - this overrides everything including API data


                            # because ROR (Research Organization Registry) is a hard fact that cannot be wrong


                            if name_type != "Organizational":


                                if name_type:


                                    logger.warning(f"Overriding nameType '{name_type}' to 'Organizational' for '{contributor_name}' (has ROR)")


                                name_type = "Organizational"


                            # Clear given/family name for organizations (they shouldn't have them)


                            given_name = ""


                            family_name = ""


                        elif name_looks_like_org:


                            # Name contains CLEAR organizational keywords (University, Institut, etc.)


                            # This overrides even API nameType because DataCite often incorrectly


                            # marks organizations as Personal when they have comma in name


                            # (e.g., "University of Potsdam, Germany" â givenName: Germany, familyName: University of Potsdam)


                            if name_type == "Personal":


                                logger.warning(f"Overriding nameType 'Personal' to 'Organizational' for '{contributor_name}' (name contains org keywords)")


                            name_type = "Organizational"


                            # Clear given/family name for organizations (they shouldn't have them)


                            given_name = ""


                            family_name = ""


                        elif name_type:


                            # API provides nameType and name doesn't look like org - TRUST IT


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


                                # Name format looks like a person (e.g., "BlÃ¶cher Guido" or "Doe, John")


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


        


        # Extract next page URL from response


        next_page_url = None


        if "links" in data and "next" in data["links"]:


            next_page_url = data["links"]["next"]


            logger.debug(f"Next page URL: {next_page_url}")


        


        return contributor_entries, next_page_url


    


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


        Validate that CSV contributors can be matched to current DataCite metadata.


        


        This is the "dry run" validation that checks:


        - Each CSV contributor can be matched to a DataCite contributor (by name or ORCID)


        - Supports partial updates (CSV can contain fewer contributors than DataCite)


        


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


            return False, f"UngÃ¼ltige Metadatenstruktur fÃ¼r DOI {doi}"


        


        # If no contributors in DataCite, CSV must also be empty


        if len(current_contributors) == 0:


            if len(csv_contributors) == 0:


                logger.info(f"DOI {doi} has no contributors in DataCite or CSV")


                return True, f"DOI {doi}: Keine Contributors vorhanden"


            else:


                return False, (


                    f"DOI {doi}: DataCite hat keine Contributors, aber CSV enthÃ¤lt {len(csv_contributors)}. "


                    f"Contributors kÃ¶nnen nicht hinzugefÃ¼gt werden."


                )


        


        # CSV cannot have more contributors than DataCite (no adding allowed)


        if len(csv_contributors) > len(current_contributors):


            return False, (


                f"DOI {doi}: CSV enthÃ¤lt mehr Contributors ({len(csv_contributors)}) als DataCite ({len(current_contributors)}). "


                f"Contributors kÃ¶nnen nicht hinzugefÃ¼gt werden."


            )


        


        # Match each CSV contributor to a DataCite contributor


        unmatched_csv = []


        for csv_contrib in csv_contributors:


            csv_name = csv_contrib.get("name", "").strip()


            csv_orcid = self._normalize_orcid_for_match(csv_contrib.get("nameIdentifier", ""))


            


            matched = False


            for dc_contrib in current_contributors:


                dc_name = dc_contrib.get("name", "").strip()


                dc_orcid = self._extract_orcid_for_match(dc_contrib)


                


                # Match by ORCID (if both have one) or by name


                if csv_orcid and dc_orcid and csv_orcid == dc_orcid:


                    matched = True


                    break


                elif csv_name.lower() == dc_name.lower():


                    matched = True


                    break


            


            if not matched:


                unmatched_csv.append(csv_name or "(unbekannt)")


        


        if unmatched_csv:


            return False, (


                f"DOI {doi}: Folgende Contributors aus CSV wurden nicht in DataCite gefunden: "


                f"{', '.join(unmatched_csv[:3])}{'...' if len(unmatched_csv) > 3 else ''}. "


                f"Nur existierende Contributors kÃ¶nnen aktualisiert werden."


            )


        


        # Partial update info


        if len(csv_contributors) < len(current_contributors):


            logger.info(


                f"DOI {doi}: Partial update - {len(csv_contributors)} of {len(current_contributors)} "


                f"contributors will be updated"


            )


            return True, (


                f"DOI {doi}: {len(csv_contributors)} von {len(current_contributors)} Contributors "


                f"werden aktualisiert (partielles Update)"


            )


        


        logger.info(f"DOI {doi}: Validation passed ({len(current_contributors)} contributors)")


        return True, f"DOI {doi}: {len(current_contributors)} Contributors validiert"


    


    def _normalize_orcid_for_match(self, orcid: str) -> str:


        """Normalize ORCID for matching (extract ID only)."""


        if not orcid:


            return ""


        orcid = orcid.strip()


        if "orcid.org/" in orcid:


            return orcid.split("orcid.org/")[-1]


        return orcid


    


    def _extract_orcid_for_match(self, contributor: dict) -> str:


        """Extract and normalize ORCID from DataCite contributor."""


        identifiers = contributor.get("nameIdentifiers", [])


        for identifier in identifiers:


            if identifier.get("nameIdentifierScheme", "").upper() == "ORCID":


                return self._normalize_orcid_for_match(identifier.get("nameIdentifier", ""))


        return ""


    


    def update_doi_contributors(


        self, 


        doi: str, 


        new_contributors: List[Dict[str, Any]], 


        current_metadata: Dict[str, Any]


    ) -> Tuple[bool, str]:


        """


        Update contributor metadata for a specific DOI.


        


        This method supports PARTIAL UPDATES: Only contributors that match (by name or ORCID)


        are updated. Unmatched contributors in DataCite are preserved unchanged.


        


        It follows the pattern: GET current metadata â Match & merge contributors â PUT full metadata.


        


        Note: Email/Website/Position are NOT sent to DataCite (only stored in local DB).


        Affiliations are preserved from current metadata and not modified.


        


        Args:


            doi: The DOI identifier


            new_contributors: List of contributor dictionaries with updated data (from CSV)


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


        


        # Get current contributors


        try:


            current_contributors = current_metadata.get("data", {}).get("attributes", {}).get("contributors", [])


        except (KeyError, AttributeError):


            current_contributors = []


        


        # Build updated contributors array with partial update support


        # Start with current contributors and update those that match CSV entries


        updated_contributors = []


        updated_count = 0


        


        for i, dc_contrib in enumerate(current_contributors):


            dc_name = dc_contrib.get("name", "").strip()


            dc_orcid = self._extract_orcid_for_match(dc_contrib)


            


            # Try to find matching CSV contributor


            matched_csv = None


            for csv_contrib in new_contributors:


                csv_name = csv_contrib.get("name", "").strip()


                csv_orcid = self._normalize_orcid_for_match(csv_contrib.get("nameIdentifier", ""))


                


                # Match by ORCID (if both have one) or by name


                if csv_orcid and dc_orcid and csv_orcid == dc_orcid:


                    matched_csv = csv_contrib


                    break


                elif csv_name.lower() == dc_name.lower():


                    matched_csv = csv_contrib


                    break


            


            if matched_csv:


                # Update this contributor with CSV data


                contributor_obj = self._build_contributor_object(matched_csv, dc_contrib)


                updated_contributors.append(contributor_obj)


                updated_count += 1


                logger.debug(f"Updated contributor {i}: {dc_name}")


            else:


                # Keep original contributor unchanged


                updated_contributors.append(dc_contrib)


                logger.debug(f"Kept contributor {i} unchanged: {dc_name}")


        


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


            error_msg = f"Fehler beim Erstellen der Payload fÃ¼r DOI {doi}: {str(e)}"


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


                logger.info(f"Successfully updated {updated_count} contributors for DOI {doi}")


                return True, f"DOI {doi}: {updated_count} von {len(current_contributors)} Contributors aktualisiert"


            


            elif response.status_code == 401:


                error_msg = f"Authentifizierung fehlgeschlagen fÃ¼r DOI {doi}"


                logger.error(f"Authentication failed for DOI update: {doi}")


                return False, error_msg


            


            elif response.status_code == 403:


                error_msg = f"Keine Berechtigung fÃ¼r DOI {doi} (gehÃ¶rt mÃ¶glicherweise einem anderen Client)"


                logger.error(f"Forbidden: No permission to update DOI {doi}")


                return False, error_msg


            


            elif response.status_code == 404:


                error_msg = f"DOI {doi} nicht gefunden"


                logger.error(f"DOI not found: {doi}")


                return False, error_msg


            


            elif response.status_code == 422:


                # Unprocessable Entity - validation error


                error_msg = f"Validierungsfehler fÃ¼r DOI {doi}: {response.text}"


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


            error_msg = f"ZeitÃ¼berschreitung bei DOI {doi}"


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


    


    def _build_contributor_object(


        self, 


        csv_contrib: Dict[str, Any], 


        dc_contrib: Dict[str, Any]


    ) -> Dict[str, Any]:


        """


        Build a contributor object for DataCite API from CSV data.


        


        Preserves affiliations from the original DataCite contributor.


        


        Args:


            csv_contrib: Contributor data from CSV


            dc_contrib: Original contributor from DataCite (for preserving affiliations)


            


        Returns:


            Contributor dict ready for DataCite API


        """


        contributor_obj = {


            "name": csv_contrib.get("name", ""),


            "nameType": csv_contrib.get("nameType", "Personal")


        }


        


        # Add contributorType (required for contributors)


        # Handle contributor types - can be a list or comma-separated string


        contributor_types = csv_contrib.get("contributorTypes", [])


        if contributor_types:


            # CSV parser returns a list, but handle string for safety


            if isinstance(contributor_types, str):


                first_type = contributor_types.split(",")[0].strip()


            else:


                # It's a list - take the first element


                first_type = contributor_types[0] if contributor_types else "Other"


            


            if first_type in self.VALID_CONTRIBUTOR_TYPES:


                contributor_obj["contributorType"] = first_type


            else:


                contributor_obj["contributorType"] = "Other"


        else:


            contributor_obj["contributorType"] = "Other"


        


        # Add given/family names if present (only for Personal contributors)


        given_name = csv_contrib.get("givenName", "")


        family_name = csv_contrib.get("familyName", "")


        


        if contributor_obj["nameType"] == "Personal":


            if given_name:


                contributor_obj["givenName"] = given_name


            if family_name:


                contributor_obj["familyName"] = family_name


        


        # Add name identifier if present


        name_identifier = csv_contrib.get("nameIdentifier", "")


        if name_identifier:


            name_identifier_scheme = csv_contrib.get("nameIdentifierScheme", "ORCID")


            scheme_uri = csv_contrib.get("schemeUri", "https://orcid.org")


            


            contributor_obj["nameIdentifiers"] = [{


                "nameIdentifier": name_identifier,


                "nameIdentifierScheme": name_identifier_scheme,


                "schemeUri": scheme_uri


            }]


        


        # Preserve affiliations from original DataCite contributor


        if "affiliation" in dc_contrib:


            contributor_obj["affiliation"] = dc_contrib["affiliation"]


        


        return contributor_obj


    


    # =========================================================================


    # Publisher Methods (DataCite Schema 4.6)


    # =========================================================================


    


    def fetch_all_dois_with_publisher(self) -> List[Tuple[str, str, str, str, str, str]]:


        """


        Fetch all DOIs with publisher information from DataCite API.


        Uses cursor-based pagination to retrieve all records without limitation.


        


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


        next_url = None  # Start with None to use initial cursor


        page_count = 0


        


        logger.info(f"Starting to fetch DOIs with publisher for client: {self.username} (using cursor pagination)")


        


        while True:


            try:


                page_count += 1


                publisher_data, next_url = self._fetch_page_with_publisher(next_url)


                all_publisher_data.extend(publisher_data)


                


                logger.info(f"Fetched page {page_count}: {len(publisher_data)} publisher entries (Total: {len(all_publisher_data)})")


                


                if not next_url:


                    break


                


            except requests.exceptions.Timeout:


                error_msg = "Die Anfrage hat zu lange gedauert. Bitte versuche es erneut."


                logger.error(f"Timeout on page {page_count}")


                raise DataCiteAPIError(error_msg)


            


            except requests.exceptions.ConnectionError as e:


                error_msg = "Verbindung zur DataCite API fehlgeschlagen. Bitte Ã¼berprÃ¼fe deine Internetverbindung."


                logger.error(f"Connection error: {e}")


                raise NetworkError(error_msg)


            


            except requests.exceptions.RequestException as e:


                error_msg = f"Netzwerkfehler bei der Kommunikation mit DataCite: {str(e)}"


                logger.error(f"Request exception: {e}")


                raise NetworkError(error_msg)


        


        logger.info(f"Successfully fetched {len(all_publisher_data)} publisher entries in total")


        return all_publisher_data


    


    def _fetch_page_with_publisher(self, next_url: Optional[str] = None) -> Tuple[List[Tuple[str, str, str, str, str, str]], Optional[str]]:


        """


        Fetch a single page of DOIs with publisher information from the API using cursor-based pagination.


        


        Args:


            next_url: Full URL for next page (from previous response), or None for first page


            


        Returns:


            Tuple of (list of publisher tuples, next_url for pagination or None if no more pages)


            Each tuple contains: (DOI, Publisher Name, Publisher Identifier,


                                 Publisher Identifier Scheme, Scheme URI, Language)


            


        Raises:


            AuthenticationError: If credentials are invalid


            DataCiteAPIError: For other API errors


        """


        if next_url:


            # Use the complete next URL from the API response


            url = next_url


            params = None


            logger.debug(f"Requesting next page with publisher: {url}")


        else:


            # First page: use cursor=1


            # Note: DataCite API explicitly requires page[cursor]=1 for the first page


            # See: https://support.datacite.org/docs/pagination (Method 2: Cursor)


            url = f"{self.base_url}/dois"


            params = {


                "client-id": self.username,


                "page[size]": self.PAGE_SIZE,


                "page[cursor]": 1


            }


            logger.debug(f"Requesting first page with publisher: {url} with params: {params}")


        


        response = requests.get(


            url,


            auth=self.auth,


            params=params,


            timeout=self.TIMEOUT,


            headers={"Accept": "application/vnd.api+json"}


        )


        


        # Handle authentication errors


        if response.status_code == 401:


            error_msg = "Anmeldung fehlgeschlagen. Bitte Ã¼berprÃ¼fe deinen Benutzernamen und dein Passwort."


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


            error_msg = "UngÃ¼ltige Antwort von der DataCite API (kein gÃ¼ltiges JSON)."


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


        


        # Extract next page URL from response


        next_page_url = None


        if "links" in data and "next" in data["links"]:


            next_page_url = data["links"]["next"]


            logger.debug(f"Next page URL: {next_page_url}")


        


        return publisher_entries, next_page_url


    


    def update_doi_publisher(


        self, 


        doi: str, 


        publisher_data: Dict[str, str],


        current_metadata: Dict[str, Any]


    ) -> Tuple[bool, str]:


        """


        Update publisher metadata for a specific DOI.


        


        This method preserves ALL existing metadata and only updates the publisher.


        It follows the pattern: GET current metadata â Replace publisher â PUT full metadata.


        


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


            error_msg = f"Publisher-Name fehlt fÃ¼r DOI {doi}"


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


            error_msg = f"Fehler beim Erstellen der Payload fÃ¼r DOI {doi}: {str(e)}"


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


                error_msg = f"Authentifizierung fehlgeschlagen fÃ¼r DOI {doi}"


                logger.error(f"Authentication failed for DOI update: {doi}")


                return False, error_msg


            


            elif response.status_code == 403:


                error_msg = f"Keine Berechtigung fÃ¼r DOI {doi} (gehÃ¶rt mÃ¶glicherweise einem anderen Client)"


                logger.error(f"Forbidden: No permission to update DOI {doi}")


                return False, error_msg


            


            elif response.status_code == 404:


                error_msg = f"DOI {doi} nicht gefunden"


                logger.error(f"DOI not found: {doi}")


                return False, error_msg


            


            elif response.status_code == 422:


                # Unprocessable Entity - validation error


                error_msg = f"Validierungsfehler fÃ¼r DOI {doi}: {response.text}"


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


            error_msg = f"ZeitÃ¼berschreitung bei DOI {doi}"


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


    
    def fetch_dois_with_schema_version(self):
        """Fetch all DOIs with their schema version. Uses cursor-based pagination."""
        all_dois = []
        next_url = None
        page_count = 0
        logger.info(f"Starting to fetch DOIs with schema version for client: {self.username}")
        while True:
            try:
                page_count += 1
                dois_page, next_url = self._fetch_page_with_schema(next_url)
                all_dois.extend(dois_page)
                logger.info(f"Fetched page {page_count}: {len(dois_page)} DOIs (Total: {len(all_dois)})")
                if not next_url:
                    break
            except requests.exceptions.Timeout:
                error_msg = "Die Anfrage hat zu lange gedauert. Bitte versuche es erneut."
                logger.error(f"Timeout on page {page_count}")
                raise DataCiteAPIError(error_msg)
            except requests.exceptions.ConnectionError as e:
                error_msg = "Verbindung zur DataCite API fehlgeschlagen. Bitte überprüfe deine Internetverbindung."
                logger.error(f"Connection error: {e}")
                raise NetworkError(error_msg)
            except requests.exceptions.RequestException as e:
                error_msg = f"Netzwerkfehler bei der Kommunikation mit DataCite: {str(e)}"
                logger.error(f"Request exception: {e}")
                raise NetworkError(error_msg)
        logger.info(f"Successfully fetched {len(all_dois)} DOIs with schema version")
        return all_dois
    
    def _fetch_page_with_schema(self, next_url=None):
        """Fetch a single page of DOIs with schema version."""
        if next_url:
            url = next_url
        else:
            url = f"{self.base_url}/dois?page[size]={self.PAGE_SIZE}&page[cursor]=1"
        try:
            response = requests.get(
                url, auth=self.auth, timeout=self.TIMEOUT,
                headers={"Accept": "application/vnd.api+json"}
            )
            if response.status_code == 401:
                error_msg = "Authentifizierung fehlgeschlagen. Bitte überprüfe deine Zugangsdaten."
                logger.error(f"Authentication failed: {response.text}")
                raise AuthenticationError(error_msg)
            if response.status_code != 200:
                error_msg = f"API Fehler (HTTP {response.status_code}): {response.text}"
                logger.error(f"Unexpected status code {response.status_code}: {response.text}")
                raise DataCiteAPIError(error_msg)
            data = response.json()
            dois = []
            for item in data.get("data", []):
                doi = item.get("id", "")
                attributes = item.get("attributes", {})
                schema_version = attributes.get("schemaVersion", "")
                dois.append((doi, schema_version))
            next_page_url = data.get("links", {}).get("next")
            return dois, next_page_url
        except requests.exceptions.RequestException:
            raise
    
    def check_schema_4_compatibility(self, doi):
        """Check if a DOI is compatible with Schema 4."""
        logger.info(f"Checking Schema 4 compatibility for DOI {doi}")
        metadata = self.get_doi_metadata(doi)
        if not metadata:
            logger.error(f"Could not fetch metadata for DOI {doi}")
            return False, {"error": "Metadaten konnten nicht abgerufen werden"}
        attributes = metadata.get("data", {}).get("attributes", {})
        missing_fields = {}
        # Check publisher
        publisher = attributes.get("publisher")
        if publisher is None:
            missing_fields["publisher"] = "missing"
        elif publisher == "":
            missing_fields["publisher"] = "empty"
        elif isinstance(publisher, str) and not publisher.strip():
            missing_fields["publisher"] = "empty"
        elif isinstance(publisher, dict) and not publisher.get("name", "").strip():
            missing_fields["publisher"] = "empty"
        # Check publication year
        pub_year = attributes.get("publicationYear")
        if pub_year is None:
            missing_fields["publicationYear"] = "missing"
        elif isinstance(pub_year, str) and not pub_year.strip():
            missing_fields["publicationYear"] = "empty"
        elif isinstance(pub_year, int) and pub_year == 0:
            missing_fields["publicationYear"] = "empty"
        # Check titles
        titles = attributes.get("titles")
        if titles is None:
            missing_fields["titles"] = "missing"
        elif not isinstance(titles, list):
            missing_fields["titles"] = "missing"
        elif len(titles) == 0:
            missing_fields["titles"] = "empty"
        elif not any(t.get("title", "").strip() for t in titles):
            missing_fields["titles"] = "empty"
        # Check creators
        creators = attributes.get("creators")
        if not creators:
            missing_fields["creators"] = "missing"
        elif not isinstance(creators, list) or len(creators) == 0:
            missing_fields["creators"] = "empty"
        else:
            invalid_name_types = []
            allowed_name_types = ["Personal", "Organizational"]
            for creator in creators:
                name_type = creator.get("nameType")
                if name_type and name_type not in allowed_name_types:
                    invalid_name_types.append(name_type)
            if invalid_name_types:
                missing_fields["invalid_name_types"] = list(set(invalid_name_types))
        # Check resource type
        types = attributes.get("types", {})
        resource_type_general = types.get("resourceTypeGeneral") if types else None
        if not resource_type_general:
            missing_fields["resourceType"] = "missing"
        elif not resource_type_general.strip():
            missing_fields["resourceType"] = "empty"
        # Check contributor types
        contributors = attributes.get("contributors")
        if contributors and isinstance(contributors, list):
            allowed_contributor_types = {
                "ContactPerson", "DataCollector", "DataCurator", "DataManager",
                "Distributor", "Editor", "HostingInstitution", "Producer",
                "ProjectLeader", "ProjectManager", "ProjectMember", "RegistrationAgency",
                "RegistrationAuthority", "RelatedPerson", "Researcher", "ResearchGroup",
                "RightsHolder", "Sponsor", "Supervisor", "WorkPackageLeader", "Other"
            }
            unknown_types = []
            for contributor in contributors:
                contrib_type = contributor.get("contributorType")
                if contrib_type and contrib_type not in allowed_contributor_types:
                    unknown_types.append(contrib_type)
            if unknown_types:
                missing_fields["unknown_contributor_types"] = list(set(unknown_types))
        is_compatible = len(missing_fields) == 0
        if is_compatible:
            logger.info(f"DOI {doi} is compatible with Schema 4")
        else:
            logger.info(f"DOI {doi} is NOT compatible with Schema 4: {missing_fields}")
        return is_compatible, missing_fields
