"""Worker thread for updating DOI landing page URLs via DataCite API."""

import logging
from PySide6.QtCore import QObject, Signal

from src.api.datacite_client import DataCiteClient, NetworkError
from src.utils.csv_parser import CSVParser, CSVParseError


logger = logging.getLogger(__name__)


class UpdateWorker(QObject):
    """Worker for updating DOI landing page URLs in a separate thread."""
    
    # Signals
    progress_update = Signal(int, int, str)  # current, total, message
    doi_updated = Signal(str, bool, str)  # doi, success, message
    finished = Signal(int, int, int, list, list)  # success_count, error_count, skipped_count, error_list, skipped_details
    error_occurred = Signal(str)  # error_message
    request_save_credentials = Signal(str, str, str)  # username, password, api_type
    
    def __init__(
        self, 
        username: str, 
        password: str, 
        csv_path: str, 
        use_test_api: bool = False,
        credentials_are_new: bool = False
    ):
        """
        Initialize the update worker.
        
        Args:
            username: DataCite username
            password: DataCite password
            csv_path: Path to CSV file with DOI/URL pairs
            use_test_api: If True, use test API instead of production
            credentials_are_new: Whether these are newly entered credentials (not from saved account)
        """
        super().__init__()
        self.username = username
        self.password = password
        self.csv_path = csv_path
        self.use_test_api = use_test_api
        self.credentials_are_new = credentials_are_new
        self._is_running = False
        self._first_success = False
    
    def run(self):
        """
        Execute the URL update process.
        
        This method will:
        1. Parse the CSV file
        2. Initialize DataCite client
        3. Update each DOI/URL pair
        4. Emit progress signals
        5. Emit final results
        """
        self._is_running = True
        success_count = 0
        error_count = 0
        skipped_count = 0
        error_list = []
        skipped_details = []  # List of (doi, reason) tuples
        
        try:
            # Step 1: Parse CSV file
            logger.info(f"Parsing CSV file: {self.csv_path}")
            self.progress_update.emit(0, 0, "CSV-Datei wird gelesen...")
            
            try:
                doi_url_pairs = CSVParser.parse_update_csv(self.csv_path)
            except (CSVParseError, FileNotFoundError) as e:
                error_msg = f"Fehler beim Lesen der CSV-Datei: {str(e)}"
                logger.error(error_msg)
                self.error_occurred.emit(error_msg)
                self.finished.emit(0, 0, 0, [], [])
                return
            
            total_dois = len(doi_url_pairs)
            logger.info(f"Found {total_dois} DOI/URL pairs to update")
            
            # Step 1: Initialize DataCite client
            self.progress_update.emit(0, total_dois, "DataCite API wird initialisiert...")
            
            try:
                client = DataCiteClient(
                    username=self.username,
                    password=self.password,
                    use_test_api=self.use_test_api
                )
            except Exception as e:
                error_msg = f"Fehler beim Initialisieren des DataCite Clients: {str(e)}"
                logger.error(error_msg)
                self.error_occurred.emit(error_msg)
                self.finished.emit(0, 0, 0, [], [])
                return
            
            # Step 2: Update each DOI
            for index, (doi, url) in enumerate(doi_url_pairs, start=1):
                if not self._is_running:
                    logger.info("Update process cancelled by user")
                    break
                
                # Emit progress
                self.progress_update.emit(
                    index, 
                    total_dois, 
                    f"Prüfe DOI {index}/{total_dois}: {doi}"
                )
                
                # Change Detection: Fetch current metadata to check if URL actually changed
                # Note: We could optimize by fetching all URLs first, but individual fetches
                # allow us to fail fast and continue with other DOIs if one fetch fails
                
                # Perform update
                try:
                    # First, fetch current metadata to check if URL changed
                    current_metadata = client.get_doi_metadata(doi)
                    if current_metadata:
                        datacite_current_url = current_metadata.get('data', {}).get('attributes', {}).get('url', '')
                        
                        # Compare current DataCite URL with CSV URL
                        if datacite_current_url == url:
                            # No change detected - skip update
                            success_count += 1  # Count as successful (no change needed)
                            skipped_count += 1
                            skipped_reason = f"URL unverändert: {url}"
                            skipped_details.append((doi, skipped_reason))
                            logger.info(f"DOI {doi}: URL unchanged ('{url}'), skipping update")
                            self.doi_updated.emit(doi, True, "Keine Änderung (übersprungen)")
                            
                            # If credentials are new and this is first successful operation, offer to save
                            if self.credentials_are_new and not self._first_success:
                                self._first_success = True
                                api_type = "test" if self.use_test_api else "production"
                                self.request_save_credentials.emit(self.username, self.password, api_type)
                            
                            continue  # Skip to next DOI
                        else:
                            # URL changed - log and proceed with update
                            logger.info(f"DOI {doi}: URL changed from '{datacite_current_url}' to '{url}'")
                    else:
                        # Could not fetch metadata - proceed with update anyway (might be new DOI)
                        logger.warning(f"DOI {doi}: Could not fetch current metadata, proceeding with update")
                    
                    success, message = client.update_doi_url(doi, url)
                    
                    if success:
                        success_count += 1
                        logger.info(f"Successfully updated: {doi}")
                        self.doi_updated.emit(doi, True, message)
                        
                        # If credentials are new and this is first successful update, offer to save them
                        if self.credentials_are_new and not self._first_success:
                            self._first_success = True
                            api_type = "test" if self.use_test_api else "production"
                            self.request_save_credentials.emit(self.username, self.password, api_type)
                    else:
                        error_count += 1
                        error_entry = f"{doi}: {message}"
                        error_list.append(error_entry)
                        logger.warning(f"Failed to update {doi}: {message}")
                        self.doi_updated.emit(doi, False, message)
                
                except NetworkError as e:
                    # Critical network error - abort process
                    error_msg = f"Netzwerkfehler: {str(e)}"
                    logger.error(error_msg)
                    self.error_occurred.emit(error_msg)
                    # Emit finished signal before breaking to ensure UI cleanup
                    self.finished.emit(success_count, error_count, skipped_count, error_list, skipped_details)
                    return
                
                except Exception as e:
                    # Unexpected error - log and continue
                    error_count += 1
                    error_entry = f"{doi}: Unerwarteter Fehler - {str(e)}"
                    error_list.append(error_entry)
                    logger.error(f"Unexpected error updating {doi}: {e}")
                    self.doi_updated.emit(doi, False, str(e))
            
            # Step 3: Emit final results
            logger.info(
                f"Update complete: {success_count} successful, {skipped_count} skipped (no changes), {error_count} failed"
            )
            # Log first 5 skipped DOIs for reference
            if skipped_details:
                count = len(skipped_details)
                if count < 5:
                    logger.info(f"Skipped DOIs ({count} total):")
                elif count == 5:
                    logger.info(f"Skipped DOIs (all {count}):")
                else:
                    logger.info(f"Skipped DOIs (first 5 of {count}):")
                for doi, reason in skipped_details[:5]:
                    logger.info(f"  - {doi}: {reason}")
            self.finished.emit(success_count, error_count, skipped_count, error_list, skipped_details)
        
        finally:
            self._is_running = False
    
    def stop(self):
        """Request the worker to stop processing."""
        logger.info("Stop requested for update worker")
        self._is_running = False
