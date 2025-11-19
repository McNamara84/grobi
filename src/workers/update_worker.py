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
    finished = Signal(int, int, list)  # success_count, error_count, error_list
    error_occurred = Signal(str)  # error_message
    
    def __init__(
        self, 
        username: str, 
        password: str, 
        csv_path: str, 
        use_test_api: bool = False
    ):
        """
        Initialize the update worker.
        
        Args:
            username: DataCite username
            password: DataCite password
            csv_path: Path to CSV file with DOI/URL pairs
            use_test_api: If True, use test API instead of production
        """
        super().__init__()
        self.username = username
        self.password = password
        self.csv_path = csv_path
        self.use_test_api = use_test_api
        self._is_running = False
    
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
        error_list = []
        
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
                self.finished.emit(0, 0, [])
                return
            
            total_dois = len(doi_url_pairs)
            logger.info(f"Found {total_dois} DOI/URL pairs to update")
            
            # Step 2: Initialize DataCite client
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
                self.finished.emit(0, 0, [])
                return
            
            # Step 3: Update each DOI
            for index, (doi, url) in enumerate(doi_url_pairs, start=1):
                if not self._is_running:
                    logger.info("Update process cancelled by user")
                    break
                
                # Emit progress
                self.progress_update.emit(
                    index, 
                    total_dois, 
                    f"Aktualisiere DOI {index}/{total_dois}: {doi}"
                )
                
                # Perform update
                try:
                    success, message = client.update_doi_url(doi, url)
                    
                    if success:
                        success_count += 1
                        logger.info(f"Successfully updated: {doi}")
                        self.doi_updated.emit(doi, True, message)
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
                    break
                
                except Exception as e:
                    # Unexpected error - log and continue
                    error_count += 1
                    error_entry = f"{doi}: Unerwarteter Fehler - {str(e)}"
                    error_list.append(error_entry)
                    logger.error(f"Unexpected error updating {doi}: {e}")
                    self.doi_updated.emit(doi, False, str(e))
            
            # Step 4: Emit final results
            logger.info(
                f"Update complete: {success_count} successful, {error_count} failed"
            )
            self.finished.emit(success_count, error_count, error_list)
        
        finally:
            self._is_running = False
    
    def stop(self):
        """Request the worker to stop processing."""
        logger.info("Stop requested for update worker")
        self._is_running = False
