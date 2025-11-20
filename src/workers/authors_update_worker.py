"""Worker thread for updating DOI creator metadata via DataCite API."""

import logging
from PySide6.QtCore import QObject, Signal

from src.api.datacite_client import DataCiteClient, NetworkError, DataCiteAPIError, AuthenticationError
from src.utils.csv_parser import CSVParser, CSVParseError


logger = logging.getLogger(__name__)


class AuthorsUpdateWorker(QObject):
    """Worker for updating DOI creator metadata in a separate thread."""
    
    # Signals
    progress_update = Signal(int, int, str)  # current, total, message
    dry_run_complete = Signal(int, int, list)  # valid_count, invalid_count, validation_results
    doi_updated = Signal(str, bool, str)  # doi, success, message
    finished = Signal(int, int, list)  # success_count, error_count, error_list
    error_occurred = Signal(str)  # error_message
    
    def __init__(
        self, 
        username: str, 
        password: str, 
        csv_path: str, 
        use_test_api: bool = False,
        dry_run_only: bool = True
    ):
        """
        Initialize the authors update worker.
        
        Args:
            username: DataCite username
            password: DataCite password
            csv_path: Path to CSV file with creator data
            use_test_api: If True, use test API instead of production
            dry_run_only: If True, only validate without updating
        """
        super().__init__()
        self.username = username
        self.password = password
        self.csv_path = csv_path
        self.use_test_api = use_test_api
        self.dry_run_only = dry_run_only
        self._is_running = False
    
    def run(self):
        """
        Execute the creator update process.
        
        This method will:
        1. Parse the CSV file
        2. Initialize DataCite client
        3. Perform dry run validation
        4. If not dry_run_only: Update each DOI with validated creators
        5. Emit progress signals and final results
        """
        self._is_running = True
        
        try:
            # Step 1: Parse CSV file
            logger.info(f"Parsing authors CSV file: {self.csv_path}")
            self.progress_update.emit(0, 0, "CSV-Datei wird gelesen und validiert...")
            
            try:
                creators_by_doi, warnings = CSVParser.parse_authors_update_csv(self.csv_path)
            except (CSVParseError, FileNotFoundError) as e:
                error_msg = f"Fehler beim Lesen der CSV-Datei: {str(e)}"
                logger.error(error_msg)
                self.error_occurred.emit(error_msg)
                self.finished.emit(0, 0, [])
                return
            
            total_dois = len(creators_by_doi)
            logger.info(f"Found {total_dois} DOIs with creator data to process")
            
            # Log warnings from CSV parsing
            if warnings:
                logger.warning(f"CSV parsing generated {len(warnings)} warnings")
                for warning in warnings:
                    logger.warning(f"  - {warning}")
            
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
            
            # Step 3: Dry Run Validation
            logger.info("Starting dry run validation...")
            valid_count = 0
            invalid_count = 0
            validation_results = []
            metadata_cache = {}  # Cache metadata for later updates
            
            for index, (doi, creators) in enumerate(creators_by_doi.items(), start=1):
                if not self._is_running:
                    logger.info("Validation process cancelled by user")
                    break
                
                # Emit progress
                self.progress_update.emit(
                    index, 
                    total_dois, 
                    f"Validiere DOI {index}/{total_dois}: {doi}"
                )
                
                # Fetch current metadata
                try:
                    metadata = client.get_doi_metadata(doi)
                    
                    if metadata is None:
                        invalid_count += 1
                        result = {
                            'doi': doi,
                            'valid': False,
                            'message': f"DOI {doi} nicht gefunden oder nicht erreichbar"
                        }
                        validation_results.append(result)
                        logger.warning(f"DOI not found or unreachable: {doi}")
                        continue
                    
                    # Cache metadata for potential updates
                    metadata_cache[doi] = metadata
                    
                    # Validate creators match
                    is_valid, message = client.validate_creators_match(doi, creators)
                    
                    if is_valid:
                        valid_count += 1
                        result = {
                            'doi': doi,
                            'valid': True,
                            'message': message,
                            'creator_count': len(creators)
                        }
                        logger.info(f"Validation passed: {doi}")
                    else:
                        invalid_count += 1
                        result = {
                            'doi': doi,
                            'valid': False,
                            'message': message
                        }
                        logger.warning(f"Validation failed: {doi} - {message}")
                    
                    validation_results.append(result)
                
                except AuthenticationError as e:
                    error_msg = f"Authentifizierungsfehler: {str(e)}"
                    logger.error(error_msg)
                    self.error_occurred.emit(error_msg)
                    self.finished.emit(0, 0, [])
                    return
                
                except NetworkError as e:
                    error_msg = f"Netzwerkfehler: {str(e)}"
                    logger.error(error_msg)
                    self.error_occurred.emit(error_msg)
                    self.finished.emit(0, 0, [])
                    return
                
                except DataCiteAPIError as e:
                    invalid_count += 1
                    result = {
                        'doi': doi,
                        'valid': False,
                        'message': f"API-Fehler: {str(e)}"
                    }
                    validation_results.append(result)
                    logger.error(f"API error for {doi}: {e}")
                
                except Exception as e:
                    invalid_count += 1
                    result = {
                        'doi': doi,
                        'valid': False,
                        'message': f"Unerwarteter Fehler: {str(e)}"
                    }
                    validation_results.append(result)
                    logger.error(f"Unexpected error validating {doi}: {e}")
            
            # Emit dry run results
            logger.info(
                f"Dry run complete: {valid_count} valid, {invalid_count} invalid"
            )
            self.dry_run_complete.emit(valid_count, invalid_count, validation_results)
            
            # If dry run only, finish here
            if self.dry_run_only:
                logger.info("Dry run only - finishing without updates")
                self.finished.emit(valid_count, invalid_count, [])
                return
            
            # Step 4: Perform actual updates (only if not dry_run_only)
            success_count = 0
            error_count = 0
            error_list = []
            
            # Only update DOIs that passed validation
            valid_dois = [result['doi'] for result in validation_results if result['valid']]
            total_updates = len(valid_dois)
            
            for index, doi in enumerate(valid_dois, start=1):
                if not self._is_running:
                    logger.info("Update process cancelled by user")
                    break
                
                # Emit progress
                self.progress_update.emit(
                    index, 
                    total_updates, 
                    f"Aktualisiere DOI {index}/{total_updates}: {doi}"
                )
                
                try:
                    creators = creators_by_doi[doi]
                    metadata = metadata_cache.get(doi)
                    
                    if metadata is None:
                        # This shouldn't happen, but handle it gracefully
                        error_count += 1
                        error_entry = f"{doi}: Metadaten nicht im Cache gefunden"
                        error_list.append(error_entry)
                        logger.error(f"Metadata not cached for {doi}")
                        self.doi_updated.emit(doi, False, "Metadaten nicht verfügbar")
                        continue
                    
                    # Perform update
                    success, message = client.update_doi_creators(doi, creators, metadata)
                    
                    if success:
                        success_count += 1
                        logger.info(f"Successfully updated creators: {doi}")
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
                    self.finished.emit(success_count, error_count, error_list)
                    return
                
                except Exception as e:
                    # Unexpected error - log and continue
                    error_count += 1
                    error_entry = f"{doi}: Unerwarteter Fehler - {str(e)}"
                    error_list.append(error_entry)
                    logger.error(f"Unexpected error updating {doi}: {e}")
                    self.doi_updated.emit(doi, False, str(e))
            
            # Step 5: Emit final results
            logger.info(
                f"Creator update complete: {success_count} successful, {error_count} failed"
            )
            self.finished.emit(success_count, error_count, error_list)
        
        finally:
            self._is_running = False
    
    def stop(self):
        """Request the worker to stop processing."""
        logger.info("Stop requested for authors update worker")
        self._is_running = False
