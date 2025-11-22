"""Worker thread for updating DOI creator metadata via DataCite API and Database."""

import logging
from typing import Optional
from PySide6.QtCore import QObject, Signal, QSettings

from src.api.datacite_client import DataCiteClient, NetworkError, DataCiteAPIError, AuthenticationError
from src.utils.csv_parser import CSVParser, CSVParseError
from src.db.sumariopmd_client import (
    SumarioPMDClient,
    DatabaseError,
    ConnectionError as DBConnectionError,
    TransactionError
)
from src.utils.credential_manager import load_db_credentials


logger = logging.getLogger(__name__)


class AuthorsUpdateWorker(QObject):
    """Worker for updating DOI creator metadata in DataCite and Database."""
    
    # Signals
    progress_update = Signal(int, int, str)  # current, total, message
    dry_run_complete = Signal(int, int, list)  # valid_count, invalid_count, validation_results
    doi_updated = Signal(str, bool, str)  # doi, success, message
    finished = Signal(int, int, list)  # success_count, error_count, error_list
    error_occurred = Signal(str)  # error_message
    request_save_credentials = Signal(str, str, str)  # username, password, api_type
    
    # New signals for Phase 3: Database-First Pattern
    validation_update = Signal(str)  # Validation phase status
    datacite_update = Signal(str)    # DataCite update status
    database_update = Signal(str)    # Database update status
    
    def __init__(
        self, 
        username: str, 
        password: str, 
        csv_path: str, 
        use_test_api: bool = False,
        dry_run_only: bool = True,
        credentials_are_new: bool = False
    ):
        """
        Initialize the authors update worker.
        
        Args:
            username: DataCite username
            password: DataCite password
            csv_path: Path to CSV file with creator data
            use_test_api: If True, use test API instead of production
            dry_run_only: If True, only validate without updating
            credentials_are_new: Whether these are newly entered credentials (not from saved account)
        """
        super().__init__()
        self.username = username
        self.password = password
        self.csv_path = csv_path
        self.use_test_api = use_test_api
        self.dry_run_only = dry_run_only
        self.credentials_are_new = credentials_are_new
        self._is_running = False
        self._first_success = False
        
        # Database client (Phase 3)
        self.db_client: Optional[SumarioPMDClient] = None
        self.db_updates_enabled = False
    
    def _initialize_db_client(self) -> bool:
        """
        Initialize database client from saved credentials.
        
        Returns:
            True if DB client successfully initialized, False otherwise
        """
        settings = QSettings("GFZ", "GROBI")
        self.db_updates_enabled = settings.value("database/enabled", False, type=bool)
        
        if not self.db_updates_enabled:
            logger.info("Database updates disabled in settings")
            return False
        
        # Load credentials from keyring
        db_creds = load_db_credentials()
        if not db_creds:
            logger.warning("No database credentials found")
            return False
        
        try:
            self.db_client = SumarioPMDClient(
                host=db_creds['host'],
                database=db_creds['database'],
                username=db_creds['username'],
                password=db_creds['password']
            )
            # Test connection
            success, message = self.db_client.test_connection()
            if success:
                logger.info(f"Database client initialized: {message}")
                return True
            else:
                logger.error(f"Database connection test failed: {message}")
                return False
        except DBConnectionError as e:
            logger.error(f"Database initialization failed: {e}")
            return False
    
    def run(self):
        """
        Execute the creator update process with Database-First Pattern.
        
        This method will:
        1. Parse the CSV file
        2. Initialize DataCite client
        3. VALIDATION PHASE: Test both DataCite and Database availability
        4. Perform dry run validation
        5. If not dry_run_only: Update each DOI (Database FIRST, then DataCite)
        6. Emit progress signals and final results
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
            
            # Step 2b: VALIDATION PHASE - Test system availability
            self.validation_update.emit("⏳ Prüfe Systemverfügbarkeit...")
            logger.info("Starting validation phase: Testing system availability")
            
            # Test DataCite availability (basic connectivity test)
            try:
                # DataCite is already initialized, assume it's working
                # We'll catch errors during actual operations
                datacite_available = True
                self.validation_update.emit("  ✓ DataCite API erreichbar")
            except Exception as e:
                error_msg = f"DataCite API nicht erreichbar: {str(e)}"
                logger.error(error_msg)
                self.validation_update.emit("  ✗ DataCite API nicht erreichbar")
                self.error_occurred.emit(error_msg)
                self.finished.emit(0, 0, [])
                return
            
            # Test Database availability (if enabled)
            db_available = self._initialize_db_client()
            
            # CRITICAL: If DB updates enabled, DB MUST be available!
            if self.db_updates_enabled and not db_available:
                error_msg = (
                    "Datenbank-Updates sind aktiviert, aber Datenbank ist nicht erreichbar.\n\n"
                    "Mögliche Ursachen:\n"
                    "- Keine VPN-Verbindung zum GFZ-Netzwerk\n"
                    "- Falsche Datenbank-Credentials in Einstellungen\n"
                    "- Datenbank-Server nicht verfügbar\n\n"
                    "Bitte prüfen Sie die Verbindung oder deaktivieren Sie Datenbank-Updates "
                    "in den Einstellungen (Strg+,)."
                )
                logger.error("Database enabled but unavailable - aborting")
                self.validation_update.emit("  ✗ Datenbank nicht erreichbar (aber aktiviert!)")
                self.error_occurred.emit(error_msg)
                self.finished.emit(0, 0, [])
                return
            
            if db_available:
                self.validation_update.emit("  ✓ Datenbank erreichbar")
                logger.info("Database available and ready")
            else:
                self.validation_update.emit("  ⚪ Datenbank-Updates deaktiviert")
                logger.info("Database updates disabled - DataCite-only mode")
            
            self.validation_update.emit("✓ Alle benötigten Systeme verfügbar")
            logger.info("Validation phase complete - all required systems available")
            
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
                    
                    # ================================================================
                    # DATABASE-FIRST PATTERN: Update Database FIRST, then DataCite
                    # ================================================================
                    
                    db_success = True
                    db_message = "Datenbank-Updates deaktiviert"
                    
                    # Phase 1: Database Update (if enabled)
                    if self.db_client and self.db_updates_enabled:
                        self.database_update.emit(f"  ⏳ Datenbank wird aktualisiert...")
                        logger.info(f"Starting database update for DOI: {doi}")
                        
                        try:
                            # Get resource_id for DOI
                            resource_id = self.db_client.get_resource_id_for_doi(doi)
                            
                            if resource_id is None:
                                # DOI not found in database - this is a warning, not a critical error
                                # We'll still update DataCite
                                db_success = False
                                db_message = "DOI nicht in Datenbank gefunden (nur DataCite wird aktualisiert)"
                                logger.warning(f"DOI {doi} not found in database - skipping DB update")
                                self.database_update.emit(f"  ⚠️ {db_message}")
                            else:
                                # Update creators transactionally
                                db_success, db_message, db_errors = self.db_client.update_creators_transactional(
                                    resource_id,
                                    creators
                                )
                                
                                if db_success:
                                    logger.info(f"Database update successful for DOI: {doi}")
                                    self.database_update.emit(f"  ✓ Datenbank erfolgreich aktualisiert")
                                else:
                                    # Database update failed with ROLLBACK!
                                    # CRITICAL: Do NOT update DataCite!
                                    logger.error(f"Database update failed for DOI {doi}: {db_message}")
                                    self.database_update.emit(f"  ✗ Datenbank-Fehler (ROLLBACK)")
                                    
                                    error_count += 1
                                    error_entry = f"{doi}: Datenbank-Update fehlgeschlagen - {db_message}"
                                    error_list.append(error_entry)
                                    self.doi_updated.emit(doi, False, error_entry)
                                    
                                    # Skip DataCite update!
                                    continue
                        
                        except (DatabaseError, DBConnectionError, TransactionError) as e:
                            # Database error - ROLLBACK already performed by client
                            db_success = False
                            db_message = f"Datenbank-Fehler: {str(e)}"
                            logger.error(f"Database error for DOI {doi}: {e}")
                            self.database_update.emit(f"  ✗ {db_message}")
                            
                            error_count += 1
                            error_entry = f"{doi}: {db_message}"
                            error_list.append(error_entry)
                            self.doi_updated.emit(doi, False, error_entry)
                            
                            # Skip DataCite update!
                            continue
                    
                    # Phase 2: DataCite Update (only if DB was successful or disabled)
                    self.datacite_update.emit(f"  ⏳ DataCite wird aktualisiert...")
                    logger.info(f"Starting DataCite update for DOI: {doi}")
                    
                    datacite_success, datacite_message = client.update_doi_creators(doi, creators, metadata)
                    
                    if datacite_success:
                        logger.info(f"DataCite update successful for DOI: {doi}")
                        self.datacite_update.emit(f"  ✓ DataCite erfolgreich aktualisiert")
                        
                        success_count += 1
                        
                        # Combine status messages
                        if self.db_updates_enabled and db_success:
                            combined_message = "✓ Beide Systeme erfolgreich aktualisiert"
                        elif self.db_updates_enabled and not db_success:
                            combined_message = f"✓ DataCite aktualisiert, ⚠️ Datenbank: {db_message}"
                        else:
                            combined_message = "✓ DataCite aktualisiert (Datenbank deaktiviert)"
                        
                        self.doi_updated.emit(doi, True, combined_message)
                        
                        # If credentials are new and this is first successful update, offer to save them
                        if self.credentials_are_new and not self._first_success:
                            self._first_success = True
                            api_type = "test" if self.use_test_api else "production"
                            self.request_save_credentials.emit(self.username, self.password, api_type)
                    
                    else:
                        # DataCite update failed!
                        logger.error(f"DataCite update failed for DOI {doi}: {datacite_message}")
                        self.datacite_update.emit(f"  ✗ DataCite-Fehler")
                        
                        # PROBLEM: If DB was successful, we now have an inconsistency!
                        if self.db_updates_enabled and db_success:
                            # CRITICAL INCONSISTENCY: DB committed, DataCite failed
                            logger.critical(
                                f"INCONSISTENCY DETECTED: Database committed but DataCite failed for DOI: {doi}"
                            )
                            
                            # Try immediate retry
                            logger.warning(f"Attempting immediate retry for DataCite update: {doi}")
                            self.datacite_update.emit(f"  ⚠️ Retry wird versucht...")
                            
                            retry_success, retry_message = client.update_doi_creators(doi, creators, metadata)
                            
                            if retry_success:
                                logger.info(f"Retry successful for DOI: {doi}")
                                self.datacite_update.emit(f"  ✓ DataCite erfolgreich aktualisiert (nach Retry)")
                                
                                success_count += 1
                                self.doi_updated.emit(
                                    doi, 
                                    True, 
                                    "✓ Beide Systeme aktualisiert (DataCite nach Retry)"
                                )
                            else:
                                # Retry also failed - CRITICAL INCONSISTENCY!
                                logger.critical(
                                    f"CRITICAL: Database committed but DataCite failed after retry for DOI: {doi}"
                                )
                                self.datacite_update.emit(f"  ✗ DataCite fehlgeschlagen (auch nach Retry)")
                                
                                error_count += 1
                                error_entry = (
                                    f"{doi}: INKONSISTENZ - Datenbank erfolgreich, DataCite fehlgeschlagen "
                                    f"(auch nach Retry). Manuelle Korrektur erforderlich! "
                                    f"DataCite-Fehler: {datacite_message}"
                                )
                                error_list.append(error_entry)
                                self.doi_updated.emit(doi, False, error_entry)
                        else:
                            # DB was not updated or disabled, so no inconsistency
                            error_count += 1
                            error_entry = f"{doi}: DataCite-Update fehlgeschlagen - {datacite_message}"
                            error_list.append(error_entry)
                            self.doi_updated.emit(doi, False, error_entry)
                
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
