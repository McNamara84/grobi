"""Worker thread for updating DOI publisher metadata via DataCite API and Database."""

import logging
from typing import Optional, Dict, Any, Tuple
from PySide6.QtCore import QObject, Signal, QSettings

from src.api.datacite_client import DataCiteClient, NetworkError, DataCiteAPIError, AuthenticationError
from src.utils.csv_parser import CSVParser, CSVParseError
from src.db.sumariopmd_client import (
    SumarioPMDClient,
    DatabaseError,
    ConnectionError as DBConnectionError
)
from src.utils.credential_manager import load_db_credentials


logger = logging.getLogger(__name__)


class PublisherUpdateWorker(QObject):
    """Worker for updating DOI publisher metadata in DataCite and Database."""
    
    # Signals
    progress_update = Signal(int, int, str)  # current, total, message
    dry_run_complete = Signal(int, int, list)  # valid_count, invalid_count, validation_results
    doi_updated = Signal(str, bool, str)  # doi, success, message
    finished = Signal(int, int, int, list, list)  # success_count, error_count, skipped_count, error_list, skipped_details
    error_occurred = Signal(str)  # error_message
    request_save_credentials = Signal(str, str, str)  # username, password, api_type
    
    # Signals for phase updates
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
        Initialize the publisher update worker.
        
        Args:
            username: DataCite username
            password: DataCite password
            csv_path: Path to CSV file with publisher data
            use_test_api: If True, use test API instead of production
            dry_run_only: If True, only validate without updating
            credentials_are_new: Whether these are newly entered credentials
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
        
        # Database client
        self.db_client: Optional[SumarioPMDClient] = None
        self.db_updates_enabled = False
    
    def _detect_publisher_changes(
        self, 
        current_metadata: dict, 
        csv_publisher: dict
    ) -> Tuple[bool, str]:
        """
        Compare current DataCite metadata with CSV data to detect publisher changes.
        
        Args:
            current_metadata: Full metadata dictionary from DataCite API
            csv_publisher: Publisher dict from CSV
        
        Returns:
            Tuple[bool, str]: (has_changes, change_description)
            - (True, "Description of changes") if differences found
            - (False, "No changes") if identical
        """
        try:
            publisher_raw = current_metadata.get("data", {}).get("attributes", {}).get("publisher", "")
        except (KeyError, AttributeError) as e:
            logger.error(f"Error extracting publisher from metadata: {e}")
            return True, "Metadaten-Struktur ungültig (Update erforderlich)"
        
        # Parse current publisher (can be string or dict)
        if isinstance(publisher_raw, dict):
            current_name = publisher_raw.get("name", "")
            current_identifier = publisher_raw.get("publisherIdentifier", "")
            current_scheme = publisher_raw.get("publisherIdentifierScheme", "")
            current_scheme_uri = publisher_raw.get("schemeUri", "")
            current_lang = publisher_raw.get("lang", "")
        elif isinstance(publisher_raw, str):
            current_name = publisher_raw
            current_identifier = ""
            current_scheme = ""
            current_scheme_uri = ""
            current_lang = ""
        else:
            current_name = str(publisher_raw) if publisher_raw else ""
            current_identifier = ""
            current_scheme = ""
            current_scheme_uri = ""
            current_lang = ""
        
        # Get CSV values
        csv_name = csv_publisher.get("name", "")
        csv_identifier = csv_publisher.get("publisherIdentifier", "")
        csv_scheme = csv_publisher.get("publisherIdentifierScheme", "")
        csv_scheme_uri = csv_publisher.get("schemeUri", "")
        csv_lang = csv_publisher.get("lang", "")
        
        # Field-by-field comparison
        changes = []
        
        if current_name != csv_name:
            changes.append(f"Name: '{current_name}' → '{csv_name}'")
        
        if current_identifier != csv_identifier:
            changes.append(f"Identifier: '{current_identifier}' → '{csv_identifier}'")
        
        if current_scheme != csv_scheme:
            changes.append(f"Scheme: '{current_scheme}' → '{csv_scheme}'")
        
        if current_scheme_uri != csv_scheme_uri:
            changes.append(f"SchemeURI: '{current_scheme_uri}' → '{csv_scheme_uri}'")
        
        if current_lang != csv_lang:
            changes.append(f"Language: '{current_lang}' → '{csv_lang}'")
        
        if changes:
            # Return first 3 changes, indicate if more exist
            change_desc = "; ".join(changes[:3])
            if len(changes) > 3:
                change_desc += f" (+ {len(changes) - 3} weitere)"
            return True, change_desc
        else:
            return False, "Keine Änderungen in Publisher-Metadaten"
    
    def _initialize_db_client(self) -> bool:
        """
        Initialize database client from saved credentials.
        
        Returns:
            True if DB client successfully initialized, False otherwise
        """
        # QSettings already imported at module level
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
        Execute the publisher update process.
        
        This method will:
        1. Parse the CSV file
        2. Initialize DataCite client
        3. VALIDATION PHASE: Test both DataCite and Database availability
        4. Perform dry run validation with change detection
        5. If not dry_run_only: Update each DOI (Database FIRST, then DataCite)
        6. Emit progress signals and final results
        """
        self._is_running = True
        
        try:
            # Step 1: Parse CSV file
            logger.info(f"Parsing publisher CSV file: {self.csv_path}")
            self.progress_update.emit(0, 0, "CSV-Datei wird gelesen und validiert...")
            
            try:
                publisher_by_doi, warnings = CSVParser.parse_publisher_update_csv(self.csv_path)
            except (CSVParseError, FileNotFoundError) as e:
                error_msg = f"Fehler beim Lesen der CSV-Datei: {str(e)}"
                logger.error(error_msg)
                self.error_occurred.emit(error_msg)
                self.finished.emit(0, 0, 0, [], [])
                return
            
            total_dois = len(publisher_by_doi)
            logger.info(f"Found {total_dois} DOIs with publisher data to process")
            
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
                self.finished.emit(0, 0, 0, [], [])
                return
            
            # Step 2b: VALIDATION PHASE - Test system availability
            self.validation_update.emit("⏳ Prüfe Systemverfügbarkeit...")
            logger.info("Starting validation phase: Testing system availability")
            
            # Test DataCite API availability with a lightweight request
            try:
                # Fetch metadata for first DOI to verify API connectivity
                first_doi = list(publisher_by_doi.keys())[0]
                client.get_doi_metadata(first_doi)
                self.validation_update.emit("  ✓ DataCite API erreichbar")
                logger.info("DataCite API connectivity verified")
            except AuthenticationError as e:
                error_msg = f"DataCite Authentifizierung fehlgeschlagen: {str(e)}"
                logger.error(error_msg)
                self.validation_update.emit("  ✗ DataCite Authentifizierung fehlgeschlagen")
                self.error_occurred.emit(error_msg)
                self.finished.emit(0, 0, 0, [], [])
                return
            except (NetworkError, DataCiteAPIError) as e:
                error_msg = f"DataCite API nicht erreichbar: {str(e)}"
                logger.error(error_msg)
                self.validation_update.emit("  ✗ DataCite API nicht erreichbar")
                self.error_occurred.emit(error_msg)
                self.finished.emit(0, 0, 0, [], [])
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
                self.finished.emit(0, 0, 0, [], [])
                return
            
            if db_available:
                self.validation_update.emit("  ✓ Datenbank erreichbar")
                logger.info("Database available and ready")
            else:
                self.validation_update.emit("  ⚪ Datenbank-Updates deaktiviert")
                logger.info("Database updates disabled - DataCite-only mode")
            
            self.validation_update.emit("✓ Alle benötigten Systeme verfügbar")
            logger.info("Validation phase complete - all required systems available")
            
            # Step 3: Dry Run Validation with Change Detection
            logger.info("Starting dry run validation with change detection...")
            valid_count = 0
            invalid_count = 0
            validation_results = []
            metadata_cache = {}  # Cache metadata for later updates
            skipped_details = []  # List of (doi, reason) tuples for skipped DOIs
            dois_with_changes = []  # DOIs that need updating
            
            for index, (doi, publisher_data) in enumerate(publisher_by_doi.items(), start=1):
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
                        validation_results.append({
                            'doi': doi,
                            'valid': False,
                            'message': f"DOI {doi} nicht gefunden (API Timeout oder nicht vorhanden)"
                        })
                        invalid_count += 1
                        continue
                    
                    # Cache metadata for later update
                    metadata_cache[doi] = metadata
                    
                    # Detect changes
                    has_changes, change_desc = self._detect_publisher_changes(metadata, publisher_data)
                    
                    if has_changes:
                        validation_results.append({
                            'doi': doi,
                            'valid': True,
                            'has_changes': True,
                            'message': f"Änderungen erkannt: {change_desc}"
                        })
                        dois_with_changes.append(doi)
                        valid_count += 1
                    else:
                        validation_results.append({
                            'doi': doi,
                            'valid': True,
                            'has_changes': False,
                            'message': change_desc
                        })
                        skipped_details.append((doi, change_desc))
                        valid_count += 1
                    
                except AuthenticationError as e:
                    error_msg = f"Authentifizierung fehlgeschlagen: {str(e)}"
                    logger.error(error_msg)
                    self.error_occurred.emit(error_msg)
                    self.finished.emit(0, 0, 0, [], [])
                    return
                
                except NetworkError as e:
                    validation_results.append({
                        'doi': doi,
                        'valid': False,
                        'message': f"Netzwerkfehler: {str(e)}"
                    })
                    invalid_count += 1
                
                except DataCiteAPIError as e:
                    validation_results.append({
                        'doi': doi,
                        'valid': False,
                        'message': f"API Fehler: {str(e)}"
                    })
                    invalid_count += 1
            
            # Emit dry run results
            self.dry_run_complete.emit(valid_count, invalid_count, validation_results)
            
            # If dry run only, emit finished and return
            if self.dry_run_only:
                logger.info("Dry run complete, stopping before updates")
                self.finished.emit(0, 0, len(skipped_details), [], skipped_details)
                return
            
            # Step 4: Perform actual updates (only for DOIs with changes)
            logger.info(f"Starting updates for {len(dois_with_changes)} DOIs with changes")
            success_count = 0
            error_count = 0
            error_list = []
            
            for index, doi in enumerate(dois_with_changes, start=1):
                if not self._is_running:
                    logger.info("Update process cancelled by user")
                    break
                
                publisher_data = publisher_by_doi[doi]
                metadata = metadata_cache.get(doi)
                
                if metadata is None:
                    error_msg = f"{doi}: Keine Metadaten im Cache"
                    logger.error(error_msg)
                    error_list.append(error_msg)
                    error_count += 1
                    self.doi_updated.emit(doi, False, error_msg)
                    continue
                
                self.progress_update.emit(
                    index,
                    len(dois_with_changes),
                    f"Aktualisiere DOI {index}/{len(dois_with_changes)}: {doi}"
                )
                
                # Step 4a: Update Database FIRST (if enabled)
                db_success = True
                if db_available and self.db_updates_enabled:
                    try:
                        # Only update publisher name in database
                        # (extended fields are only in DataCite)
                        current_db_publisher = self.db_client.get_publisher_for_doi(doi)
                        new_publisher_name = publisher_data.get("name", "")
                        
                        if current_db_publisher != new_publisher_name:
                            self.database_update.emit(f"  📊 DB Update: {doi}")
                            db_success_result, db_message = self.db_client.update_publisher(doi, new_publisher_name)
                            if db_success_result:
                                self.database_update.emit(f"    ✓ {db_message}")
                            else:
                                # Check if DOI not found in DB (not a fatal error)
                                if "nicht in der Datenbank gefunden" in db_message:
                                    self.database_update.emit(f"  ⚠️ DOI nicht in DB: {doi}")
                                    logger.warning(f"DOI {doi} not found in database")
                                else:
                                    db_success = False
                                    self.database_update.emit(f"    ✗ {db_message}")
                        else:
                            self.database_update.emit(f"  📊 DB: Keine Änderung für {doi}")
                    
                    except DatabaseError as e:
                        db_success = False
                        self.database_update.emit(f"    ✗ DB Fehler: {str(e)}")
                        logger.error(f"Database error for {doi}: {e}")
                    
                    except Exception as e:
                        db_success = False
                        self.database_update.emit(f"    ✗ Unerwarteter Fehler: {str(e)}")
                        logger.error(f"Unexpected database error for {doi}: {e}")
                
                # Step 4b: Update DataCite (only if DB succeeded or DB not enabled)
                if db_success:
                    try:
                        self.datacite_update.emit(f"  🌐 DataCite Update: {doi}")
                        success, message = client.update_doi_publisher(doi, publisher_data, metadata)
                        
                        if success:
                            success_count += 1
                            self.datacite_update.emit(f"    ✓ {message}")
                            self.doi_updated.emit(doi, True, message)
                            
                            # Offer to save credentials on first success
                            if self.credentials_are_new and not self._first_success:
                                self._first_success = True
                                api_type = "test" if self.use_test_api else "production"
                                self.request_save_credentials.emit(self.username, self.password, api_type)
                        else:
                            error_count += 1
                            error_list.append(f"{doi}: {message}")
                            self.datacite_update.emit(f"    ✗ {message}")
                            self.doi_updated.emit(doi, False, message)
                    
                    except NetworkError as e:
                        error_msg = f"{doi}: Netzwerkfehler - {str(e)}"
                        error_count += 1
                        error_list.append(error_msg)
                        self.datacite_update.emit(f"    ✗ {error_msg}")
                        self.doi_updated.emit(doi, False, error_msg)
                else:
                    # DB failed, skip DataCite update
                    error_msg = f"{doi}: Übersprungen (Datenbank-Update fehlgeschlagen)"
                    error_count += 1
                    error_list.append(error_msg)
                    self.doi_updated.emit(doi, False, error_msg)
            
            # Emit final results
            skipped_count = len(skipped_details)
            logger.info(
                f"Publisher update complete: {success_count} succeeded, "
                f"{error_count} failed, {skipped_count} skipped"
            )
            self.finished.emit(success_count, error_count, skipped_count, error_list, skipped_details)
            
        except Exception as e:
            error_msg = f"Unerwarteter Fehler: {str(e)}"
            logger.error(f"Unexpected error in publisher update worker: {e}", exc_info=True)
            self.error_occurred.emit(error_msg)
            self.finished.emit(0, 0, 0, [], [])
        
        finally:
            self._is_running = False
    
    def stop(self):
        """Request the worker to stop processing."""
        logger.info("Stop requested for publisher update worker")
        self._is_running = False
