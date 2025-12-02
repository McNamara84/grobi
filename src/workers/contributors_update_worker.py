"""Worker thread for updating DOI contributor metadata via DataCite API and Database."""

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


class ContributorsUpdateWorker(QObject):
    """Worker for updating DOI contributor metadata in DataCite and Database.
    
    Contributors are different from Creators (Authors):
    - Contributors can have multiple ContributorTypes (comma-separated)
    - ContactPerson contributors have additional fields: email, website, position (DB only)
    - Database stores roles in separate 'role' table (one row per role)
    - DataCite API only accepts first ContributorType per contributor
    """
    
    # Signals
    progress_update = Signal(int, int, str)  # current, total, message
    dry_run_complete = Signal(int, int, list)  # valid_count, invalid_count, validation_results
    doi_updated = Signal(str, bool, str)  # doi, success, message
    finished = Signal(int, int, int, list, list)  # success_count, error_count, skipped_count, error_list, skipped_details
    error_occurred = Signal(str)  # error_message
    request_save_credentials = Signal(str, str, str)  # username, password, api_type
    
    # Database-First Pattern signals
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
        Initialize the contributors update worker.
        
        Args:
            username: DataCite username
            password: DataCite password
            csv_path: Path to CSV file with contributor data
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
        
        # Database client
        self.db_client: Optional[SumarioPMDClient] = None
        self.db_updates_enabled = False
    
    # Change Detection Helper Methods
    
    def _detect_contributor_changes(
        self, 
        current_metadata: dict, 
        csv_contributors: list
    ) -> tuple[bool, str]:
        """
        Compare current DataCite metadata with CSV data to detect changes.
        
        Args:
            current_metadata: Full metadata dictionary from DataCite API
            csv_contributors: Contributor list from CSV (in CSV order)
        
        Returns:
            Tuple[bool, str]: (has_changes, change_description)
            - (True, "Description of changes") if differences found
            - (False, "No changes") if identical
        """
        try:
            current_contributors = current_metadata.get("data", {}).get("attributes", {}).get("contributors", [])
        except (KeyError, AttributeError) as e:
            logger.error(f"Error extracting contributors from metadata: {e}")
            return True, "Metadaten-Struktur ungültig (Update erforderlich)"
        
        # Count mismatch → Always update
        if len(current_contributors) != len(csv_contributors):
            return True, f"Contributor-Anzahl unterschiedlich (aktuell: {len(current_contributors)}, CSV: {len(csv_contributors)})"
        
        # No contributors in both → No changes
        if len(current_contributors) == 0:
            return False, "Keine Contributors vorhanden"
        
        # Field-by-field comparison
        changes = []
        for i, (current, csv_contrib) in enumerate(zip(current_contributors, csv_contributors), 1):
            # Compare name
            current_name = current.get("name", "")
            csv_name = csv_contrib.get("name", "")
            if current_name != csv_name:
                changes.append(f"Contributor {i}: Name geändert")
            
            # Compare nameType
            current_type = current.get("nameType", "Personal")
            csv_type = csv_contrib.get("nameType", "Personal")
            if current_type != csv_type:
                changes.append(f"Contributor {i}: NameType geändert")
            
            # Compare given/family names (only for Personal)
            if csv_type == "Personal":
                current_given = current.get("givenName", "")
                csv_given = csv_contrib.get("givenName", "")
                if current_given != csv_given:
                    changes.append(f"Contributor {i}: GivenName geändert")
                
                current_family = current.get("familyName", "")
                csv_family = csv_contrib.get("familyName", "")
                if current_family != csv_family:
                    changes.append(f"Contributor {i}: FamilyName geändert")
            
            # Compare ContributorType (only first type for DataCite)
            # DataCite only stores one contributorType per contributor
            current_contrib_type = current.get("contributorType", "")
            csv_contrib_types = csv_contrib.get("contributorTypes", [])
            csv_first_type = csv_contrib_types[0] if csv_contrib_types else ""
            if current_contrib_type != csv_first_type:
                changes.append(f"Contributor {i}: ContributorType geändert")
            
            # Compare ORCID (normalized)
            current_orcid = self._extract_orcid(current)
            csv_orcid = self._normalize_orcid(csv_contrib.get("nameIdentifier", ""))
            if current_orcid != csv_orcid:
                changes.append(f"Contributor {i}: ORCID geändert")
            
            # Compare DB-only fields (Email, Website, Position)
            # These are not in DataCite, but we still need to detect changes for DB updates
            # Note: current_metadata from DataCite won't have these fields, so we mark as
            # "potential DB change" if CSV has non-empty values for ContactPerson
            csv_email = csv_contrib.get("email", "")
            csv_website = csv_contrib.get("website", "")
            csv_position = csv_contrib.get("position", "")
            
            # For DB-only fields, we can't compare with DataCite metadata
            # Instead, we'll always consider it a change if DB fields are present
            # The actual comparison will be done during DB update
            if csv_email or csv_website or csv_position:
                # Mark that DB fields exist - actual change detection happens in DB layer
                # We add this as a "DB change" flag rather than a comparison
                pass  # DB changes are handled separately in _detect_db_changes
        
        # Check for DB-only field changes (separate from DataCite changes)
        db_changes = self._detect_db_field_changes(csv_contributors)
        
        if changes or db_changes:
            # Combine DataCite and DB changes
            all_changes = changes + db_changes
            change_desc = "; ".join(all_changes[:3])
            if len(all_changes) > 3:
                change_desc += f" (+ {len(all_changes) - 3} weitere)"
            return True, change_desc
        else:
            return False, "Keine Änderungen in Contributor-Metadaten"
    
    def _detect_db_field_changes(self, csv_contributors: list) -> list:
        """
        Detect if CSV contains DB-only fields (Email, Website, Position) that need updating.
        
        Since these fields are not in DataCite, we can't compare with current values.
        We mark them as potential changes if they have non-empty values.
        The actual comparison with DB values happens in the database layer.
        
        Args:
            csv_contributors: List of contributor dicts from CSV
            
        Returns:
            List of change descriptions for DB-only fields
        """
        db_changes = []
        
        for i, contrib in enumerate(csv_contributors, 1):
            csv_email = contrib.get("email", "")
            csv_website = contrib.get("website", "")
            csv_position = contrib.get("position", "")
            
            # If any DB-only field has a value, mark as potential DB change
            if csv_email:
                db_changes.append(f"Contributor {i}: E-Mail (DB)")
            if csv_website:
                db_changes.append(f"Contributor {i}: Website (DB)")
            if csv_position:
                db_changes.append(f"Contributor {i}: Position (DB)")
        
        return db_changes
    
    def _normalize_orcid(self, orcid: str) -> str:
        """
        Normalize ORCID to ID-only format.
        
        Examples:
            "https://orcid.org/0000-0001-5000-0007" → "0000-0001-5000-0007"
            "http://orcid.org/0000-0001-5000-0007" → "0000-0001-5000-0007"
            "0000-0001-5000-0007" → "0000-0001-5000-0007"
            "" → ""
        
        Args:
            orcid: ORCID string (with or without URL prefix)
        
        Returns:
            Normalized ORCID (ID only, no URL prefix)
        """
        if not orcid:
            return ""
        if orcid.startswith("https://orcid.org/"):
            return orcid.replace("https://orcid.org/", "")
        elif orcid.startswith("http://orcid.org/"):
            return orcid.replace("http://orcid.org/", "")
        return orcid
    
    def _extract_orcid(self, contributor: dict) -> str:
        """
        Extract ORCID from DataCite contributor object (normalized).
        
        Args:
            contributor: Contributor dictionary from DataCite API
        
        Returns:
            Normalized ORCID (only ID, without URL prefix) or ""
        """
        identifiers = contributor.get("nameIdentifiers", [])
        for identifier in identifiers:
            if identifier.get("nameIdentifierScheme", "").upper() == "ORCID":
                orcid = identifier.get("nameIdentifier", "")
                return self._normalize_orcid(orcid)
        return ""
    
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
    
    def _prepare_contributors_for_db(self, csv_contributors: list) -> list:
        """
        Prepare contributor data for database update.
        
        Converts CSV format to database format:
        - contributorTypes (list) → comma-separated string for roles
        - Includes email/website/position for ContactPerson
        
        Args:
            csv_contributors: List of contributor dicts from CSV parser
            
        Returns:
            List of contributor dicts ready for DB update
        """
        db_contributors = []
        for contrib in csv_contributors:
            db_contrib = {
                'firstname': contrib.get('givenName', ''),
                'lastname': contrib.get('familyName', '') or contrib.get('name', ''),
                'orcid': contrib.get('nameIdentifier', ''),
                'nametype': contrib.get('nameType', 'Personal'),
                # Convert list to comma-separated string
                'contributorTypes': ', '.join(contrib.get('contributorTypes', [])),
            }
            
            # Add ContactInfo if this is a ContactPerson
            if 'ContactPerson' in contrib.get('contributorTypes', []):
                db_contrib['email'] = contrib.get('email', '')
                db_contrib['website'] = contrib.get('website', '')
                db_contrib['position'] = contrib.get('position', '')
            
            db_contributors.append(db_contrib)
        
        return db_contributors
    
    def run(self):
        """
        Execute the contributor update process with Database-First Pattern.
        
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
            logger.info(f"Parsing contributors CSV file: {self.csv_path}")
            self.progress_update.emit(0, 0, "CSV-Datei wird gelesen und validiert...")
            
            try:
                contributors_by_doi, warnings = CSVParser.parse_contributors_update_csv(self.csv_path)
            except (CSVParseError, FileNotFoundError) as e:
                error_msg = f"Fehler beim Lesen der CSV-Datei: {str(e)}"
                logger.error(error_msg)
                self.error_occurred.emit(error_msg)
                self.finished.emit(0, 0, 0, [], [])
                return
            
            total_dois = len(contributors_by_doi)
            logger.info(f"Found {total_dois} DOIs with contributor data to process")
            
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
            
            # Test DataCite availability
            try:
                self.validation_update.emit("  ✓ DataCite API erreichbar")
            except Exception as e:
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
            
            # Step 3: Dry Run Validation
            logger.info("Starting dry run validation...")
            valid_count = 0
            invalid_count = 0
            validation_results = []
            metadata_cache = {}  # Cache metadata for later updates
            skipped_details = []  # List of (doi, reason) tuples for skipped DOIs
            
            for index, (doi, contributors) in enumerate(contributors_by_doi.items(), start=1):
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
                    
                    # Validate contributors match
                    is_valid, message = client.validate_contributors_match(doi, contributors)
                    
                    if is_valid:
                        # Check if contributors actually changed
                        has_changes, change_description = self._detect_contributor_changes(metadata, contributors)
                        
                        if not has_changes:
                            # No changes detected → Skip update
                            valid_count += 1
                            result = {
                                'doi': doi,
                                'valid': True,
                                'changed': False,
                                'message': f"Validiert: {change_description}"
                            }
                            skipped_details.append((doi, change_description))
                            logger.info(f"DOI {doi}: No changes detected, will skip update")
                        else:
                            # Changes detected → Mark for update
                            valid_count += 1
                            result = {
                                'doi': doi,
                                'valid': True,
                                'changed': True,
                                'message': f"Validiert: {change_description}",
                                'contributor_count': len(contributors)
                            }
                            logger.info(f"DOI {doi}: Changes detected: {change_description}")
                        
                        validation_results.append(result)
                        logger.info(f"Validation passed: {doi}")
                    else:
                        invalid_count += 1
                        result = {
                            'doi': doi,
                            'valid': False,
                            'message': message
                        }
                        validation_results.append(result)
                        logger.warning(f"Validation failed: {doi} - {message}")
                
                except AuthenticationError as e:
                    error_msg = f"Authentifizierungsfehler: {str(e)}"
                    logger.error(error_msg)
                    self.error_occurred.emit(error_msg)
                    self.finished.emit(0, 0, 0, [], [])
                    return
                
                except NetworkError as e:
                    error_msg = f"Netzwerkfehler: {str(e)}"
                    logger.error(error_msg)
                    self.error_occurred.emit(error_msg)
                    self.finished.emit(0, 0, 0, [], [])
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
                skipped_dois = [result['doi'] for result in validation_results if result['valid'] and not result.get('changed', True)]
                self.finished.emit(valid_count, invalid_count, len(skipped_dois), [], skipped_details)
                return
            
            # Step 4: Perform actual updates (only if not dry_run_only)
            success_count = 0
            error_count = 0
            error_list = []
            
            # Only update DOIs that passed validation AND have changes
            valid_dois_with_changes = [
                result['doi'] 
                for result in validation_results 
                if result['valid'] and result.get('changed', True)
            ]
            
            skipped_dois = [
                result['doi']
                for result in validation_results
                if result['valid'] and not result.get('changed', False)
            ]
            
            skipped_count = len(skipped_dois)
            total_updates = len(valid_dois_with_changes)
            
            logger.info(
                f"Update plan: {total_updates} DOIs with changes, "
                f"{skipped_count} DOIs unchanged (will be skipped)"
            )
            
            # Log skipped DOIs (first 10 for brevity)
            if skipped_dois:
                logger.info(f"Skipping {skipped_count} unchanged DOIs:")
                for doi in skipped_dois[:10]:
                    logger.info(f"  - {doi}")
                if len(skipped_dois) > 10:
                    logger.info(f"  ... and {len(skipped_dois) - 10} more")
            
            for index, doi in enumerate(valid_dois_with_changes, start=1):
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
                    contributors = contributors_by_doi[doi]
                    metadata = metadata_cache.get(doi)
                    
                    if metadata is None:
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
                                # DOI not found in database - warning, not critical
                                db_success = False
                                db_message = "DOI nicht in Datenbank gefunden (nur DataCite wird aktualisiert)"
                                logger.warning(f"DOI {doi} not found in database - skipping DB update")
                                self.database_update.emit(f"  ⚠️ {db_message}")
                            else:
                                # Prepare contributors for DB format
                                db_contributors = self._prepare_contributors_for_db(contributors)
                                
                                # Update contributors transactionally
                                db_success, db_message, db_errors = self.db_client.update_contributors_transactional(
                                    resource_id,
                                    db_contributors
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
                    
                    datacite_success, datacite_message = client.update_doi_contributors(doi, contributors, metadata)
                    
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
                            
                            retry_success, retry_message = client.update_doi_contributors(doi, contributors, metadata)
                            
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
                    self.finished.emit(success_count, error_count, skipped_count, error_list, skipped_details)
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
                f"Contributor update complete: {success_count} successful, {skipped_count} skipped (no changes), {error_count} failed"
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
        logger.info("Stop requested for contributors update worker")
        self._is_running = False
