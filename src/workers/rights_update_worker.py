"""Worker thread for updating DOI rights metadata via DataCite API."""

import logging
from PySide6.QtCore import QObject, Signal

from src.api.datacite_client import DataCiteClient, NetworkError
from src.utils.csv_parser import CSVParser, CSVParseError


logger = logging.getLogger(__name__)


def _normalize_rights_entry(r: dict) -> tuple:
    """
    Create a hashable tuple from rights dict for comparison.
    
    Normalizes all fields to lowercase and strips whitespace for
    case-insensitive, order-independent comparison. This normalization is
    intentional for comparison purposes only - the actual API submission
    preserves the original case from the CSV file.
    
    Note: DataCite API may return fields with different casing than what was
    submitted (e.g., SPDX identifiers are often normalized to lowercase by
    DataCite). This function ensures that such differences don't cause
    false-positive change detection.
    
    Args:
        r: Rights dictionary with fields like 'rights', 'rightsUri', etc.
        
    Returns:
        Tuple of normalized field values for set-based comparison.
    """
    return (
        r.get("rights", "").strip().lower(),
        r.get("rightsUri", "").strip().lower(),
        r.get("schemeUri", "").strip().lower(),
        r.get("rightsIdentifier", "").strip().lower(),
        r.get("rightsIdentifierScheme", "").strip().lower(),
        r.get("lang", "").strip().lower()
    )


class RightsUpdateWorker(QObject):
    """Worker for updating DOI rights in a separate thread."""
    
    # Signals
    progress_update = Signal(int, int, str)  # current, total, message
    doi_updated = Signal(str, bool, str)  # doi, success, message
    finished = Signal(int, int, int, list, list)  # success_count, skipped_count, error_count, error_list, skipped_details
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
        Initialize the rights update worker.
        
        Args:
            username: DataCite username
            password: DataCite password
            csv_path: Path to CSV file with rights data
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
        Execute the rights update process.
        
        This method will:
        1. Parse the CSV file
        2. Initialize DataCite client
        3. For each DOI, compare current rights with CSV and update if changed
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
                rights_by_doi, warnings = CSVParser.parse_rights_update_csv(self.csv_path)
                
                # Log warnings
                for warning in warnings:
                    logger.warning(warning)
                    
            except (CSVParseError, FileNotFoundError) as e:
                error_msg = f"Fehler beim Lesen der CSV-Datei: {str(e)}"
                logger.error(error_msg)
                self.error_occurred.emit(error_msg)
                self.finished.emit(0, 0, 0, [], [])
                return
            
            total_dois = len(rights_by_doi)
            logger.info(f"Found {total_dois} DOIs with rights data to process")
            
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
            
            # Step 3: Update each DOI
            for index, (doi, csv_rights) in enumerate(rights_by_doi.items(), start=1):
                if not self._is_running:
                    logger.info("Update process cancelled by user")
                    break
                
                # Emit progress
                self.progress_update.emit(
                    index, 
                    total_dois, 
                    f"Prüfe DOI {index}/{total_dois}: {doi}"
                )
                
                try:
                    # Fetch current metadata to check if rights actually changed
                    current_metadata = client.get_doi_metadata(doi)
                    
                    if current_metadata:
                        current_rights = current_metadata.get('data', {}).get('attributes', {}).get('rightsList', [])
                        
                        # Compare current rights with CSV rights
                        has_changes, change_description = self._detect_rights_changes(current_rights, csv_rights)
                        
                        if not has_changes:
                            # No change detected - skip update (count as skipped, not success)
                            skipped_count += 1
                            skipped_reason = "Rights unverändert"
                            skipped_details.append((doi, skipped_reason))
                            logger.info(f"DOI {doi}: Rights unchanged, skipping update")
                            self.doi_updated.emit(doi, True, "Keine Änderung (übersprungen)")
                            
                            # If credentials are new and this is first successful operation, offer to save
                            if self.credentials_are_new and not self._first_success:
                                self._first_success = True
                                api_type = "test" if self.use_test_api else "production"
                                self.request_save_credentials.emit(self.username, self.password, api_type)
                            
                            continue
                        else:
                            # Rights changed - log and proceed with update
                            logger.info(f"DOI {doi}: {change_description}")
                    else:
                        # Could not fetch metadata - proceed with update anyway
                        logger.warning(f"DOI {doi}: Could not fetch current metadata, proceeding with update")
                    
                    # Log warning if removing all rights
                    if not csv_rights:
                        logger.warning(f"DOI {doi}: Alle Rights werden entfernt (leere Rights-Liste)")
                        self.progress_update.emit(
                            index, 
                            total_dois, 
                            f"⚠️ DOI {doi}: Alle Rights werden entfernt"
                        )
                    
                    # Perform update
                    success, message = client.update_doi_rights(doi, csv_rights)
                    
                    if success:
                        success_count += 1
                        logger.info(f"Successfully updated rights: {doi}")
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
                    self.finished.emit(success_count, skipped_count, error_count, error_list, skipped_details)
                    return
                
                except Exception as e:
                    # Unexpected error - log and continue
                    error_count += 1
                    error_entry = f"{doi}: Unerwarteter Fehler - {str(e)}"
                    error_list.append(error_entry)
                    logger.error(f"Unexpected error updating {doi}: {e}")
                    self.doi_updated.emit(doi, False, str(e))
            
            # Step 4: Emit final results
            logger.info(
                f"Rights update complete: {success_count} successful, {skipped_count} skipped (no changes), {error_count} failed"
            )
            
            # Log first 5 skipped DOIs for reference
            if skipped_details:
                count = len(skipped_details)
                if count <= 5:
                    logger.info(f"Skipped DOIs ({count} total):")
                else:
                    logger.info(f"Skipped DOIs (first 5 of {count}):")
                for doi, reason in skipped_details[:5]:
                    logger.info(f"  - {doi}: {reason}")
                    
            self.finished.emit(success_count, skipped_count, error_count, error_list, skipped_details)
        
        finally:
            self._is_running = False
    
    def _detect_rights_changes(
        self, 
        current_rights: list, 
        csv_rights: list
    ) -> tuple:
        """
        Compare current DataCite rights with CSV data to detect changes.
        
        Uses order-independent comparison since DataCite API might return
        rights in a different order than they were uploaded.
        
        Args:
            current_rights: Rights list from DataCite API
            csv_rights: Rights list from CSV (in CSV order)
        
        Returns:
            Tuple[bool, str]: (has_changes, change_description)
            - (True, "Description of changes") if differences found
            - (False, "No changes") if identical
        """
        # Count mismatch → Always update
        if len(current_rights) != len(csv_rights):
            return True, f"Rights-Anzahl unterschiedlich (aktuell: {len(current_rights)}, CSV: {len(csv_rights)})"
        
        # Both empty → No changes
        if len(current_rights) == 0:
            return False, "Keine Rights vorhanden"
        
        # Create normalized sets for order-independent comparison
        current_normalized = set(_normalize_rights_entry(r) for r in current_rights)
        csv_normalized = set(_normalize_rights_entry(r) for r in csv_rights)
        
        # Compare sets
        if current_normalized == csv_normalized:
            return False, "Keine Änderungen in Rights-Metadaten"
        
        # Determine what changed for the description
        only_in_current = current_normalized - csv_normalized
        only_in_csv = csv_normalized - current_normalized
        
        changes = []
        if only_in_csv:
            changes.append(f"{len(only_in_csv)} Rights hinzugefügt/geändert")
        if only_in_current:
            changes.append(f"{len(only_in_current)} Rights entfernt/geändert")
        
        return True, "; ".join(changes) if changes else "Rights geändert"
    
    def stop(self):
        """Request the worker to stop processing."""
        logger.info("Stop requested for rights update worker")
        self._is_running = False
