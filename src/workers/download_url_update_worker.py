"""Worker for updating download URLs in database from CSV."""

import logging
from typing import Dict
from PySide6.QtCore import QObject, Signal

from src.db.sumariopmd_client import SumarioPMDClient, DatabaseError, ConnectionError as DBConnectionError
from src.utils.csv_parser import CSVParser, CSVParseError

logger = logging.getLogger(__name__)


class DownloadURLUpdateWorker(QObject):
    """Worker for updating download URLs in the database from a CSV file."""
    
    # Signals
    progress = Signal(int, int, str)  # current, total, message
    entry_updated = Signal(str, str, bool, str)  # doi, filename, success, message
    finished = Signal(int, int, int, list, list)  # success_count, error_count, skipped_count, error_list, skipped_details
    error = Signal(str)  # error_message
    
    def __init__(
        self, 
        csv_path: str,
        db_host: str, 
        db_name: str, 
        db_user: str, 
        db_password: str
    ):
        """
        Initialize worker with CSV path and database credentials.
        
        Args:
            csv_path: Path to CSV file with download URL data
            db_host: Database host
            db_name: Database name
            db_user: Database username
            db_password: Database password
        """
        super().__init__()
        self.csv_path = csv_path
        self.db_host = db_host
        self.db_name = db_name
        self.db_user = db_user
        self.db_password = db_password
        self._is_cancelled = False
    
    def cancel(self):
        """Request cancellation of the update process."""
        self._is_cancelled = True
        logger.info("Download URL update cancelled by user")
    
    def run(self):
        """Execute the download URL update process."""
        success_count = 0
        error_count = 0
        skipped_count = 0
        error_list = []  # List of error messages
        skipped_details = []  # List of (doi, filename, reason) tuples
        
        try:
            # Step 1: Parse CSV file
            self.progress.emit(0, 0, "CSV-Datei wird gelesen...")
            
            try:
                entries = CSVParser.parse_download_urls_csv(self.csv_path)
            except (CSVParseError, FileNotFoundError) as e:
                error_msg = f"Fehler beim Lesen der CSV-Datei: {str(e)}"
                logger.error(error_msg)
                self.error.emit(error_msg)
                return
            
            if not entries:
                self.error.emit("Keine gültigen Einträge in der CSV-Datei gefunden.")
                return
            
            total_entries = len(entries)
            self.progress.emit(0, total_entries, f"{total_entries} Einträge gefunden")
            
            # Step 2: Connect to database
            self.progress.emit(0, total_entries, "Verbindung zur Datenbank wird hergestellt...")
            
            try:
                db_client = SumarioPMDClient(
                    host=self.db_host,
                    database=self.db_name,
                    username=self.db_user,
                    password=self.db_password
                )
                
                success, message = db_client.test_connection()
                if not success:
                    self.error.emit(f"Datenbankverbindung fehlgeschlagen: {message}")
                    return
                    
            except DBConnectionError as e:
                self.error.emit(f"Datenbankverbindung fehlgeschlagen: {str(e)}")
                return
            
            # Step 3: Process each entry
            for idx, entry in enumerate(entries, start=1):
                if self._is_cancelled:
                    self.progress.emit(idx, total_entries, "Abgebrochen durch Benutzer")
                    break
                
                doi = entry['doi']
                filename = entry['filename']
                
                self.progress.emit(idx, total_entries, f"Verarbeite {doi} / {filename}")
                
                try:
                    result = self._process_entry(db_client, entry)
                    
                    if result == 'updated':
                        success_count += 1
                        self.entry_updated.emit(doi, filename, True, "Aktualisiert")
                    elif result == 'skipped':
                        skipped_count += 1
                        skipped_details.append((doi, filename, "Keine Änderungen"))
                        self.entry_updated.emit(doi, filename, True, "Übersprungen (keine Änderungen)")
                    elif result == 'not_found':
                        error_count += 1
                        error_msg = f"{doi} / {filename}: Eintrag nicht in Datenbank gefunden"
                        error_list.append(error_msg)
                        self.entry_updated.emit(doi, filename, False, "Nicht gefunden")
                        
                except DatabaseError as e:
                    error_count += 1
                    error_msg = f"{doi} / {filename}: Datenbankfehler - {str(e)}"
                    error_list.append(error_msg)
                    logger.error(error_msg)
                    self.entry_updated.emit(doi, filename, False, f"Fehler: {str(e)}")
                    
                except Exception as e:
                    error_count += 1
                    error_msg = f"{doi} / {filename}: Unerwarteter Fehler - {str(e)}"
                    error_list.append(error_msg)
                    logger.error(error_msg, exc_info=True)
                    self.entry_updated.emit(doi, filename, False, f"Fehler: {str(e)}")
            
            # Step 4: Emit results
            self.progress.emit(
                total_entries, 
                total_entries, 
                f"Fertig: {success_count} aktualisiert, {skipped_count} übersprungen, {error_count} Fehler"
            )
            
            self.finished.emit(success_count, error_count, skipped_count, error_list, skipped_details)
            
        except Exception as e:
            error_msg = f"Unerwarteter Fehler: {str(e)}"
            logger.error(error_msg, exc_info=True)
            self.error.emit(error_msg)
    
    def _process_entry(self, db_client: SumarioPMDClient, entry: Dict) -> str:
        """
        Process a single CSV entry.
        
        Args:
            db_client: Database client instance
            entry: Dictionary with entry data
            
        Returns:
            'updated' if entry was updated
            'skipped' if no changes detected
            'not_found' if entry not in database
            
        Raises:
            DatabaseError: If database operation fails
        """
        doi = entry['doi']
        filename = entry['filename']
        
        # Get current database entry
        current = db_client.get_file_by_doi_and_filename(doi, filename)
        
        if current is None:
            return 'not_found'
        
        # Check what has changed
        changes = {}
        
        # Compare URL
        csv_url = entry['download_url']
        db_url = current.get('url', '') or ''
        if csv_url != db_url:
            changes['url'] = csv_url
        
        # Compare Description
        csv_desc = entry['description']
        db_desc = current.get('description', '') or ''
        if csv_desc != db_desc:
            changes['description'] = csv_desc
        
        # Compare Format (filemimetype)
        csv_format = entry['format']
        db_format = current.get('filemimetype', '') or ''
        if csv_format != db_format:
            changes['filemimetype'] = csv_format
        
        # Compare Size
        csv_size = entry['size_bytes']
        db_size = current.get('size', 0) or 0
        if csv_size != db_size:
            changes['size'] = csv_size
        
        # If no changes, skip
        if not changes:
            logger.debug(f"No changes for {doi} / {filename}")
            return 'skipped'
        
        # Perform update
        logger.info(f"Updating {doi} / {filename}: {list(changes.keys())}")
        
        resource_id = current['resource_id']
        
        db_client.update_file_entry(
            resource_id=resource_id,
            filename=filename,
            url=changes.get('url'),
            description=changes.get('description'),
            filemimetype=changes.get('filemimetype'),
            size=changes.get('size')
        )
        
        return 'updated'
