"""Worker thread for exporting pending DOIs from SUMARIOPMD database."""

import logging
from PySide6.QtCore import QObject, Signal

from src.db.sumariopmd_client import SumarioPMDClient, DatabaseError, ConnectionError
from src.utils.csv_exporter import export_pending_dois, CSVExportError


logger = logging.getLogger(__name__)


class PendingExportWorker(QObject):
    """Worker for exporting pending DOIs from SUMARIOPMD database in a separate thread."""
    
    # Signals
    progress = Signal(str)  # Progress message
    progress_count = Signal(int, int)  # current, total for progress bar
    finished = Signal(str, int)  # file_path, count
    error = Signal(str)  # Error message
    
    def __init__(
        self,
        db_host: str,
        db_name: str,
        db_user: str,
        db_password: str,
        output_path: str
    ):
        """
        Initialize the pending export worker.
        
        Args:
            db_host: Database host
            db_name: Database name
            db_user: Database username
            db_password: Database password
            output_path: Path to save the CSV file
        """
        super().__init__()
        self.db_host = db_host
        self.db_name = db_name
        self.db_user = db_user
        self.db_password = db_password
        self.output_path = output_path
        self._is_running = False
    
    def run(self):
        """
        Execute the pending DOIs export process.
        
        This method will:
        1. Connect to SUMARIOPMD database
        2. Fetch all pending DOIs with title and first author
        3. Export to CSV file with UTF-8 BOM
        4. Emit progress and result signals
        """
        self._is_running = True
        
        try:
            # Step 1: Connect to database
            self.progress.emit("Verbindung zur Datenbank wird hergestellt...")
            self.progress_count.emit(0, 100)
            
            try:
                client = SumarioPMDClient(
                    host=self.db_host,
                    database=self.db_name,
                    username=self.db_user,
                    password=self.db_password
                )
            except ConnectionError as e:
                error_msg = f"Datenbankverbindung fehlgeschlagen: {str(e)}"
                logger.error(error_msg)
                self.error.emit(error_msg)
                return
            
            # Step 2: Fetch pending DOIs
            self.progress.emit("Pending DOIs werden aus der Datenbank abgerufen...")
            self.progress_count.emit(25, 100)
            
            try:
                pending_data = client.fetch_pending_dois()
            except DatabaseError as e:
                error_msg = f"Fehler beim Abrufen der Daten: {str(e)}"
                logger.error(error_msg)
                self.error.emit(error_msg)
                return
            
            if not pending_data:
                self.progress.emit("[WARNUNG] Keine pending DOIs gefunden.")
                self.finished.emit("", 0)
                return
            
            self.progress_count.emit(50, 100)
            self.progress.emit(f"{len(pending_data)} pending DOIs gefunden...")
            
            # Step 3: Export to CSV
            self.progress.emit("CSV-Datei wird erstellt...")
            self.progress_count.emit(75, 100)
            
            try:
                export_pending_dois(pending_data, self.output_path)
            except CSVExportError as e:
                error_msg = f"Fehler beim Speichern der CSV-Datei: {str(e)}"
                logger.error(error_msg)
                self.error.emit(error_msg)
                return
            
            # Step 4: Complete
            self.progress_count.emit(100, 100)
            self.progress.emit(f"[OK] {len(pending_data)} pending DOIs erfolgreich exportiert")
            self.finished.emit(self.output_path, len(pending_data))
            
        except Exception as e:
            error_msg = f"Unerwarteter Fehler: {str(e)}"
            logger.error(error_msg, exc_info=True)
            self.error.emit(error_msg)
        
        finally:
            self._is_running = False
    
    def stop(self):
        """Request the worker to stop."""
        self._is_running = False
