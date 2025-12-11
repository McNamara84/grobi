"""Worker for fetching DOIs with download URLs from database."""

import logging
from typing import List, Tuple
from PySide6.QtCore import QObject, Signal

from src.db.sumariopmd_client import SumarioPMDClient, DatabaseError, ConnectionError as DBConnectionError

logger = logging.getLogger(__name__)


class DownloadURLFetchWorker(QObject):
    """Worker for fetching DOIs with download URLs in a separate thread."""
    
    # Signals
    progress = Signal(str)  # Progress messages
    finished = Signal(list)  # List of (DOI, Filename, URL, Format, Size) tuples
    error = Signal(str)     # Error message
    
    def __init__(self, db_host: str, db_name: str, db_user: str, db_password: str):
        """
        Initialize worker with database credentials.
        
        Args:
            db_host: Database host
            db_name: Database name
            db_user: Database username
            db_password: Database password
        """
        super().__init__()
        self.db_host = db_host
        self.db_name = db_name
        self.db_user = db_user
        self.db_password = db_password
    
    def run(self):
        """Fetch DOIs with download URLs from database."""
        try:
            self.progress.emit("Verbindung zur Datenbank wird hergestellt...")
            
            # Initialize database client
            db_client = SumarioPMDClient(
                host=self.db_host,
                database=self.db_name,
                username=self.db_user,
                password=self.db_password
            )
            
            # Test connection
            success, message = db_client.test_connection()
            if not success:
                self.error.emit(f"Datenbankverbindung fehlgeschlagen: {message}")
                return
            
            self.progress.emit("DOIs und Download-URLs werden abgerufen...")
            
            # Fetch all DOIs with download URLs
            dois_files = db_client.fetch_all_dois_with_downloads()
            
            if not dois_files:
                self.error.emit("Keine DOIs mit Download-URLs gefunden.")
                return
            
            # Count unique DOIs
            unique_dois = len(set(doi for doi, _, _, _, _ in dois_files))
            self.progress.emit(
                f"[OK] {len(dois_files)} Dateien für {unique_dois} DOIs gefunden"
            )
            
            self.finished.emit(dois_files)
            
        except DBConnectionError as e:
            error_msg = f"Datenbankverbindung fehlgeschlagen: {str(e)}"
            logger.error(error_msg)
            self.error.emit(error_msg)
            
        except DatabaseError as e:
            error_msg = f"Datenbankfehler: {str(e)}"
            logger.error(error_msg)
            self.error.emit(error_msg)
            
        except Exception as e:
            error_msg = f"Unerwarteter Fehler: {str(e)}"
            logger.error(error_msg, exc_info=True)
            self.error.emit(error_msg)
