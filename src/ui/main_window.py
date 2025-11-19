"""Main application window for GROBI."""

import logging
import os
from pathlib import Path

from PySide6.QtWidgets import (
    QMainWindow, QWidget, QVBoxLayout, QPushButton,
    QTextEdit, QProgressBar, QLabel, QMessageBox
)
from PySide6.QtCore import QThread, Signal, QObject
from PySide6.QtGui import QFont

from src.ui.credentials_dialog import CredentialsDialog
from src.api.datacite_client import DataCiteClient, DataCiteAPIError, AuthenticationError, NetworkError
from src.utils.csv_exporter import export_dois_to_csv, CSVExportError


logger = logging.getLogger(__name__)


class DOIFetchWorker(QObject):
    """Worker for fetching DOIs in a separate thread."""
    
    # Signals
    progress = Signal(str)  # Progress message
    finished = Signal(list, str)  # List of (DOI, URL) tuples and username
    error = Signal(str)  # Error message
    
    def __init__(self, username, password, use_test_api):
        """
        Initialize the worker.
        
        Args:
            username: DataCite username
            password: DataCite password
            use_test_api: Whether to use test API
        """
        super().__init__()
        self.username = username
        self.password = password
        self.use_test_api = use_test_api
    
    def run(self):
        """Fetch DOIs from DataCite API."""
        try:
            self.progress.emit("Verbindung zur DataCite API wird hergestellt...")
            
            client = DataCiteClient(
                self.username,
                self.password,
                self.use_test_api
            )
            
            self.progress.emit("DOIs werden abgerufen...")
            dois = client.fetch_all_dois()
            
            self.progress.emit(f"[OK] {len(dois)} DOIs erfolgreich abgerufen")
            self.finished.emit(dois, self.username)
            
        except AuthenticationError as e:
            self.error.emit(str(e))
        except NetworkError as e:
            self.error.emit(str(e))
        except DataCiteAPIError as e:
            self.error.emit(str(e))
        except Exception as e:
            self.error.emit(f"Unerwarteter Fehler: {str(e)}")


class MainWindow(QMainWindow):
    """Main application window."""
    
    def __init__(self):
        """Initialize the main window."""
        super().__init__()
        self.setWindowTitle("GROBI - GFZ Data Services Tool")
        self.setMinimumSize(800, 600)
        
        # Thread and worker
        self.thread = None
        self.worker = None
        
        self._setup_ui()
        self._apply_styles()
        
        logger.info("Main window initialized")
    
    def _setup_ui(self):
        """Set up the user interface."""
        # Central widget
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        
        layout = QVBoxLayout(central_widget)
        layout.setSpacing(20)
        layout.setContentsMargins(30, 30, 30, 30)
        
        # Title
        title = QLabel("GROBI")
        title_font = QFont()
        title_font.setPointSize(24)
        title_font.setBold(True)
        title.setFont(title_font)
        layout.addWidget(title)
        
        # Subtitle
        subtitle = QLabel("GFZ Research Data Repository Operations & Batch Interface")
        subtitle_font = QFont()
        subtitle_font.setPointSize(10)
        subtitle.setFont(subtitle_font)
        subtitle.setStyleSheet("color: #666;")
        layout.addWidget(subtitle)
        
        # Add spacing
        layout.addSpacing(20)
        
        # Load DOIs button
        self.load_button = QPushButton("📥 DOIs laden")
        self.load_button.setMinimumHeight(50)
        self.load_button.clicked.connect(self._on_load_dois_clicked)
        layout.addWidget(self.load_button)
        
        # Progress bar
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        self.progress_bar.setMinimum(0)
        self.progress_bar.setMaximum(0)  # Indeterminate progress
        layout.addWidget(self.progress_bar)
        
        # Log area
        log_label = QLabel("Status:")
        log_label_font = QFont()
        log_label_font.setBold(True)
        log_label.setFont(log_label_font)
        layout.addWidget(log_label)
        
        self.log_text = QTextEdit()
        self.log_text.setReadOnly(True)
        self.log_text.setMaximumHeight(200)
        layout.addWidget(self.log_text)
        
        # Add stretch to push everything to the top
        layout.addStretch()
        
        # Initial log message
        self._log("Bereit. Klicke auf 'DOIs laden' um zu beginnen.")
    
    def _apply_styles(self):
        """Apply modern styling to the window."""
        self.setStyleSheet("""
            QMainWindow {
                background-color: #f5f5f5;
            }
            QPushButton {
                background-color: #0078d4;
                color: white;
                border: none;
                border-radius: 4px;
                padding: 12px;
                font-size: 14px;
                font-weight: bold;
            }
            QPushButton:hover {
                background-color: #106ebe;
            }
            QPushButton:pressed {
                background-color: #005a9e;
            }
            QPushButton:disabled {
                background-color: #cccccc;
                color: #666666;
            }
            QTextEdit {
                background-color: white;
                border: 1px solid #ccc;
                border-radius: 4px;
                padding: 8px;
                font-family: 'Consolas', 'Courier New', monospace;
                font-size: 10pt;
            }
            QProgressBar {
                border: 1px solid #ccc;
                border-radius: 4px;
                text-align: center;
                height: 20px;
            }
            QProgressBar::chunk {
                background-color: #0078d4;
            }
        """)
    
    def _log(self, message):
        """
        Add a message to the log area.
        
        Args:
            message: Message to log
        """
        self.log_text.append(message)
        logger.info(message)
    
    def _on_load_dois_clicked(self):
        """Handle load DOIs button click."""
        # Show credentials dialog
        dialog = CredentialsDialog(self)
        credentials = dialog.get_credentials()
        
        if credentials is None:
            self._log("Vorgang abgebrochen.")
            return
        
        username, password, use_test_api = credentials
        
        api_type = "Test-API" if use_test_api else "Produktions-API"
        self._log(f"Starte Abruf für Benutzer '{username}' ({api_type})...")
        
        # Disable button and show progress
        self.load_button.setEnabled(False)
        self.progress_bar.setVisible(True)
        
        # Create worker and thread
        self.worker = DOIFetchWorker(username, password, use_test_api)
        self.thread = QThread()
        self.worker.moveToThread(self.thread)
        
        # Connect signals
        self.thread.started.connect(self.worker.run)
        self.worker.progress.connect(self._log)
        self.worker.finished.connect(self._on_fetch_finished)
        self.worker.error.connect(self._on_fetch_error)
        
        # Clean up after worker finishes or errors
        self.worker.finished.connect(self.worker.deleteLater)
        self.worker.error.connect(self.worker.deleteLater)
        self.worker.finished.connect(self.thread.quit)
        self.worker.error.connect(self.thread.quit)
        
        # Clean up thread when it finishes
        self.thread.finished.connect(self.thread.deleteLater)
        self.thread.finished.connect(self._cleanup_thread)
        
        # Start the thread
        self.thread.start()
    
    def _on_fetch_finished(self, dois, username):
        """
        Handle successful DOI fetch.
        
        Args:
            dois: List of (DOI, URL) tuples
            username: DataCite username
        """
        if not dois:
            self._log("[WARNUNG] Keine DOIs gefunden.")
            QMessageBox.information(
                self,
                "Keine DOIs",
                f"Für den Benutzer '{username}' wurden keine DOIs gefunden."
            )
            return
        
        # Export to CSV
        try:
            output_dir = os.getcwd()
            filepath = export_dois_to_csv(dois, username, output_dir)
            
            self._log(f"[OK] CSV-Datei erfolgreich erstellt: {filepath}")
            
            QMessageBox.information(
                self,
                "Erfolg",
                f"{len(dois)} DOIs wurden erfolgreich exportiert.\n\n"
                f"Datei: {Path(filepath).name}\n"
                f"Verzeichnis: {output_dir}"
            )
            
        except CSVExportError as e:
            self._log(f"[FEHLER] Fehler beim CSV-Export: {str(e)}")
            QMessageBox.critical(
                self,
                "CSV-Export Fehler",
                f"Die CSV-Datei konnte nicht erstellt werden:\n\n{str(e)}"
            )
    
    def _on_fetch_error(self, error_message):
        """
        Handle fetch error.
        
        Args:
            error_message: Error message
        """
        self._log(f"[FEHLER] {error_message}")
        
        QMessageBox.critical(
            self,
            "Fehler",
            f"Beim Abrufen der DOIs ist ein Fehler aufgetreten:\n\n{error_message}"
        )
    
    def _cleanup_thread(self):
        """Clean up thread and worker after completion."""
        self.progress_bar.setVisible(False)
        self.load_button.setEnabled(True)
        
        # Reset references (objects are deleted via deleteLater)
        self.thread = None
        self.worker = None
        
        self._log("Bereit für nächsten Vorgang.")
    
    def closeEvent(self, event):
        """
        Handle window close event.
        
        Args:
            event: Close event
        """
        # If thread is running, wait for it to finish
        if self.thread is not None and self.thread.isRunning():
            self._log("Warte auf Abschluss der laufenden Aufgabe...")
            self.thread.quit()
            self.thread.wait(3000)  # Wait max 3 seconds
        
        event.accept()
