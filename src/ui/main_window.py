"""Main application window for GROBI."""

import logging
import os
from datetime import datetime
from pathlib import Path

from PySide6.QtWidgets import (
    QMainWindow, QWidget, QVBoxLayout, QPushButton,
    QTextEdit, QProgressBar, QLabel, QMessageBox
)
from PySide6.QtCore import QThread, Signal, QObject
from PySide6.QtGui import QFont

from src.ui.credentials_dialog import CredentialsDialog
from src.ui.theme_manager import ThemeManager, Theme
from src.api.datacite_client import DataCiteClient, DataCiteAPIError, AuthenticationError, NetworkError
from src.utils.csv_exporter import export_dois_to_csv, CSVExportError
from src.workers.update_worker import UpdateWorker


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
        
        # Thread and worker for DOI fetch
        self.thread = None
        self.worker = None
        
        # Thread and worker for URL update
        self.update_thread = None
        self.update_worker = None
        
        # Initialize theme manager
        self.theme_manager = ThemeManager()
        self.theme_manager.theme_changed.connect(self._on_theme_changed)
        
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
        self.subtitle = QLabel("GFZ Research Data Repository Operations & Batch Interface")
        subtitle_font = QFont()
        subtitle_font.setPointSize(10)
        self.subtitle.setFont(subtitle_font)
        effective_theme = self.theme_manager.get_effective_theme()
        subtitle_color = "#999" if effective_theme == Theme.DARK else "#666"
        self.subtitle.setStyleSheet(f"color: {subtitle_color};")
        layout.addWidget(self.subtitle)
        
        # Add spacing
        layout.addSpacing(20)
        
        # Load DOIs button
        self.load_button = QPushButton("📥 DOIs und Landing Page URLs laden")
        self.load_button.setMinimumHeight(50)
        self.load_button.clicked.connect(self._on_load_dois_clicked)
        layout.addWidget(self.load_button)
        
        # Update URLs button
        self.update_button = QPushButton("🔄 Landing Page URLs aktualisieren")
        self.update_button.setMinimumHeight(50)
        self.update_button.clicked.connect(self._on_update_urls_clicked)
        layout.addWidget(self.update_button)
        
        # Theme toggle button
        self.theme_button = QPushButton(self._get_theme_button_text())
        self.theme_button.setMinimumHeight(40)
        self.theme_button.clicked.connect(self._on_theme_toggle)
        layout.addWidget(self.theme_button)
        
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
        self._log("Bereit. Klicke auf 'DOIs und Landing Page URLs laden' um zu beginnen.")
    
    def _apply_styles(self):
        """Apply styling to the window based on current theme."""
        stylesheet = self.theme_manager.get_main_window_stylesheet()
        self.setStyleSheet(stylesheet)
        
        # Update subtitle color based on effective theme
        effective_theme = self.theme_manager.get_effective_theme()
        subtitle_color = "#999" if effective_theme == Theme.DARK else "#666"
        self.subtitle.setStyleSheet(f"color: {subtitle_color};")
    
    def _apply_styles_old(self):
        """Legacy styling method - kept for reference."""
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
    
    def _get_theme_button_text(self) -> str:
        """
        Get appropriate button text based on current theme.
        
        Returns:
            str: Button text
        """
        current_theme = self.theme_manager.get_current_theme()
        if current_theme == Theme.AUTO:
            effective = self.theme_manager.get_effective_theme()
            return f"🔄 Auto ({'Light' if effective == Theme.LIGHT else 'Dark'})"
        elif current_theme == Theme.LIGHT:
            return "🌙 Dark Mode"
        else:
            return "☀️ Light Mode"
    
    def _on_theme_toggle(self):
        """Handle theme toggle button click."""
        self.theme_manager.toggle_theme()
    
    def _on_theme_changed(self, theme: Theme):
        """
        Handle theme change.
        
        Args:
            theme: New theme
        """
        # Update button text
        self.theme_button.setText(self._get_theme_button_text())
        
        # Log message
        if theme == Theme.AUTO:
            effective = self.theme_manager.get_effective_theme()
            self._log(f"🔄 Auto Mode aktiviert (System: {('Light' if effective == Theme.LIGHT else 'Dark')} Mode)")
        elif theme == Theme.DARK:
            self._log("🌙 Dark Mode aktiviert")
        else:
            self._log("☀️ Light Mode aktiviert")
        
        # Apply new styles
        self._apply_styles()
        
        logger.info(f"Theme changed to: {theme.value}")
    
    def _on_load_dois_clicked(self):
        """Handle load DOIs button click."""
        # Show credentials dialog
        dialog = CredentialsDialog(self)
        credentials = dialog.get_credentials()
        
        if credentials is None:
            self._log("Vorgang abgebrochen.")
            return
        
        username, password, csv_path, use_test_api = credentials
        # csv_path is None in export mode, we don't need it here
        
        api_type = "Test-API" if use_test_api else "Produktions-API"
        self._log(f"Starte Abruf für Benutzer '{username}' ({api_type})...")
        
        # Disable buttons and show progress
        self.load_button.setEnabled(False)
        self.update_button.setEnabled(False)
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
        self.update_button.setEnabled(True)
        
        # Reset references (objects are deleted via deleteLater)
        self.thread = None
        self.worker = None
        
        self._log("Bereit für nächsten Vorgang.")
    
    def _on_update_urls_clicked(self):
        """Handle update URLs button click."""
        # Show credentials dialog in update mode
        dialog = CredentialsDialog(self, mode="update")
        credentials = dialog.get_credentials()
        
        if credentials is None:
            self._log("URL-Update abgebrochen.")
            return
        
        username, password, csv_path, use_test_api = credentials
        
        api_type = "Test-API" if use_test_api else "Produktions-API"
        self._log(f"Starte Landing Page URL Update für Benutzer '{username}' ({api_type})...")
        self._log(f"CSV-Datei: {Path(csv_path).name}")
        
        # Disable buttons and show progress
        self.load_button.setEnabled(False)
        self.update_button.setEnabled(False)
        self.progress_bar.setVisible(True)
        
        # Create worker and thread
        self.update_worker = UpdateWorker(username, password, csv_path, use_test_api)
        self.update_thread = QThread()
        self.update_worker.moveToThread(self.update_thread)
        
        # Connect signals
        self.update_thread.started.connect(self.update_worker.run)
        self.update_worker.progress_update.connect(self._on_update_progress)
        self.update_worker.doi_updated.connect(self._on_doi_updated)
        self.update_worker.finished.connect(self._on_update_finished)
        self.update_worker.error_occurred.connect(self._on_update_error)
        
        # Clean up after worker finishes (finished is always emitted, even on error)
        self.update_worker.finished.connect(self.update_worker.deleteLater)
        self.update_worker.finished.connect(self.update_thread.quit)
        
        # Clean up thread when it finishes
        self.update_thread.finished.connect(self.update_thread.deleteLater)
        self.update_thread.finished.connect(self._cleanup_update_thread)
        
        # Start the thread
        self.update_thread.start()
    
    def _on_update_progress(self, current, total, message):
        """
        Handle update progress signal.
        
        Args:
            current: Current DOI number
            total: Total number of DOIs
            message: Progress message
        """
        self._log(message)
        
        # Update progress bar if we have valid numbers
        if total > 0:
            self.progress_bar.setMaximum(total)
            self.progress_bar.setValue(current)
    
    def _on_doi_updated(self, doi, success, message):
        """
        Handle individual DOI update result.
        
        Args:
            doi: DOI that was updated
            success: Whether update was successful
            message: Result message
        """
        # Only log errors to avoid cluttering the log
        if not success:
            self._log(f"[FEHLER] {doi}: {message}")
    
    def _on_update_finished(self, success_count, error_count, error_list):
        """
        Handle update completion.
        
        Args:
            success_count: Number of successful updates
            error_count: Number of failed updates
            error_list: List of error messages
        """
        total = success_count + error_count
        
        self._log("=" * 60)
        if total > 0:
            self._log(f"Update abgeschlossen: {success_count}/{total} erfolgreich")
        else:
            self._log("Update abgeschlossen: Keine DOIs verarbeitet")
        self._log("=" * 60)
        
        # Show summary dialog
        if total == 0:
            QMessageBox.warning(
                self,
                "Keine DOIs verarbeitet",
                "Die CSV-Datei enthielt keine gültigen DOIs zum Verarbeiten."
            )
        elif error_count == 0:
            QMessageBox.information(
                self,
                "Update erfolgreich",
                f"Alle {success_count} DOIs wurden erfolgreich aktualisiert!"
            )
        else:
            error_details = "\n".join(error_list[:10])  # Show first 10 errors
            if len(error_list) > 10:
                error_details += f"\n... und {len(error_list) - 10} weitere Fehler"
            
            QMessageBox.warning(
                self,
                "Update abgeschlossen mit Fehlern",
                f"Erfolgreich: {success_count}\n"
                f"Fehlgeschlagen: {error_count}\n\n"
                f"Erste Fehler:\n{error_details}\n\n"
                f"Siehe Log-Datei für Details."
            )
        
        # Create log file
        self._create_update_log(success_count, error_count, error_list)
    
    def _on_update_error(self, error_message):
        """
        Handle critical update error.
        
        Args:
            error_message: Error message
        """
        self._log(f"[KRITISCHER FEHLER] {error_message}")
        
        QMessageBox.critical(
            self,
            "Update Fehler",
            f"Ein kritischer Fehler ist aufgetreten:\n\n{error_message}"
        )
    
    def _cleanup_update_thread(self):
        """Clean up update thread and worker after completion."""
        self.progress_bar.setVisible(False)
        self.progress_bar.setMaximum(0)  # Reset to indeterminate
        self.load_button.setEnabled(True)
        self.update_button.setEnabled(True)
        
        # Reset references (objects are deleted via deleteLater)
        self.update_thread = None
        self.update_worker = None
        
        self._log("Bereit für nächsten Vorgang.")
    
    def _create_update_log(self, success_count, error_count, error_list):
        """
        Create a log file with update results.
        
        Args:
            success_count: Number of successful updates
            error_count: Number of failed updates
            error_list: List of error messages
        """
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            log_filename = f"update_log_{timestamp}.txt"
            log_path = Path(os.getcwd()) / log_filename
            
            with open(log_path, 'w', encoding='utf-8') as f:
                f.write("=" * 70 + "\n")
                f.write("GROBI - Landing Page URL Update Log\n")
                f.write("=" * 70 + "\n")
                f.write(f"Datum: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write("\n")
                f.write("ZUSAMMENFASSUNG:\n")
                f.write(f"  Gesamt: {success_count + error_count} DOIs\n")
                f.write(f"  Erfolgreich: {success_count}\n")
                f.write(f"  Fehlgeschlagen: {error_count}\n")
                f.write("\n")
                
                if error_list:
                    f.write("=" * 70 + "\n")
                    f.write("FEHLER:\n")
                    f.write("=" * 70 + "\n")
                    for error in error_list:
                        f.write(f"  - {error}\n")
                else:
                    f.write("Keine Fehler aufgetreten.\n")
                
                f.write("\n")
                f.write("=" * 70 + "\n")
            
            self._log(f"[OK] Log-Datei erstellt: {log_filename}")
            
        except Exception as e:
            self._log(f"[WARNUNG] Log-Datei konnte nicht erstellt werden: {str(e)}")
    
    def closeEvent(self, event):
        """
        Handle window close event.
        
        Args:
            event: Close event
        """
        # If fetch thread is running, wait for it to finish
        if self.thread is not None and self.thread.isRunning():
            self._log("Warte auf Abschluss der laufenden Aufgabe...")
            self.thread.quit()
            self.thread.wait(3000)  # Wait max 3 seconds
        
        # If update thread is running, stop worker and wait for it to finish
        if self.update_thread is not None and self.update_thread.isRunning():
            self._log("Warte auf Abschluss des URL-Updates...")
            if self.update_worker is not None:
                self.update_worker.stop()
            self.update_thread.quit()
            self.update_thread.wait(3000)  # Wait max 3 seconds
        
        event.accept()
