"""Main application window for GROBI."""

import logging
import os
from datetime import datetime
from pathlib import Path

from PySide6.QtWidgets import (
    QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, QPushButton,
    QTextEdit, QProgressBar, QLabel, QMessageBox, QGroupBox
)
from PySide6.QtCore import QThread, Signal, QObject, QUrl, Qt
from PySide6.QtGui import QFont, QIcon, QAction, QActionGroup, QDesktopServices, QPixmap

from src.ui.credentials_dialog import CredentialsDialog
from src.ui.save_credentials_dialog import SaveCredentialsDialog
from src.ui.about_dialog import AboutDialog
from src.ui.theme_manager import ThemeManager, Theme
from src.api.datacite_client import DataCiteClient, DataCiteAPIError, AuthenticationError, NetworkError
from src.utils.csv_exporter import export_dois_to_csv, export_dois_with_creators_to_csv, CSVExportError
from src.workers.update_worker import UpdateWorker
from src.workers.authors_update_worker import AuthorsUpdateWorker


logger = logging.getLogger(__name__)


class DOIFetchWorker(QObject):
    """Worker for fetching DOIs in a separate thread."""
    
    # Signals
    progress = Signal(str)  # Progress message
    finished = Signal(list, str)  # List of (DOI, URL) tuples and username
    error = Signal(str)  # Error message
    request_save_credentials = Signal(str, str, str)  # username, password, api_type
    
    def __init__(self, username, password, use_test_api, credentials_are_new=False):
        """
        Initialize the worker.
        
        Args:
            username: DataCite username
            password: DataCite password
            use_test_api: Whether to use test API
            credentials_are_new: Whether these are newly entered credentials (not from saved account)
        """
        super().__init__()
        self.username = username
        self.password = password
        self.use_test_api = use_test_api
        self.credentials_are_new = credentials_are_new
    
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
            
            # If credentials are new and API call was successful, offer to save them
            if self.credentials_are_new and dois:
                api_type = "test" if self.use_test_api else "production"
                self.request_save_credentials.emit(self.username, self.password, api_type)
            
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


class DOICreatorFetchWorker(QObject):
    """Worker for fetching DOIs with creator information in a separate thread."""
    
    # Signals
    progress = Signal(str)  # Progress message
    finished = Signal(list, str)  # List of creator tuples and username
    error = Signal(str)  # Error message
    request_save_credentials = Signal(str, str, str)  # username, password, api_type
    
    def __init__(self, username, password, use_test_api, credentials_are_new=False):
        """
        Initialize the worker.
        
        Args:
            username: DataCite username
            password: DataCite password
            use_test_api: Whether to use test API
            credentials_are_new: Whether these are newly entered credentials (not from saved account)
        """
        super().__init__()
        self.username = username
        self.password = password
        self.use_test_api = use_test_api
        self.credentials_are_new = credentials_are_new
    
    def run(self):
        """Fetch DOIs with creator information from DataCite API."""
        try:
            self.progress.emit("Verbindung zur DataCite API wird hergestellt...")
            
            client = DataCiteClient(
                self.username,
                self.password,
                self.use_test_api
            )
            
            self.progress.emit("DOIs und Autoren werden abgerufen...")
            creator_data = client.fetch_all_dois_with_creators()
            
            # If credentials are new and API call was successful, offer to save them
            if self.credentials_are_new and creator_data:
                api_type = "test" if self.use_test_api else "production"
                self.request_save_credentials.emit(self.username, self.password, api_type)
            
            # Count unique DOIs for better user feedback
            unique_dois = len(set(row[0] for row in creator_data))
            self.progress.emit(f"[OK] {unique_dois} DOIs mit {len(creator_data)} Autoren erfolgreich abgerufen")
            self.finished.emit(creator_data, self.username)
            
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
        
        # Set window icon
        icon_path = Path(__file__).parent / "GROBI-Logo.ico"
        if icon_path.exists():
            self.setWindowIcon(QIcon(str(icon_path)))
        
        # Thread and worker for DOI fetch
        self.thread = None
        self.worker = None
        
        # Thread and worker for DOI creator fetch
        self.creator_thread = None
        self.creator_worker = None
        
        # Thread and worker for URL update
        self.update_thread = None
        self.update_worker = None
        
        # Thread and worker for authors update
        self.authors_update_thread = None
        self.authors_update_worker = None
        
        # Track current username for CSV detection
        self._current_username = None
        
        # Initialize theme manager
        self.theme_manager = ThemeManager()
        self.theme_manager.theme_changed.connect(self._on_theme_changed)
        
        self._setup_menubar()
        self._setup_ui()
        self._apply_styles()
        
        logger.info("Main window initialized")
    
    def _setup_menubar(self):
        """Set up menu bar."""
        menubar = self.menuBar()
        
        # Einstellungen-Menü
        settings_menu = menubar.addMenu("Einstellungen")
        
        settings_action = QAction("Einstellungen...", self)
        settings_action.setShortcut("Ctrl+,")
        settings_action.triggered.connect(self._open_settings_dialog)
        settings_menu.addAction(settings_action)
        
        # Hilfe-Menü
        help_menu = menubar.addMenu("Hilfe")
        
        about_action = QAction("Über GROBI...", self)
        about_action.triggered.connect(self._show_about_dialog)
        help_menu.addAction(about_action)
        
        changelog_action = QAction("Changelog anzeigen", self)
        changelog_action.triggered.connect(self._show_changelog)
        help_menu.addAction(changelog_action)
        
        help_menu.addSeparator()
        
        github_action = QAction("GitHub-Repository öffnen", self)
        github_action.triggered.connect(self._open_github)
        help_menu.addAction(github_action)
        
        logger.info("Menu bar initialized")
    
    def _setup_ui(self):
        """Set up the user interface."""
        # Central widget
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        
        layout = QVBoxLayout(central_widget)
        layout.setSpacing(20)
        layout.setContentsMargins(30, 30, 30, 30)
        
        # Header with logo and title
        header_layout = QHBoxLayout()
        header_layout.setSpacing(10)
        
        # Logo
        logo_label = QLabel()
        logo_path = Path(__file__).parent / "GROBI-Logo.ico"
        if logo_path.exists():
            pixmap = QPixmap(str(logo_path))
            # Scale to 32x32 for compact display next to title
            pixmap = pixmap.scaled(32, 32, Qt.KeepAspectRatio, Qt.SmoothTransformation)
            logo_label.setPixmap(pixmap)
            logo_label.setFixedSize(32, 32)
        header_layout.addWidget(logo_label)
        
        # Title and subtitle container
        title_container = QVBoxLayout()
        title_container.setSpacing(0)
        
        # Title
        title = QLabel("GROBI")
        title_font = QFont()
        title_font.setPointSize(24)
        title_font.setBold(True)
        title.setFont(title_font)
        title_container.addWidget(title)
        
        # Subtitle
        self.subtitle = QLabel("GFZ Research Data Repository Operations & Batch Interface")
        subtitle_font = QFont()
        subtitle_font.setPointSize(10)
        self.subtitle.setFont(subtitle_font)
        effective_theme = self.theme_manager.get_effective_theme()
        subtitle_color = "#999" if effective_theme == Theme.DARK else "#666"
        self.subtitle.setStyleSheet(f"color: {subtitle_color};")
        title_container.addWidget(self.subtitle)
        
        header_layout.addLayout(title_container)
        header_layout.addStretch()  # Push everything to the left
        
        layout.addLayout(header_layout)
        
        # Add spacing
        layout.addSpacing(20)
        
        # GroupBox 1: Landing Page URLs
        urls_group = QGroupBox("🔗 Landing Page URLs")
        urls_layout = QVBoxLayout()
        urls_layout.setSpacing(10)
        
        # Status label for URLs
        self.urls_status_label = QLabel("⚪ Keine CSV-Datei gefunden")
        urls_layout.addWidget(self.urls_status_label)
        
        # Buttons for URLs workflow
        self.load_button = QPushButton("📥 DOIs und URLs exportieren")
        self.load_button.setMinimumHeight(40)
        self.load_button.clicked.connect(self._on_load_dois_clicked)
        urls_layout.addWidget(self.load_button)
        
        self.update_button = QPushButton("🔄 Landing Page URLs aktualisieren")
        self.update_button.setMinimumHeight(40)
        self.update_button.setEnabled(False)  # Initially disabled
        self.update_button.clicked.connect(self._on_update_urls_clicked)
        urls_layout.addWidget(self.update_button)
        
        urls_group.setLayout(urls_layout)
        layout.addWidget(urls_group)
        
        # GroupBox 2: Authors Metadata
        authors_group = QGroupBox("👥 Autoren-Metadaten")
        authors_layout = QVBoxLayout()
        authors_layout.setSpacing(10)
        
        # Status label for authors
        self.authors_status_label = QLabel("⚪ Keine CSV-Datei gefunden")
        authors_layout.addWidget(self.authors_status_label)
        
        # Buttons for authors workflow
        self.load_authors_button = QPushButton("📥 DOIs und Autoren exportieren")
        self.load_authors_button.setMinimumHeight(40)
        self.load_authors_button.clicked.connect(self._on_load_authors_clicked)
        authors_layout.addWidget(self.load_authors_button)
        
        self.update_authors_button = QPushButton("🖊️ Autoren aktualisieren")
        self.update_authors_button.setMinimumHeight(40)
        self.update_authors_button.setEnabled(False)  # Initially disabled
        self.update_authors_button.clicked.connect(self._on_update_authors_clicked)
        authors_layout.addWidget(self.update_authors_button)
        
        authors_group.setLayout(authors_layout)
        layout.addWidget(authors_group)
        
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
        
        # Check for existing CSV files
        self._check_csv_files()
        
        # Initial log message
        self._log("Bereit. Klicke auf 'DOIs und URLs exportieren' um zu beginnen.")
    
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
    
    def _check_csv_files(self):
        """Check if CSV files exist and update UI accordingly."""
        output_dir = Path(os.getcwd())
        
        # Check for URLs CSV
        urls_csv_found = False
        urls_csv_name = None
        
        # Check for authors CSV
        authors_csv_found = False
        authors_csv_name = None
        
        # If we have a username, check for specific files
        if self._current_username:
            urls_csv_path = output_dir / f"{self._current_username}_urls.csv"
            authors_csv_path = output_dir / f"{self._current_username}_authors.csv"
            
            if urls_csv_path.exists():
                urls_csv_found = True
                urls_csv_name = urls_csv_path.name
            
            if authors_csv_path.exists():
                authors_csv_found = True
                authors_csv_name = authors_csv_path.name
        else:
            # Check for any *_urls.csv and *_authors.csv files
            urls_files = list(output_dir.glob("*_urls.csv"))
            authors_files = list(output_dir.glob("*_authors.csv"))
            
            if urls_files:
                urls_csv_found = True
                urls_csv_name = urls_files[0].name
            
            if authors_files:
                authors_csv_found = True
                authors_csv_name = authors_files[0].name
        
        # Update URLs status
        if urls_csv_found:
            self.urls_status_label.setText(f"🟢 CSV bereit: {urls_csv_name}")
            self.update_button.setEnabled(True)
        else:
            self.urls_status_label.setText("⚪ Keine CSV-Datei gefunden")
            self.update_button.setEnabled(False)
        
        # Update authors status
        if authors_csv_found:
            self.authors_status_label.setText(f"🟢 CSV bereit: {authors_csv_name}")
            self.update_authors_button.setEnabled(True)
        else:
            self.authors_status_label.setText("⚪ Keine CSV-Datei gefunden")
            self.update_authors_button.setEnabled(False)
    
    def _log(self, message):
        """
        Add a message to the log area.
        
        Args:
            message: Message to log
        """
        self.log_text.append(message)
        logger.info(message)
    
    def _open_settings_dialog(self):
        """Open settings dialog."""
        try:
            from src.ui.settings_dialog import SettingsDialog
            
            dialog = SettingsDialog(self.theme_manager, self)
            
            # Connect theme change signal
            dialog.theme_changed.connect(self._on_settings_theme_changed)
            
            dialog.exec()
            logger.info("Settings dialog closed")
        except Exception as e:
            logger.error(f"Error opening settings dialog: {e}")
            QMessageBox.warning(
                self,
                "Fehler",
                f"Der Einstellungen-Dialog konnte nicht geöffnet werden:\n\n{str(e)}"
            )
    
    def _show_about_dialog(self):
        """Show About dialog."""
        try:
            dialog = AboutDialog(self)
            dialog.exec()
            logger.info("About dialog shown")
        except Exception as e:
            logger.error(f"Error showing About dialog: {e}")
            QMessageBox.warning(
                self,
                "Fehler",
                f"Der About-Dialog konnte nicht geöffnet werden:\n\n{str(e)}"
            )
    
    def _show_changelog(self):
        """Open CHANGELOG.md in default application."""
        try:
            changelog_path = Path(__file__).parent.parent.parent / "CHANGELOG.md"
            
            if changelog_path.exists():
                url = QUrl.fromLocalFile(str(changelog_path.resolve()))
                QDesktopServices.openUrl(url)
                logger.info(f"Opened CHANGELOG.md: {changelog_path}")
            else:
                # Fallback: Open GitHub Releases
                from src.__version__ import __url__
                releases_url = f"{__url__}/releases"
                QDesktopServices.openUrl(QUrl(releases_url))
                logger.info(f"CHANGELOG.md not found, opened releases: {releases_url}")
        except Exception as e:
            logger.error(f"Error opening changelog: {e}")
            QMessageBox.warning(
                self,
                "Fehler",
                f"Der Changelog konnte nicht geöffnet werden:\n\n{str(e)}"
            )
    
    def _open_github(self):
        """Open GitHub repository in browser."""
        try:
            from src.__version__ import __url__
            QDesktopServices.openUrl(QUrl(__url__))
            logger.info(f"Opened GitHub repository: {__url__}")
        except Exception as e:
            logger.error(f"Error opening GitHub: {e}")
            QMessageBox.warning(
                self,
                "Fehler",
                f"Das GitHub-Repository konnte nicht geöffnet werden:\n\n{str(e)}"
            )
    
    def _on_theme_changed(self, theme: Theme):
        """
        Handle theme change.
        
        Args:
            theme: New theme
        """
        # Log message
        if theme == Theme.AUTO:
            effective = self.theme_manager.get_effective_theme()
            self._log(f"🔄 Auto Mode aktiviert (System: {('Light' if effective == Theme.LIGHT else 'Dark')} Mode)")
        elif theme == Theme.DARK:
            self._log("🌙 Dark Mode aktiviert")
        else:
            self._log("☀️ Light Mode aktiviert")
    
    def _on_settings_theme_changed(self, theme: Theme):
        """
        Handle theme change from settings dialog.
        
        Args:
            theme: New theme
        """
        # Theme is already set in ThemeManager by SettingsDialog
        # Just log the change
        self._on_theme_changed(theme)
        
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
        
        # Check if user selected new credentials or loaded saved account
        credentials_are_new = dialog.is_new_credentials()
        
        api_type = "Test-API" if use_test_api else "Produktions-API"
        self._log(f"Starte Abruf für Benutzer '{username}' ({api_type})...")
        
        # Disable buttons and show progress
        self.load_button.setEnabled(False)
        self.load_authors_button.setEnabled(False)
        self.update_button.setEnabled(False)
        self.update_authors_button.setEnabled(False)
        self.progress_bar.setVisible(True)
        
        # Create worker and thread
        self.worker = DOIFetchWorker(username, password, use_test_api, credentials_are_new)
        self.thread = QThread()
        self.worker.moveToThread(self.thread)
        
        # Connect signals
        self.thread.started.connect(self.worker.run)
        self.worker.progress.connect(self._log)
        self.worker.finished.connect(self._on_fetch_finished)
        self.worker.error.connect(self._on_fetch_error)
        self.worker.request_save_credentials.connect(self._on_request_save_credentials)
        
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
            
            # Update username and check CSV files
            self._current_username = username
            self._check_csv_files()
            
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
        self.load_authors_button.setEnabled(True)
        self.update_button.setEnabled(True)
        self.update_authors_button.setEnabled(True)
        
        # Reset references (objects are deleted via deleteLater)
        self.thread = None
        self.worker = None
        
        self._log("Bereit für nächsten Vorgang.")
    
    def _on_request_save_credentials(self, username: str, password: str, api_type: str):
        """
        Handle request to save credentials after successful authentication.
        
        Args:
            username: The DataCite username
            password: The DataCite password
            api_type: Either "test" or "production"
        """
        logger.info(f"Received request to save credentials for {username} ({api_type})")
        
        try:
            # Show SaveCredentialsDialog asynchronously
            SaveCredentialsDialog.ask_save_credentials(username, password, api_type, self)
        except Exception as e:
            logger.error(f"Error saving credentials: {e}")
    
    def _on_load_authors_clicked(self):
        """Handle load authors button click."""
        # Show credentials dialog
        dialog = CredentialsDialog(self)
        credentials = dialog.get_credentials()
        
        if credentials is None:
            self._log("Vorgang abgebrochen.")
            return
        
        username, password, csv_path, use_test_api = credentials
        # csv_path is None in export mode, we don't need it here
        
        # Check if user selected new credentials or loaded saved account
        credentials_are_new = dialog.is_new_credentials()
        
        api_type = "Test-API" if use_test_api else "Produktions-API"
        self._log(f"Starte Autoren-Abruf für Benutzer '{username}' ({api_type})...")
        
        # Disable buttons and show progress
        self.load_button.setEnabled(False)
        self.load_authors_button.setEnabled(False)
        self.update_button.setEnabled(False)
        self.update_authors_button.setEnabled(False)
        self.progress_bar.setVisible(True)
        
        # Create worker and thread
        self.creator_worker = DOICreatorFetchWorker(username, password, use_test_api, credentials_are_new)
        self.creator_thread = QThread()
        self.creator_worker.moveToThread(self.creator_thread)
        
        # Connect signals
        self.creator_thread.started.connect(self.creator_worker.run)
        self.creator_worker.progress.connect(self._log)
        self.creator_worker.finished.connect(self._on_creator_fetch_finished)
        self.creator_worker.error.connect(self._on_creator_fetch_error)
        self.creator_worker.request_save_credentials.connect(self._on_request_save_credentials)
        
        # Clean up after worker finishes or errors
        self.creator_worker.finished.connect(self.creator_worker.deleteLater)
        self.creator_worker.error.connect(self.creator_worker.deleteLater)
        self.creator_worker.finished.connect(self.creator_thread.quit)
        self.creator_worker.error.connect(self.creator_thread.quit)
        
        # Clean up thread when it finishes
        self.creator_thread.finished.connect(self.creator_thread.deleteLater)
        self.creator_thread.finished.connect(self._cleanup_creator_thread)
        
        # Start the thread
        self.creator_thread.start()
    
    def _on_creator_fetch_finished(self, creator_data, username):
        """
        Handle successful creator fetch.
        
        Args:
            creator_data: List of creator tuples (DOI, Creator Name, Name Type, etc.)
            username: DataCite username
        """
        if not creator_data:
            self._log("[WARNUNG] Keine DOIs mit Autoren gefunden.")
            QMessageBox.information(
                self,
                "Keine Autoren",
                f"Für den Benutzer '{username}' wurden keine DOIs mit Autoren gefunden."
            )
            return
        
        # Export to CSV
        try:
            output_dir = os.getcwd()
            filepath = export_dois_with_creators_to_csv(creator_data, username, output_dir)
            
            # Count unique DOIs
            unique_dois = len(set(row[0] for row in creator_data))
            
            self._log(f"[OK] CSV-Datei erfolgreich erstellt: {filepath}")
            
            # Update username and check CSV files
            self._current_username = username
            self._check_csv_files()
            
            QMessageBox.information(
                self,
                "Erfolg",
                f"{unique_dois} DOIs mit {len(creator_data)} Autoren wurden erfolgreich exportiert.\n\n"
                f"Datei: {Path(filepath).name}\n"
                f"Verzeichnis: {output_dir}"
            )
            
        except CSVExportError as e:
            self._log(f"[FEHLER] Fehler beim CSV-Export: {str(e)}")
            QMessageBox.critical(
                self,
                "Fehler beim Export",
                f"Die CSV-Datei konnte nicht erstellt werden:\n\n{str(e)}"
            )
    
    def _on_creator_fetch_error(self, error_message):
        """
        Handle creator fetch error.
        
        Args:
            error_message: Error message
        """
        self._log(f"[FEHLER] {error_message}")
        
        QMessageBox.critical(
            self,
            "Fehler",
            f"Beim Abrufen der Autoren ist ein Fehler aufgetreten:\n\n{error_message}"
        )
    
    def _cleanup_creator_thread(self):
        """Clean up creator thread and worker after completion."""
        self.progress_bar.setVisible(False)
        self.load_button.setEnabled(True)
        self.load_authors_button.setEnabled(True)
        self.update_button.setEnabled(True)
        self.update_authors_button.setEnabled(True)
        
        # Reset references (objects are deleted via deleteLater)
        self.creator_thread = None
        self.creator_worker = None
        
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
        
        # Check if user selected new credentials or loaded saved account
        credentials_are_new = dialog.is_new_credentials()
        
        api_type = "Test-API" if use_test_api else "Produktions-API"
        self._log(f"Starte Landing Page URL Update für Benutzer '{username}' ({api_type})...")
        self._log(f"CSV-Datei: {Path(csv_path).name}")
        
        # Disable buttons and show progress
        self.load_button.setEnabled(False)
        self.load_authors_button.setEnabled(False)
        self.update_button.setEnabled(False)
        self.update_authors_button.setEnabled(False)
        self.progress_bar.setVisible(True)
        
        # Create worker and thread
        self.update_worker = UpdateWorker(username, password, csv_path, use_test_api, credentials_are_new)
        self.update_thread = QThread()
        self.update_worker.moveToThread(self.update_thread)
        
        # Connect signals
        self.update_thread.started.connect(self.update_worker.run)
        self.update_worker.progress_update.connect(self._on_update_progress)
        self.update_worker.doi_updated.connect(self._on_doi_updated)
        self.update_worker.finished.connect(self._on_update_finished)
        self.update_worker.request_save_credentials.connect(self._on_request_save_credentials)
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
    
    def _on_update_finished(self, success_count, error_count, skipped_count, error_list, skipped_details):
        """
        Handle update completion.
        
        Args:
            success_count: Number of successful updates
            error_count: Number of failed updates
            skipped_count: Number of skipped DOIs (no changes)
            error_list: List of error messages
            skipped_details: List of (doi, reason) tuples for skipped DOIs
        """
        total = success_count + skipped_count + error_count
        
        self._log("=" * 60)
        if total > 0:
            self._log(f"Update abgeschlossen: {success_count} erfolgreich, {skipped_count} übersprungen (keine Änderungen), {error_count} fehlgeschlagen")
        else:
            self._log("Update abgeschlossen: Keine DOIs verarbeitet")
        self._log("=" * 60)
        
        # Check CSV files (in case user deleted/modified them during update)
        self._check_csv_files()
        
        # Show summary dialog
        if total == 0:
            QMessageBox.warning(
                self,
                "Keine DOIs verarbeitet",
                "Die CSV-Datei enthielt keine gültigen DOIs zum Verarbeiten."
            )
        elif error_count == 0 and skipped_count == 0:
            QMessageBox.information(
                self,
                "Update erfolgreich",
                f"Alle {success_count} DOIs wurden erfolgreich aktualisiert!"
            )
        elif error_count == 0:
            # Some skipped, none failed
            QMessageBox.information(
                self,
                "Update abgeschlossen",
                f"✅ Erfolgreich aktualisiert: {success_count}\n"
                f"⏭️ Übersprungen (keine Änderungen): {skipped_count}\n\n"
                f"Effizienz: {skipped_count} unnötige API-Calls vermieden!"
            )
        else:
            error_details = "\n".join(error_list[:10])  # Show first 10 errors
            if len(error_list) > 10:
                error_details += f"\n... und {len(error_list) - 10} weitere Fehler"
            
            QMessageBox.warning(
                self,
                "Update abgeschlossen mit Fehlern",
                f"✅ Erfolgreich: {success_count}\n"
                f"⏭️ Übersprungen (keine Änderungen): {skipped_count}\n"
                f"❌ Fehlgeschlagen: {error_count}\n\n"
                f"Erste Fehler:\n{error_details}\n\n"
                f"Siehe Log-Datei für Details."
            )
        
        # Create log file
        self._create_update_log(success_count, skipped_count, error_count, error_list, skipped_details)
    
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
        self.load_authors_button.setEnabled(True)
        self.update_button.setEnabled(True)
        self.update_authors_button.setEnabled(True)
        
        # Reset references (objects are deleted via deleteLater)
        self.update_thread = None
        self.update_worker = None
        
        self._log("Bereit für nächsten Vorgang.")
    
    def _create_update_log(self, success_count, skipped_count, error_count, error_list, skipped_details):
        """
        Create a log file with update results.
        
        Args:
            success_count: Number of successful updates
            skipped_count: Number of skipped DOIs (no changes)
            error_count: Number of failed updates
            error_list: List of error messages
            skipped_details: List of (doi, reason) tuples for skipped DOIs
        """
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            log_filename = f"update_log_{timestamp}.txt"
            log_path = Path(os.getcwd()) / log_filename
            
            total = success_count + skipped_count + error_count
            efficiency_gain = (skipped_count / total * 100) if total > 0 else 0
            
            with open(log_path, 'w', encoding='utf-8') as f:
                f.write("=" * 70 + "\n")
                f.write("GROBI - Landing Page URL Update Log\n")
                f.write("=" * 70 + "\n")
                f.write(f"Datum: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write("\n")
                f.write("ZUSAMMENFASSUNG:\n")
                f.write(f"  Gesamt: {total} DOIs\n")
                f.write(f"  Erfolgreich aktualisiert: {success_count}\n")
                f.write(f"  Übersprungen (keine Änderungen): {skipped_count}\n")
                f.write(f"  Fehlgeschlagen: {error_count}\n")
                f.write("\n")
                f.write("EFFIZIENZ:\n")
                f.write(f"  API-Calls vermieden: {skipped_count}/{total} ({efficiency_gain:.1f}%)\n")
                f.write(f"  Nur DOIs mit tatsächlichen Änderungen wurden aktualisiert\n")
                f.write("\n")
                
                # Detailed list of skipped DOIs
                if skipped_details:
                    f.write("=" * 70 + "\n")
                    f.write("ÜBERSPRUNGENE DOIs (keine Änderungen):\n")
                    f.write("=" * 70 + "\n")
                    for doi, reason in skipped_details:
                        f.write(f"  - {doi}\n")
                        f.write(f"    Grund: {reason}\n")
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
    
    def _on_update_authors_clicked(self):
        """Handle update authors button click."""
        # Show credentials dialog in update_authors mode
        dialog = CredentialsDialog(self, mode="update_authors")
        credentials = dialog.get_credentials()
        
        if credentials is None:
            self._log("Autoren-Update abgebrochen.")
            return
        
        username, password, csv_path, use_test_api = credentials
        
        # Check if user selected new credentials or loaded saved account
        credentials_are_new = dialog.is_new_credentials()
        
        # Store credentials for second worker (actual update)
        self._authors_update_username = username
        self._authors_update_password = password
        self._authors_update_csv_path = csv_path
        self._authors_update_use_test_api = use_test_api
        self._authors_update_credentials_are_new = credentials_are_new
        
        api_type = "Test-API" if use_test_api else "Produktions-API"
        self._log(f"Starte Autoren-Update für Benutzer '{username}' ({api_type})...")
        self._log(f"CSV-Datei: {Path(csv_path).name}")
        
        # Disable buttons and show progress
        self.load_button.setEnabled(False)
        self.load_authors_button.setEnabled(False)
        self.update_button.setEnabled(False)
        self.update_authors_button.setEnabled(False)
        self.progress_bar.setVisible(True)
        
        # Create worker and thread for DRY RUN
        self.authors_update_worker = AuthorsUpdateWorker(
            username, password, csv_path, use_test_api, dry_run_only=True, credentials_are_new=False
        )
        self.authors_update_thread = QThread()
        self.authors_update_worker.moveToThread(self.authors_update_thread)
        
        # Connect signals
        self.authors_update_thread.started.connect(self.authors_update_worker.run)
        self.authors_update_worker.progress_update.connect(self._on_authors_update_progress)
        self.authors_update_worker.validation_update.connect(self._on_validation_update)
        self.authors_update_worker.dry_run_complete.connect(self._on_dry_run_complete)
        self.authors_update_worker.finished.connect(self._on_authors_update_finished)
        self.authors_update_worker.error_occurred.connect(self._on_authors_update_error)
        
        # Clean up after worker finishes
        self.authors_update_worker.finished.connect(self.authors_update_worker.deleteLater)
        self.authors_update_worker.finished.connect(self.authors_update_thread.quit)
        
        # Clean up thread when it finishes
        self.authors_update_thread.finished.connect(self.authors_update_thread.deleteLater)
        self.authors_update_thread.finished.connect(self._cleanup_authors_update_thread)
        
        # Start the thread
        self.authors_update_thread.start()
    
    def _on_authors_update_progress(self, current, total, message):
        """
        Handle authors update progress signal.
        
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
    
    def _on_validation_update(self, message):
        """
        Handle validation phase progress signal.
        
        Args:
            message: Validation status message
        """
        self._log(message)
    
    def _on_database_update(self, message):
        """
        Handle database update progress signal.
        
        Args:
            message: Database update status message
        """
        self._log(message)
    
    def _on_datacite_update(self, message):
        """
        Handle DataCite update progress signal.
        
        Args:
            message: DataCite update status message
        """
        self._log(message)
    
    def _on_dry_run_complete(self, valid_count, invalid_count, validation_results):
        """
        Handle dry run validation completion.
        
        Args:
            valid_count: Number of valid DOIs
            invalid_count: Number of invalid DOIs
            validation_results: List of validation result dicts
        """
        total = valid_count + invalid_count
        
        self._log("=" * 60)
        self._log(f"Dry Run abgeschlossen: {valid_count}/{total} validiert")
        self._log("=" * 60)
        
        # Show validation errors if any
        if invalid_count > 0:
            self._log(f"\n[WARNUNG] {invalid_count} DOI(s) sind ungültig:")
            for result in validation_results:
                if not result['valid']:
                    self._log(f"  - {result['doi']}: {result['message']}")
        
        # Show dry run results dialog
        if total == 0:
            QMessageBox.warning(
                self,
                "Keine DOIs gefunden",
                "Die CSV-Datei enthielt keine DOIs zum Validieren."
            )
            return
        
        if invalid_count > 0:
            # Show errors and ask if user wants to continue
            error_details = "\n".join(
                f"• {r['doi']}: {r['message']}"
                for r in validation_results if not r['valid']
            )[:500]  # Limit to 500 chars
            
            reply = QMessageBox.question(
                self,
                "Validierung abgeschlossen mit Fehlern",
                f"Dry Run Ergebnisse:\n\n"
                f"✓ Gültig: {valid_count}\n"
                f"✗ Ungültig: {invalid_count}\n\n"
                f"Erste Fehler:\n{error_details}\n\n"
                f"Möchtest du nur die {valid_count} gültigen DOIs aktualisieren?",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No
            )
            
            if reply == QMessageBox.Yes:
                self._start_actual_authors_update()
            else:
                self._log("Autoren-Update abgebrochen.")
        else:
            # All valid - ask for confirmation
            reply = QMessageBox.question(
                self,
                "Validierung erfolgreich",
                f"Alle {valid_count} DOIs wurden erfolgreich validiert.\n\n"
                f"Möchtest du jetzt die Autoren-Metadaten bei DataCite aktualisieren?",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.Yes
            )
            
            if reply == QMessageBox.Yes:
                self._start_actual_authors_update()
            else:
                self._log("Autoren-Update abgebrochen.")
    
    def _start_actual_authors_update(self):
        """Start the actual authors update process (not dry run)."""
        self._log("\n" + "=" * 60)
        self._log("Starte ECHTES Update der Autoren-Metadaten...")
        self._log("=" * 60)
        
        # Get credentials from stored instance variables (not from worker which may be deleted)
        username = self._authors_update_username
        password = self._authors_update_password
        csv_path = self._authors_update_csv_path
        use_test_api = self._authors_update_use_test_api
        credentials_are_new = self._authors_update_credentials_are_new
        
        # Disable buttons again
        self.load_button.setEnabled(False)
        self.load_authors_button.setEnabled(False)
        self.update_button.setEnabled(False)
        self.update_authors_button.setEnabled(False)
        self.progress_bar.setVisible(True)
        self.progress_bar.setMaximum(0)  # Indeterminate
        
        # Create new worker with dry_run_only=False
        self.authors_update_worker = AuthorsUpdateWorker(
            username, password, csv_path, use_test_api, dry_run_only=False, credentials_are_new=credentials_are_new
        )
        self.authors_update_thread = QThread()
        self.authors_update_worker.moveToThread(self.authors_update_thread)
        
        # Connect signals (no dry_run_complete this time)
        self.authors_update_thread.started.connect(self.authors_update_worker.run)
        self.authors_update_worker.progress_update.connect(self._on_authors_update_progress)
        self.authors_update_worker.validation_update.connect(self._on_validation_update)
        self.authors_update_worker.database_update.connect(self._on_database_update)
        self.authors_update_worker.datacite_update.connect(self._on_datacite_update)
        self.authors_update_worker.doi_updated.connect(self._on_author_doi_updated)
        self.authors_update_worker.finished.connect(self._on_authors_update_finished)
        self.authors_update_worker.error_occurred.connect(self._on_authors_update_error)
        self.authors_update_worker.request_save_credentials.connect(self._on_request_save_credentials)
        
        # Clean up after worker finishes
        self.authors_update_worker.finished.connect(self.authors_update_worker.deleteLater)
        self.authors_update_worker.finished.connect(self.authors_update_thread.quit)
        
        # Clean up thread when it finishes
        self.authors_update_thread.finished.connect(self.authors_update_thread.deleteLater)
        self.authors_update_thread.finished.connect(self._cleanup_authors_update_thread)
        
        # Start the thread
        self.authors_update_thread.start()
    
    def _on_author_doi_updated(self, doi, success, message):
        """
        Handle individual DOI author update result.
        
        Args:
            doi: DOI that was updated
            success: Whether update was successful
            message: Result message (may include system status)
        """
        # Log all updates with appropriate prefix
        if success:
            self._log(f"[OK] {doi}: {message}")
        else:
            self._log(f"[FEHLER] {doi}: {message}")
    
    def _on_authors_update_finished(self, success_count, error_count, skipped_count, error_list, skipped_details):
        """
        Handle authors update completion.
        
        Args:
            success_count: Number of successful updates
            error_count: Number of failed updates
            skipped_count: Number of skipped DOIs (no changes)
            error_list: List of error messages
            skipped_details: List of (doi, reason) tuples for skipped DOIs
        """
        total = success_count + skipped_count + error_count
        
        self._log("=" * 60)
        if total > 0:
            self._log(f"Autoren-Update abgeschlossen: {success_count} erfolgreich, {skipped_count} übersprungen (keine Änderungen), {error_count} fehlgeschlagen")
        else:
            self._log("Autoren-Update abgeschlossen")
        self._log("=" * 60)
        
        # Check CSV files (in case user deleted/modified them during update)
        self._check_csv_files()
        
        # Show summary dialog (only if not dry run)
        if self.authors_update_worker and not self.authors_update_worker.dry_run_only:
            if total == 0:
                QMessageBox.information(
                    self,
                    "Keine Updates",
                    "Es wurden keine DOIs aktualisiert (möglicherweise waren alle ungültig)."
                )
            elif error_count == 0 and skipped_count == 0:
                QMessageBox.information(
                    self,
                    "Update erfolgreich",
                    f"Alle {success_count} DOIs wurden erfolgreich aktualisiert!"
                )
            elif error_count == 0:
                # Some skipped, none failed
                QMessageBox.information(
                    self,
                    "Update abgeschlossen",
                    f"✅ Erfolgreich aktualisiert: {success_count}\n"
                    f"⏭️ Übersprungen (keine Änderungen): {skipped_count}\n\n"
                    f"Effizienz: {skipped_count} unnötige API-Calls vermieden!"
                )
            else:
                error_details = "\n".join(error_list[:10])  # Show first 10 errors
                if len(error_list) > 10:
                    error_details += f"\n... und {len(error_list) - 10} weitere Fehler"
                
                QMessageBox.warning(
                    self,
                    "Update abgeschlossen mit Fehlern",
                    f"✅ Erfolgreich: {success_count}\n"
                    f"⏭️ Übersprungen (keine Änderungen): {skipped_count}\n"
                    f"❌ Fehlgeschlagen: {error_count}\n\n"
                    f"Erste Fehler:\n{error_details}"
                )
            
            # Create log file for actual updates
            if total > 0:
                self._create_authors_update_log(success_count, skipped_count, error_count, error_list, skipped_details)
    
    def _on_authors_update_error(self, error_message):
        """
        Handle critical authors update error.
        
        Args:
            error_message: Error message
        """
        self._log(f"[KRITISCHER FEHLER] {error_message}")
        
        QMessageBox.critical(
            self,
            "Autoren-Update Fehler",
            f"Ein kritischer Fehler ist aufgetreten:\n\n{error_message}"
        )
    
    def _cleanup_authors_update_thread(self):
        """Clean up authors update thread and worker after completion."""
        self.progress_bar.setVisible(False)
        self.progress_bar.setMaximum(0)  # Reset to indeterminate
        self.load_button.setEnabled(True)
        self.load_authors_button.setEnabled(True)
        self.update_button.setEnabled(True)
        self.update_authors_button.setEnabled(True)
        
        # Reset references (objects are deleted via deleteLater)
        self.authors_update_thread = None
        self.authors_update_worker = None
        
        self._log("Bereit für nächsten Vorgang.")
    
    def _create_authors_update_log(self, success_count, skipped_count, error_count, error_list, skipped_details):
        """
        Create a log file with authors update results.
        
        Args:
            success_count: Number of successful updates
            skipped_count: Number of skipped DOIs (no changes)
            error_count: Number of failed updates
            error_list: List of error messages
            skipped_details: List of (doi, reason) tuples for skipped DOIs
        """
        try:
            # Check if database sync is enabled
            from PySide6.QtCore import QSettings
            settings = QSettings("GFZ", "GROBI")
            db_enabled = settings.value("database/enabled", False, type=bool)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            log_filename = f"authors_update_log_{timestamp}.txt"
            log_path = Path(os.getcwd()) / log_filename
            
            total = success_count + skipped_count + error_count
            efficiency_gain = (skipped_count / total * 100) if total > 0 else 0
            
            with open(log_path, 'w', encoding='utf-8') as f:
                f.write("=" * 70 + "\n")
                f.write("GROBI - Autoren-Metadaten Update Log\n")
                f.write("=" * 70 + "\n")
                f.write(f"Datum: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"Datenbank-Synchronisation: {'Aktiviert' if db_enabled else 'Deaktiviert'}\n")
                f.write("\n")
                f.write("ZUSAMMENFASSUNG:\n")
                f.write(f"  Gesamt: {total} DOIs\n")
                f.write(f"  Erfolgreich aktualisiert: {success_count}\n")
                f.write(f"  Übersprungen (keine Änderungen): {skipped_count}\n")
                f.write(f"  Fehlgeschlagen: {error_count}\n")
                f.write("\n")
                f.write("EFFIZIENZ:\n")
                f.write(f"  API-Calls vermieden: {skipped_count}/{total} ({efficiency_gain:.1f}%)\n")
                f.write(f"  Nur DOIs mit tatsächlichen Änderungen wurden aktualisiert\n")
                
                if db_enabled:
                    # Count inconsistency warnings
                    inconsistencies = [e for e in error_list if "INKONSISTENZ" in e]
                    if inconsistencies:
                        f.write(f"\n")
                        f.write(f"  ⚠️ KRITISCHE INKONSISTENZEN: {len(inconsistencies)}\n")
                        f.write(f"     (Datenbank erfolgreich, DataCite fehlgeschlagen)\n")
                
                f.write("\n")
                
                # Detailed list of skipped DOIs
                if skipped_details:
                    f.write("=" * 70 + "\n")
                    f.write("ÜBERSPRUNGENE DOIs (keine Änderungen):\n")
                    f.write("=" * 70 + "\n")
                    for doi, reason in skipped_details:
                        f.write(f"  - {doi}\n")
                        f.write(f"    Grund: {reason}\n")
                    f.write("\n")
                
                if error_list:
                    f.write("=" * 70 + "\n")
                    f.write("FEHLER:\n")
                    f.write("=" * 70 + "\n")
                    for error in error_list:
                        f.write(f"  - {error}\n")
                else:
                    f.write("Keine Fehler aufgetreten.\n")
                
                if db_enabled:
                    f.write("\n")
                    f.write("=" * 70 + "\n")
                    f.write("DATABASE-FIRST UPDATE PATTERN:\n")
                    f.write("=" * 70 + "\n")
                    f.write("1. Datenbank wird ZUERST aktualisiert (mit ROLLBACK bei Fehlern)\n")
                    f.write("2. DataCite wird DANACH aktualisiert (nur wenn DB erfolgreich)\n")
                    f.write("3. Bei DataCite-Fehlern erfolgt automatischer Retry\n")
                    f.write("\n")
                    f.write("HINWEIS: Falls INKONSISTENZEN auftraten, müssen diese manuell\n")
                    f.write("         korrigiert werden (Datenbank committed, DataCite failed).\n")
                
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
        
        # If authors update thread is running, stop worker and wait for it to finish
        if self.authors_update_thread is not None and self.authors_update_thread.isRunning():
            self._log("Warte auf Abschluss des Autoren-Updates...")
            if self.authors_update_worker is not None:
                self.authors_update_worker.stop()
            self.authors_update_thread.quit()
            self.authors_update_thread.wait(3000)  # Wait max 3 seconds
        
        event.accept()
