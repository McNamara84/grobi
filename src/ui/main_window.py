"""Main application window for GROBI."""

import csv
import logging
import os
from datetime import datetime
from pathlib import Path

from PySide6.QtWidgets import (
    QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, QPushButton,
    QTextEdit, QProgressBar, QLabel, QMessageBox, QDialog,
    QScrollArea, QSizePolicy
)
from PySide6.QtCore import QThread, Signal, QObject, QUrl, Qt, QSettings
from PySide6.QtGui import QFont, QIcon, QAction, QDesktopServices, QPixmap, QGuiApplication, QShortcut, QKeySequence, QDragEnterEvent, QDropEvent

from src.ui.credentials_dialog import CredentialsDialog
from src.ui.save_credentials_dialog import SaveCredentialsDialog
from src.ui.about_dialog import AboutDialog
from src.ui.csv_splitter_dialog import CSVSplitterDialog
from src.ui.theme_manager import ThemeManager, Theme
from src.ui.fuji_results_window import FujiResultsWindow
from src.ui.flow_layout import FlowLayout
from src.ui.components import ActionCard, CollapsibleSection
from src.api.datacite_client import DataCiteClient, DataCiteAPIError, AuthenticationError, NetworkError
from src.api.fuji_client import FujiClient
from src.utils.csv_exporter import export_dois_to_csv, export_dois_with_creators_to_csv, export_dois_with_publisher_to_csv, export_dois_with_contributors_to_csv, export_dois_with_rights_to_csv, CSVExportError
from src.utils.csv_parser import SPDXValidationError, LanguageCodeError
from src.workers.update_worker import UpdateWorker
from src.workers.authors_update_worker import AuthorsUpdateWorker
from src.workers.publisher_update_worker import PublisherUpdateWorker
from src.workers.contributors_update_worker import ContributorsUpdateWorker
from src.workers.rights_update_worker import RightsUpdateWorker
from src.workers.pending_export_worker import PendingExportWorker
from src.workers.fuji_worker import FujiAssessmentThread, StreamingFujiThread


logger = logging.getLogger(__name__)

# Window size constants
DEFAULT_WINDOW_WIDTH = 900
MINIMUM_WINDOW_HEIGHT = 600
MAXIMUM_WINDOW_WIDTH = 1200
SCREEN_HEIGHT_RATIO = 0.80
SCREEN_WIDTH_RATIO = 0.60
FALLBACK_WINDOW_HEIGHT = 800

# Minimum visible area (in pixels) to consider a window "on screen"
# Used to detect if window is still visible after monitor changes
MINIMUM_VISIBLE_WINDOW_SIZE = 100

# Minimum pixels of title bar that must be visible for the window to be draggable
# 50 pixels is approximately the width needed for a user to grab and move the window
# on most operating systems with standard title bar heights (typically 20-40 pixels)
TITLE_BAR_MIN_VISIBLE = 50

# QSettings keys for window geometry
SETTINGS_GEOMETRY = "window/geometry"
SETTINGS_WINDOW_STATE = "window/state"
SETTINGS_WINDOW_MAXIMIZED = "window/maximized"


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


class DOIPublisherFetchWorker(QObject):
    """Worker for fetching DOIs with publisher information in a separate thread."""
    
    # Signals
    progress = Signal(str)  # Progress message
    finished = Signal(list, str, int)  # List of publisher tuples, username, warnings count
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
        """Fetch DOIs with publisher information from DataCite API."""
        try:
            self.progress.emit("Verbindung zur DataCite API wird hergestellt...")
            
            client = DataCiteClient(
                self.username,
                self.password,
                self.use_test_api
            )
            
            self.progress.emit("DOIs und Publisher-Metadaten werden abgerufen...")
            publisher_data = client.fetch_all_dois_with_publisher()
            
            # If credentials are new and API call was successful, offer to save them
            if self.credentials_are_new and publisher_data:
                api_type = "test" if self.use_test_api else "production"
                self.request_save_credentials.emit(self.username, self.password, api_type)
            
            self.progress.emit(f"[OK] {len(publisher_data)} DOIs mit Publisher-Daten erfolgreich abgerufen")
            # warnings_count will be calculated during export
            self.finished.emit(publisher_data, self.username, 0)
            
        except AuthenticationError as e:
            self.error.emit(str(e))
        except NetworkError as e:
            self.error.emit(str(e))
        except DataCiteAPIError as e:
            self.error.emit(str(e))
        except Exception as e:
            self.error.emit(f"Unerwarteter Fehler: {str(e)}")


class DOIContributorFetchWorker(QObject):
    """Worker for fetching DOIs with contributor information in a separate thread."""
    
    # Signals
    progress = Signal(str)  # Progress message
    finished = Signal(list, str)  # List of contributor tuples and username
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
        """Fetch DOIs with contributor information from DataCite API."""
        try:
            self.progress.emit("Verbindung zur DataCite API wird hergestellt...")
            
            client = DataCiteClient(
                self.username,
                self.password,
                self.use_test_api
            )
            
            self.progress.emit("DOIs und Contributors werden abgerufen...")
            contributor_data = client.fetch_all_dois_with_contributors()
            
            # If credentials are new and API call was successful, offer to save them
            if self.credentials_are_new and contributor_data:
                api_type = "test" if self.use_test_api else "production"
                self.request_save_credentials.emit(self.username, self.password, api_type)
            
            # Try to enrich with database data if DB is enabled and credentials are saved
            if contributor_data:
                try:
                    from PySide6.QtCore import QSettings
                    from src.utils.credential_manager import load_db_credentials
                    from src.db.sumariopmd_client import SumarioPMDClient
                    
                    settings = QSettings("GFZ", "GROBI")
                    db_enabled = settings.value("database/enabled", False, type=bool)
                    
                    if db_enabled:
                        db_creds = load_db_credentials()
                        if db_creds:
                            self.progress.emit("ContactInfo aus Datenbank wird abgerufen...")
                            
                            db_client = SumarioPMDClient(
                                host=db_creds['host'],
                                username=db_creds['username'],
                                password=db_creds['password'],
                                database=db_creds['database']
                            )
                            
                            contributor_data = DataCiteClient.enrich_contributors_with_db_data(
                                contributor_data, db_client
                            )
                            
                            self.progress.emit("ContactInfo erfolgreich hinzugefügt")
                        else:
                            self.progress.emit("[INFO] Keine DB-Zugangsdaten gespeichert - ContactInfo nicht verfügbar")
                    else:
                        self.progress.emit("[INFO] Datenbank-Synchronisation deaktiviert - ContactInfo nicht verfügbar")
                        
                except Exception as db_error:
                    # Log but don't fail - continue without DB enrichment
                    self.progress.emit(f"[WARNUNG] ContactInfo konnte nicht geladen werden: {str(db_error)}")
            
            # Count unique DOIs for better user feedback
            unique_dois = len(set(row[0] for row in contributor_data))
            self.progress.emit(f"[OK] {unique_dois} DOIs mit {len(contributor_data)} Contributors erfolgreich abgerufen")
            self.finished.emit(contributor_data, self.username)
            
        except AuthenticationError as e:
            self.error.emit(str(e))
        except NetworkError as e:
            self.error.emit(str(e))
        except DataCiteAPIError as e:
            self.error.emit(str(e))
        except Exception as e:
            self.error.emit(f"Unerwarteter Fehler: {str(e)}")


class DOIRightsFetchWorker(QObject):
    """Worker for fetching DOIs with rights information in a separate thread."""
    
    # Signals
    progress = Signal(str)  # Progress message
    finished = Signal(list, str)  # List of rights tuples and username
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
        """Fetch DOIs with rights information from DataCite API."""
        try:
            self.progress.emit("Verbindung zur DataCite API wird hergestellt...")
            
            client = DataCiteClient(
                self.username,
                self.password,
                self.use_test_api
            )
            
            self.progress.emit("DOIs und Rights werden abgerufen...")
            rights_data = client.fetch_all_dois_with_rights()
            
            # If credentials are new and API call was successful, offer to save them
            if self.credentials_are_new and rights_data:
                api_type = "test" if self.use_test_api else "production"
                self.request_save_credentials.emit(self.username, self.password, api_type)
            
            # Count unique DOIs for better user feedback
            unique_dois = len(set(row[0] for row in rights_data))
            self.progress.emit(f"[OK] {unique_dois} DOIs mit {len(rights_data)} Rights-Einträgen erfolgreich abgerufen")
            self.finished.emit(rights_data, self.username)
            
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
        self.setMinimumSize(DEFAULT_WINDOW_WIDTH, MINIMUM_WINDOW_HEIGHT)
        
        # Restore saved window geometry or use smart defaults
        self._restore_window_geometry()
        
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
        
        # Thread and worker for DOI publisher fetch
        self.publisher_thread = None
        self.publisher_worker = None
        
        # Thread and worker for publisher update
        self.publisher_update_thread = None
        self.publisher_update_worker = None
        
        # Thread and worker for DOI contributor fetch
        self.contributor_thread = None
        self.contributor_worker = None
        
        # Thread and worker for contributors update
        self.contributors_update_thread = None
        self.contributors_update_worker = None
        
        # Thread and worker for download URL update
        self.download_url_update_thread = None
        self.download_url_update_worker = None

        # Thread and worker for dead link checks
        self.dead_links_thread = None
        self.dead_links_worker = None
        
        # Thread and worker for DOI rights fetch
        self.rights_thread = None
        self.rights_worker = None
        
        # Thread and worker for rights update
        self.rights_update_thread = None
        self.rights_update_worker = None
        
        # Flag to prevent double dialogs on rights update errors
        self._rights_update_had_critical_error = False
        
        # F-UJI FAIR Assessment
        self.fuji_thread = None
        self.fuji_results_window = None
        
        # Track current username for CSV detection
        self._current_username = None
        
        # Path to CSV file dropped via drag & drop (used by import methods)
        self.pending_csv_path = None
        
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
        
        # Werkzeuge-Menü
        tools_menu = menubar.addMenu("Werkzeuge")
        
        csv_splitter_action = QAction("CSV-Datei aufsplitten...", self)
        csv_splitter_action.setShortcut("Ctrl+Shift+S")
        csv_splitter_action.triggered.connect(self._open_csv_splitter)
        tools_menu.addAction(csv_splitter_action)
        
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
        """Set up the user interface with modern card-based layout."""
        # Central widget with scroll area for responsive design
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        
        main_layout = QVBoxLayout(central_widget)
        main_layout.setContentsMargins(0, 0, 0, 0)
        main_layout.setSpacing(0)
        
        # Scroll area for content
        scroll_area = QScrollArea()
        scroll_area.setWidgetResizable(True)
        scroll_area.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        scroll_area.setFrameShape(QScrollArea.NoFrame)
        
        # Content widget inside scroll area
        content_widget = QWidget()
        content_widget.setObjectName("scrollContent")
        content_layout = QVBoxLayout(content_widget)
        content_layout.setSpacing(16)
        content_layout.setContentsMargins(24, 24, 24, 24)
        
        # ===== HEADER =====
        header_layout = QHBoxLayout()
        header_layout.setSpacing(12)
        
        # Logo
        logo_label = QLabel()
        logo_path = Path(__file__).parent / "GROBI-Logo.ico"
        if logo_path.exists():
            pixmap = QPixmap(str(logo_path))
            pixmap = pixmap.scaled(40, 40, Qt.KeepAspectRatio, Qt.SmoothTransformation)
            logo_label.setPixmap(pixmap)
            logo_label.setFixedSize(40, 40)
        header_layout.addWidget(logo_label)
        
        # Title container
        title_container = QVBoxLayout()
        title_container.setSpacing(2)
        
        title = QLabel("GROBI")
        title_font = QFont()
        title_font.setPointSize(22)
        title_font.setBold(True)
        title.setFont(title_font)
        title_container.addWidget(title)
        
        self.subtitle = QLabel("GFZ Research Data Repository Operations & Batch Interface")
        subtitle_font = QFont()
        subtitle_font.setPointSize(10)
        self.subtitle.setFont(subtitle_font)
        effective_theme = self.theme_manager.get_effective_theme()
        subtitle_color = "#999" if effective_theme == Theme.DARK else "#666"
        self.subtitle.setStyleSheet(f"color: {subtitle_color};")
        title_container.addWidget(self.subtitle)
        
        header_layout.addLayout(title_container)
        header_layout.addStretch()
        
        content_layout.addLayout(header_layout)
        content_layout.addSpacing(16)
        
        # ===== SECTION 1: Metadaten-Verwaltung =====
        self.metadata_section = CollapsibleSection("📊 Metadaten-Verwaltung", expanded=True)
        
        # FlowLayout for cards
        metadata_flow = FlowLayout(h_spacing=16, v_spacing=16)
        
        # Card 1: Landing Page URLs
        self.urls_card = ActionCard(
            icon="🔗",
            title="Landing Page URLs",
            description="DOI-URLs verwalten und aktualisieren",
            primary_text="📥 Exportieren",
            default_status="⚪ Keine CSV-Datei geladen"
        )
        self.urls_card.setToolTip(
            "Exportiert alle DOIs mit ihren Landing Page URLs.\n"
            "Die URLs können dann in einer CSV-Datei bearbeitet\n"
            "und wieder importiert werden.\n\n"
            "Tastenkürzel: Ctrl+1"
        )
        self.urls_card.add_action("Aus CSV aktualisieren", "update", "🔄")
        self.urls_card.primary_clicked.connect(self._on_load_dois_clicked)
        self.urls_card.action_triggered.connect(self._on_urls_card_action)
        metadata_flow.addWidget(self.urls_card)
        
        # Card 2: Autoren
        self.authors_card = ActionCard(
            icon="👥",
            title="Autoren",
            description="Creator-Metadaten bearbeiten",
            primary_text="📥 Exportieren",
            default_status="⚪ Keine CSV-Datei geladen"
        )
        self.authors_card.setToolTip(
            "Exportiert Creator-Informationen (Name, ORCID, Affiliationen).\n"
            "Achtung: Nur Creator-Rollen werden bearbeitet,\n"
            "Contributors bleiben unverändert.\n\n"
            "Tastenkürzel: Ctrl+2"
        )
        self.authors_card.add_action("Aus CSV aktualisieren", "update", "🔄")
        self.authors_card.primary_clicked.connect(self._on_load_authors_clicked)
        self.authors_card.action_triggered.connect(self._on_authors_card_action)
        metadata_flow.addWidget(self.authors_card)
        
        # Card 3: Publisher
        self.publisher_card = ActionCard(
            icon="📦",
            title="Publisher",
            description="Publisher-Metadaten verwalten",
            primary_text="📥 Exportieren",
            default_status="⚪ Keine CSV-Datei geladen"
        )
        self.publisher_card.setToolTip(
            "Exportiert und aktualisiert Publisher-Informationen.\n"
            "Typischerweise 'GFZ Data Services' für alle DOIs.\n\n"
            "Tastenkürzel: Ctrl+3"
        )
        self.publisher_card.add_action("Aus CSV aktualisieren", "update", "🔄")
        self.publisher_card.primary_clicked.connect(self._on_load_publisher_clicked)
        self.publisher_card.action_triggered.connect(self._on_publisher_card_action)
        metadata_flow.addWidget(self.publisher_card)
        
        # Card 4: Contributors
        self.contributors_card = ActionCard(
            icon="🤝",
            title="Contributors",
            description="Contributor-Metadaten bearbeiten",
            primary_text="📥 Exportieren",
            default_status="⚪ Keine CSV-Datei geladen"
        )
        self.contributors_card.setToolTip(
            "Exportiert Contributor-Informationen mit Rollen.\n"
            "Unterstützte Rollen: ContactPerson, DataCurator,\n"
            "ProjectLeader, Researcher, etc.\n\n"
            "Tastenkürzel: Ctrl+4"
        )
        self.contributors_card.add_action("Aus CSV aktualisieren", "update", "🔄")
        self.contributors_card.primary_clicked.connect(self._on_load_contributors_clicked)
        self.contributors_card.action_triggered.connect(self._on_contributors_card_action)
        metadata_flow.addWidget(self.contributors_card)
        
        # Card 5: Rights
        self.rights_card = ActionCard(
            icon="⚖️",
            title="Rights",
            description="Lizenz-Metadaten verwalten",
            primary_text="📥 Exportieren",
            default_status="⚪ Keine CSV-Datei geladen"
        )
        self.rights_card.setToolTip(
            "Exportiert und aktualisiert Lizenzinformationen.\n"
            "Unterstützt SPDX-Identifikatoren (CC-BY-4.0, etc.)\n"
            "und valide Lizenz-URIs.\n\n"
            "Tastenkürzel: Ctrl+5"
        )
        self.rights_card.add_action("Aus CSV aktualisieren", "update", "🔄")
        self.rights_card.primary_clicked.connect(self._on_load_rights_clicked)
        self.rights_card.action_triggered.connect(self._on_rights_card_action)
        metadata_flow.addWidget(self.rights_card)
        
        # Card 6: Download URLs
        self.downloads_card = ActionCard(
            icon="📥",
            title="Download-URLs",
            description="contentUrl-Felder bearbeiten",
            primary_text="📥 Exportieren"
        )
        self.downloads_card.setToolTip(
            "Exportiert contentUrl-Felder (Download-Links).\n"
            "Diese URLs werden im DataCite-Schema als\n"
            "direkter Zugang zu den Daten verwendet.\n\n"
            "Tastenkürzel: Ctrl+6"
        )
        self.downloads_card.set_status("Bereit zum Exportieren", is_ready=True)
        self.downloads_card.add_action("Aus CSV aktualisieren", "update", "🔄")
        self.downloads_card.primary_clicked.connect(self._on_export_download_urls_clicked)
        self.downloads_card.action_triggered.connect(self._on_downloads_card_action)
        metadata_flow.addWidget(self.downloads_card)
        
        self.metadata_section.set_content_layout(metadata_flow)
        content_layout.addWidget(self.metadata_section)
        
        # ===== SECTION 2: Datenbank & Analyse =====
        self.tools_section = CollapsibleSection("🔧 Datenbank & Analyse", expanded=True)
        
        tools_flow = FlowLayout(h_spacing=16, v_spacing=16)
        
        # Card 7: Pending DOIs
        self.pending_card = ActionCard(
            icon="⏳",
            title="Pending DOIs",
            description="DOIs aus Datenbank exportieren",
            primary_text="📥 Exportieren"
        )
        self.pending_card.set_status("Datenbank-Export", is_ready=True)
        self.pending_card.setToolTip(
            "Exportiert alle DOIs mit Status 'pending' aus der SUMARIOPMD-Datenbank.\n"
            "Enthält DOI, Titel und Erstautor.\n\n"
            "Tastenkürzel: Ctrl+7"
        )
        self.pending_card.primary_clicked.connect(self._on_export_pending_clicked)
        tools_flow.addWidget(self.pending_card)
        
        # Card 8: F-UJI FAIR Assessment
        self.fuji_card = ActionCard(
            icon="🎯",
            title="F-UJI FAIR",
            description="FAIR-Konformität prüfen",
            primary_text="🔍 Check starten"
        )
        self.fuji_card.set_status("FAIR-Analyse bereit", is_ready=True)
        self.fuji_card.setToolTip(
            "Bewertet alle DOIs des DataCite-Accounts nach FAIR-Kriterien.\n"
            "Verwendet den F-UJI FAIR Assessment Service.\n\n"
            "Tastenkürzel: Ctrl+8"
        )
        self.fuji_card.primary_clicked.connect(self._on_fuji_check_clicked)
        tools_flow.addWidget(self.fuji_card)

        # Card 9: Dead Links Check
        self.dead_links_card = ActionCard(
            icon="🧪",
            title="Dead Links",
            description="404-Links aus SUMARIOPMD finden",
            primary_text="🔍 Prüfen"
        )
        self.dead_links_card.set_status("Bereit zum Prüfen", is_ready=True)
        self.dead_links_card.setToolTip(
            "Prüft alle contentUrl-Links aus der SUMARIOPMD-Datenbank\n"
            "auf HTTP 404 und exportiert Treffer als CSV.\n\n"
            "Tastenkürzel: Ctrl+9"
        )
        self.dead_links_card.primary_clicked.connect(self._on_check_dead_links_clicked)
        tools_flow.addWidget(self.dead_links_card)
        
        self.tools_section.set_content_layout(tools_flow)
        content_layout.addWidget(self.tools_section)
        
        # ===== PROGRESS BAR =====
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        self.progress_bar.setMinimum(0)
        self.progress_bar.setMaximum(0)  # Indeterminate progress
        self.progress_bar.setFixedHeight(6)
        content_layout.addWidget(self.progress_bar)
        
        # ===== LOG SECTION =====
        self.log_section = CollapsibleSection("📋 Status-Log", expanded=True)
        
        log_container = QVBoxLayout()
        log_container.setContentsMargins(0, 0, 0, 0)
        
        self.log_text = QTextEdit()
        self.log_text.setReadOnly(True)
        self.log_text.setMinimumHeight(120)
        self.log_text.setMaximumHeight(200)
        log_container.addWidget(self.log_text)
        
        log_widget = QWidget()
        log_widget.setLayout(log_container)
        self.log_section.add_widget(log_widget)
        content_layout.addWidget(self.log_section)
        
        # Add stretch at the end
        content_layout.addStretch()
        
        scroll_area.setWidget(content_widget)
        main_layout.addWidget(scroll_area)
        
        # ===== Compatibility: Create references to old button names =====
        # This ensures existing code that references these buttons still works
        self._create_legacy_button_references()
        
        # Set up keyboard shortcuts for cards
        self._setup_keyboard_shortcuts()
        
        # Enable drag & drop for CSV files
        self._setup_drag_drop()
        
        # Check for existing CSV files
        self._check_csv_files()
        
        # Initial log message
        self._log("Bereit. Wähle eine Aktion um zu beginnen.")
    
    def _create_legacy_button_references(self):
        """
        Create references to maintain compatibility with old button names.
        
        Note: In the new card-based UI, the distinction between "load/export" and
        "update" buttons has changed:
        - Export: Primary action (main button)
        - Update: Secondary action in dropdown menu
        
        IMPORTANT for legacy code:
        - self.load_* buttons: Point to primary button (export functionality)
        - self.update_* buttons: Point to dropdown button (menu access)
        
        To check if "update" action is enabled, use:
            self.urls_card.split_button.is_action_enabled("update")
        
        To enable/disable the update action specifically:
            self.urls_card.set_action_enabled("update", True/False)
        """
        # Primary buttons - used by _set_buttons_enabled for disabling entire cards
        self.load_button = self.urls_card.split_button.primary_button
        self.load_authors_button = self.authors_card.split_button.primary_button
        self.load_publisher_button = self.publisher_card.split_button.primary_button
        self.load_contributors_button = self.contributors_card.split_button.primary_button
        self.load_rights_button = self.rights_card.split_button.primary_button
        self.export_download_urls_btn = self.downloads_card.split_button.primary_button
        self.export_pending_btn = self.pending_card.split_button.primary_button
        self.fuji_check_btn = self.fuji_card.split_button.primary_button
        self.dead_links_check_btn = self.dead_links_card.split_button.primary_button
        
        # DEPRECATED LEGACY REFERENCES - BREAKING CHANGE in v2.0
        # These dropdown button references are DEPRECATED and will be removed in a future version.
        # They do NOT behave like the old buttons:
        #   - isEnabled() returns dropdown visibility, NOT menu action state
        #   - setEnabled() affects the dropdown, NOT individual actions
        # 
        # MIGRATION: Use the new card-based API instead:
        #   - Check action state:  card.split_button.is_action_enabled("update")
        #   - Set action state:    card.set_action_enabled("update", True/False)
        #   - Primary button:      card.split_button.primary_button
        # 
        # These references are kept temporarily for backward compatibility with tests.
        self.update_button = self.urls_card.split_button.dropdown_button
        self.update_authors_button = self.authors_card.split_button.dropdown_button
        self.update_publisher_button = self.publisher_card.split_button.dropdown_button
        self.update_contributors_button = self.contributors_card.split_button.dropdown_button
        self.update_rights_button = self.rights_card.split_button.dropdown_button
        self.update_download_urls_btn = self.downloads_card.split_button.dropdown_button
        
        # Status labels - create new references to card status labels
        self.urls_status_label = self.urls_card.status_label
        self.authors_status_label = self.authors_card.status_label
        self.publisher_status_label = self.publisher_card.status_label
        self.contributors_status_label = self.contributors_card.status_label
        self.rights_status_label = self.rights_card.status_label
    
    def _setup_keyboard_shortcuts(self):
        """Set up keyboard shortcuts for quick card access."""
        # Ctrl+1 to Ctrl+9 for cards
        shortcuts = [
            ("Ctrl+1", self._on_load_dois_clicked, "Landing Page URLs exportieren"),
            ("Ctrl+2", self._on_load_authors_clicked, "Autoren exportieren"),
            ("Ctrl+3", self._on_load_publisher_clicked, "Publisher exportieren"),
            ("Ctrl+4", self._on_load_contributors_clicked, "Contributors exportieren"),
            ("Ctrl+5", self._on_load_rights_clicked, "Rights exportieren"),
            ("Ctrl+6", self._on_export_download_urls_clicked, "Download-URLs exportieren"),
            ("Ctrl+7", self._on_export_pending_clicked, "Pending DOIs exportieren"),
            ("Ctrl+8", self._on_fuji_check_clicked, "F-UJI Check starten"),
            ("Ctrl+9", self._on_check_dead_links_clicked, "Dead Links prüfen"),
        ]
        
        self._shortcuts = []  # Keep references to prevent garbage collection
        for key_seq, callback, description in shortcuts:
            shortcut = QShortcut(QKeySequence(key_seq), self)
            # WindowShortcut context: fires when window has focus, even in text fields.
            # This is acceptable because:
            # 1. The log area is read-only (users don't type there)
            # 2. Ctrl+1-8 are not typical text editing combinations
            # 3. The shortcuts don't fire when modal dialogs are open
            shortcut.setContext(Qt.ShortcutContext.WindowShortcut)
            shortcut.activated.connect(callback)
            shortcut.setWhatsThis(description)
            self._shortcuts.append(shortcut)
    
    def _setup_drag_drop(self):
        """Enable drag and drop for CSV files."""
        self.setAcceptDrops(True)
    
    def dragEnterEvent(self, event: QDragEnterEvent):
        """
        Handle drag enter event.
        
        Accept the drag if it contains file URLs that look like CSV files.
        """
        if event.mimeData().hasUrls():
            for url in event.mimeData().urls():
                if url.isLocalFile() and url.toLocalFile().lower().endswith('.csv'):
                    event.acceptProposedAction()
                    return
        event.ignore()
    
    def dropEvent(self, event: QDropEvent):
        """
        Handle drop event for CSV files.
        
        Analyzes the dropped CSV and triggers the appropriate import action.
        """
        if event.mimeData().hasUrls():
            for url in event.mimeData().urls():
                if url.isLocalFile():
                    file_path = url.toLocalFile()
                    if file_path.lower().endswith('.csv'):
                        event.acceptProposedAction()
                        self._handle_dropped_csv(file_path)
                        return
        event.ignore()
    
    def _handle_dropped_csv(self, file_path: str):
        """
        Handle a dropped CSV file by detecting its type and triggering import.
        
        The detection order is important - more specific headers are checked first
        to avoid false positives (e.g., contributors CSV with 'doi' column being
        detected as URL CSV).
        
        Args:
            file_path: Path to the dropped CSV file
        """
        self._log(f"📁 CSV-Datei erhalten: {Path(file_path).name}")
        
        try:
            # Read first line to detect CSV type
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                try:
                    header = next(reader, None)
                except csv.Error as csv_err:
                    self._log(f"[FEHLER] Ungültiges CSV-Format: {str(csv_err)}")
                    return
            
            if header is None:
                self._log("[FEHLER] Leere CSV-Datei")
                return
            
            # Normalize headers for consistent matching:
            # 1. Convert to lowercase
            # 2. Strip whitespace
            # 3. Replace spaces with underscores
            # 4. Convert CamelCase to snake_case (e.g., "RightsIdentifier" → "rights_identifier")
            import re
            def normalize_header(h: str) -> str:
                # First, handle CamelCase: insert underscore before uppercase letters
                # that follow lowercase letters (e.g., "RightsIdentifier" → "Rights_Identifier")
                h = re.sub(r'([a-z])([A-Z])', r'\1_\2', h)
                # Then lowercase, strip, and replace spaces with underscores
                return h.lower().strip().replace(' ', '_')
            
            header_normalized = [normalize_header(h) for h in header]
            header_set = set(header_normalized)
            
            # Detect CSV type by headers - order from most specific to least specific
            # to avoid false positives when multiple type-indicators are present
            
            # Define expected column sets for each type for robust validation
            # A type is detected if its required columns AND at least some optional columns exist
            
            # Pre-define indicator sets for each CSV type
            # With CamelCase normalization, we now consistently get snake_case
            rights_indicators = {'rights_identifier', 'right_identifier'}
            contributor_type_indicators = {'contributor_type'}
            content_url_indicators = {'content_url'}
            creator_name_indicators = {'creator_name'}
            name_parts_present = ('given_name' in header_set and 'family_name' in header_set)
            is_not_contributor = not (header_set & contributor_type_indicators)
            
            # 1. Rights (most specific - has unique identifiers AND doi)
            # Expected: doi, rights, rights_identifier, rights_uri
            if header_set & rights_indicators and 'doi' in header_set:
                self._log("→ Erkannt als: Rights/Lizenz-Daten")
                self.pending_csv_path = file_path
                self._on_update_rights_clicked()
            
            # 2. Contributors (specific - has contributor_type AND doi)
            # Expected: doi, contributor_name, contributor_type, etc.
            elif header_set & contributor_type_indicators and 'doi' in header_set:
                self._log("→ Erkannt als: Contributors-Daten")
                self.pending_csv_path = file_path
                self._on_update_contributors_clicked()
            
            # 3. Download URLs (specific - has content_url AND doi)
            # Expected: doi, content_url
            elif header_set & content_url_indicators and 'doi' in header_set:
                self._log("→ Erkannt als: Download-URLs")
                self.pending_csv_path = file_path
                self._on_update_download_urls_clicked()
            
            # 4. Authors/Creators (has creator-specific fields AND doi)
            # Expected: doi, creator_name or (given_name + family_name), optionally name_identifier
            # Note: After normalization, "creator name" becomes "creator_name"
            # Exclude files with contributor_type to avoid misclassifying Contributors as Authors
            elif ((header_set & creator_name_indicators) or name_parts_present) and \
                 'doi' in header_set and is_not_contributor:
                self._log("→ Erkannt als: Autoren-Daten")
                self.pending_csv_path = file_path
                self._on_update_authors_clicked()
            
            # 5. Publisher (has publisher column AND doi, but not creator-specific fields)
            # Expected: doi, publisher, optionally publisher_identifier
            elif 'publisher' in header_set and 'doi' in header_set and \
                 not (header_set & creator_name_indicators):
                self._log("→ Erkannt als: Publisher-Daten")
                self.pending_csv_path = file_path
                self._on_update_publisher_clicked()
            
            # 6. Landing Page URLs - only accept if explicit header is present
            # WARNING: We only auto-import when "landing_page_url" header is explicit.
            # For ambiguous cases (just "doi" + "url"), we don't auto-import to prevent
            # false positives - user must use the manual import function instead.
            elif 'landing_page_url' in header_set:
                self._log("→ Erkannt als: Landing Page URLs")
                self.pending_csv_path = file_path
                self._on_update_urls_clicked()
            elif 'doi' in header_set and 'url' in header_set:
                # Ambiguous case - don't auto-import, inform user with dialog
                # Note: header_set contains normalized headers (lowercase, spaces→underscores)
                self._log("[HINWEIS] CSV hat generische Header (doi, url).")
                QMessageBox.information(
                    self,
                    "CSV-Typ nicht eindeutig",
                    "Die CSV-Datei hat generische Header (doi, url).\n\n"
                    "Bitte verwenden Sie die spezifische Import-Funktion:\n"
                    "• Landing Page URLs → Aus CSV aktualisieren\n"
                    "• Oder CSV mit 'landing_page_url' Header exportieren\n\n"
                    "Diese Sicherheitsabfrage verhindert versehentliche\n"
                    "Datenänderungen bei mehrdeutigen CSV-Dateien.",
                    QMessageBox.Ok
                )
                # Don't set pending_csv_path or trigger import - user must do manually
            
            else:
                self._log(f"[WARNUNG] CSV-Typ nicht erkannt. Header: {', '.join(header[:5])}...")
                self._log("Bitte manuell die passende Import-Funktion wählen.")
                
        except Exception as e:
            self._log(f"[FEHLER] Konnte CSV nicht analysieren: {str(e)}")
    
    def _on_urls_card_action(self, action_id: str):
        """Handle action from URLs card dropdown."""
        if action_id == "update":
            self._on_update_urls_clicked()
    
    def _on_authors_card_action(self, action_id: str):
        """Handle action from Authors card dropdown."""
        if action_id == "update":
            self._on_update_authors_clicked()
    
    def _on_publisher_card_action(self, action_id: str):
        """Handle action from Publisher card dropdown."""
        if action_id == "update":
            self._on_update_publisher_clicked()
    
    def _on_contributors_card_action(self, action_id: str):
        """Handle action from Contributors card dropdown."""
        if action_id == "update":
            self._on_update_contributors_clicked()
    
    def _on_rights_card_action(self, action_id: str):
        """Handle action from Rights card dropdown."""
        if action_id == "update":
            self._on_update_rights_clicked()
    
    def _on_downloads_card_action(self, action_id: str):
        """Handle action from Downloads card dropdown."""
        if action_id == "update":
            self._on_update_download_urls_clicked()
    
    def _apply_styles(self):
        """Apply styling to the window based on current theme."""
        # Combine main window styles with component styles
        main_stylesheet = self.theme_manager.get_main_window_stylesheet()
        components_stylesheet = self.theme_manager.get_components_stylesheet()
        self.setStyleSheet(main_stylesheet + "\n" + components_stylesheet)
        
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
        
        # Check for publisher CSV
        publisher_csv_found = False
        publisher_csv_name = None
        
        # Check for contributors CSV
        contributors_csv_found = False
        contributors_csv_name = None
        
        # Check for rights CSV
        rights_csv_found = False
        rights_csv_name = None
        
        # If we have a username, check for specific files
        if self._current_username:
            urls_csv_path = output_dir / f"{self._current_username}_urls.csv"
            authors_csv_path = output_dir / f"{self._current_username}_authors.csv"
            publisher_csv_path = output_dir / f"{self._current_username}_publishers.csv"
            contributors_csv_path = output_dir / f"{self._current_username}_contributors.csv"
            rights_csv_path = output_dir / f"{self._current_username}_rights.csv"
            
            if urls_csv_path.exists():
                urls_csv_found = True
                urls_csv_name = urls_csv_path.name
            
            if authors_csv_path.exists():
                authors_csv_found = True
                authors_csv_name = authors_csv_path.name
            
            if publisher_csv_path.exists():
                publisher_csv_found = True
                publisher_csv_name = publisher_csv_path.name
            
            if contributors_csv_path.exists():
                contributors_csv_found = True
                contributors_csv_name = contributors_csv_path.name
            
            if rights_csv_path.exists():
                rights_csv_found = True
                rights_csv_name = rights_csv_path.name
        else:
            # Check for any *_urls.csv, *_authors.csv, *_publishers.csv, *_contributors.csv, *_rights.csv files
            urls_files = list(output_dir.glob("*_urls.csv"))
            authors_files = list(output_dir.glob("*_authors.csv"))
            publisher_files = list(output_dir.glob("*_publishers.csv"))
            contributors_files = list(output_dir.glob("*_contributors.csv"))
            rights_files = list(output_dir.glob("*_rights.csv"))
            
            if urls_files:
                urls_csv_found = True
                urls_csv_name = urls_files[0].name
            
            if authors_files:
                authors_csv_found = True
                authors_csv_name = authors_files[0].name
            
            if publisher_files:
                publisher_csv_found = True
                publisher_csv_name = publisher_files[0].name
            
            if contributors_files:
                contributors_csv_found = True
                contributors_csv_name = contributors_files[0].name
            
            if rights_files:
                rights_csv_found = True
                rights_csv_name = rights_files[0].name
        
        # Update URLs status
        if urls_csv_found:
            self.urls_card.set_status(f"CSV bereit: {urls_csv_name}", is_ready=True)
            self.urls_card.set_action_enabled("update", True)
        else:
            self.urls_card.set_status("Keine CSV-Datei gefunden", is_ready=False)
            self.urls_card.set_action_enabled("update", False)
        
        # Update authors status
        if authors_csv_found:
            self.authors_card.set_status(f"CSV bereit: {authors_csv_name}", is_ready=True)
            self.authors_card.set_action_enabled("update", True)
        else:
            self.authors_card.set_status("Keine CSV-Datei gefunden", is_ready=False)
            self.authors_card.set_action_enabled("update", False)
        
        # Update publisher status
        if publisher_csv_found:
            self.publisher_card.set_status(f"CSV bereit: {publisher_csv_name}", is_ready=True)
            self.publisher_card.set_action_enabled("update", True)
        else:
            self.publisher_card.set_status("Keine CSV-Datei gefunden", is_ready=False)
            self.publisher_card.set_action_enabled("update", False)
        
        # Update contributors status
        if contributors_csv_found:
            self.contributors_card.set_status(f"CSV bereit: {contributors_csv_name}", is_ready=True)
            self.contributors_card.set_action_enabled("update", True)
        else:
            self.contributors_card.set_status("Keine CSV-Datei gefunden", is_ready=False)
            self.contributors_card.set_action_enabled("update", False)
        
        # Update rights status
        if rights_csv_found:
            self.rights_card.set_status(f"CSV bereit: {rights_csv_name}", is_ready=True)
            self.rights_card.set_action_enabled("update", True)
        else:
            self.rights_card.set_status("Keine CSV-Datei gefunden", is_ready=False)
            self.rights_card.set_action_enabled("update", False)
    
    def _log(self, message):
        """
        Add a message to the log area.
        
        Args:
            message: Message to log
        """
        self.log_text.append(message)
        logger.info(message)
    
    def _set_buttons_enabled(self, enabled: bool):
        """
        Enable or disable all main action cards.
        
        Args:
            enabled: True to enable cards, False to disable
        """
        # Enable/disable all action cards
        self.urls_card.setEnabled(enabled)
        self.authors_card.setEnabled(enabled)
        self.publisher_card.setEnabled(enabled)
        self.contributors_card.setEnabled(enabled)
        self.rights_card.setEnabled(enabled)
        self.downloads_card.setEnabled(enabled)
        self.pending_card.setEnabled(enabled)
        self.fuji_card.setEnabled(enabled)
        self.dead_links_card.setEnabled(enabled)
    
    def _format_error_list(self, items: list, max_items: int = 10, bullet: str = "") -> str:
        """
        Format a list of items for display, limiting to first N items.
        
        Args:
            items: List of items (strings) to format
            max_items: Maximum number of items to show (default: 10)
            bullet: Optional bullet character to prefix each item
            
        Returns:
            Formatted string with items, possibly truncated with "... und X weitere"
        """
        if not items:
            return ""
        
        prefix = f"{bullet} " if bullet else ""
        display_items = items[:max_items]
        result = "\n".join(f"{prefix}{item}" for item in display_items)
        
        if len(items) > max_items:
            result += f"\n... und {len(items) - max_items} weitere Fehler"
        
        return result
    
    def _open_csv_splitter(self):
        """Open CSV splitter dialog."""
        try:
            dialog = CSVSplitterDialog(self)
            dialog.exec()
            logger.info("CSV Splitter dialog closed")
            # Note: deleteLater() not needed - modal dialog will be garbage collected
        except Exception as e:
            logger.error(f"Error opening CSV splitter dialog: {e}")
            QMessageBox.warning(
                self,
                "Fehler",
                f"Der CSV-Splitter-Dialog konnte nicht geöffnet werden:\n\n{str(e)}"
            )
    
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
        # Apply new styles immediately
        self._apply_styles()
        
        # Log message
        if theme == Theme.AUTO:
            effective = self.theme_manager.get_effective_theme()
            self._log(f"🔄 Auto Mode aktiviert (System: {('Light' if effective == Theme.LIGHT else 'Dark')} Mode)")
        elif theme == Theme.DARK:
            self._log("🌙 Dark Mode aktiviert")
        else:
            self._log("☀️ Light Mode aktiviert")
        
        logger.info(f"Theme changed to: {theme.value}")

    
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
        self.load_publisher_button.setEnabled(False)
        self.update_button.setEnabled(False)
        self.update_authors_button.setEnabled(False)
        self.update_publisher_button.setEnabled(False)
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
        self._set_buttons_enabled(True)
        
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
        self._set_buttons_enabled(False)
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
        self._set_buttons_enabled(True)
        
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
        self._set_buttons_enabled(False)
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
            error_details = self._format_error_list(error_list, max_items=10)
            
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
        self._set_buttons_enabled(True)
        
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
        self._set_buttons_enabled(False)
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
            # Show errors and ask if user wants to continue (show first 10 errors)
            invalid_results = [r for r in validation_results if not r['valid']]
            error_items = [f"{r['doi']}: {r['message']}" for r in invalid_results[:10]]
            error_details = self._format_error_list(error_items, max_items=10, bullet="•")
            if len(invalid_results) > 10:
                error_details += f"\n... und {len(invalid_results) - 10} weitere Fehler"
            
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
        self._set_buttons_enabled(False)
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
                error_details = self._format_error_list(error_list, max_items=10)
                
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
        self._set_buttons_enabled(True)
        
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
    
    # ==================== PUBLISHER METHODS ====================
    
    def _on_load_publisher_clicked(self):
        """Handle load publisher button click."""
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
        self._log(f"Starte Publisher-Abruf für Benutzer '{username}' ({api_type})...")
        
        # Disable buttons and show progress
        self._set_buttons_enabled(False)
        self.progress_bar.setVisible(True)
        
        # Create worker and thread
        self.publisher_worker = DOIPublisherFetchWorker(username, password, use_test_api, credentials_are_new)
        self.publisher_thread = QThread()
        self.publisher_worker.moveToThread(self.publisher_thread)
        
        # Connect signals
        self.publisher_thread.started.connect(self.publisher_worker.run)
        self.publisher_worker.progress.connect(self._log)
        self.publisher_worker.finished.connect(self._on_publisher_fetch_finished)
        self.publisher_worker.error.connect(self._on_publisher_fetch_error)
        self.publisher_worker.request_save_credentials.connect(self._on_request_save_credentials)
        
        # Clean up after worker finishes or errors
        self.publisher_worker.finished.connect(self.publisher_worker.deleteLater)
        self.publisher_worker.error.connect(self.publisher_worker.deleteLater)
        self.publisher_worker.finished.connect(self.publisher_thread.quit)
        self.publisher_worker.error.connect(self.publisher_thread.quit)
        
        # Clean up thread when it finishes
        self.publisher_thread.finished.connect(self.publisher_thread.deleteLater)
        self.publisher_thread.finished.connect(self._cleanup_publisher_thread)
        
        # Start the thread
        self.publisher_thread.start()
    
    def _on_publisher_fetch_finished(self, publisher_data, username, _warnings_count):
        """
        Handle successful publisher fetch.
        
        Args:
            publisher_data: List of publisher tuples (DOI, Name, Identifier, ...)
            username: DataCite username
            _warnings_count: Ignored, we calculate warnings during export
        """
        if not publisher_data:
            self._log("[WARNUNG] Keine DOIs mit Publisher-Daten gefunden.")
            QMessageBox.information(
                self,
                "Keine Publisher",
                f"Für den Benutzer '{username}' wurden keine DOIs mit Publisher-Daten gefunden."
            )
            return
        
        # Export to CSV
        try:
            output_dir = os.getcwd()
            filepath, warnings_count = export_dois_with_publisher_to_csv(publisher_data, username, output_dir)
            
            self._log(f"[OK] CSV-Datei erfolgreich erstellt: {filepath}")
            
            if warnings_count > 0:
                self._log(f"[WARNUNG] {warnings_count} DOI(s) ohne Publisher Identifier")
            
            # Update username and check CSV files
            self._current_username = username
            self._check_csv_files()
            
            message = (
                f"{len(publisher_data)} DOIs mit Publisher-Daten wurden erfolgreich exportiert.\n\n"
                f"Datei: {Path(filepath).name}\n"
                f"Verzeichnis: {output_dir}"
            )
            if warnings_count > 0:
                message += f"\n\n⚠️ {warnings_count} DOI(s) haben keinen Publisher Identifier."
            
            QMessageBox.information(
                self,
                "Erfolg",
                message
            )
            
        except CSVExportError as e:
            self._log(f"[FEHLER] Fehler beim CSV-Export: {str(e)}")
            QMessageBox.critical(
                self,
                "Fehler beim Export",
                f"Die CSV-Datei konnte nicht erstellt werden:\n\n{str(e)}"
            )
    
    def _on_publisher_fetch_error(self, error_message):
        """
        Handle publisher fetch error.
        
        Args:
            error_message: Error message
        """
        self._log(f"[FEHLER] {error_message}")
        
        QMessageBox.critical(
            self,
            "Fehler",
            f"Beim Abrufen der Publisher-Daten ist ein Fehler aufgetreten:\n\n{error_message}"
        )
    
    def _cleanup_publisher_thread(self):
        """Clean up publisher thread and worker after completion."""
        self.progress_bar.setVisible(False)
        self._set_buttons_enabled(True)
        
        # Reset references (objects are deleted via deleteLater)
        self.publisher_thread = None
        self.publisher_worker = None
        
        self._log("Bereit für nächsten Vorgang.")
    
    def _on_update_publisher_clicked(self):
        """Handle update publisher button click."""
        # Show credentials dialog in update_publisher mode
        dialog = CredentialsDialog(self, mode="update_publisher")
        credentials = dialog.get_credentials()
        
        if credentials is None:
            self._log("Publisher-Update abgebrochen.")
            return
        
        username, password, csv_path, use_test_api = credentials
        
        # Check if user selected new credentials or loaded saved account
        credentials_are_new = dialog.is_new_credentials()
        
        # Store credentials for second worker (actual update)
        self._publisher_update_username = username
        self._publisher_update_password = password
        self._publisher_update_csv_path = csv_path
        self._publisher_update_use_test_api = use_test_api
        self._publisher_update_credentials_are_new = credentials_are_new
        
        api_type = "Test-API" if use_test_api else "Produktions-API"
        self._log(f"Starte Publisher-Update für Benutzer '{username}' ({api_type})...")
        self._log(f"CSV-Datei: {Path(csv_path).name}")
        
        # Disable buttons and show progress
        self._set_buttons_enabled(False)
        self.progress_bar.setVisible(True)
        
        # Create worker and thread for DRY RUN (validation phase)
        self.publisher_update_worker = PublisherUpdateWorker(
            username, password, csv_path, use_test_api, dry_run_only=True, credentials_are_new=credentials_are_new
        )
        self.publisher_update_thread = QThread()
        self.publisher_update_worker.moveToThread(self.publisher_update_thread)
        
        # Connect signals
        self.publisher_update_thread.started.connect(self.publisher_update_worker.run)
        self.publisher_update_worker.progress_update.connect(self._on_publisher_update_progress)
        self.publisher_update_worker.validation_update.connect(self._on_publisher_validation_update)
        self.publisher_update_worker.dry_run_complete.connect(self._on_publisher_dry_run_complete)
        self.publisher_update_worker.finished.connect(self._on_publisher_update_finished)
        self.publisher_update_worker.error_occurred.connect(self._on_publisher_update_error)
        
        # Clean up after worker finishes
        self.publisher_update_worker.finished.connect(self.publisher_update_worker.deleteLater)
        self.publisher_update_worker.finished.connect(self.publisher_update_thread.quit)
        
        # Clean up thread when it finishes
        self.publisher_update_thread.finished.connect(self.publisher_update_thread.deleteLater)
        self.publisher_update_thread.finished.connect(self._cleanup_publisher_update_thread)
        
        # Start the thread
        self.publisher_update_thread.start()
    
    def _on_publisher_update_progress(self, current, total, message):
        """
        Handle publisher update progress signal.
        
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
    
    def _on_publisher_validation_update(self, message):
        """
        Handle publisher validation phase progress signal.
        
        Args:
            message: Validation status message
        """
        self._log(message)
    
    def _on_publisher_database_update(self, message):
        """
        Handle publisher database update progress signal.
        
        Args:
            message: Database update status message
        """
        self._log(message)
    
    def _on_publisher_datacite_update(self, message):
        """
        Handle publisher DataCite update progress signal.
        
        Args:
            message: DataCite update status message
        """
        self._log(message)
    
    def _on_publisher_dry_run_complete(self, valid_count, invalid_count, validation_results):
        """
        Handle publisher dry run validation completion.
        
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
            # Show errors and ask if user wants to continue (show first 10 errors)
            invalid_results = [r for r in validation_results if not r['valid']]
            error_items = [f"{r['doi']}: {r['message']}" for r in invalid_results[:10]]
            error_details = self._format_error_list(error_items, max_items=10, bullet="•")
            if len(invalid_results) > 10:
                error_details += f"\n... und {len(invalid_results) - 10} weitere Fehler"
            
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
                self._start_actual_publisher_update()
            else:
                self._log("Publisher-Update abgebrochen.")
        else:
            # All valid - ask for confirmation
            reply = QMessageBox.question(
                self,
                "Validierung erfolgreich",
                f"Alle {valid_count} DOIs wurden erfolgreich validiert.\n\n"
                f"Möchtest du jetzt die Publisher-Metadaten bei DataCite aktualisieren?",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.Yes
            )
            
            if reply == QMessageBox.Yes:
                self._start_actual_publisher_update()
            else:
                self._log("Publisher-Update abgebrochen.")
    
    def _start_actual_publisher_update(self):
        """Start the actual publisher update process (not dry run)."""
        self._log("\n" + "=" * 60)
        self._log("Starte ECHTES Update der Publisher-Metadaten...")
        self._log("=" * 60)
        
        # Get credentials from stored instance variables (not from worker which may be deleted)
        username = self._publisher_update_username
        password = self._publisher_update_password
        csv_path = self._publisher_update_csv_path
        use_test_api = self._publisher_update_use_test_api
        credentials_are_new = self._publisher_update_credentials_are_new
        
        # Disable buttons again
        self._set_buttons_enabled(False)
        self.progress_bar.setVisible(True)
        self.progress_bar.setMaximum(0)  # Indeterminate
        
        # Create new worker with dry_run_only=False
        self.publisher_update_worker = PublisherUpdateWorker(
            username, password, csv_path, use_test_api, dry_run_only=False, credentials_are_new=credentials_are_new
        )
        self.publisher_update_thread = QThread()
        self.publisher_update_worker.moveToThread(self.publisher_update_thread)
        
        # Connect signals (no dry_run_complete this time)
        self.publisher_update_thread.started.connect(self.publisher_update_worker.run)
        self.publisher_update_worker.progress_update.connect(self._on_publisher_update_progress)
        self.publisher_update_worker.validation_update.connect(self._on_publisher_validation_update)
        self.publisher_update_worker.database_update.connect(self._on_publisher_database_update)
        self.publisher_update_worker.datacite_update.connect(self._on_publisher_datacite_update)
        self.publisher_update_worker.doi_updated.connect(self._on_publisher_doi_updated)
        self.publisher_update_worker.finished.connect(self._on_publisher_update_finished)
        self.publisher_update_worker.error_occurred.connect(self._on_publisher_update_error)
        self.publisher_update_worker.request_save_credentials.connect(self._on_request_save_credentials)
        
        # Clean up after worker finishes
        self.publisher_update_worker.finished.connect(self.publisher_update_worker.deleteLater)
        self.publisher_update_worker.finished.connect(self.publisher_update_thread.quit)
        
        # Clean up thread when it finishes
        self.publisher_update_thread.finished.connect(self.publisher_update_thread.deleteLater)
        self.publisher_update_thread.finished.connect(self._cleanup_publisher_update_thread)
        
        # Start the thread
        self.publisher_update_thread.start()
    
    def _on_publisher_doi_updated(self, doi, success, message):
        """
        Handle individual DOI publisher update result.
        
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
    
    def _on_publisher_update_finished(self, success_count, error_count, skipped_count, error_list, skipped_details):
        """
        Handle publisher update completion.
        
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
            self._log(f"Publisher-Update abgeschlossen: {success_count} erfolgreich, {skipped_count} übersprungen (keine Änderungen), {error_count} fehlgeschlagen")
        else:
            self._log("Publisher-Update abgeschlossen")
        self._log("=" * 60)
        
        # Check CSV files (in case user deleted/modified them during update)
        self._check_csv_files()
        
        # Show summary dialog (only if not dry run)
        if self.publisher_update_worker and not self.publisher_update_worker.dry_run_only:
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
                error_details = self._format_error_list(error_list, max_items=10)
                
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
                self._create_publisher_update_log(success_count, skipped_count, error_count, error_list, skipped_details)
    
    def _on_publisher_update_error(self, error_message):
        """
        Handle critical publisher update error.
        
        Args:
            error_message: Error message
        """
        self._log(f"[KRITISCHER FEHLER] {error_message}")
        
        QMessageBox.critical(
            self,
            "Publisher-Update Fehler",
            f"Ein kritischer Fehler ist aufgetreten:\n\n{error_message}"
        )
    
    def _cleanup_publisher_update_thread(self):
        """Clean up publisher update thread and worker after completion."""
        self.progress_bar.setVisible(False)
        self.progress_bar.setMaximum(0)  # Reset to indeterminate
        self._set_buttons_enabled(True)
        
        # Reset references (objects are deleted via deleteLater)
        self.publisher_update_thread = None
        self.publisher_update_worker = None
        
        self._log("Bereit für nächsten Vorgang.")
    
    def _create_publisher_update_log(self, success_count, skipped_count, error_count, error_list, skipped_details):
        """
        Create a log file with publisher update results.
        
        Args:
            success_count: Number of successful updates
            skipped_count: Number of skipped DOIs (no changes)
            error_count: Number of failed updates
            error_list: List of error messages
            skipped_details: List of (doi, reason) tuples for skipped DOIs
        """
        try:
            # Check if database sync is enabled (QSettings already imported at module level)
            settings = QSettings("GFZ", "GROBI")
            db_enabled = settings.value("database/enabled", False, type=bool)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            log_filename = f"publisher_update_log_{timestamp}.txt"
            log_path = Path(os.getcwd()) / log_filename
            
            total = success_count + skipped_count + error_count
            efficiency_gain = (skipped_count / total * 100) if total > 0 else 0
            
            with open(log_path, 'w', encoding='utf-8') as f:
                f.write("=" * 70 + "\n")
                f.write("GROBI - Publisher-Metadaten Update Log\n")
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
                    f.write("1. Datenbank wird ZUERST aktualisiert (sofortiges COMMIT)\n")
                    f.write("2. DataCite wird DANACH aktualisiert (nur wenn DB erfolgreich)\n")
                    f.write("3. Bei DataCite-Fehlern wird der Fehler protokolliert\n")
                    f.write("\n")
                    f.write("HINWEIS: Falls INKONSISTENZEN auftraten, müssen diese manuell\n")
                    f.write("         korrigiert werden (Datenbank committed, DataCite failed).\n")
                
                f.write("\n")
                f.write("=" * 70 + "\n")
            
            self._log(f"[OK] Log-Datei erstellt: {log_filename}")
            
        except Exception as e:
            self._log(f"[WARNUNG] Log-Datei konnte nicht erstellt werden: {str(e)}")
    
    # ==================== CONTRIBUTORS METHODS ====================
    
    def _on_load_contributors_clicked(self):
        """Handle load contributors button click."""
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
        self._log(f"Starte Contributors-Abruf für Benutzer '{username}' ({api_type})...")
        
        # Disable buttons and show progress
        self._set_buttons_enabled(False)
        self.progress_bar.setVisible(True)
        
        # Create worker and thread
        self.contributor_worker = DOIContributorFetchWorker(username, password, use_test_api, credentials_are_new)
        self.contributor_thread = QThread()
        self.contributor_worker.moveToThread(self.contributor_thread)
        
        # Connect signals
        self.contributor_thread.started.connect(self.contributor_worker.run)
        self.contributor_worker.progress.connect(self._log)
        self.contributor_worker.finished.connect(self._on_contributor_fetch_finished)
        self.contributor_worker.error.connect(self._on_contributor_fetch_error)
        self.contributor_worker.request_save_credentials.connect(self._on_request_save_credentials)
        
        # Clean up after worker finishes or errors
        self.contributor_worker.finished.connect(self.contributor_worker.deleteLater)
        self.contributor_worker.error.connect(self.contributor_worker.deleteLater)
        self.contributor_worker.finished.connect(self.contributor_thread.quit)
        self.contributor_worker.error.connect(self.contributor_thread.quit)
        
        # Clean up thread when it finishes
        self.contributor_thread.finished.connect(self.contributor_thread.deleteLater)
        self.contributor_thread.finished.connect(self._cleanup_contributor_thread)
        
        # Start the thread
        self.contributor_thread.start()
    
    def _on_contributor_fetch_finished(self, contributor_data, username):
        """
        Handle successful contributor fetch.
        
        Args:
            contributor_data: List of contributor tuples (DOI, Contributor Name, Name Type, etc.)
            username: DataCite username
        """
        if not contributor_data:
            self._log("[WARNUNG] Keine DOIs mit Contributors gefunden.")
            QMessageBox.information(
                self,
                "Keine Contributors",
                f"Für den Benutzer '{username}' wurden keine DOIs mit Contributors gefunden."
            )
            return
        
        # Export to CSV
        try:
            output_dir = os.getcwd()
            filepath = export_dois_with_contributors_to_csv(contributor_data, username, output_dir)
            
            # Count unique DOIs
            unique_dois = len(set(row[0] for row in contributor_data))
            
            self._log(f"[OK] CSV-Datei erfolgreich erstellt: {filepath}")
            
            # Update username and check CSV files
            self._current_username = username
            self._check_csv_files()
            
            QMessageBox.information(
                self,
                "Erfolg",
                f"{unique_dois} DOIs mit {len(contributor_data)} Contributors wurden erfolgreich exportiert.\n\n"
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
    
    def _on_contributor_fetch_error(self, error_message):
        """
        Handle contributor fetch error.
        
        Args:
            error_message: Error message
        """
        self._log(f"[FEHLER] {error_message}")
        
        QMessageBox.critical(
            self,
            "Fehler",
            f"Beim Abrufen der Contributors ist ein Fehler aufgetreten:\n\n{error_message}"
        )
    
    def _cleanup_contributor_thread(self):
        """Clean up contributor thread and worker after completion."""
        self.progress_bar.setVisible(False)
        self._set_buttons_enabled(True)
        
        # Reset references (objects are deleted via deleteLater)
        self.contributor_thread = None
        self.contributor_worker = None
        
        self._log("Bereit für nächsten Vorgang.")
    
    def _on_update_contributors_clicked(self):
        """Handle update contributors button click."""
        # Show credentials dialog in update_contributors mode
        dialog = CredentialsDialog(self, mode="update_contributors")
        credentials = dialog.get_credentials()
        
        if credentials is None:
            self._log("Contributors-Update abgebrochen.")
            return
        
        username, password, csv_path, use_test_api = credentials
        
        # Check if user selected new credentials or loaded saved account
        credentials_are_new = dialog.is_new_credentials()
        
        # Store credentials for second worker (actual update)
        self._contributors_update_username = username
        self._contributors_update_password = password
        self._contributors_update_csv_path = csv_path
        self._contributors_update_use_test_api = use_test_api
        self._contributors_update_credentials_are_new = credentials_are_new
        
        api_type = "Test-API" if use_test_api else "Produktions-API"
        self._log(f"Starte Contributors-Update für Benutzer '{username}' ({api_type})...")
        self._log(f"CSV-Datei: {Path(csv_path).name}")
        
        # Disable buttons and show progress
        self._set_buttons_enabled(False)
        self.progress_bar.setVisible(True)
        
        # Create worker and thread for DRY RUN
        self.contributors_update_worker = ContributorsUpdateWorker(
            username, password, csv_path, use_test_api, dry_run_only=True, credentials_are_new=False
        )
        self.contributors_update_thread = QThread()
        self.contributors_update_worker.moveToThread(self.contributors_update_thread)
        
        # Connect signals
        self.contributors_update_thread.started.connect(self.contributors_update_worker.run)
        self.contributors_update_worker.progress_update.connect(self._on_contributors_update_progress)
        self.contributors_update_worker.validation_update.connect(self._on_validation_update)
        self.contributors_update_worker.dry_run_complete.connect(self._on_contributors_dry_run_complete)
        self.contributors_update_worker.finished.connect(self._on_contributors_update_finished)
        self.contributors_update_worker.error_occurred.connect(self._on_contributors_update_error)
        
        # Clean up after worker finishes
        self.contributors_update_worker.finished.connect(self.contributors_update_worker.deleteLater)
        self.contributors_update_worker.finished.connect(self.contributors_update_thread.quit)
        
        # Clean up thread when it finishes
        self.contributors_update_thread.finished.connect(self.contributors_update_thread.deleteLater)
        self.contributors_update_thread.finished.connect(self._cleanup_contributors_update_thread)
        
        # Start the thread
        self.contributors_update_thread.start()
    
    def _on_contributors_update_progress(self, current, total, message):
        """
        Handle contributors update progress signal.
        
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
    
    def _on_contributors_dry_run_complete(self, valid_count, invalid_count, validation_results):
        """
        Handle contributors dry run validation completion.
        
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
            # Show errors and ask if user wants to continue (show first 10 errors)
            invalid_results = [r for r in validation_results if not r['valid']]
            error_items = [f"{r['doi']}: {r['message']}" for r in invalid_results[:10]]
            error_details = self._format_error_list(error_items, max_items=10, bullet="•")
            if len(invalid_results) > 10:
                error_details += f"\n... und {len(invalid_results) - 10} weitere Fehler"
            
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
                self._start_actual_contributors_update()
            else:
                self._log("Contributors-Update abgebrochen.")
        else:
            # All valid - ask for confirmation
            reply = QMessageBox.question(
                self,
                "Validierung erfolgreich",
                f"Alle {valid_count} DOIs wurden erfolgreich validiert.\n\n"
                f"Möchtest du jetzt die Contributors-Metadaten bei DataCite aktualisieren?",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.Yes
            )
            
            if reply == QMessageBox.Yes:
                self._start_actual_contributors_update()
            else:
                self._log("Contributors-Update abgebrochen.")
    
    def _start_actual_contributors_update(self):
        """Start the actual contributors update process (not dry run)."""
        self._log("\n" + "=" * 60)
        self._log("Starte ECHTES Update der Contributors-Metadaten...")
        self._log("=" * 60)
        
        # Get credentials from stored instance variables (not from worker which may be deleted)
        username = self._contributors_update_username
        password = self._contributors_update_password
        csv_path = self._contributors_update_csv_path
        use_test_api = self._contributors_update_use_test_api
        credentials_are_new = self._contributors_update_credentials_are_new
        
        # Disable buttons again
        self._set_buttons_enabled(False)
        self.progress_bar.setVisible(True)
        self.progress_bar.setMaximum(0)  # Indeterminate
        
        # Create new worker with dry_run_only=False
        self.contributors_update_worker = ContributorsUpdateWorker(
            username, password, csv_path, use_test_api, dry_run_only=False, credentials_are_new=credentials_are_new
        )
        self.contributors_update_thread = QThread()
        self.contributors_update_worker.moveToThread(self.contributors_update_thread)
        
        # Connect signals (no dry_run_complete this time)
        self.contributors_update_thread.started.connect(self.contributors_update_worker.run)
        self.contributors_update_worker.progress_update.connect(self._on_contributors_update_progress)
        self.contributors_update_worker.validation_update.connect(self._on_validation_update)
        self.contributors_update_worker.database_update.connect(self._on_database_update)
        self.contributors_update_worker.datacite_update.connect(self._on_datacite_update)
        self.contributors_update_worker.doi_updated.connect(self._on_contributor_doi_updated)
        self.contributors_update_worker.finished.connect(self._on_contributors_update_finished)
        self.contributors_update_worker.error_occurred.connect(self._on_contributors_update_error)
        self.contributors_update_worker.request_save_credentials.connect(self._on_request_save_credentials)
        
        # Clean up after worker finishes
        self.contributors_update_worker.finished.connect(self.contributors_update_worker.deleteLater)
        self.contributors_update_worker.finished.connect(self.contributors_update_thread.quit)
        
        # Clean up thread when it finishes
        self.contributors_update_thread.finished.connect(self.contributors_update_thread.deleteLater)
        self.contributors_update_thread.finished.connect(self._cleanup_contributors_update_thread)
        
        # Start the thread
        self.contributors_update_thread.start()
    
    def _on_contributor_doi_updated(self, doi, success, message):
        """
        Handle individual DOI contributor update result.
        
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
    
    def _on_contributors_update_finished(self, success_count, error_count, skipped_count, error_list, skipped_details):
        """
        Handle contributors update completion.
        
        Args:
            success_count: Number of successful updates
            error_count: Number of failed updates
            skipped_count: Number of skipped DOIs (no changes)
            error_list: List of error messages
            skipped_details: List of (doi, reason) tuples for skipped DOIs
        """
        total = success_count + error_count + skipped_count
        
        self._log("=" * 60)
        self._log(f"Contributors-Update abgeschlossen: {success_count} OK, {skipped_count} übersprungen, {error_count} Fehler")
        self._log("=" * 60)
        
        if total == 0:
            QMessageBox.warning(
                self,
                "Keine DOIs verarbeitet",
                "Es wurden keine DOIs verarbeitet."
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
            error_details = self._format_error_list(error_list, max_items=10)
            
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
            self._create_contributors_update_log(success_count, skipped_count, error_count, error_list, skipped_details)
    
    def _on_contributors_update_error(self, error_message):
        """
        Handle critical contributors update error.
        
        Args:
            error_message: Error message
        """
        self._log(f"[KRITISCHER FEHLER] {error_message}")
        
        QMessageBox.critical(
            self,
            "Contributors-Update Fehler",
            f"Ein kritischer Fehler ist aufgetreten:\n\n{error_message}"
        )
    
    def _cleanup_contributors_update_thread(self):
        """Clean up contributors update thread and worker after completion."""
        self.progress_bar.setVisible(False)
        self.progress_bar.setMaximum(0)  # Reset to indeterminate
        self._set_buttons_enabled(True)
        
        # Reset references (objects are deleted via deleteLater)
        self.contributors_update_thread = None
        self.contributors_update_worker = None
        
        self._log("Bereit für nächsten Vorgang.")
    
    def _create_contributors_update_log(self, success_count, skipped_count, error_count, error_list, skipped_details):
        """
        Create a log file with contributors update results.
        
        Args:
            success_count: Number of successful updates
            skipped_count: Number of skipped DOIs (no changes)
            error_count: Number of failed updates
            error_list: List of error messages
            skipped_details: List of (doi, reason) tuples for skipped DOIs
        """
        try:
            # Check if database sync is enabled
            settings = QSettings("GFZ", "GROBI")
            db_enabled = settings.value("database/enabled", False, type=bool)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            log_filename = f"contributors_update_log_{timestamp}.txt"
            log_path = Path(os.getcwd()) / log_filename
            
            total = success_count + skipped_count + error_count
            efficiency_gain = (skipped_count / total * 100) if total > 0 else 0
            
            with open(log_path, 'w', encoding='utf-8') as f:
                f.write("=" * 70 + "\n")
                f.write("GROBI - Contributors-Metadaten Update Log\n")
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
                    f.write("CONTRIBUTORS-BESONDERHEITEN:\n")
                    f.write("- Mehrere ContributorTypes pro Contributor werden unterstützt\n")
                    f.write("- Email/Website/Position nur für ContactPerson in DB gespeichert\n")
                    f.write("- DataCite erhält nur den ersten ContributorType\n")
                
                f.write("\n")
                f.write("=" * 70 + "\n")
            
            self._log(f"[OK] Log-Datei erstellt: {log_filename}")
            
        except Exception as e:
            self._log(f"[WARNUNG] Log-Datei konnte nicht erstellt werden: {str(e)}")
    
    # ==================== DOWNLOAD URLs METHODS ====================
    
    def _on_export_download_urls_clicked(self):
        """Handle export download URLs button click."""
        # Check if database is configured
        settings = QSettings("GFZ", "GROBI")
        db_enabled = settings.value("database/enabled", False, type=bool)
        
        if not db_enabled:
            QMessageBox.warning(
                self,
                "Datenbank nicht konfiguriert",
                "Die Datenbank-Verbindung ist nicht aktiviert.\n\n"
                "Bitte konfiguriere die Datenbank-Verbindung in den Einstellungen "
                "(Einstellungen → Datenbank-Verbindung)."
            )
            return
        
        # Load DB credentials
        from src.utils.credential_manager import load_db_credentials
        
        db_creds = load_db_credentials()
        if not db_creds:
            QMessageBox.warning(
                self,
                "Keine DB-Credentials",
                "Keine Datenbank-Zugangsdaten gespeichert.\n\n"
                "Bitte konfiguriere die Datenbank in den Einstellungen."
            )
            return
        
        # Start worker
        self._start_download_url_fetch(db_creds)
    
    def _start_download_url_fetch(self, db_creds: dict):
        """Start worker to fetch DOIs with download URLs from database."""
        self._log("Starte Download-URL Export...")
        
        # Import worker
        from src.workers.download_url_fetch_worker import DownloadURLFetchWorker
        
        # Create worker
        self.download_url_worker = DownloadURLFetchWorker(
            db_host=db_creds['host'],
            db_name=db_creds['database'],
            db_user=db_creds['username'],
            db_password=db_creds['password']
        )
        
        # Connect signals
        self.download_url_worker.progress.connect(self._log)
        self.download_url_worker.finished.connect(self._on_download_urls_fetched)
        self.download_url_worker.error.connect(self._on_download_urls_error)
        
        # Create thread
        self.download_url_thread = QThread()
        self.download_url_worker.moveToThread(self.download_url_thread)
        
        # Connect thread signals
        self.download_url_thread.started.connect(self.download_url_worker.run)
        self.download_url_worker.finished.connect(self.download_url_thread.quit)
        self.download_url_worker.error.connect(self.download_url_thread.quit)
        
        # Clean up after worker finishes or errors
        self.download_url_worker.finished.connect(self.download_url_worker.deleteLater)
        self.download_url_worker.error.connect(self.download_url_worker.deleteLater)
        
        # Clean up thread when it finishes
        self.download_url_thread.finished.connect(self.download_url_thread.deleteLater)
        self.download_url_thread.finished.connect(self._cleanup_download_url_worker)
        
        # Start thread
        self.download_url_thread.start()
        
        # Disable button during fetch
        self.export_download_urls_btn.setEnabled(False)
        self.progress_bar.setVisible(True)
    
    def _on_download_urls_fetched(self, dois_files):
        """Called when DOIs with download URLs have been fetched."""
        from PySide6.QtWidgets import QFileDialog
        from src.utils.csv_exporter import export_dois_download_urls
        
        self._log(f"[OK] {len(dois_files)} Datei-Einträge abgerufen")
        
        # Use current username if available, otherwise use generic name
        if self._current_username:
            default_filename = f"{self._current_username}_download_urls.csv"
        else:
            default_filename = "download_urls.csv"
        
        filepath, _ = QFileDialog.getSaveFileName(
            self,
            "CSV-Datei speichern",
            default_filename,
            "CSV Files (*.csv)"
        )
        
        if filepath:
            try:
                export_dois_download_urls(dois_files, filepath)
                
                # Count unique DOIs
                unique_dois = len(set(doi for doi, _, _, _, _, _ in dois_files))
                
                self._log(f"[OK] CSV-Datei gespeichert: {filepath}")
                QMessageBox.information(
                    self,
                    "Export erfolgreich",
                    f"DOIs und Download-URLs wurden exportiert:\n\n"
                    f"Datei: {Path(filepath).name}\n"
                    f"DOIs: {unique_dois}\n"
                    f"Dateien: {len(dois_files)}"
                )
            except Exception as e:
                self._log(f"[FEHLER] CSV-Export fehlgeschlagen: {e}")
                QMessageBox.critical(
                    self,
                    "Fehler",
                    f"CSV-Export fehlgeschlagen:\n{e}"
                )
    
    def _on_download_urls_error(self, error_msg: str):
        """Called when fetch failed."""
        self._log(f"[FEHLER] {error_msg}")
        QMessageBox.critical(self, "Fehler", error_msg)
    
    def _cleanup_download_url_worker(self):
        """Clean up worker and thread."""
        self.progress_bar.setVisible(False)
        self.export_download_urls_btn.setEnabled(True)
        
        # Reset references (objects are deleted via deleteLater)
        self.download_url_thread = None
        self.download_url_worker = None
        
        self._log("Bereit für nächsten Vorgang.")
    
    # =========================================================================
    # Download URL Update Methods
    # =========================================================================
    
    def _on_update_download_urls_clicked(self):
        """Handle update download URLs button click."""
        from PySide6.QtWidgets import QFileDialog
        
        # Check if database is configured
        settings = QSettings("GFZ", "GROBI")
        db_enabled = settings.value("database/enabled", False, type=bool)
        
        if not db_enabled:
            QMessageBox.warning(
                self,
                "Datenbank nicht konfiguriert",
                "Die Datenbank-Verbindung ist nicht aktiviert.\n\n"
                "Bitte konfiguriere die Datenbank-Verbindung in den Einstellungen "
                "(Einstellungen → Datenbank-Verbindung)."
            )
            return
        
        # Load DB credentials
        from src.utils.credential_manager import load_db_credentials
        
        db_creds = load_db_credentials()
        if not db_creds:
            QMessageBox.warning(
                self,
                "Keine DB-Credentials",
                "Keine Datenbank-Zugangsdaten gespeichert.\n\n"
                "Bitte konfiguriere die Datenbank in den Einstellungen."
            )
            return
        
        # Select CSV file
        filepath, _ = QFileDialog.getOpenFileName(
            self,
            "CSV-Datei mit Download-URLs auswählen",
            "",
            "CSV Files (*.csv);;All Files (*)"
        )
        
        if not filepath:
            return  # User cancelled
        
        self._log(f"CSV-Datei ausgewählt: {filepath}")
        
        # Start worker
        self._start_download_url_update(filepath, db_creds)
    
    def _start_download_url_update(self, csv_path: str, db_creds: dict):
        """Start worker to update download URLs in database."""
        self._log("Starte Download-URL Update...")
        
        # Import worker
        from src.workers.download_url_update_worker import DownloadURLUpdateWorker
        
        # Create worker
        self.download_url_update_worker = DownloadURLUpdateWorker(
            csv_path=csv_path,
            db_host=db_creds['host'],
            db_name=db_creds['database'],
            db_user=db_creds['username'],
            db_password=db_creds['password']
        )
        
        # Connect signals
        self.download_url_update_worker.progress_update.connect(self._on_download_url_update_progress)
        self.download_url_update_worker.entry_updated.connect(self._on_download_url_entry_updated)
        self.download_url_update_worker.finished.connect(self._on_download_url_update_finished)
        self.download_url_update_worker.error_occurred.connect(self._on_download_url_update_error)
        
        # Create thread
        self.download_url_update_thread = QThread()
        self.download_url_update_worker.moveToThread(self.download_url_update_thread)
        
        # Connect thread signals
        self.download_url_update_thread.started.connect(self.download_url_update_worker.run)
        self.download_url_update_worker.finished.connect(self.download_url_update_thread.quit)
        self.download_url_update_worker.error_occurred.connect(self.download_url_update_thread.quit)
        
        # Clean up after worker finishes or errors
        self.download_url_update_worker.finished.connect(self.download_url_update_worker.deleteLater)
        self.download_url_update_worker.error_occurred.connect(self.download_url_update_worker.deleteLater)
        
        # Clean up thread when it finishes
        self.download_url_update_thread.finished.connect(self.download_url_update_thread.deleteLater)
        self.download_url_update_thread.finished.connect(self._cleanup_download_url_update_worker)
        
        # Start thread
        self.download_url_update_thread.start()
        
        # Disable buttons during update
        self.update_download_urls_btn.setEnabled(False)
        self.export_download_urls_btn.setEnabled(False)
        self.progress_bar.setVisible(True)
        self.progress_bar.setMaximum(0)  # Indeterminate initially
    
    def _on_download_url_update_progress(self, current: int, total: int, message: str):
        """Handle progress updates from download URL update worker."""
        if total > 0:
            self.progress_bar.setMaximum(total)
            self.progress_bar.setValue(current)
        self._log(message)
    
    def _on_download_url_entry_updated(self, doi: str, filename: str, success: bool, message: str):
        """Handle individual entry update result."""
        status = "[OK]" if success else "[FEHLER]"
        self._log(f"{status} {doi} / {filename}: {message}")
    
    def _on_download_url_update_finished(
        self, 
        success_count: int, 
        error_count: int, 
        skipped_count: int,
        error_list: list,
        skipped_details: list
    ):
        """Handle download URL update completion."""
        total = success_count + error_count + skipped_count
        
        self._log(f"[OK] Download-URL Update abgeschlossen:")
        self._log(f"     Aktualisiert: {success_count}")
        self._log(f"     Übersprungen: {skipped_count}")
        self._log(f"     Fehler: {error_count}")
        
        # Build result message
        result_msg = (
            f"Download-URL Update abgeschlossen:\n\n"
            f"Verarbeitet: {total} Einträge\n"
            f"Aktualisiert: {success_count}\n"
            f"Übersprungen (keine Änderungen): {skipped_count}\n"
            f"Fehler: {error_count}"
        )
        
        if error_count > 0:
            result_msg += "\n\nFehler-Details:\n"
            for err in error_list[:10]:  # Show max 10 errors
                result_msg += f"• {err}\n"
            if len(error_list) > 10:
                result_msg += f"... und {len(error_list) - 10} weitere Fehler"
        
        if error_count > 0:
            QMessageBox.warning(self, "Update mit Fehlern abgeschlossen", result_msg)
        else:
            QMessageBox.information(self, "Update erfolgreich", result_msg)
    
    def _on_download_url_update_error(self, error_msg: str):
        """Handle download URL update error."""
        self._log(f"[FEHLER] {error_msg}")
        QMessageBox.critical(self, "Fehler", error_msg)
    
    def _cleanup_download_url_update_worker(self):
        """Clean up download URL update worker and thread."""
        self.progress_bar.setVisible(False)
        self.progress_bar.setMaximum(0)
        self.update_download_urls_btn.setEnabled(True)
        self.export_download_urls_btn.setEnabled(True)
        
        # Reset references
        self.download_url_update_thread = None
        self.download_url_update_worker = None
        
        self._log("Bereit für nächsten Vorgang.")

    # =========================================================================
    # Dead Links Check Methods
    # =========================================================================

    def _on_check_dead_links_clicked(self):
        """Handle dead links check button click."""
        # Check if database is configured
        settings = QSettings("GFZ", "GROBI")
        db_enabled = settings.value("database/enabled", False, type=bool)

        if not db_enabled:
            QMessageBox.warning(
                self,
                "Datenbank nicht konfiguriert",
                "Die Datenbank-Verbindung ist nicht aktiviert.\n\n"
                "Bitte konfiguriere die Datenbank-Verbindung in den Einstellungen "
                "(Einstellungen → Datenbank-Verbindung)."
            )
            return

        from src.utils.credential_manager import load_db_credentials

        db_creds = load_db_credentials()
        if not db_creds:
            QMessageBox.warning(
                self,
                "Keine DB-Credentials",
                "Keine Datenbank-Zugangsdaten gespeichert.\n\n"
                "Bitte konfiguriere die Datenbank in den Einstellungen."
            )
            return

        self._start_dead_links_check(db_creds)

    def _start_dead_links_check(self, db_creds: dict):
        """Start worker to check dead download links in database."""
        self._log("Starte Dead-Link-Check...")

        from src.workers.dead_links_check_worker import DeadLinksCheckWorker

        self.dead_links_worker = DeadLinksCheckWorker(
            db_host=db_creds['host'],
            db_name=db_creds['database'],
            db_user=db_creds['username'],
            db_password=db_creds['password']
        )

        self.dead_links_worker.progress_update.connect(self._on_dead_links_check_progress)
        self.dead_links_worker.finished.connect(self._on_dead_links_check_finished)
        self.dead_links_worker.error_occurred.connect(self._on_dead_links_check_error)

        self.dead_links_thread = QThread()
        self.dead_links_worker.moveToThread(self.dead_links_thread)

        self.dead_links_thread.started.connect(self.dead_links_worker.run)
        self.dead_links_worker.finished.connect(self.dead_links_thread.quit)
        self.dead_links_worker.error_occurred.connect(self.dead_links_thread.quit)

        self.dead_links_worker.finished.connect(self.dead_links_worker.deleteLater)
        self.dead_links_worker.error_occurred.connect(self.dead_links_worker.deleteLater)

        self.dead_links_thread.finished.connect(self.dead_links_thread.deleteLater)
        self.dead_links_thread.finished.connect(self._cleanup_dead_links_worker)

        self.dead_links_thread.start()

        self.dead_links_check_btn.setEnabled(False)
        self.progress_bar.setVisible(True)
        self.progress_bar.setMaximum(0)

    def _on_dead_links_check_progress(self, current: int, total: int, message: str):
        """Handle progress updates from dead link check worker."""
        if total > 0:
            self.progress_bar.setMaximum(total)
            self.progress_bar.setValue(current)
        self._log(message)

    def _on_dead_links_check_finished(
        self,
        dead_links: list,
        checked_count: int,
        skipped_count: int,
        error_count: int
    ):
        """Handle dead link check completion."""
        from PySide6.QtWidgets import QFileDialog
        from src.utils.csv_exporter import export_dead_links_to_csv

        self._log(
            f"[OK] Dead-Link-Check abgeschlossen: {checked_count} geprüft, "
            f"{len(dead_links)} mit 404"
        )

        if self._current_username:
            default_filename = f"{self._current_username}_dead_links.csv"
        else:
            default_filename = "dead_links.csv"

        filepath, _ = QFileDialog.getSaveFileName(
            self,
            "CSV-Datei speichern",
            default_filename,
            "CSV Files (*.csv)"
        )

        if filepath:
            try:
                export_dead_links_to_csv(dead_links, filepath)
                self._log(f"[OK] CSV-Datei gespeichert: {filepath}")
                QMessageBox.information(
                    self,
                    "Check abgeschlossen",
                    f"Dead-Link-Check abgeschlossen:\n\n"
                    f"Geprüft: {checked_count}\n"
                    f"404 gefunden: {len(dead_links)}\n"
                    f"Übersprungen: {skipped_count}\n"
                    f"Fehler: {error_count}\n\n"
                    f"Datei: {Path(filepath).name}"
                )
            except Exception as e:
                self._log(f"[FEHLER] CSV-Export fehlgeschlagen: {e}")
                QMessageBox.critical(
                    self,
                    "Fehler",
                    f"CSV-Export fehlgeschlagen:\n{e}"
                )
        else:
            self._log("Dead-Link-Check abgeschlossen (kein Export gewählt).")

    def _on_dead_links_check_error(self, error_msg: str):
        """Handle dead link check error."""
        self._log(f"[FEHLER] {error_msg}")
        QMessageBox.critical(self, "Fehler", error_msg)

    def _cleanup_dead_links_worker(self):
        """Clean up dead link check worker and thread."""
        self.progress_bar.setVisible(False)
        self.progress_bar.setMaximum(0)
        self.dead_links_check_btn.setEnabled(True)

        self.dead_links_thread = None
        self.dead_links_worker = None

        self._log("Bereit für nächsten Vorgang.")
    
    # =========================================================================
    # Pending DOIs Export Methods (SUMARIOPMD Database)
    # =========================================================================
    
    def _on_export_pending_clicked(self):
        """Handle export pending DOIs button click."""
        from PySide6.QtWidgets import QFileDialog
        from src.utils.credential_manager import load_db_credentials
        
        # Check if database is enabled in settings
        settings = QSettings("GFZ", "GROBI")
        db_enabled = settings.value("database/enabled", False, type=bool)
        
        if not db_enabled:
            QMessageBox.warning(
                self,
                "Datenbank nicht aktiviert",
                "Die Datenbank-Verbindung ist nicht aktiviert.\n\n"
                "Bitte aktivieren Sie die Datenbank-Verbindung in den Einstellungen "
                "(Einstellungen → Datenbank-Verbindung)."
            )
            return
        
        # Check if database credentials are configured
        try:
            db_creds = load_db_credentials()
        except Exception as e:
            self._log(f"[FEHLER] Datenbank-Zugangsdaten nicht verfügbar: {e}")
            QMessageBox.warning(
                self,
                "Datenbank nicht konfiguriert",
                "Die Datenbank-Zugangsdaten sind nicht konfiguriert.\n\n"
                "Bitte konfigurieren Sie die SUMARIOPMD-Datenbank unter:\n"
                "Einstellungen → Datenbank"
            )
            return
        
        if db_creds is None:
            self._log("[FEHLER] Keine Datenbank-Zugangsdaten gespeichert")
            QMessageBox.warning(
                self,
                "Datenbank nicht konfiguriert",
                "Die Datenbank-Zugangsdaten sind nicht konfiguriert.\n\n"
                "Bitte konfigurieren Sie die SUMARIOPMD-Datenbank unter:\n"
                "Einstellungen → Datenbank"
            )
            return
        
        # Ask for save location
        default_filename = "pending_dois.csv"
        filepath, _ = QFileDialog.getSaveFileName(
            self,
            "Pending DOIs speichern",
            default_filename,
            "CSV Files (*.csv)"
        )
        
        if not filepath:
            self._log("Export abgebrochen.")
            return
        
        self._log(f"Starte Export der pending DOIs aus {db_creds['database']}...")
        
        # Disable button and show progress
        self.export_pending_btn.setEnabled(False)
        self.progress_bar.setVisible(True)
        self.progress_bar.setMinimum(0)
        self.progress_bar.setMaximum(100)
        self.progress_bar.setValue(0)
        
        # Create worker and thread
        self.pending_export_worker = PendingExportWorker(
            db_host=db_creds['host'],
            db_name=db_creds['database'],
            db_user=db_creds['username'],
            db_password=db_creds['password'],
            output_path=filepath
        )
        self.pending_export_thread = QThread()
        self.pending_export_worker.moveToThread(self.pending_export_thread)
        
        # Connect signals
        self.pending_export_thread.started.connect(self.pending_export_worker.run)
        self.pending_export_worker.progress.connect(self._log)
        self.pending_export_worker.progress_count.connect(self._on_pending_export_progress)
        self.pending_export_worker.finished.connect(self._on_pending_export_finished)
        self.pending_export_worker.error.connect(self._on_pending_export_error)
        
        # Clean up after worker finishes or errors
        self.pending_export_worker.finished.connect(self.pending_export_worker.deleteLater)
        self.pending_export_worker.error.connect(self.pending_export_worker.deleteLater)
        self.pending_export_worker.finished.connect(self.pending_export_thread.quit)
        self.pending_export_worker.error.connect(self.pending_export_thread.quit)
        
        # Clean up thread when it finishes
        self.pending_export_thread.finished.connect(self.pending_export_thread.deleteLater)
        self.pending_export_thread.finished.connect(self._cleanup_pending_export_worker)
        
        # Start the thread
        self.pending_export_thread.start()
    
    def _on_pending_export_progress(self, current: int, total: int):
        """Update progress bar for pending export."""
        self.progress_bar.setMaximum(total)
        self.progress_bar.setValue(current)
    
    def _on_pending_export_finished(self, filepath: str, count: int):
        """Handle successful pending DOIs export."""
        if count == 0:
            QMessageBox.information(
                self,
                "Keine Daten",
                "Es wurden keine pending DOIs in der Datenbank gefunden."
            )
            return
        
        self._log(f"[OK] {count} pending DOIs nach {Path(filepath).name} exportiert")
        QMessageBox.information(
            self,
            "Export erfolgreich",
            f"Pending DOIs wurden erfolgreich exportiert:\n\n"
            f"Datei: {Path(filepath).name}\n"
            f"Anzahl: {count} DOIs"
        )
    
    def _on_pending_export_error(self, error_msg: str):
        """Handle pending export error."""
        self._log(f"[FEHLER] {error_msg}")
        QMessageBox.critical(self, "Fehler", error_msg)
    
    def _cleanup_pending_export_worker(self):
        """Clean up pending export worker and thread."""
        self.progress_bar.setVisible(False)
        self.progress_bar.setMaximum(0)
        self.export_pending_btn.setEnabled(True)
        
        # Reset references
        self.pending_export_thread = None
        self.pending_export_worker = None
        
        self._log("Bereit für nächsten Vorgang.")
    
    # =========================================================================
    # F-UJI FAIR Assessment Methods
    # =========================================================================
    
    def _on_fuji_check_clicked(self):
        """Handle F-UJI FAIR Check button click."""
        logger.info("F-UJI Check button clicked")
        self._log("F-UJI Check wird gestartet...")
        
        # Show credentials dialog in fuji_check mode
        dialog = CredentialsDialog(self, mode="fuji_check")
        logger.info("CredentialsDialog created, showing now...")
        
        if dialog.exec() != QDialog.Accepted:
            logger.info("Dialog cancelled by user")
            self._log("F-UJI Check abgebrochen.")
            return
        
        # Get credentials from dialog attributes
        username = dialog.username_input.text().strip()
        # Use loaded password if available, otherwise get from input
        if dialog.loaded_password:
            password = dialog.loaded_password
        else:
            password = dialog.password_input.text().strip()
        use_test_api = dialog.test_api_checkbox.isChecked()
        
        # Check if credentials are new (not from saved account)
        credentials_are_new = dialog.is_new_credentials
        
        logger.info(f"Credentials obtained for user: {username}")
        
        # Disable button during operation
        self.fuji_check_btn.setEnabled(False)
        self.progress_bar.setVisible(True)
        self._log("Starte F-UJI FAIR Assessment (Streaming-Modus)...")
        
        # Offer to save credentials if new
        if credentials_are_new:
            api_type = "test" if use_test_api else "production"
            self._offer_save_credentials(username, password, api_type)
        
        try:
            # Create DataCite client for streaming
            datacite_client = DataCiteClient(username, password, use_test_api)
            
            # Open results window in streaming mode
            self.fuji_results_window = FujiResultsWindow(self, self.theme_manager)
            self.fuji_results_window.start_streaming_assessment()
            self.fuji_results_window.assessment_cancelled.connect(self._on_fuji_cancelled)
            self.fuji_results_window.closed.connect(self._cleanup_fuji_check)
            self.fuji_results_window.show()
            
            try:
                # Start streaming assessment thread
                self.fuji_thread = StreamingFujiThread(datacite_client, max_workers=5)
                
                # Connect signals for streaming mode
                self.fuji_thread.worker.doi_discovered.connect(self.fuji_results_window.add_pending_tile)
                self.fuji_thread.worker.doi_assessed.connect(self.fuji_results_window.add_result)
                self.fuji_thread.worker.fetch_complete.connect(self.fuji_results_window.set_total_dois)
                self.fuji_thread.worker.progress.connect(self._log)
                self.fuji_thread.worker.error.connect(self._on_fuji_error)
                self.fuji_thread.worker.finished.connect(self._on_fuji_finished)
                self.fuji_thread.start()
                
                self._log("DOI-Abruf und Bewertung laufen parallel...")
            except Exception as e:
                # Thread setup failed after window was created - close window
                if self.fuji_results_window:
                    self.fuji_results_window.close()
                raise
            
        except AuthenticationError as e:
            self._log(f"[FEHLER] Authentifizierung fehlgeschlagen: {e}")
            QMessageBox.critical(self, "Authentifizierungsfehler", str(e))
            self._cleanup_fuji_check()
        except NetworkError as e:
            self._log(f"[FEHLER] Netzwerkfehler: {e}")
            QMessageBox.critical(self, "Netzwerkfehler", str(e))
            self._cleanup_fuji_check()
        except DataCiteAPIError as e:
            self._log(f"[FEHLER] API-Fehler: {e}")
            QMessageBox.critical(self, "API-Fehler", str(e))
            self._cleanup_fuji_check()
        except Exception as e:
            self._log(f"[FEHLER] Unerwarteter Fehler: {e}")
            logger.exception("Unexpected error in FUJI check")
            QMessageBox.critical(self, "Fehler", f"Unerwarteter Fehler: {e}")
            self._cleanup_fuji_check()
    
    def _on_fuji_cancelled(self):
        """Handle FAIR assessment cancellation."""
        if self.fuji_thread and self.fuji_thread.isRunning():
            self.fuji_thread.cancel()
            self.fuji_thread.quit()
            self.fuji_thread.wait(3000)
        self._log("FAIR Assessment abgebrochen.")
    
    def _on_fuji_error(self, error_msg: str):
        """Handle FAIR assessment error."""
        self._log(f"[FEHLER] {error_msg}")
        QMessageBox.warning(self, "F-UJI Fehler", error_msg)
    
    def _on_fuji_finished(self):
        """Handle FAIR assessment completion."""
        self._log("[OK] FAIR Assessment abgeschlossen.")
        self.progress_bar.setVisible(False)
        self.fuji_check_btn.setEnabled(True)
    
    def _cleanup_fuji_check(self):
        """Clean up F-UJI check resources."""
        self.progress_bar.setVisible(False)
        self.fuji_check_btn.setEnabled(True)
        
        if self.fuji_thread and self.fuji_thread.isRunning():
            self.fuji_thread.cancel()
            self.fuji_thread.quit()
            self.fuji_thread.wait(3000)
        
        self.fuji_thread = None
        self._log("Bereit für nächsten Vorgang.")
    
    # ==================== RIGHTS METHODS ====================
    
    def _on_load_rights_clicked(self):
        """Handle load rights button click."""
        # Show credentials dialog
        dialog = CredentialsDialog(self)
        credentials = dialog.get_credentials()
        
        if credentials is None:
            self._log("Abruf der Rights abgebrochen.")
            return
        
        username, password, csv_path, use_test_api = credentials
        # csv_path is None in export mode, we don't need it here
        
        # Check if user selected new credentials or loaded saved account
        credentials_are_new = dialog.is_new_credentials()
        
        api_type = "Test-API" if use_test_api else "Produktions-API"
        self._log(f"Starte Abruf der DOIs mit Rights für Benutzer '{username}' ({api_type})...")
        
        # Disable buttons and show progress
        self._set_buttons_enabled(False)
        self.progress_bar.setVisible(True)
        
        # Create worker and thread
        self.rights_worker = DOIRightsFetchWorker(username, password, use_test_api, credentials_are_new)
        self.rights_thread = QThread()
        self.rights_worker.moveToThread(self.rights_thread)
        
        # Connect signals
        self.rights_thread.started.connect(self.rights_worker.run)
        self.rights_worker.progress.connect(self._log)
        self.rights_worker.finished.connect(self._on_rights_fetch_finished)
        self.rights_worker.error.connect(self._on_rights_fetch_error)
        self.rights_worker.request_save_credentials.connect(self._on_request_save_credentials)
        
        # Clean up after worker finishes
        self.rights_worker.finished.connect(self.rights_worker.deleteLater)
        self.rights_worker.finished.connect(self.rights_thread.quit)
        
        # Clean up thread when it finishes
        self.rights_thread.finished.connect(self.rights_thread.deleteLater)
        self.rights_thread.finished.connect(self._cleanup_rights_thread)
        
        # Start the thread
        self.rights_thread.start()
    
    def _on_rights_fetch_finished(self, rights_data, username):
        """
        Handle successful rights fetch.
        
        Args:
            rights_data: List of rights tuples (DOI, rights, rightsUri, etc.)
            username: DataCite username
        """
        if not rights_data:
            self._log("[WARNUNG] Keine DOIs mit Rights gefunden.")
            QMessageBox.information(
                self,
                "Keine Rights",
                f"Für den Benutzer '{username}' wurden keine DOIs gefunden."
            )
            return
        
        # Export to CSV
        try:
            output_dir = os.getcwd()
            filepath = export_dois_with_rights_to_csv(rights_data, username, output_dir)
            
            # Count unique DOIs
            unique_dois = len(set(row[0] for row in rights_data))
            
            self._log(f"[OK] CSV-Datei erfolgreich erstellt: {filepath}")
            
            # Update username and check CSV files
            self._current_username = username
            self._check_csv_files()
            
            QMessageBox.information(
                self,
                "Erfolg",
                f"{unique_dois} DOIs mit {len(rights_data)} Rights-Einträgen wurden erfolgreich exportiert.\n\n"
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
    
    def _on_rights_fetch_error(self, error_message):
        """
        Handle rights fetch error.
        
        Args:
            error_message: Error message
        """
        self._log(f"[FEHLER] {error_message}")
        
        QMessageBox.critical(
            self,
            "Fehler",
            f"Beim Abrufen der Rights ist ein Fehler aufgetreten:\n\n{error_message}"
        )
    
    def _cleanup_rights_thread(self):
        """Clean up rights thread and worker after completion."""
        self.progress_bar.setVisible(False)
        self._set_buttons_enabled(True)
        
        # Reset references (objects are deleted via deleteLater)
        self.rights_thread = None
        self.rights_worker = None
        
        self._log("Bereit für nächsten Vorgang.")
    
    def _on_update_rights_clicked(self):
        """Handle update rights button click."""
        # Show credentials dialog with CSV selection
        dialog = CredentialsDialog(self, mode="update_rights")
        credentials = dialog.get_credentials()
        
        if credentials is None:
            self._log("Rights-Update abgebrochen.")
            return
        
        username, password, csv_path, use_test_api = credentials
        
        if not csv_path:
            self._log("[FEHLER] Keine CSV-Datei ausgewählt.")
            QMessageBox.warning(
                self,
                "Keine CSV-Datei",
                "Bitte wählen Sie eine Rights-CSV-Datei aus."
            )
            return
        
        rights_csv_path = Path(csv_path)
        
        # Check if user selected new credentials or loaded saved account
        credentials_are_new = dialog.is_new_credentials()
        
        api_type = "Test-API" if use_test_api else "Produktions-API"
        self._log(f"Starte Rights-Update für Benutzer '{username}' ({api_type})...")
        self._log(f"CSV-Datei: {rights_csv_path.name}")
        
        # Disable buttons and show progress
        self._set_buttons_enabled(False)
        self.progress_bar.setVisible(True)
        self.progress_bar.setMaximum(100)  # Will be updated by worker
        
        # Create worker and thread
        self.rights_update_worker = RightsUpdateWorker(
            username, password, str(rights_csv_path), use_test_api, credentials_are_new
        )
        self.rights_update_thread = QThread()
        self.rights_update_worker.moveToThread(self.rights_update_thread)
        
        # Connect signals
        self.rights_update_thread.started.connect(self.rights_update_worker.run)
        self.rights_update_worker.progress_update.connect(self._on_rights_update_progress)
        self.rights_update_worker.finished.connect(self._on_rights_update_finished)
        self.rights_update_worker.error_occurred.connect(self._on_rights_update_error)
        self.rights_update_worker.request_save_credentials.connect(self._on_request_save_credentials)
        
        # Clean up after worker finishes
        self.rights_update_worker.finished.connect(self.rights_update_worker.deleteLater)
        self.rights_update_worker.finished.connect(self.rights_update_thread.quit)
        
        # Clean up thread when it finishes
        self.rights_update_thread.finished.connect(self.rights_update_thread.deleteLater)
        self.rights_update_thread.finished.connect(self._cleanup_rights_update_thread)
        
        # Start the thread
        self.rights_update_thread.start()
    
    def _on_rights_update_progress(self, current, total, message):
        """
        Handle rights update progress signal.
        
        Args:
            current: Current DOI number
            total: Total number of DOIs
            message: Progress message
        """
        self._log(message)
        
        if total > 0:
            self.progress_bar.setMaximum(total)
            self.progress_bar.setValue(current)
    
    def _on_rights_update_finished(self, success_count, skipped_count, error_count, error_list, skipped_details):
        """
        Handle rights update completion.
        
        Args:
            success_count: Number of successful updates
            skipped_count: Number of skipped DOIs (no changes)
            error_count: Number of failed updates
            error_list: List of error messages
            skipped_details: List of (doi, reason) tuples for skipped DOIs
        """
        total = success_count + skipped_count + error_count
        
        # If total is 0 and there's a critical error flag, don't show success dialog
        # (the error dialog was already shown by _on_rights_update_error)
        if total == 0 and self._rights_update_had_critical_error:
            # Reset flag and skip success dialog
            self._rights_update_had_critical_error = False
            self._log("Rights-Update wurde aufgrund eines Fehlers abgebrochen.")
            return
        
        # Create log file (only if there was actual processing)
        if total > 0 or error_list:
            self._create_rights_update_log(success_count, skipped_count, error_count, error_list, skipped_details)
        
        # Prepare result message
        self._log(f"[OK] Rights-Update abgeschlossen: {success_count} erfolgreich, {skipped_count} übersprungen, {error_count} Fehler")
        
        if error_count > 0 and success_count == 0 and skipped_count == 0:
            # Only errors, no success - show warning
            message = f"Rights-Update fehlgeschlagen.\n\n{error_count} DOI(s) konnten nicht aktualisiert werden:\n\n"
            for err in error_list[:5]:
                message += f"• {err}\n"
            if len(error_list) > 5:
                message += f"\n... und {len(error_list) - 5} weitere Fehler (siehe Log-Datei)"
            
            QMessageBox.warning(self, "Rights-Update fehlgeschlagen", message)
        elif error_count > 0:
            # Mixed results
            message = (
                f"Rights-Update abgeschlossen mit Fehlern.\n\n"
                f"✅ Erfolgreich aktualisiert: {success_count}\n"
                f"⏭️ Übersprungen (keine Änderungen): {skipped_count}\n"
                f"❌ Fehlgeschlagen: {error_count}\n\n"
                f"Fehler Details:\n"
            )
            for err in error_list[:5]:
                message += f"• {err}\n"
            if len(error_list) > 5:
                message += f"\n... und {len(error_list) - 5} weitere Fehler (siehe Log-Datei)"
            
            QMessageBox.warning(self, "Rights-Update mit Fehlern", message)
        elif skipped_count > 0 and success_count == 0:
            # All skipped (no changes needed)
            message = (
                f"Keine Änderungen notwendig.\n\n"
                f"Alle {skipped_count} DOI(s) haben bereits die korrekten Rights-Metadaten.\n\n"
                f"Effizienz: {skipped_count} unnötige API-Calls vermieden!"
            )
            QMessageBox.information(self, "Keine Änderungen", message)
        elif total == 0:
            # No DOIs processed at all
            QMessageBox.information(
                self,
                "Keine DOIs verarbeitet",
                "Es wurden keine DOIs in der CSV-Datei gefunden oder alle wurden übersprungen."
            )
        else:
            # Success!
            message = (
                f"Rights-Update erfolgreich abgeschlossen!\n\n"
                f"✅ Erfolgreich aktualisiert: {success_count}\n"
                f"⏭️ Übersprungen (keine Änderungen): {skipped_count}"
            )
            if skipped_count > 0:
                message += f"\n\nEffizienz: {skipped_count} unnötige API-Calls vermieden!"
            QMessageBox.information(self, "Rights-Update erfolgreich", message)
    
    def _on_rights_update_error(self, error_message):
        """
        Handle rights update error.
        
        Args:
            error_message: Error message
        """
        self._log(f"[FEHLER] Rights-Update: {error_message}")
        
        # Set flag to prevent double dialog (error + finished with 0 DOIs)
        self._rights_update_had_critical_error = True
        
        # Check if this is a CSV validation error for better user messaging
        if "Ungültiger SPDX-Identifier" in error_message:
            # Extract DOI and identifier from message
            QMessageBox.warning(
                self,
                "CSV-Validierungsfehler",
                f"Die CSV-Datei enthält ungültige Daten:\n\n{error_message}\n\n"
                f"Hinweis: SPDX-Identifier müssen einem gültigen SPDX-Lizenz-Identifier entsprechen "
                f"(z.B. 'CC-BY-4.0', 'MIT', 'Apache-2.0'). Groß-/Kleinschreibung wird ignoriert.\n\n"
                f"Eine Liste gültiger Identifier finden Sie unter:\nhttps://spdx.org/licenses/"
            )
        elif "Ungültiger Sprachcode" in error_message:
            QMessageBox.warning(
                self,
                "CSV-Validierungsfehler",
                f"Die CSV-Datei enthält ungültige Daten:\n\n{error_message}\n\n"
                f"Hinweis: Sprachcodes müssen dem ISO 639-1 Standard entsprechen (z.B. 'en', 'de', 'fr')."
            )
        elif "CSV-Datei" in error_message or "Zeile" in error_message:
            QMessageBox.warning(
                self,
                "CSV-Fehler",
                f"Fehler beim Verarbeiten der CSV-Datei:\n\n{error_message}"
            )
        else:
            QMessageBox.critical(
                self,
                "Rights-Update Fehler",
                f"Ein kritischer Fehler ist aufgetreten:\n\n{error_message}"
            )
    
    def _cleanup_rights_update_thread(self):
        """Clean up rights update thread and worker after completion."""
        self.progress_bar.setVisible(False)
        self.progress_bar.setMaximum(0)  # Reset to indeterminate
        self._set_buttons_enabled(True)
        
        # Reset references (objects are deleted via deleteLater)
        self.rights_update_thread = None
        self.rights_update_worker = None
        
        self._log("Bereit für nächsten Vorgang.")
    
    def _create_rights_update_log(self, success_count, skipped_count, error_count, error_list, skipped_details):
        """
        Create a log file with rights update results.
        
        Args:
            success_count: Number of successful updates
            skipped_count: Number of skipped DOIs (no changes)
            error_count: Number of failed updates
            error_list: List of error messages
            skipped_details: List of (doi, reason) tuples for skipped DOIs
        """
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            log_filename = f"rights_update_log_{timestamp}.txt"
            log_path = Path(os.getcwd()) / log_filename
            
            total = success_count + skipped_count + error_count
            efficiency_gain = (skipped_count / total * 100) if total > 0 else 0
            
            with open(log_path, 'w', encoding='utf-8') as f:
                f.write("=" * 70 + "\n")
                f.write("GROBI - Rights-Metadaten Update Log\n")
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
                f.write("RIGHTS PROPERTIES:\n")
                f.write("=" * 70 + "\n")
                f.write("- rights: Freitext-Beschreibung der Lizenz\n")
                f.write("- rightsUri: URI zur Lizenz (z.B. CC-Lizenz-URL)\n")
                f.write("- schemeUri: URI zum Identifier-Schema (z.B. https://spdx.org/licenses/)\n")
                f.write("- rightsIdentifier: SPDX-Identifier (z.B. CC-BY-4.0)\n")
                f.write("- rightsIdentifierScheme: Name des Schemas (z.B. SPDX)\n")
                f.write("- lang: Sprachcode nach ISO 639-1 (z.B. en, de)\n")
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
        
        # If publisher thread is running, wait for it to finish
        if self.publisher_thread is not None and self.publisher_thread.isRunning():
            self._log("Warte auf Abschluss des Publisher-Abrufs...")
            self.publisher_thread.quit()
            self.publisher_thread.wait(3000)  # Wait max 3 seconds
        
        # If publisher update thread is running, stop worker and wait for it to finish
        if self.publisher_update_thread is not None and self.publisher_update_thread.isRunning():
            self._log("Warte auf Abschluss des Publisher-Updates...")
            if self.publisher_update_worker is not None:
                self.publisher_update_worker.stop()
            self.publisher_update_thread.quit()
            self.publisher_update_thread.wait(3000)  # Wait max 3 seconds
        
        # If contributor thread is running, wait for it to finish
        if self.contributor_thread is not None and self.contributor_thread.isRunning():
            self._log("Warte auf Abschluss des Contributors-Abrufs...")
            self.contributor_thread.quit()
            self.contributor_thread.wait(3000)  # Wait max 3 seconds
        
        # If contributors update thread is running, stop worker and wait for it to finish
        if self.contributors_update_thread is not None and self.contributors_update_thread.isRunning():
            self._log("Warte auf Abschluss des Contributors-Updates...")
            if self.contributors_update_worker is not None:
                self.contributors_update_worker.stop()
            self.contributors_update_thread.quit()
            self.contributors_update_thread.wait(3000)  # Wait max 3 seconds
        
        # If download URL thread is running, wait for it to finish
        if hasattr(self, 'download_url_thread') and self.download_url_thread is not None and self.download_url_thread.isRunning():
            self._log("Warte auf Abschluss des Download-URL Exports...")
            self.download_url_thread.quit()
            self.download_url_thread.wait(3000)  # Wait max 3 seconds
        
        # If download URL update thread is running, cancel and wait
        if hasattr(self, 'download_url_update_thread') and self.download_url_update_thread is not None and self.download_url_update_thread.isRunning():
            self._log("Warte auf Abschluss des Download-URL Updates...")
            if hasattr(self, 'download_url_update_worker') and self.download_url_update_worker is not None:
                self.download_url_update_worker.stop()
            self.download_url_update_thread.quit()
            self.download_url_update_thread.wait(3000)  # Wait max 3 seconds
        
        # If pending export thread is running, wait for it to finish
        if hasattr(self, 'pending_export_thread') and self.pending_export_thread is not None and self.pending_export_thread.isRunning():
            self._log("Warte auf Abschluss des Pending-DOIs Exports...")
            if hasattr(self, 'pending_export_worker') and self.pending_export_worker is not None:
                self.pending_export_worker.stop()
            self.pending_export_thread.quit()
            self.pending_export_thread.wait(3000)  # Wait max 3 seconds
        
        # If rights thread is running, wait for it to finish
        if self.rights_thread is not None and self.rights_thread.isRunning():
            self._log("Warte auf Abschluss des Rights-Abrufs...")
            self.rights_thread.quit()
            self.rights_thread.wait(3000)  # Wait max 3 seconds
        
        # If rights update thread is running, stop worker and wait for it to finish
        if self.rights_update_thread is not None and self.rights_update_thread.isRunning():
            self._log("Warte auf Abschluss des Rights-Updates...")
            if self.rights_update_worker is not None:
                self.rights_update_worker.stop()
            self.rights_update_thread.quit()
            self.rights_update_thread.wait(3000)  # Wait max 3 seconds
        
        # If F-UJI assessment thread is running, cancel and wait
        if self.fuji_thread is not None and self.fuji_thread.isRunning():
            self._log("Warte auf Abschluss des FAIR Assessments...")
            self.fuji_thread.cancel()
            self.fuji_thread.quit()
            self.fuji_thread.wait(3000)  # Wait max 3 seconds
        
        # Save window geometry for next session
        self._save_window_geometry()
        
        event.accept()
    
    def _save_window_geometry(self):
        """
        Save current window position and size to QSettings.
        
        This allows the window to restore its position on next application start.
        Maximized state is saved separately to handle screen changes properly.
        """
        settings = QSettings("GFZ", "GROBI")
        settings.setValue(SETTINGS_GEOMETRY, self.saveGeometry())
        settings.setValue(SETTINGS_WINDOW_STATE, self.saveState())
        settings.setValue(SETTINGS_WINDOW_MAXIMIZED, self.isMaximized())
    
    def _restore_window_geometry(self):
        """
        Restore window geometry from QSettings or use smart defaults.
        
        On first run, the window is centered with appropriate proportions.
        On subsequent runs, the saved position is restored if it's still
        on a visible screen. Maximized state is restored separately to
        handle cases where the screen configuration has changed.
        """
        settings = QSettings("GFZ", "GROBI")
        saved_geometry = settings.value(SETTINGS_GEOMETRY)
        was_maximized = settings.value(SETTINGS_WINDOW_MAXIMIZED, False, type=bool)
        
        if saved_geometry is not None:
            # Try to restore saved geometry
            self.restoreGeometry(saved_geometry)
            saved_state = settings.value(SETTINGS_WINDOW_STATE)
            if saved_state is not None:
                self.restoreState(saved_state)
            
            # Verify the window is still on a visible screen
            if self._is_window_on_screen():
                # Restore maximized state after geometry restoration
                if was_maximized:
                    self.showMaximized()
                return  # Successfully restored
        
        # First run or saved position is off-screen: use smart defaults
        self._set_default_geometry()
        # Even if position was off-screen, honor maximized preference
        if was_maximized:
            self.showMaximized()
    
    def _is_window_on_screen(self) -> bool:
        """
        Check if the window is usable (visible and draggable) on any screen.
        
        This handles cases where a monitor was disconnected or resolution changed.
        The window is considered usable if:
        1. At least MINIMUM_VISIBLE_WINDOW_SIZE pixels are visible on any screen
        2. The top edge (title bar) is accessible for dragging
        
        Returns:
            bool: True if the window is usable on any connected screen.
                  False if off-screen, not sufficiently visible, or title bar
                  is inaccessible.
        """
        window_rect = self.frameGeometry()
        
        # Check all available screens
        for screen in QGuiApplication.screens():
            screen_rect = screen.availableGeometry()
            
            # Check if at least MINIMUM_VISIBLE_WINDOW_SIZE pixels are visible
            intersection = window_rect.intersected(screen_rect)
            if intersection.width() >= MINIMUM_VISIBLE_WINDOW_SIZE and \
               intersection.height() >= MINIMUM_VISIBLE_WINDOW_SIZE:
                
                # Additionally check if the title bar is accessible for dragging.
                # The window top must be within the screen's visible area so users
                # can grab the title bar. We allow the window to extend slightly
                # above the screen (up to TITLE_BAR_MIN_VISIBLE pixels) to handle
                # cases where the title bar is partially visible.
                title_bar_visible = (
                    window_rect.top() >= screen_rect.top() - TITLE_BAR_MIN_VISIBLE and
                    window_rect.top() < screen_rect.bottom() - TITLE_BAR_MIN_VISIBLE
                )
                if title_bar_visible:
                    return True
        
        return False
    
    def _set_default_geometry(self):
        """
        Set intelligent default window size and center on primary screen.
        
        Window size is proportional to screen size with min/max constraints.
        """
        screen_obj = QGuiApplication.primaryScreen()
        if screen_obj is None:
            # Fallback if no screen detected
            self.resize(DEFAULT_WINDOW_WIDTH, FALLBACK_WINDOW_HEIGHT)
            return
        
        screen = screen_obj.availableGeometry()
        
        # Calculate proportional size with constraints
        window_width = min(
            max(int(screen.width() * SCREEN_WIDTH_RATIO), DEFAULT_WINDOW_WIDTH),
            MAXIMUM_WINDOW_WIDTH
        )
        window_height = int(screen.height() * SCREEN_HEIGHT_RATIO)
        
        self.resize(window_width, window_height)
        
        # Center on screen
        center_x = screen.x() + (screen.width() - window_width) // 2
        center_y = screen.y() + (screen.height() - window_height) // 2
        self.move(center_x, center_y)
