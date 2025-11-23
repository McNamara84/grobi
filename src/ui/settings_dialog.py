"""
Settings Dialog for GROBI application.

Provides a tab-based interface for configuring:
- General settings (Theme)
- Database connection settings
"""

import logging
from typing import Optional

from PySide6.QtWidgets import (
    QDialog, QTabWidget, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QLineEdit, QPushButton, QCheckBox, QRadioButton,
    QGroupBox, QButtonGroup, QMessageBox
)
from PySide6.QtCore import Qt, QSettings, Signal, QObject, QThread

from src.ui.theme_manager import Theme, ThemeManager
from src.utils.credential_manager import (
    save_db_credentials,
    load_db_credentials,
    db_credentials_exist,
    delete_db_credentials,
    CredentialStorageError
)

logger = logging.getLogger(__name__)


class ConnectionTestWorker(QObject):
    """Worker for non-blocking database connection test."""
    
    finished = Signal(bool, str)  # success, message
    
    def __init__(self, host: str, database: str, username: str, password: str):
        """
        Initialize worker with DB credentials.
        
        Args:
            host: Database host
            database: Database name
            username: Username
            password: Password
        """
        super().__init__()
        self.host = host
        self.database = database
        self.username = username
        self.password = password
    
    def run(self):
        """Test database connection."""
        try:
            import mysql.connector
            from mysql.connector import Error
            
            # Attempt connection
            conn = mysql.connector.connect(
                host=self.host,
                database=self.database,
                user=self.username,
                password=self.password,
                connect_timeout=10,
                auth_plugin='mysql_native_password',
                use_pure=True
            )
            
            # Test query
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            conn.close()
            
            self.finished.emit(True, "✓ Verbindung erfolgreich")
            
        except Error as e:
            logger.error(f"DB connection test failed: {e}")
            self.finished.emit(False, f"✗ Fehler: {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected error in connection test: {e}")
            self.finished.emit(False, f"✗ Unerwarteter Fehler: {str(e)}")


class SettingsDialog(QDialog):
    """Settings dialog with tabs for General and Database configuration."""
    
    # Signal emitted when theme changes
    theme_changed = Signal(Theme)
    
    def __init__(self, theme_manager: ThemeManager, parent=None):
        """
        Initialize settings dialog.
        
        Args:
            theme_manager: ThemeManager instance for theme settings
            parent: Parent widget
        """
        super().__init__(parent)
        self.theme_manager = theme_manager
        self.settings = QSettings("GFZ", "GROBI")
        
        self.connection_test_thread = None
        self.connection_test_worker = None
        
        self._setup_ui()
        self._load_settings()
        self._apply_theme()
    
    def _setup_ui(self):
        """Setup the user interface."""
        self.setWindowTitle("Einstellungen")
        self.setMinimumWidth(600)
        self.setMinimumHeight(500)
        
        # Main layout
        layout = QVBoxLayout(self)
        
        # Tab widget
        self.tabs = QTabWidget()
        
        # Tab 1: General (Theme)
        self.general_tab = self._create_general_tab()
        self.tabs.addTab(self.general_tab, "Allgemein")
        
        # Tab 2: Database
        self.database_tab = self._create_database_tab()
        self.tabs.addTab(self.database_tab, "Datenbank")
        
        layout.addWidget(self.tabs)
        
        # Buttons
        button_layout = QHBoxLayout()
        button_layout.addStretch()
        
        self.cancel_button = QPushButton("Abbrechen")
        self.cancel_button.clicked.connect(self.reject)
        button_layout.addWidget(self.cancel_button)
        
        self.save_button = QPushButton("Speichern")
        self.save_button.clicked.connect(self._save_and_close)
        self.save_button.setDefault(True)
        button_layout.addWidget(self.save_button)
        
        layout.addLayout(button_layout)
    
    def _create_general_tab(self) -> QWidget:
        """
        Create General settings tab.
        
        Returns:
            QWidget with theme settings
        """
        widget = QWidget()
        layout = QVBoxLayout(widget)
        
        # Theme selection group
        theme_group = QGroupBox("Darstellung")
        theme_layout = QVBoxLayout()
        
        # Radio buttons for theme
        self.theme_button_group = QButtonGroup()
        
        self.auto_theme_radio = QRadioButton("Automatisch (folgt Systemeinstellung)")
        self.theme_button_group.addButton(self.auto_theme_radio, 0)  # ID: 0 = AUTO
        theme_layout.addWidget(self.auto_theme_radio)
        
        self.light_theme_radio = QRadioButton("Hell")
        self.theme_button_group.addButton(self.light_theme_radio, 1)  # ID: 1 = LIGHT
        theme_layout.addWidget(self.light_theme_radio)
        
        self.dark_theme_radio = QRadioButton("Dunkel")
        self.theme_button_group.addButton(self.dark_theme_radio, 2)  # ID: 2 = DARK
        theme_layout.addWidget(self.dark_theme_radio)
        
        theme_group.setLayout(theme_layout)
        layout.addWidget(theme_group)
        
        layout.addStretch()
        
        return widget
    
    def _create_database_tab(self) -> QWidget:
        """
        Create Database settings tab.
        
        Returns:
            QWidget with database credentials UI
        """
        widget = QWidget()
        layout = QVBoxLayout(widget)
        
        # Enable/Disable checkbox
        self.db_enabled_checkbox = QCheckBox("Datenbank-Updates aktivieren")
        self.db_enabled_checkbox.toggled.connect(self._on_db_enabled_toggled)
        layout.addWidget(self.db_enabled_checkbox)
        
        # Info label
        info_label = QLabel(
            "Wenn aktiviert, werden Autoren-Metadaten sowohl bei DataCite als auch "
            "in der internen GFZ-Datenbank aktualisiert. Updates erfolgen nur, wenn "
            "BEIDE Systeme erreichbar sind.\n\n"
            "⚠️ Wichtig: Erfordert VPN-Verbindung zum GFZ-Netzwerk!"
        )
        info_label.setWordWrap(True)
        info_label.setStyleSheet("color: #666; padding: 10px; background-color: #f0f0f0; border-radius: 4px;")
        layout.addWidget(info_label)
        
        # Credentials group
        creds_group = QGroupBox("Datenbank-Zugangsdaten")
        creds_layout = QVBoxLayout()
        
        # Host
        host_layout = QHBoxLayout()
        host_layout.addWidget(QLabel("Host:"))
        self.host_input = QLineEdit()
        self.host_input.setPlaceholderText("rz-mysql3.gfz-potsdam.de")
        host_layout.addWidget(self.host_input)
        creds_layout.addLayout(host_layout)
        
        # Database
        db_layout = QHBoxLayout()
        db_layout.addWidget(QLabel("Datenbank:"))
        self.database_input = QLineEdit()
        self.database_input.setPlaceholderText("sumario-pmd")
        db_layout.addWidget(self.database_input)
        creds_layout.addLayout(db_layout)
        
        # Username
        user_layout = QHBoxLayout()
        user_layout.addWidget(QLabel("Benutzername:"))
        self.username_input = QLineEdit()
        self.username_input.setPlaceholderText("sumario-pmd_rw")
        user_layout.addWidget(self.username_input)
        creds_layout.addLayout(user_layout)
        
        # Password
        pass_layout = QHBoxLayout()
        pass_layout.addWidget(QLabel("Passwort:"))
        self.password_input = QLineEdit()
        self.password_input.setEchoMode(QLineEdit.EchoMode.Password)
        pass_layout.addWidget(self.password_input)
        creds_layout.addLayout(pass_layout)
        
        # Connection test button and status
        test_layout = QHBoxLayout()
        self.test_button = QPushButton("Verbindung testen")
        self.test_button.clicked.connect(self._test_connection)
        test_layout.addWidget(self.test_button)
        
        self.connection_status = QLabel("⚪ Nicht getestet")
        test_layout.addWidget(self.connection_status)
        test_layout.addStretch()
        creds_layout.addLayout(test_layout)
        
        creds_group.setLayout(creds_layout)
        layout.addWidget(creds_group)
        
        layout.addStretch()
        
        return widget
    
    def _load_settings(self):
        """Load settings from QSettings and Keyring."""
        # Load theme
        current_theme = self.theme_manager.get_current_theme()
        if current_theme == Theme.AUTO:
            self.auto_theme_radio.setChecked(True)
        elif current_theme == Theme.LIGHT:
            self.light_theme_radio.setChecked(True)
        else:  # DARK
            self.dark_theme_radio.setChecked(True)
        
        # Load database settings
        db_enabled = self.settings.value("database/enabled", False, type=bool)
        self.db_enabled_checkbox.setChecked(db_enabled)
        
        # Load DB credentials from QSettings (metadata) if they exist
        # Security: No default values - user must enter all credentials explicitly
        db_host = self.settings.value("database/host", "")
        db_name = self.settings.value("database/name", "")
        db_user = self.settings.value("database/username", "")
        
        self.host_input.setText(db_host)
        self.database_input.setText(db_name)
        self.username_input.setText(db_user)
        
        # Password is loaded from Keyring only when actually needed
        # Don't pre-fill password field for security
        
        self._on_db_enabled_toggled(db_enabled)
    
    def _on_db_enabled_toggled(self, checked: bool):
        """
        Handle database enabled checkbox toggle.
        
        Args:
            checked: New checkbox state
        """
        # Enable/disable credential inputs
        self.host_input.setEnabled(checked)
        self.database_input.setEnabled(checked)
        self.username_input.setEnabled(checked)
        self.password_input.setEnabled(checked)
        self.test_button.setEnabled(checked)
    
    def _test_connection(self):
        """Test database connection (non-blocking)."""
        # Validate inputs
        host = self.host_input.text().strip()
        database = self.database_input.text().strip()
        username = self.username_input.text().strip()
        password = self.password_input.text()
        
        if not all([host, database, username, password]):
            QMessageBox.warning(
                self,
                "Unvollständige Eingabe",
                "Bitte füllen Sie alle Felder aus."
            )
            return
        
        # Disable button during test
        self.test_button.setEnabled(False)
        self.connection_status.setText("🔄 Teste Verbindung...")
        
        # Create worker thread
        self.connection_test_thread = QThread()
        self.connection_test_worker = ConnectionTestWorker(host, database, username, password)
        self.connection_test_worker.moveToThread(self.connection_test_thread)
        
        # Connect signals
        self.connection_test_thread.started.connect(self.connection_test_worker.run)
        self.connection_test_worker.finished.connect(self._on_connection_test_finished)
        self.connection_test_worker.finished.connect(self.connection_test_thread.quit)
        self.connection_test_worker.finished.connect(self.connection_test_worker.deleteLater)
        self.connection_test_thread.finished.connect(self.connection_test_thread.deleteLater)
        
        # Start test
        self.connection_test_thread.start()
    
    def _on_connection_test_finished(self, success: bool, message: str):
        """
        Handle connection test result.
        
        Args:
            success: Whether connection was successful
            message: Status message
        """
        self.connection_status.setText(message)
        self.test_button.setEnabled(True)
        
        if success:
            self.connection_status.setStyleSheet("color: green; font-weight: bold;")
        else:
            self.connection_status.setStyleSheet("color: red;")
    
    def _save_and_close(self):
        """Save settings and close dialog."""
        try:
            # Save theme
            if self.auto_theme_radio.isChecked():
                new_theme = Theme.AUTO
            elif self.light_theme_radio.isChecked():
                new_theme = Theme.LIGHT
            else:
                new_theme = Theme.DARK
            
            if new_theme != self.theme_manager.get_current_theme():
                self.theme_manager.set_theme(new_theme)
                self.theme_changed.emit(new_theme)
            
            # Save database settings
            db_enabled = self.db_enabled_checkbox.isChecked()
            self.settings.setValue("database/enabled", db_enabled)
            
            if db_enabled:
                host = self.host_input.text().strip()
                database = self.database_input.text().strip()
                username = self.username_input.text().strip()
                password = self.password_input.text().strip()
                
                # Validate stripped values to prevent whitespace-only entries
                if not all([host, database, username, password]):
                    QMessageBox.warning(
                        self,
                        "Unvollständige Eingabe",
                        "Bitte füllen Sie alle Datenbank-Felder aus oder "
                        "deaktivieren Sie Datenbank-Updates."
                    )
                    return
                
                # Save credentials to Keyring
                save_db_credentials(host, database, username, password)
                
                # Save metadata to QSettings
                self.settings.setValue("database/host", host)
                self.settings.setValue("database/name", database)
                self.settings.setValue("database/username", username)
                self.settings.setValue("database/configured", True)
                
                logger.info("Database credentials saved successfully")
            else:
                # Clear configured flag if disabled
                self.settings.setValue("database/configured", False)
            
            self.accept()
            
        except CredentialStorageError as e:
            logger.error(f"Failed to save credentials: {e}")
            QMessageBox.critical(
                self,
                "Fehler beim Speichern",
                f"Credentials konnten nicht gespeichert werden:\n{str(e)}"
            )
        except Exception as e:
            logger.exception("Unexpected error saving settings")
            QMessageBox.critical(
                self,
                "Fehler",
                f"Ein unerwarteter Fehler ist aufgetreten:\n{str(e)}"
            )
    
    def _apply_theme(self):
        """Apply current theme to dialog."""
        stylesheet = self.theme_manager.get_credentials_dialog_stylesheet()
        self.setStyleSheet(stylesheet)
