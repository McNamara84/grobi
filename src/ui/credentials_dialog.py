"""Credentials Dialog for DataCite API authentication."""

from pathlib import Path
from typing import Optional
import logging

from PySide6.QtWidgets import (
    QDialog, QVBoxLayout, QLabel, 
    QLineEdit, QCheckBox, QFormLayout,
    QDialogButtonBox, QPushButton, QFileDialog, QHBoxLayout,
    QComboBox, QMessageBox
)
from PySide6.QtGui import QFont
from PySide6.QtCore import Signal

from src.utils.credential_manager import (
    CredentialManager,
    CredentialNotFoundError,
    CredentialStorageError
)

logger = logging.getLogger(__name__)


class CredentialsDialog(QDialog):
    """Dialog for entering DataCite credentials."""
    
    # Signals
    csv_file_selected = Signal(str)  # Emitted when CSV file is selected
    
    def __init__(self, parent=None, mode="export"):
        """
        Initialize the credentials dialog.
        
        Args:
            parent: Parent widget
            mode: Dialog mode - "export" for DOI export or "update" for URL update
        """
        super().__init__(parent)
        self.mode = mode
        self.csv_file_path = None
        
        # Initialize credential manager
        self.credential_manager = None
        self.saved_accounts = []
        self.selected_account_id = None
        self._loaded_password = None  # Stores password when loading from saved account
        self._is_new_credentials = True  # Initialize flag, updated when account is loaded
    
        try:
            self.credential_manager = CredentialManager()
            self.saved_accounts = self.credential_manager.list_accounts()
            logger.info(f"Loaded {len(self.saved_accounts)} saved accounts")
        except CredentialStorageError as e:
            logger.warning(f"Failed to initialize credential manager: {e}")
            # Continue without credential management
        
        # Get theme manager from parent if available
        self.theme_manager = None
        if parent and hasattr(parent, 'theme_manager'):
            self.theme_manager = parent.theme_manager
        
        # Set window title based on mode
        if mode == "update":
            self.setWindowTitle("Landing Page URLs aktualisieren")
        elif mode == "update_authors":
            self.setWindowTitle("Autoren-Metadaten aktualisieren")
        elif mode == "update_publisher":
            self.setWindowTitle("Publisher-Metadaten aktualisieren")
        elif mode == "update_rights":
            self.setWindowTitle("Rights-Metadaten aktualisieren")
        elif mode == "fuji_check":
            self.setWindowTitle("F-UJI FAIR Assessment")
        else:
            self.setWindowTitle("DataCite Anmeldung")
        
        self.setModal(True)
        self.setMinimumWidth(400)
        
        self._setup_ui()
        self._apply_styles()
    
    @property
    def loaded_password(self) -> Optional[str]:
        """Get the password loaded from a saved account.
        
        Returns:
            The password if loaded from saved account, None otherwise.
        """
        return self._loaded_password
    
    @property
    def is_new_credentials(self) -> bool:
        """Check if credentials are new (not from saved account).
        
        Returns:
            True if credentials are new, False if loaded from saved account.
        """
        return self._is_new_credentials

    def _setup_ui(self):
        """Set up the user interface."""
        layout = QVBoxLayout(self)
        layout.setSpacing(15)
        
        # Title
        title = QLabel("DataCite API Zugangsdaten")
        title_font = QFont()
        title_font.setPointSize(12)
        title_font.setBold(True)
        title.setFont(title_font)
        layout.addWidget(title)
        
        # Description - varies by mode
        if self.mode == "update":
            description_text = (
                "Gib deine DataCite Zugangsdaten ein und wähle eine CSV-Datei "
                "mit DOIs und Landing Page URLs aus."
            )
        elif self.mode == "update_authors":
            description_text = (
                "Gib deine DataCite Zugangsdaten ein und wähle eine CSV-Datei "
                "mit DOIs und Autoren-Metadaten aus."
            )
        elif self.mode == "update_publisher":
            description_text = (
                "Gib deine DataCite Zugangsdaten ein und wähle eine CSV-Datei "
                "mit DOIs und Publisher-Metadaten aus."
            )
        elif self.mode == "update_contributors":
            description_text = (
                "Gib deine DataCite Zugangsdaten ein und wähle eine CSV-Datei "
                "mit DOIs und Contributor-Metadaten aus."
            )
        elif self.mode == "fuji_check":
            description_text = (
                "Gib deine DataCite Zugangsdaten ein, um alle DOIs abzurufen "
                "und nach FAIR-Kriterien zu bewerten."
            )
        else:
            description_text = "Gib deine DataCite Zugangsdaten ein, um DOIs abzurufen."
        
        description = QLabel(description_text)
        description.setWordWrap(True)
        layout.addWidget(description)
        
        # Account selector (if credential manager available)
        if self.credential_manager and self.saved_accounts:
            layout.addSpacing(5)
            
            account_label = QLabel("Gespeicherte Zugangsdaten:")
            account_label_font = QFont()
            account_label_font.setBold(True)
            account_label.setFont(account_label_font)
            layout.addWidget(account_label)
            
            # Dropdown with delete button
            account_layout = QHBoxLayout()
            
            self.account_selector = QComboBox()
            self.account_selector.setToolTip(
                "Wählen Sie einen gespeicherten Account oder geben Sie neue Zugangsdaten ein"
            )
            self.account_selector.addItem("➕ Neue Zugangsdaten eingeben", None)
            
            # Add saved accounts
            for account in self.saved_accounts:
                api_label = "Test-API" if account.api_type == "test" else "Produktiv-API"
                display_text = f"{account.display_name} ({account.username} - {api_label})"
                self.account_selector.addItem(display_text, account.account_id)
            
            self.account_selector.currentIndexChanged.connect(self._on_account_selected)
            account_layout.addWidget(self.account_selector, 1)
            
            # Delete button
            self.delete_button = QPushButton("🗑️")
            self.delete_button.setToolTip("Ausgewählten Account löschen")
            self.delete_button.setFixedWidth(40)
            self.delete_button.setEnabled(False)  # Disabled initially
            self.delete_button.clicked.connect(self._on_delete_account_clicked)
            account_layout.addWidget(self.delete_button)
            
            layout.addLayout(account_layout)
            
            # Separator
            separator = QLabel("─" * 50)
            separator.setStyleSheet("color: #ccc;")
            layout.addWidget(separator)
        
        # Form layout for input fields
        form_layout = QFormLayout()
        form_layout.setSpacing(10)
        
        # Username field
        self.username_input = QLineEdit()
        self.username_input.setPlaceholderText("z.B. TIB.GFZ")
        form_layout.addRow("Benutzername:", self.username_input)
        
        # Password field
        self.password_input = QLineEdit()
        self.password_input.setEchoMode(QLineEdit.Password)
        self.password_input.setPlaceholderText("Passwort eingeben")
        form_layout.addRow("Passwort:", self.password_input)
        
        layout.addLayout(form_layout)
        
        # Test API checkbox
        self.test_api_checkbox = QCheckBox("Test-API verwenden (https://api.test.datacite.org)")
        self.test_api_checkbox.setToolTip(
            "Aktiviere diese Option, um die Test-API anstelle der Produktions-API zu verwenden."
        )
        layout.addWidget(self.test_api_checkbox)
        
        # CSV file selection (only for update modes)
        if self.mode in ["update", "update_authors", "update_publisher", "update_contributors", "update_rights"]:
            layout.addSpacing(10)
            
            csv_label = QLabel("CSV-Datei auswählen:")
            csv_label_font = QFont()
            csv_label_font.setBold(True)
            csv_label.setFont(csv_label_font)
            layout.addWidget(csv_label)
            
            # CSV file selection layout
            csv_layout = QHBoxLayout()
            
            self.csv_file_label = QLabel("Keine Datei ausgewählt")
            self.csv_file_label.setStyleSheet("color: #666;")
            csv_layout.addWidget(self.csv_file_label, 1)
            
            self.csv_browse_button = QPushButton("Durchsuchen...")
            self.csv_browse_button.clicked.connect(self._browse_csv_file)
            csv_layout.addWidget(self.csv_browse_button)
            
            layout.addLayout(csv_layout)
        
        # Add some spacing
        layout.addSpacing(10)
        
        # Button box
        button_box = QDialogButtonBox(
            QDialogButtonBox.Ok | QDialogButtonBox.Cancel
        )
        button_box.accepted.connect(self._validate_and_accept)
        button_box.rejected.connect(self.reject)
        
        # Customize button text based on mode
        self.ok_button = button_box.button(QDialogButtonBox.Ok)
        if self.mode == "update":
            self.ok_button.setText("Landing Page URLs aktualisieren")
            # Disable button initially for update mode (needs CSV file)
            self.ok_button.setEnabled(False)
        elif self.mode == "update_authors":
            self.ok_button.setText("Autoren-Metadaten aktualisieren")
            # Disable button initially for update_authors mode (needs CSV file)
            self.ok_button.setEnabled(False)
        elif self.mode == "update_publisher":
            self.ok_button.setText("Publisher-Metadaten aktualisieren")
            # Disable button initially for update_publisher mode (needs CSV file)
            self.ok_button.setEnabled(False)
        elif self.mode == "update_contributors":
            self.ok_button.setText("Contributor-Metadaten aktualisieren")
            # Disable button initially for update_contributors mode (needs CSV file)
            self.ok_button.setEnabled(False)
        elif self.mode == "update_rights":
            self.ok_button.setText("Rights-Metadaten aktualisieren")
            # Disable button initially for update_rights mode (needs CSV file)
            self.ok_button.setEnabled(False)
        elif self.mode == "fuji_check":
            self.ok_button.setText("FAIR Check starten")
        else:
            self.ok_button.setText("DOIs holen")
        
        cancel_button = button_box.button(QDialogButtonBox.Cancel)
        cancel_button.setText("Abbrechen")
        
        layout.addWidget(button_box)
        
        # Pre-select last used account AFTER ok_button is created
        # (needs ok_button for _check_update_ready in update modes)
        if self.credential_manager and self.saved_accounts:
            self._preselect_last_used_account()
        
        # Set focus to username field
        self.username_input.setFocus()
        
        # Connect input changes to validation for update modes
        if self.mode in ["update", "update_authors", "update_publisher", "update_contributors", "update_rights"]:
            self.username_input.textChanged.connect(self._check_update_ready)
            self.password_input.textChanged.connect(self._check_update_ready)
        
        # Log dialog initialization
        logger.info(f"CredentialsDialog initialized: mode={self.mode}, ok_button_enabled={self.ok_button.isEnabled()}")
    
    def _apply_styles(self):
        """Apply styling to the dialog based on current theme."""
        if self.theme_manager:
            stylesheet = self.theme_manager.get_credentials_dialog_stylesheet()
            self.setStyleSheet(stylesheet)
        else:
            # Fallback to light theme if no theme manager available
            self._apply_light_styles()
    
    def _apply_light_styles(self):
        """Apply light theme styling (fallback)."""
        self.setStyleSheet("""
            QDialog {
                background-color: #f5f5f5;
            }
            QLabel {
                color: #333;
            }
            QLineEdit {
                padding: 8px;
                border: 1px solid #ccc;
                border-radius: 4px;
                background-color: white;
            }
            QLineEdit:focus {
                border: 2px solid #0078d4;
            }
            QCheckBox {
                color: #333;
                spacing: 8px;
            }
            QCheckBox::indicator {
                width: 18px;
                height: 18px;
            }
            QPushButton {
                padding: 8px 16px;
                border-radius: 4px;
                background-color: #0078d4;
                color: white;
                border: none;
                min-width: 80px;
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
        """)
    
    def _validate_and_accept(self):
        """Validate input before accepting the dialog."""
        username = self.username_input.text().strip()
        password = self.password_input.text().strip()
        
        if not username:
            self.username_input.setFocus()
            self.username_input.setStyleSheet(
                "QLineEdit { border: 2px solid #d32f2f; }"
            )
            return
        
        if not password:
            self.password_input.setFocus()
            self.password_input.setStyleSheet(
                "QLineEdit { border: 2px solid #d32f2f; }"
            )
            return
        
        self.accept()
    
    def _browse_csv_file(self):
        """Open file dialog to select CSV file."""
        file_path, _ = QFileDialog.getOpenFileName(
            self,
            "CSV-Datei auswählen",
            "",
            "CSV-Dateien (*.csv);;Alle Dateien (*.*)"
        )
        
        if file_path:
            self.csv_file_path = file_path
            # Show shortened path in label
            self.csv_file_label.setText(Path(file_path).name)
            self.csv_file_label.setStyleSheet("color: #333; font-weight: bold;")
            self.csv_file_selected.emit(file_path)
            
            # Check if we can enable the OK button
            self._check_update_ready()
    
    def _check_update_ready(self):
        """Check if all requirements for update are met and enable/disable OK button."""
        if self.mode in ["update", "update_authors", "update_publisher", "update_contributors", "update_rights"]:
            has_credentials = (
                bool(self.username_input.text().strip()) and 
                bool(self.password_input.text().strip())
            )
            has_csv = self.csv_file_path is not None
            
            self.ok_button.setEnabled(has_credentials and has_csv)
    
    def _preselect_last_used_account(self):
        """Pre-select the last used account if available."""
        if not self.credential_manager:
            return
        
        last_used_id = self.credential_manager.get_last_used_account()
        if not last_used_id:
            return
        
        # Find index of last used account in combo box
        for i in range(1, self.account_selector.count()):  # Skip index 0 (new credentials)
            account_id = self.account_selector.itemData(i)
            if account_id == last_used_id:
                self.account_selector.setCurrentIndex(i)
                logger.info(f"Pre-selected last used account: {last_used_id}")
                # Explicitly load credentials (setCurrentIndex doesn't trigger signal during init)
                self._load_account_credentials(last_used_id)
                break
    
    def _on_account_selected(self, index):
        """Handle account selection from dropdown."""
        if index == 0:  # "Neue Zugangsdaten eingeben"
            self._clear_fields()
            self._enable_input_fields(True)
            self.delete_button.setEnabled(False)
            self.selected_account_id = None
            self._loaded_password = None
            self._is_new_credentials = True
        else:
            # Load account credentials
            account_id = self.account_selector.itemData(index)
            if account_id:
                self._load_account_credentials(account_id)
                self.delete_button.setEnabled(True)
    
    def _load_account_credentials(self, account_id: str):
        """Load credentials for selected account."""
        try:
            username, password, api_type = self.credential_manager.get_credentials(account_id)
            
            # Fill in fields
            self.username_input.setText(username)
            self.password_input.setText("●●●●●●●●")  # Masked password
            self.test_api_checkbox.setChecked(api_type == "test")
            
            # Store for later retrieval
            self._loaded_password = password
            self.selected_account_id = account_id
            
            # Mark as saved credentials (not new)
            self._is_new_credentials = False
            
            # Make fields read-only
            self._enable_input_fields(False)
            
            # Check if OK button should be enabled (for update modes, also needs CSV)
            self._check_update_ready()
            
            logger.info(f"Loaded credentials for account: {account_id}")
            
        except (CredentialNotFoundError, CredentialStorageError) as e:
            logger.error(f"Failed to load credentials: {e}")
            QMessageBox.warning(
                self,
                "Fehler beim Laden",
                f"Die Zugangsdaten konnten nicht geladen werden:\n{str(e)}"
            )
            # Reset to "Neue Zugangsdaten"
            self.account_selector.setCurrentIndex(0)
    
    def _clear_fields(self):
        """Clear all input fields."""
        self.username_input.clear()
        self.password_input.clear()
        self.test_api_checkbox.setChecked(False)
    
    def _enable_input_fields(self, enabled: bool):
        """Enable or disable input fields."""
        self.username_input.setEnabled(enabled)
        self.password_input.setEnabled(enabled)
        self.test_api_checkbox.setEnabled(enabled)
        
        if enabled:
            # Reset password field to normal input
            self.password_input.clear()
            self.password_input.setEchoMode(QLineEdit.Password)
        else:
            # Read-only mode - show masked password
            self.password_input.setEchoMode(QLineEdit.Normal)
            self.username_input.setStyleSheet("QLineEdit { background-color: #f0f0f0; }")
            self.password_input.setStyleSheet("QLineEdit { background-color: #f0f0f0; }")
    
    def _on_delete_account_clicked(self):
        """Handle delete button click."""
        if not self.selected_account_id:
            return
        
        # Find account details for confirmation dialog
        account = None
        for acc in self.saved_accounts:
            if acc.account_id == self.selected_account_id:
                account = acc
                break
        
        if not account:
            return
        
        # Confirmation dialog
        reply = QMessageBox.question(
            self,
            "Account löschen",
            f"Möchten Sie den Account '{account.display_name}' wirklich löschen?\n\n"
            f"Username: {account.username}\n"
            f"API-Typ: {'Test-API' if account.api_type == 'test' else 'Produktiv-API'}\n\n"
            f"Diese Aktion kann nicht rückgängig gemacht werden.",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No
        )
        
        if reply == QMessageBox.Yes:
            try:
                self.credential_manager.delete_account(self.selected_account_id)
                logger.info(f"Deleted account: {account.display_name}")
                
                # Refresh account list
                self._refresh_account_list()
                
                QMessageBox.information(
                    self,
                    "Erfolgreich gelöscht",
                    f"Der Account '{account.display_name}' wurde gelöscht."
                )
                
            except Exception as e:
                logger.error(f"Failed to delete account: {e}")
                QMessageBox.warning(
                    self,
                    "Fehler beim Löschen",
                    f"Der Account konnte nicht gelöscht werden:\n{str(e)}"
                )
    
    def _refresh_account_list(self):
        """Refresh the account dropdown after changes."""
        self.account_selector.clear()
        
        # Reload accounts
        self.saved_accounts = self.credential_manager.list_accounts()
        
        # Re-populate dropdown
        self.account_selector.addItem("➕ Neue Zugangsdaten eingeben", None)
        for account in self.saved_accounts:
            api_label = "Test-API" if account.api_type == "test" else "Produktiv-API"
            display_text = f"{account.display_name} ({account.username} - {api_label})"
            self.account_selector.addItem(display_text, account.account_id)
        
        # Reset to "Neue Zugangsdaten"
        self.account_selector.setCurrentIndex(0)
    
    def is_new_credentials(self) -> bool:
        """
        Check if user is entering new credentials (not using saved account).
        
        Returns:
            True if new credentials are being entered, False if using saved account
        """
        if not self.credential_manager or not hasattr(self, 'account_selector'):
            return True  # No credential manager or selector = always new
        
        return self.account_selector.currentIndex() == 0  # Index 0 = "Neue Zugangsdaten"
    
    def get_credentials(self):
        """
        Get the entered credentials.
        
        Returns:
            Tuple of (username, password, csv_path, use_test_api) or None if canceled.
            For export mode, csv_path will be None.
        """
        if self.exec() == QDialog.Accepted:
            username = self.username_input.text().strip()
            
            # Use loaded password if available (from saved account), otherwise get from input
            if self._loaded_password:
                password = self._loaded_password
                # Mark account as last used
                if self.selected_account_id and self.credential_manager:
                    try:
                        self.credential_manager.set_last_used_account(self.selected_account_id)
                        logger.info(f"Marked account as last used: {self.selected_account_id}")
                    except Exception as e:
                        logger.warning(f"Failed to set last used account: {e}")
            else:
                password = self.password_input.text().strip()
            
            use_test_api = self.test_api_checkbox.isChecked()
            
            # Always return 4-tuple for consistency
            if self.mode in ["update", "update_authors", "update_publisher", "update_contributors", "update_rights"]:
                csv_path = self.csv_file_path
            else:
                csv_path = None
            
            return (username, password, csv_path, use_test_api)
        return None
