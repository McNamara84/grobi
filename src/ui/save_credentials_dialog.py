"""
Dialog for saving DataCite credentials after successful authentication.

This dialog appears after the first successful API call when using new credentials,
asking the user if they want to save the credentials for future use.
"""

import logging
from PySide6.QtWidgets import (
    QDialog,
    QVBoxLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QMessageBox,
)

from ..utils.credential_manager import (
    CredentialManager,
    CredentialStorageError,
)

logger = logging.getLogger(__name__)


class SaveCredentialsDialog(QDialog):
    """
    Dialog for saving DataCite credentials after successful authentication.
    
    Displays username and API type (read-only), allows user to enter a display name,
    and provides information about Windows Credential Manager security.
    """

    def __init__(self, username: str, api_type: str, parent=None):
        """
        Initialize the save credentials dialog.
        
        Args:
            username: The DataCite username
            api_type: Either "test" or "production"
            parent: Parent widget
        """
        super().__init__(parent)
        self.username = username
        self.api_type = api_type
        self.credential_manager = CredentialManager()
        self.saved_account_id = None
        
        self.setWindowTitle("Zugangsdaten speichern")
        self.setModal(True)
        self.setMinimumWidth(500)
        
        self._setup_ui()
        
    def _setup_ui(self):
        """Set up the user interface."""
        layout = QVBoxLayout()
        layout.setSpacing(15)
        
        # Title
        title_label = QLabel("💾 Zugangsdaten speichern?")
        title_font = title_label.font()
        title_font.setPointSize(12)
        title_font.setBold(True)
        title_label.setFont(title_font)
        layout.addWidget(title_label)
        
        # Success message
        success_label = QLabel(
            "Die Authentifizierung war erfolgreich!\n"
            "Möchten Sie diese Zugangsdaten für zukünftige Verwendung speichern?"
        )
        success_label.setWordWrap(True)
        layout.addWidget(success_label)
        
        # Account name input
        name_label = QLabel("Name für diesen Account:")
        layout.addWidget(name_label)
        
        self.name_input = QLineEdit()
        self.name_input.setPlaceholderText("z.B. 'GFZ Produktiv-Account' oder 'Test-Umgebung'")
        self.name_input.setMaxLength(100)
        self.name_input.setToolTip(
            "Geben Sie einen eindeutigen Namen ein, um diesen Account später wiederzuerkennen (max. 100 Zeichen)"
        )
        self.name_input.textChanged.connect(self._check_save_ready)
        layout.addWidget(self.name_input)
        
        # Display username (read-only)
        username_label = QLabel(f"<b>Benutzername:</b> {self.username}")
        layout.addWidget(username_label)
        
        # Display API type (read-only)
        api_label_text = "Test-API" if self.api_type == "test" else "Produktiv-API"
        api_label = QLabel(f"<b>API-Typ:</b> {api_label_text}")
        layout.addWidget(api_label)
        
        # Security info
        info_label = QLabel(
            "🔒 <b>Sicherheitshinweis:</b><br>"
            "Das Passwort wird sicher im Windows Credential Manager gespeichert<br>"
            "und ist nur für Ihr Windows-Benutzerkonto zugänglich."
        )
        info_label.setWordWrap(True)
        info_label.setStyleSheet("color: #666; padding: 10px; background-color: #f5f5f5; border-radius: 5px;")
        layout.addWidget(info_label)
        
        # Buttons
        button_layout = QHBoxLayout()
        button_layout.addStretch()
        
        self.cancel_button = QPushButton("Nicht speichern")
        self.cancel_button.clicked.connect(self.reject)
        button_layout.addWidget(self.cancel_button)
        
        self.save_button = QPushButton("💾 Speichern")
        self.save_button.setDefault(True)
        self.save_button.setEnabled(False)  # Initially disabled
        self.save_button.clicked.connect(self._save_credentials)
        button_layout.addWidget(self.save_button)
        
        layout.addLayout(button_layout)
        
        self.setLayout(layout)
        
        # Focus on name input
        self.name_input.setFocus()
        
    def _check_save_ready(self):
        """Enable save button only when name is entered."""
        name = self.name_input.text().strip()
        self.save_button.setEnabled(len(name) > 0)
        
    def _save_credentials(self):
        """Save the credentials with the entered display name."""
        display_name = self.name_input.text().strip()
        
        if not display_name:
            QMessageBox.warning(
                self,
                "Eingabe erforderlich",
                "Bitte geben Sie einen Namen für diesen Account ein."
            )
            return
        
        # Note: Password is not passed here as it needs to be provided by the caller
        # who has access to the actual password. This dialog only handles the UI part.
        self.display_name = display_name
        self.accept()
        
    def get_display_name(self) -> str:
        """
        Get the entered display name.
        
        Returns:
            The display name entered by the user, or empty string if dialog was canceled.
        """
        return getattr(self, 'display_name', '')
    
    @staticmethod
    def ask_save_credentials(username: str, password: str, api_type: str, parent=None) -> bool:
        """
        Static method to show dialog and save credentials if user confirms.
        
        Args:
            username: The DataCite username
            password: The DataCite password (will be securely stored)
            api_type: Either "test" or "production"
            parent: Parent widget
            
        Returns:
            True if credentials were saved, False otherwise
        """
        dialog = SaveCredentialsDialog(username, api_type, parent)
        
        if dialog.exec() == QDialog.Accepted:
            display_name = dialog.get_display_name()
            
            try:
                manager = CredentialManager()
                account_id = manager.save_credentials(
                    display_name=display_name,
                    username=username,
                    password=password,
                    api_type=api_type
                )
                
                # Set as last used account
                manager.set_last_used_account(account_id)
                
                logger.info(f"Credentials saved successfully: {display_name} ({username}, {api_type})")
                
                QMessageBox.information(
                    parent,
                    "Gespeichert",
                    f"Die Zugangsdaten wurden erfolgreich als '{display_name}' gespeichert."
                )
                
                return True
                
            except CredentialStorageError as e:
                logger.error(f"Failed to save credentials: {e}")
                QMessageBox.critical(
                    parent,
                    "Fehler beim Speichern",
                    f"Die Zugangsdaten konnten nicht gespeichert werden:\n{str(e)}"
                )
                return False
            except Exception as e:
                logger.error(f"Unexpected error saving credentials: {e}")
                QMessageBox.critical(
                    parent,
                    "Fehler",
                    f"Ein unerwarteter Fehler ist aufgetreten:\n{str(e)}"
                )
                return False
        
        return False
