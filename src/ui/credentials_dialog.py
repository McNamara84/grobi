"""Credentials Dialog for DataCite API authentication."""

from PySide6.QtWidgets import (
    QDialog, QVBoxLayout, QLabel, 
    QLineEdit, QCheckBox, QFormLayout,
    QDialogButtonBox
)
from PySide6.QtGui import QFont


class CredentialsDialog(QDialog):
    """Dialog for entering DataCite credentials."""
    
    def __init__(self, parent=None):
        """
        Initialize the credentials dialog.
        
        Args:
            parent: Parent widget
        """
        super().__init__(parent)
        self.setWindowTitle("DataCite Anmeldung")
        self.setModal(True)
        self.setMinimumWidth(400)
        
        self._setup_ui()
        self._apply_styles()
    
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
        
        # Description
        description = QLabel(
            "Geben Sie Ihre DataCite Zugangsdaten ein, um DOIs abzurufen."
        )
        description.setWordWrap(True)
        layout.addWidget(description)
        
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
            "Aktivieren Sie diese Option, um die Test-API anstelle der Produktions-API zu verwenden."
        )
        layout.addWidget(self.test_api_checkbox)
        
        # Add some spacing
        layout.addSpacing(10)
        
        # Button box
        button_box = QDialogButtonBox(
            QDialogButtonBox.Ok | QDialogButtonBox.Cancel
        )
        button_box.accepted.connect(self._validate_and_accept)
        button_box.rejected.connect(self.reject)
        
        # Customize button text
        ok_button = button_box.button(QDialogButtonBox.Ok)
        ok_button.setText("DOIs holen")
        cancel_button = button_box.button(QDialogButtonBox.Cancel)
        cancel_button.setText("Abbrechen")
        
        layout.addWidget(button_box)
        
        # Set focus to username field
        self.username_input.setFocus()
    
    def _apply_styles(self):
        """Apply modern styling to the dialog."""
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
    
    def get_credentials(self):
        """
        Get the entered credentials.
        
        Returns:
            Tuple of (username, password, use_test_api) or None if canceled
        """
        if self.exec() == QDialog.Accepted:
            return (
                self.username_input.text().strip(),
                self.password_input.text().strip(),
                self.test_api_checkbox.isChecked()
            )
        return None
