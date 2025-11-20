"""Credentials Dialog for DataCite API authentication."""

from pathlib import Path

from PySide6.QtWidgets import (
    QDialog, QVBoxLayout, QLabel, 
    QLineEdit, QCheckBox, QFormLayout,
    QDialogButtonBox, QPushButton, QFileDialog, QHBoxLayout
)
from PySide6.QtGui import QFont
from PySide6.QtCore import Signal


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
        
        # Set window title based on mode
        if mode == "update":
            self.setWindowTitle("Landing Page URLs aktualisieren")
        else:
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
        
        # Description - varies by mode
        if self.mode == "update":
            description_text = (
                "Gib deine DataCite Zugangsdaten ein und wähle eine CSV-Datei "
                "mit DOIs und Landing Page URLs aus."
            )
        else:
            description_text = "Gib deine DataCite Zugangsdaten ein, um DOIs abzurufen."
        
        description = QLabel(description_text)
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
            "Aktiviere diese Option, um die Test-API anstelle der Produktions-API zu verwenden."
        )
        layout.addWidget(self.test_api_checkbox)
        
        # CSV file selection (only for update mode)
        if self.mode == "update":
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
        else:
            self.ok_button.setText("DOIs holen")
        
        cancel_button = button_box.button(QDialogButtonBox.Cancel)
        cancel_button.setText("Abbrechen")
        
        layout.addWidget(button_box)
        
        # Connect input changes to validation for update mode
        if self.mode == "update":
            self.username_input.textChanged.connect(self._check_update_ready)
            self.password_input.textChanged.connect(self._check_update_ready)
        
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
        if self.mode == "update":
            has_credentials = (
                bool(self.username_input.text().strip()) and 
                bool(self.password_input.text().strip())
            )
            has_csv = self.csv_file_path is not None
            
            self.ok_button.setEnabled(has_credentials and has_csv)
    
    def get_credentials(self):
        """
        Get the entered credentials.
        
        Returns:
            Tuple of (username, password, csv_path, use_test_api) or None if canceled.
            For export mode, csv_path will be None.
        """
        if self.exec() == QDialog.Accepted:
            username = self.username_input.text().strip()
            password = self.password_input.text().strip()
            use_test_api = self.test_api_checkbox.isChecked()
            
            # Always return 4-tuple for consistency
            if self.mode == "update":
                csv_path = self.csv_file_path
            else:
                csv_path = None
            
            return (username, password, csv_path, use_test_api)
        return None
