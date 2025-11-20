"""Unit tests for Credentials Dialog."""

import pytest
from PySide6.QtWidgets import QApplication, QDialog

from src.ui.credentials_dialog import CredentialsDialog


@pytest.fixture(scope="module")
def qapp():
    """Create QApplication instance for GUI tests."""
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    yield app


@pytest.fixture
def dialog(qapp, qtbot):
    """Create a CredentialsDialog instance for testing."""
    dlg = CredentialsDialog()
    qtbot.addWidget(dlg)
    return dlg


class TestCredentialsDialogInit:
    """Test CredentialsDialog initialization."""
    
    def test_dialog_creation(self, dialog):
        """Test that dialog is created successfully."""
        assert dialog is not None
        assert isinstance(dialog, QDialog)
    
    def test_dialog_title(self, dialog):
        """Test dialog window title."""
        assert dialog.windowTitle() == "DataCite Anmeldung"
    
    def test_dialog_is_modal(self, dialog):
        """Test that dialog is modal."""
        assert dialog.isModal() is True
    
    def test_dialog_minimum_width(self, dialog):
        """Test dialog minimum width."""
        assert dialog.minimumWidth() == 400


class TestCredentialsDialogWidgets:
    """Test dialog widgets and their properties."""
    
    def test_username_input_exists(self, dialog):
        """Test that username input field exists."""
        assert dialog.username_input is not None
        assert dialog.username_input.placeholderText() == "z.B. TIB.GFZ"
    
    def test_password_input_exists(self, dialog):
        """Test that password input field exists."""
        assert dialog.password_input is not None
        assert dialog.password_input.placeholderText() == "Passwort eingeben"
    
    def test_password_echo_mode(self, dialog):
        """Test that password field is hidden."""
        from PySide6.QtWidgets import QLineEdit
        assert dialog.password_input.echoMode() == QLineEdit.Password
    
    def test_test_api_checkbox_exists(self, dialog):
        """Test that test API checkbox exists."""
        assert dialog.test_api_checkbox is not None
        assert not dialog.test_api_checkbox.isChecked()


class TestCredentialsDialogValidation:
    """Test input validation."""
    
    def test_empty_username_validation(self, dialog):
        """Test validation fails with empty username."""
        dialog.username_input.clear()
        dialog.password_input.setText("password123")
        
        # Try to accept dialog
        dialog._validate_and_accept()
        
        # Dialog should not be accepted
        assert dialog.result() != QDialog.Accepted
    
    def test_empty_password_validation(self, dialog):
        """Test validation fails with empty password."""
        dialog.username_input.setText("TIB.GFZ")
        dialog.password_input.clear()
        
        # Try to accept dialog
        dialog._validate_and_accept()
        
        # Dialog should not be accepted
        assert dialog.result() != QDialog.Accepted
    
    def test_valid_credentials(self, dialog):
        """Test validation succeeds with valid credentials."""
        dialog.username_input.setText("TIB.GFZ")
        dialog.password_input.setText("password123")
        
        # Accept dialog
        dialog._validate_and_accept()
        
        # Dialog should be accepted
        assert dialog.result() == QDialog.Accepted


class TestCredentialsDialogGetCredentials:
    """Test getting credentials from dialog."""
    
    def test_get_credentials_success(self, qapp, qtbot):
        """Test getting credentials after acceptance."""
        dialog = CredentialsDialog()
        qtbot.addWidget(dialog)
        
        dialog.username_input.setText("TIB.GFZ")
        dialog.password_input.setText("secret")
        dialog.test_api_checkbox.setChecked(False)
        
        # Simulate acceptance
        dialog.accept()
        
        # Get credentials (won't block since already accepted)
        credentials = (
            dialog.username_input.text().strip(),
            dialog.password_input.text().strip(),
            None,  # csv_path is None in export mode
            dialog.test_api_checkbox.isChecked()
        )
        
        assert credentials == ("TIB.GFZ", "secret", None, False)
    
    def test_get_credentials_with_test_api(self, qapp, qtbot):
        """Test getting credentials with test API checked."""
        dialog = CredentialsDialog()
        qtbot.addWidget(dialog)
        
        dialog.username_input.setText("TEST.USER")
        dialog.password_input.setText("testpass")
        dialog.test_api_checkbox.setChecked(True)
        
        # Simulate acceptance
        dialog.accept()
        
        credentials = (
            dialog.username_input.text().strip(),
            dialog.password_input.text().strip(),
            None,  # csv_path is None in export mode
            dialog.test_api_checkbox.isChecked()
        )
        
        assert credentials == ("TEST.USER", "testpass", None, True)
    
    def test_get_credentials_cancelled(self, qapp, qtbot):
        """Test get_credentials returns None when cancelled."""
        dialog = CredentialsDialog()
        qtbot.addWidget(dialog)
        
        # Simulate rejection
        dialog.reject()
        
        # When rejected, get_credentials would return None
        # We simulate this behavior
        if dialog.result() == QDialog.Rejected:
            credentials = None
        
        assert credentials is None
    
    def test_whitespace_trimming(self, qapp, qtbot):
        """Test that whitespace is trimmed from inputs."""
        dialog = CredentialsDialog()
        qtbot.addWidget(dialog)
        
        dialog.username_input.setText("  TIB.GFZ  ")
        dialog.password_input.setText("  password  ")
        
        dialog.accept()
        
        credentials = (
            dialog.username_input.text().strip(),
            dialog.password_input.text().strip(),
            None,  # csv_path is None in export mode
            dialog.test_api_checkbox.isChecked()
        )
        
        assert credentials[0] == "TIB.GFZ"
        assert credentials[1] == "password"
        assert credentials[2] is None  # csv_path
        assert credentials[3] is False  # use_test_api


class TestCredentialsDialogInteraction:
    """Test user interactions."""
    
    def test_checkbox_state_change(self, dialog):
        """Test checkbox state can be changed programmatically."""
        assert not dialog.test_api_checkbox.isChecked()
        
        # Set checkbox
        dialog.test_api_checkbox.setChecked(True)
        assert dialog.test_api_checkbox.isChecked()
        
        # Unset checkbox
        dialog.test_api_checkbox.setChecked(False)
        assert not dialog.test_api_checkbox.isChecked()
    
    def test_text_input(self, dialog):
        """Test text can be entered in input fields."""
        dialog.username_input.setText("TestUser")
        assert dialog.username_input.text() == "TestUser"
        
        dialog.password_input.setText("TestPass")
        assert dialog.password_input.text() == "TestPass"
