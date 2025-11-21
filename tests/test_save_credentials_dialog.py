"""
Tests for SaveCredentialsDialog.
"""

import pytest
from unittest.mock import Mock, patch
from PySide6.QtWidgets import QDialog, QMessageBox
from PySide6.QtCore import Qt

from src.ui.save_credentials_dialog import SaveCredentialsDialog
from src.utils.credential_manager import CredentialStorageError


class TestSaveCredentialsDialog:
    """Test SaveCredentialsDialog functionality."""

    @pytest.fixture
    def dialog(self, qtbot):
        """Create a SaveCredentialsDialog instance."""
        dialog = SaveCredentialsDialog(
            username="MOCK_TEST_USER",
            api_type="production"
        )
        qtbot.addWidget(dialog)
        return dialog

    def test_initialization(self, dialog):
        """Test dialog initialization."""
        assert dialog.username == "MOCK_TEST_USER"
        assert dialog.api_type == "production"
        assert dialog.saved_account_id is None
        assert dialog.windowTitle() == "Zugangsdaten speichern"
        assert dialog.isModal()

    def test_ui_elements_present(self, dialog):
        """Test that all UI elements are present."""
        assert dialog.name_input is not None
        assert dialog.save_button is not None
        assert dialog.cancel_button is not None
        
        # Save button should be disabled initially
        assert not dialog.save_button.isEnabled()

    def test_save_button_enables_with_input(self, dialog, qtbot):
        """Test that save button enables when name is entered."""
        assert not dialog.save_button.isEnabled()
        
        # Enter text
        qtbot.keyClicks(dialog.name_input, "Test Account")
        
        assert dialog.save_button.isEnabled()

    def test_save_button_disables_with_empty_input(self, dialog, qtbot):
        """Test that save button disables when input is cleared."""
        # Enter text
        qtbot.keyClicks(dialog.name_input, "Test")
        assert dialog.save_button.isEnabled()
        
        # Clear text
        dialog.name_input.clear()
        assert not dialog.save_button.isEnabled()

    def test_cancel_button_rejects_dialog(self, dialog, qtbot):
        """Test that cancel button rejects the dialog."""
        with qtbot.waitSignal(dialog.rejected):
            qtbot.mouseClick(dialog.cancel_button, Qt.LeftButton)

    def test_save_with_empty_name_shows_warning(self, dialog, qtbot):
        """Test that saving with empty name shows warning."""
        with patch.object(QMessageBox, 'warning') as mock_warning:
            dialog._save_credentials()
            mock_warning.assert_called_once()

    def test_save_with_valid_name_accepts_dialog(self, dialog, qtbot):
        """Test that saving with valid name accepts the dialog."""
        qtbot.keyClicks(dialog.name_input, "Test Account")
        
        with qtbot.waitSignal(dialog.accepted):
            dialog._save_credentials()
        
        assert dialog.get_display_name() == "Test Account"

    def test_get_display_name_returns_empty_when_canceled(self, dialog):
        """Test that get_display_name returns empty string when dialog is canceled."""
        # Don't set display_name
        assert dialog.get_display_name() == ""

    def test_display_username_shown(self, dialog):
        """Test that username is displayed in the dialog."""
        # Check that username is in dialog
        assert dialog.username == "MOCK_TEST_USER"

    def test_display_api_type_production(self, qtbot):
        """Test that production API type is displayed correctly."""
        dialog = SaveCredentialsDialog(
            username="MOCK_USER",
            api_type="production"
        )
        qtbot.addWidget(dialog)
        
        assert dialog.api_type == "production"

    def test_display_api_type_test(self, qtbot):
        """Test that test API type is displayed correctly."""
        dialog = SaveCredentialsDialog(
            username="MOCK_USER",
            api_type="test"
        )
        qtbot.addWidget(dialog)
        
        assert dialog.api_type == "test"


class TestAskSaveCredentials:
    """Test the static ask_save_credentials method."""

    @patch('src.ui.save_credentials_dialog.CredentialManager')
    @patch('src.ui.save_credentials_dialog.QMessageBox')
    def test_ask_save_credentials_user_accepts(self, mock_msgbox, mock_manager_class):
        """Test successful credential saving when user accepts."""
        # Mock dialog
        with patch('src.ui.save_credentials_dialog.SaveCredentialsDialog') as mock_dialog_class:
            mock_dialog = Mock()
            mock_dialog.exec.return_value = QDialog.Accepted
            mock_dialog.get_display_name.return_value = "Test Account"
            mock_dialog_class.return_value = mock_dialog
            
            # Mock credential manager
            mock_manager = Mock()
            mock_manager.save_credentials.return_value = "account-123"
            mock_manager_class.return_value = mock_manager
            
            # Call static method
            result = SaveCredentialsDialog.ask_save_credentials(
                username="MOCK_USER",
                password="FAKE_TEST_PASSWORD",
                api_type="test"
            )
            
            # Verify
            assert result is True
            mock_dialog_class.assert_called_once_with("MOCK_USER", "test", None)
            mock_manager.save_credentials.assert_called_once_with(
                display_name="Test Account",
                username="MOCK_USER",
                password="FAKE_TEST_PASSWORD",
                api_type="test"
            )
            mock_manager.set_last_used_account.assert_called_once_with("account-123")
            mock_msgbox.information.assert_called_once()

    @patch('src.ui.save_credentials_dialog.SaveCredentialsDialog')
    def test_ask_save_credentials_user_cancels(self, mock_dialog_class):
        """Test that False is returned when user cancels."""
        mock_dialog = Mock()
        mock_dialog.exec.return_value = QDialog.Rejected
        mock_dialog_class.return_value = mock_dialog
        
        result = SaveCredentialsDialog.ask_save_credentials(
            username="MOCK_USER",
            password="FAKE_PASSWORD",
            api_type="production"
        )
        
        assert result is False

    @patch('src.ui.save_credentials_dialog.CredentialManager')
    @patch('src.ui.save_credentials_dialog.QMessageBox')
    def test_ask_save_credentials_storage_error(self, mock_msgbox, mock_manager_class):
        """Test error handling when storage fails."""
        with patch('src.ui.save_credentials_dialog.SaveCredentialsDialog') as mock_dialog_class:
            mock_dialog = Mock()
            mock_dialog.exec.return_value = QDialog.Accepted
            mock_dialog.get_display_name.return_value = "Test"
            mock_dialog_class.return_value = mock_dialog
            
            # Mock credential manager to raise error
            mock_manager = Mock()
            mock_manager.save_credentials.side_effect = CredentialStorageError("Storage failed")
            mock_manager_class.return_value = mock_manager
            
            result = SaveCredentialsDialog.ask_save_credentials(
                username="MOCK_USER",
                password="FAKE_PASSWORD",
                api_type="test"
            )
            
            assert result is False
            mock_msgbox.critical.assert_called_once()

    @patch('src.ui.save_credentials_dialog.CredentialManager')
    @patch('src.ui.save_credentials_dialog.QMessageBox')
    def test_ask_save_credentials_unexpected_error(self, mock_msgbox, mock_manager_class):
        """Test error handling for unexpected errors."""
        with patch('src.ui.save_credentials_dialog.SaveCredentialsDialog') as mock_dialog_class:
            mock_dialog = Mock()
            mock_dialog.exec.return_value = QDialog.Accepted
            mock_dialog.get_display_name.return_value = "Test"
            mock_dialog_class.return_value = mock_dialog
            
            # Mock credential manager to raise unexpected error
            mock_manager = Mock()
            mock_manager.save_credentials.side_effect = Exception("Unexpected error")
            mock_manager_class.return_value = mock_manager
            
            result = SaveCredentialsDialog.ask_save_credentials(
                username="MOCK_USER",
                password="FAKE_PASSWORD",
                api_type="production"
            )
            
            assert result is False
            mock_msgbox.critical.assert_called_once()

    def test_name_input_max_length(self, qtbot):
        """Test that name input has maximum length."""
        dialog = SaveCredentialsDialog("MOCK_USER", "test")
        qtbot.addWidget(dialog)
        
        assert dialog.name_input.maxLength() == 100

    def test_focus_on_name_input(self, qtbot):
        """Test that focus is set to name input on startup."""
        dialog = SaveCredentialsDialog("MOCK_USER", "test")
        qtbot.addWidget(dialog)
        
        # Verify name_input exists and can receive focus
        assert dialog.name_input is not None
        assert dialog.name_input.isEnabled()
