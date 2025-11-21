"""Tests for CredentialsDialog initialization in update modes with saved accounts.

This test specifically addresses the bug where pre-selecting saved credentials
in update/update_authors modes would fail because _preselect_last_used_account()
was called before ok_button was created, causing _check_update_ready() to crash.
"""

import os
import pytest
from unittest.mock import MagicMock, patch
from PySide6.QtWidgets import QApplication

from src.ui.credentials_dialog import CredentialsDialog
from src.utils.credential_manager import CredentialAccount

# Skip GUI tests in CI environments (no display server available)
skip_in_ci = pytest.mark.skipif(
    os.environ.get('CI') == 'true',
    reason="GUI tests require display server (not available in CI)"
)


@pytest.fixture(scope="module")
def qapp():
    """Create QApplication instance for GUI tests."""
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    yield app


@pytest.fixture
def mock_credential_manager():
    """Create a mock CredentialManager with one saved account."""
    mock_cm = MagicMock()
    
    # Mock saved account
    saved_account = CredentialAccount(
        account_id="test-account-123",
        username="TEST.USER",
        api_type="test",
        display_name="Test Account",
        created_at="2025-11-21T10:00:00",
        last_modified="2025-11-21T10:00:00"
    )
    
    mock_cm.list_accounts.return_value = [saved_account]
    mock_cm.get_last_used_account.return_value = "test-account-123"
    mock_cm.get_credentials.return_value = ("TEST.USER", "test-password", "test")
    
    return mock_cm


class TestUpdateModeDialogInitWithSavedAccounts:
    """Test dialog initialization in update modes with saved accounts.
    
    This addresses the critical bug where calling _preselect_last_used_account()
    before ok_button creation would cause AttributeError in _check_update_ready().
    """
    
    @skip_in_ci
    def test_update_mode_dialog_opens_with_preselected_account(
        self, qapp, qtbot, mock_credential_manager
    ):
        """Test that update mode dialog can be created with preselected saved account.
        
        Regression test for bug where _preselect_last_used_account() was called
        before ok_button creation, causing _check_update_ready() to fail with
        AttributeError when trying to access self.ok_button.
        """
        with patch('src.ui.credentials_dialog.CredentialManager') as MockCM:
            MockCM.return_value = mock_credential_manager
            
            # Create dialog in update mode - should not raise exception
            dialog = CredentialsDialog(mode="update")
            qtbot.addWidget(dialog)
            
            # Verify dialog was created successfully
            assert dialog is not None
            assert dialog.mode == "update"
            
            # Verify ok_button exists and is in correct state
            assert hasattr(dialog, 'ok_button')
            assert dialog.ok_button is not None
            
            # In update mode with preselected credentials but no CSV,
            # ok_button should be disabled
            assert dialog.ok_button.isEnabled() is False
            
            # Verify credentials were loaded
            assert dialog.username_input.text() == "TEST.USER"
            assert dialog.password_input.text() == "●●●●●●●●"  # Masked
            assert dialog.test_api_checkbox.isChecked() is True
            
            # Verify account is selected in dropdown (index 1, not 0 which is "new")
            assert dialog.account_selector.currentIndex() == 1
            
            dialog.close()
    
    @skip_in_ci
    def test_update_authors_mode_dialog_opens_with_preselected_account(
        self, qapp, qtbot, mock_credential_manager
    ):
        """Test that update_authors mode dialog can be created with preselected account.
        
        Similar regression test for update_authors mode.
        """
        with patch('src.ui.credentials_dialog.CredentialManager') as MockCM:
            MockCM.return_value = mock_credential_manager
            
            # Create dialog in update_authors mode - should not raise exception
            dialog = CredentialsDialog(mode="update_authors")
            qtbot.addWidget(dialog)
            
            # Verify dialog was created successfully
            assert dialog is not None
            assert dialog.mode == "update_authors"
            
            # Verify ok_button exists and is in correct state
            assert hasattr(dialog, 'ok_button')
            assert dialog.ok_button is not None
            
            # In update_authors mode with preselected credentials but no CSV,
            # ok_button should be disabled
            assert dialog.ok_button.isEnabled() is False
            
            # Verify credentials were loaded
            assert dialog.username_input.text() == "TEST.USER"
            assert dialog.password_input.text() == "●●●●●●●●"  # Masked
            
            dialog.close()
    
    @skip_in_ci
    def test_export_mode_dialog_opens_with_preselected_account(
        self, qapp, qtbot, mock_credential_manager
    ):
        """Test that export mode dialog works correctly with preselected account.
        
        Export mode should have ok_button enabled after credentials are loaded.
        """
        with patch('src.ui.credentials_dialog.CredentialManager') as MockCM:
            MockCM.return_value = mock_credential_manager
            
            # Create dialog in export mode
            dialog = CredentialsDialog(mode="export")
            qtbot.addWidget(dialog)
            
            # Verify dialog was created successfully
            assert dialog is not None
            assert dialog.mode == "export"
            
            # Verify ok_button exists
            assert hasattr(dialog, 'ok_button')
            assert dialog.ok_button is not None
            
            # In export mode with credentials loaded, ok_button should be enabled
            # (no CSV required for export mode)
            assert dialog.ok_button.isEnabled() is True
            
            # Verify credentials were loaded
            assert dialog.username_input.text() == "TEST.USER"
            
            dialog.close()
    
    @skip_in_ci
    def test_ok_button_exists_before_preselect_is_called(
        self, qapp, qtbot, mock_credential_manager
    ):
        """Test that ok_button is created before _preselect_last_used_account is called.
        
        This is the core fix: ok_button must exist when _check_update_ready() is
        called from _load_account_credentials().
        """
        with patch('src.ui.credentials_dialog.CredentialManager') as MockCM:
            MockCM.return_value = mock_credential_manager
            
            # Patch _preselect_last_used_account to verify ok_button exists
            original_preselect = CredentialsDialog._preselect_last_used_account
            ok_button_existed = False
            
            def patched_preselect(self):
                nonlocal ok_button_existed
                # Check if ok_button exists before original method runs
                ok_button_existed = hasattr(self, 'ok_button') and self.ok_button is not None
                return original_preselect(self)
            
            with patch.object(
                CredentialsDialog,
                '_preselect_last_used_account',
                patched_preselect
            ):
                dialog = CredentialsDialog(mode="update")
                qtbot.addWidget(dialog)
                
                # Verify that ok_button existed when _preselect_last_used_account ran
                assert ok_button_existed is True
                
                dialog.close()
    
    @skip_in_ci
    def test_csv_selection_enables_ok_button_in_update_mode(
        self, qapp, qtbot, mock_credential_manager, tmp_path
    ):
        """Test that selecting CSV enables ok_button in update mode.
        
        After credentials are loaded, ok_button should be disabled.
        After CSV is selected, ok_button should be enabled.
        """
        with patch('src.ui.credentials_dialog.CredentialManager') as MockCM:
            MockCM.return_value = mock_credential_manager
            
            dialog = CredentialsDialog(mode="update")
            qtbot.addWidget(dialog)
            
            # Initially disabled (credentials loaded but no CSV)
            assert dialog.ok_button.isEnabled() is False
            
            # Create a temporary CSV file
            csv_file = tmp_path / "test.csv"
            csv_file.write_text("DOI,URL\n10.5880/test,https://example.com")
            
            # Simulate CSV selection
            dialog.csv_file_path = str(csv_file)
            dialog._check_update_ready()
            
            # Now ok_button should be enabled
            assert dialog.ok_button.isEnabled() is True
            
            dialog.close()
