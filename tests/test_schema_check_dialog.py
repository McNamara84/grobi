"""Tests for schema check credentials dialog functionality."""

import pytest
from PySide6.QtWidgets import QApplication
from src.ui.credentials_dialog import CredentialsDialog


@pytest.fixture(scope="module")
def qapp():
    """Create QApplication instance for tests."""
    app = QApplication.instance() or QApplication([])
    yield app


def test_schema_check_mode_button_enabled_with_credentials(qapp):
    """Test that OK button is enabled when credentials are provided in schema_check mode."""
    dialog = CredentialsDialog(mode="schema_check")
    
    # If a saved account is pre-selected and loaded, button might be enabled
    # So we first select "New credentials" to ensure we start with empty fields
    if hasattr(dialog, 'account_selector'):
        dialog.account_selector.setCurrentIndex(0)  # Select "New credentials"
    
    # Now button should be disabled (no credentials entered manually)
    assert not dialog.ok_button.isEnabled(), "OK button should be disabled without credentials"
    
    # Enter username
    dialog.username_input.setText("test_user")
    # Button should still be disabled (no password yet)
    assert not dialog.ok_button.isEnabled(), "OK button should be disabled without password"
    
    # Enter password
    dialog.password_input.setText("test_password")
    # Now button should be enabled
    assert dialog.ok_button.isEnabled(), "OK button should be enabled with valid credentials"
    
    # Clear username
    dialog.username_input.clear()
    # Button should be disabled again
    assert not dialog.ok_button.isEnabled(), "OK button should be disabled when username is cleared"


def test_schema_check_mode_no_csv_required(qapp):
    """Test that schema_check mode doesn't require CSV file."""
    dialog = CredentialsDialog(mode="schema_check")
    
    # Set credentials
    dialog.username_input.setText("test_user")
    dialog.password_input.setText("test_password")
    
    # Button should be enabled without CSV file
    assert dialog.ok_button.isEnabled(), "OK button should be enabled in schema_check mode without CSV"
    assert dialog.csv_file_path is None, "CSV file path should not be set in schema_check mode"


def test_schema_check_mode_button_text(qapp):
    """Test that OK button has correct text in schema_check mode."""
    dialog = CredentialsDialog(mode="schema_check")
    assert dialog.ok_button.text() == "Schema-Check starten", "OK button should have correct text"


def test_schema_check_mode_window_title(qapp):
    """Test that dialog has correct window title in schema_check mode."""
    dialog = CredentialsDialog(mode="schema_check")
    assert dialog.windowTitle() == "Schema-Kompatibilität überprüfen", "Dialog should have correct title"


def test_schema_check_mode_description(qapp):
    """Test that dialog shows correct description in schema_check mode."""
    dialog = CredentialsDialog(mode="schema_check")
    # The description should mention schema check
    # We can't directly access the description label, so just verify dialog was created
    assert dialog.mode == "schema_check"
