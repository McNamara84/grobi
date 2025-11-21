"""
Tests für Edge-Cases im Credential-Save-Flow.

Testet Sonderfälle wie:
- Dialog-Cancel durch User
- Storage-Errors während des Speicherns
- Leerer Display-Name
- Display-Name mit Sonderzeichen
"""

import os
import pytest
from unittest.mock import Mock
from PySide6.QtWidgets import QMessageBox

from src.utils.credential_manager import CredentialManager

# Skip GUI tests in CI environments (no display server available)
skip_in_ci = pytest.mark.skipif(
    os.environ.get('CI') == 'true',
    reason="GUI tests require display server (not available in CI)"
)


class TestSaveCredentialsDialogEdgeCases:
    """Tests für Edge-Cases im SaveCredentialsDialog."""

    @pytest.fixture
    def app(self, qapp):
        """Qt Application Fixture."""
        return qapp

    @skip_in_ci
    def test_display_name_with_special_characters_accepted(self, app):
        """Test: Display-Name mit Sonderzeichen wird akzeptiert."""
        from src.ui.save_credentials_dialog import SaveCredentialsDialog
        
        # Arrange
        dialog = SaveCredentialsDialog("test_user", "production", None)
        display_name = "Test-Account (Produktion) #1 & Co."
        
        # Act
        dialog.name_input.setText(display_name)
        
        # Assert
        assert dialog.name_input.text() == display_name
        assert dialog.save_button.isEnabled()  # Button sollte enabled sein

    @skip_in_ci
    def test_ask_save_credentials_with_storage_error(self, app, monkeypatch):
        """Test: ask_save_credentials fängt Storage-Errors ab."""
        from src.ui.save_credentials_dialog import SaveCredentialsDialog
        from src.utils.credential_manager import CredentialStorageError
        
        # Mock dialog.exec to return Accepted
        mock_exec = Mock(return_value=1)  # 1 = Accepted
        monkeypatch.setattr(SaveCredentialsDialog, "exec", mock_exec)
        
        # Mock get_display_name to return valid name
        mock_get_name = Mock(return_value="Test Account")
        monkeypatch.setattr(SaveCredentialsDialog, "get_display_name", mock_get_name)
        
        # Mock CredentialManager.save_credentials to raise error
        mock_save = Mock(side_effect=CredentialStorageError("Storage failed"))
        monkeypatch.setattr(CredentialManager, "save_credentials", mock_save)
        
        # Mock QMessageBox.critical
        mock_critical = Mock()
        monkeypatch.setattr(QMessageBox, "critical", mock_critical)
        
        # Act
        result = SaveCredentialsDialog.ask_save_credentials("user", "pass", "production", None)
        
        # Assert
        assert result is False, "Sollte False bei Storage-Error zurückgeben"
        mock_critical.assert_called_once()  # Error dialog sollte angezeigt werden

    @skip_in_ci
    def test_ask_save_credentials_success(self, app, monkeypatch):
        """Test: ask_save_credentials erfolgreich."""
        from src.ui.save_credentials_dialog import SaveCredentialsDialog
        
        # Mock dialog.exec to return Accepted
        mock_exec = Mock(return_value=1)
        monkeypatch.setattr(SaveCredentialsDialog, "exec", mock_exec)
        
        # Mock get_display_name
        mock_get_name = Mock(return_value="Test Account")
        monkeypatch.setattr(SaveCredentialsDialog, "get_display_name", mock_get_name)
        
        # Mock CredentialManager methods
        mock_save = Mock(return_value="account_id_123")
        mock_set_last = Mock()
        monkeypatch.setattr(CredentialManager, "save_credentials", mock_save)
        monkeypatch.setattr(CredentialManager, "set_last_used_account", mock_set_last)
        
        # Mock QMessageBox.information
        mock_info = Mock()
        monkeypatch.setattr(QMessageBox, "information", mock_info)
        
        # Act
        result = SaveCredentialsDialog.ask_save_credentials("user", "pass", "production", None)
        
        # Assert
        assert result is True
        mock_save.assert_called_once()
        mock_set_last.assert_called_once_with("account_id_123")


class TestCredentialManagerEdgeCases:
    """Tests für Edge-Cases im CredentialManager."""

    def test_delete_account_nonexistent_returns_false(self):
        """Test: delete_account für nicht existierenden Account gibt False zurück."""
        # Arrange
        manager = CredentialManager()
        
        # Act
        result = manager.delete_account("nonexistent_account_id_12345")
        
        # Assert
        assert result is False

    def test_get_credentials_for_nonexistent_account_raises_error(self):
        """Test: get_credentials für nicht existierenden Account wirft CredentialNotFoundError."""
        from src.utils.credential_manager import CredentialNotFoundError
        
        # Arrange
        manager = CredentialManager()
        
        # Act & Assert
        with pytest.raises(CredentialNotFoundError):
            manager.get_credentials("nonexistent_account_id_12345")


class TestMainWindowEdgeCases:
    """Tests für Edge-Cases in MainWindow._on_request_save_credentials."""

    @pytest.fixture
    def app(self, qapp):
        """Qt Application Fixture."""
        return qapp

    @skip_in_ci
    def test_slot_called_twice_in_quick_succession(self, app, monkeypatch):
        """Test: Slot wird zweimal schnell hintereinander aufgerufen."""
        from src.ui.main_window import MainWindow
        from src.ui.save_credentials_dialog import SaveCredentialsDialog
        
        # Arrange
        window = MainWindow()
        
        # Mock SaveCredentialsDialog.ask_save_credentials
        mock_ask = Mock()
        monkeypatch.setattr(SaveCredentialsDialog, "ask_save_credentials", mock_ask)
        
        # Act
        window._on_request_save_credentials("test_user", "test_pass", "production")
        window._on_request_save_credentials("test_user", "test_pass", "production")  # Zweiter Aufruf
        
        # Assert
        assert mock_ask.call_count == 2, "Dialog sollte beide Male aufgerufen werden"

    @skip_in_ci
    def test_slot_handles_exception_gracefully(self, app, monkeypatch):
        """Test: Slot fängt Exceptions ab und loggt sie."""
        from src.ui.main_window import MainWindow
        from src.ui.save_credentials_dialog import SaveCredentialsDialog
        
        # Arrange
        window = MainWindow()
        
        # Mock SaveCredentialsDialog.ask_save_credentials um Exception zu werfen
        mock_ask = Mock(side_effect=Exception("Test error"))
        monkeypatch.setattr(SaveCredentialsDialog, "ask_save_credentials", mock_ask)
        
        # Act & Assert
        # Sollte nicht crashen, sondern Exception loggen
        window._on_request_save_credentials("test_user", "test_pass", "production")
        
        # Assert: Methode sollte aufgerufen worden sein
        mock_ask.assert_called_once()
