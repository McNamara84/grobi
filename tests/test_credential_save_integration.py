"""Integration tests for credential save workflow - focusing on component interaction."""

import pytest
from unittest.mock import Mock
from PySide6.QtWidgets import QApplication
from src.ui.main_window import MainWindow
from src.ui.save_credentials_dialog import SaveCredentialsDialog


@pytest.fixture
def app(qtbot):
    """Create QApplication instance."""
    return QApplication.instance() or QApplication([])


@pytest.fixture
def main_window(qtbot, app):
    """Create MainWindow instance."""
    window = MainWindow()
    qtbot.addWidget(window)
    return window


class TestMainWindowSaveCredentialsSlot:
    """Test the _on_request_save_credentials slot in MainWindow."""

    def test_slot_exists(self, main_window):
        """Test that the slot exists and is callable."""
        assert hasattr(main_window, "_on_request_save_credentials")
        assert callable(main_window._on_request_save_credentials)

    def test_slot_calls_save_dialog(self, main_window, qtbot, monkeypatch):
        """Test that the slot calls SaveCredentialsDialog.ask_save_credentials."""
        # Mock the static method
        called_with = []

        def mock_ask_save(username, password, api_type, parent):
            called_with.append((username, password, api_type, parent))
            return True

        monkeypatch.setattr(SaveCredentialsDialog, "ask_save_credentials", mock_ask_save)

        # Call the slot
        main_window._on_request_save_credentials("test_user", "test_pass", "production")

        # Verify the static method was called with correct arguments
        assert len(called_with) == 1
        assert called_with[0] == ("test_user", "test_pass", "production", main_window)

    def test_slot_handles_exception_gracefully(self, main_window, qtbot, monkeypatch):
        """Test that the slot handles exceptions without crashing."""

        def mock_ask_save_raises(username, password, api_type, parent):
            raise Exception("Storage error")

        monkeypatch.setattr(SaveCredentialsDialog, "ask_save_credentials", mock_ask_save_raises)

        # This should not raise an exception
        try:
            main_window._on_request_save_credentials("test_user", "test_pass", "production")
        except Exception:
            pytest.fail("_on_request_save_credentials should handle exceptions gracefully")


class TestWorkerSignalConnections:
    """Test that worker signals are properly connected to MainWindow slot."""

    def test_doi_fetch_worker_has_signal(self):
        """Test that DOIFetchWorker has request_save_credentials signal."""
        from src.ui.main_window import DOIFetchWorker

        # Check signal exists in class
        assert hasattr(DOIFetchWorker, "request_save_credentials")

    def test_doi_creator_fetch_worker_has_signal(self):
        """Test that DOICreatorFetchWorker has request_save_credentials signal."""
        from src.ui.main_window import DOICreatorFetchWorker

        # Check signal exists in class
        assert hasattr(DOICreatorFetchWorker, "request_save_credentials")

    def test_update_worker_has_signal(self):
        """Test that UpdateWorker has request_save_credentials signal."""
        from src.workers.update_worker import UpdateWorker

        # Check signal exists in class
        assert hasattr(UpdateWorker, "request_save_credentials")

    def test_authors_update_worker_has_signal(self):
        """Test that AuthorsUpdateWorker has request_save_credentials signal."""
        from src.workers.authors_update_worker import AuthorsUpdateWorker

        # Check signal exists in class
        assert hasattr(AuthorsUpdateWorker, "request_save_credentials")


class TestWorkerCredentialsAreNewParameter:
    """Test that workers accept and store credentials_are_new parameter."""

    def test_update_worker_accepts_parameter(self):
        """Test that UpdateWorker accepts credentials_are_new parameter."""
        from src.workers.update_worker import UpdateWorker

        worker = UpdateWorker(
            username="test",
            password="pass",
            csv_path="/test/path.csv",
            use_test_api=False,
            credentials_are_new=True,
        )

        assert worker.credentials_are_new is True
        assert worker._first_success is False

    def test_authors_update_worker_accepts_parameter(self):
        """Test that AuthorsUpdateWorker accepts credentials_are_new parameter."""
        from src.workers.authors_update_worker import AuthorsUpdateWorker

        worker = AuthorsUpdateWorker(
            username="test",
            password="pass",
            csv_path="/test/path.csv",
            use_test_api=False,
            dry_run_only=False,
            credentials_are_new=True,
        )

        assert worker.credentials_are_new is True
        assert worker._first_success is False

    def test_worker_default_value_is_false(self):
        """Test that credentials_are_new defaults to False."""
        from src.workers.update_worker import UpdateWorker

        worker = UpdateWorker(
            username="test", password="pass", csv_path="/test/path.csv", use_test_api=False
        )

        assert worker.credentials_are_new is False


class TestCredentialsDialogNewCredentialsFlag:
    """Test that CredentialsDialog tracks new vs saved credentials."""

    def test_is_new_credentials_method_exists(self):
        """Test that is_new_credentials method exists."""
        from src.ui.credentials_dialog import CredentialsDialog

        assert hasattr(CredentialsDialog, "is_new_credentials")

    def test_new_credentials_flag_after_manual_entry(self, qtbot):
        """Test that flag is True after manual credential entry."""
        from src.ui.credentials_dialog import CredentialsDialog

        dialog = CredentialsDialog(None)
        qtbot.addWidget(dialog)

        # Simulate manual entry
        dialog.username_input.setText("new_user")
        dialog.password_input.setText("new_pass")

        # Flag should be True for new credentials
        assert dialog.is_new_credentials() is True

    def test_saved_credentials_flag_after_load(self, qtbot, monkeypatch):
        """Test that flag is False after loading saved credentials."""
        from src.ui.credentials_dialog import CredentialsDialog
        from types import SimpleNamespace

        # Mock CredentialManager to return saved credentials
        mock_manager = Mock()
        # Return accounts as objects with attributes (not dicts)
        mock_accounts = [
            SimpleNamespace(
                display_name="Saved Account",
                username="saved_user",
                api_type="production",
                account_id="saved_user:production",
            )
        ]
        mock_manager.list_accounts.return_value = mock_accounts
        # get_credentials returns (username, password, api_type) - 3 values!
        mock_manager.get_credentials.return_value = ("saved_user", "saved_pass", "production")

        monkeypatch.setattr("src.ui.credentials_dialog.CredentialManager", lambda: mock_manager)

        dialog = CredentialsDialog(None)
        qtbot.addWidget(dialog)

        # Simulate selecting saved account in dropdown (index 1 = first saved account)
        dialog.account_selector.setCurrentIndex(1)
        
        # Flag should be False when saved account is selected (index != 0)
        assert dialog.is_new_credentials() is False
        
        # Now select "New credentials" again (index 0)
        dialog.account_selector.setCurrentIndex(0)
        
        # Flag should be True for new credentials
        assert dialog.is_new_credentials() is True
