"""Unit tests for Main Window - Basic functionality only."""

import pytest
from unittest.mock import Mock, patch

from PySide6.QtWidgets import QApplication

from src.ui.main_window import MainWindow, DOIFetchWorker


@pytest.fixture(scope="module")
def qapp():
    """Create QApplication instance for GUI tests."""
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    yield app


@pytest.fixture
def main_window(qapp, qtbot):
    """Create a MainWindow instance for testing."""
    window = MainWindow()
    qtbot.addWidget(window)
    yield window
    # Cleanup
    if window.thread and window.thread.isRunning():
        window.thread.quit()
        window.thread.wait(1000)


class TestMainWindowBasics:
    """Test basic MainWindow functionality."""
    
    def test_window_creation(self, main_window):
        """Test that main window is created successfully."""
        assert main_window is not None
        assert "GROBI" in main_window.windowTitle()
    
    def test_window_has_size(self, main_window):
        """Test window has size set."""
        assert main_window.width() >= 600
        assert main_window.height() >= 400
    
    def test_thread_initial_state(self, main_window):
        """Test that thread and worker are initially None."""
        assert main_window.thread is None
        assert main_window.worker is None


class TestMainWindowLogging:
    """Test log message functionality."""
    
    def test_log_message(self, main_window):
        """Test adding a log message."""
        main_window._log("Test message")
        assert "Test message" in main_window.log_text.toPlainText()
    
    def test_multiple_log_messages(self, main_window):
        """Test adding multiple log messages."""
        main_window._log("First message")
        main_window._log("Second message")
        
        log_text = main_window.log_text.toPlainText()
        assert "First message" in log_text
        assert "Second message" in log_text


class TestDOIFetchWorker:
    """Test DOIFetchWorker class."""
    
    def test_worker_creation(self):
        """Test worker can be created."""
        worker = DOIFetchWorker("user", "pass", False)
        assert worker is not None
        assert worker.username == "user"
        assert worker.password == "pass"
        assert worker.use_test_api is False
    
    def test_worker_with_test_api(self):
        """Test worker creation with test API."""
        worker = DOIFetchWorker("test_user", "test_pass", True)
        assert worker.use_test_api is True
    
    def test_worker_signals_exist(self):
        """Test that worker has required signals."""
        worker = DOIFetchWorker("user", "pass", False)
        # Verify signals exist
        assert hasattr(worker, 'progress')
        assert hasattr(worker, 'finished')
        assert hasattr(worker, 'error')


class TestMainWindowDialogIntegration:
    """Test dialog integration."""
    
    @patch('src.ui.main_window.CredentialsDialog')
    def test_load_dois_cancelled(self, mock_dialog_class, main_window):
        """Test that nothing happens when credentials dialog is cancelled."""
        # Mock dialog to return None (cancelled)
        mock_dialog = Mock()
        mock_dialog.exec.return_value = 0  # Rejected
        mock_dialog.get_credentials.return_value = None
        mock_dialog_class.return_value = mock_dialog
        
        # Call method directly
        main_window._on_load_dois_clicked()
        
        # Verify no thread was created
        assert main_window.thread is None
        assert main_window.worker is None


class TestMainWindowAuthorsUpdate:
    """Test authors update functionality and credential storage."""
    
    @patch('src.ui.main_window.CredentialsDialog')
    @patch('src.ui.main_window.AuthorsUpdateWorker')
    @patch('src.ui.main_window.QThread')
    def test_credentials_stored_before_worker_creation(self, mock_thread_class, mock_worker_class, mock_dialog_class, main_window):
        """Test that credentials are stored in instance variables before creating worker."""
        # Mock dialog to return credentials
        mock_dialog = Mock()
        mock_dialog.get_credentials.return_value = ("test_user", "test_pass", "/path/to/file.csv", True)
        mock_dialog_class.return_value = mock_dialog
        
        # Mock worker and thread
        mock_worker = Mock()
        mock_thread = Mock()
        mock_worker_class.return_value = mock_worker
        mock_thread_class.return_value = mock_thread
        
        # Call the method
        main_window._on_update_authors_clicked()
        
        # Verify credentials are stored in instance variables
        assert hasattr(main_window, '_authors_update_username')
        assert hasattr(main_window, '_authors_update_password')
        assert hasattr(main_window, '_authors_update_csv_path')
        assert hasattr(main_window, '_authors_update_use_test_api')
        
        assert main_window._authors_update_username == "test_user"
        assert main_window._authors_update_password == "test_pass"
        assert main_window._authors_update_csv_path == "/path/to/file.csv"
        assert main_window._authors_update_use_test_api is True
        
        # Verify worker was created with correct credentials
        mock_worker_class.assert_called_once_with(
            "test_user", "test_pass", "/path/to/file.csv", True, dry_run_only=True, credentials_are_new=False
        )
    
    @patch('src.ui.main_window.AuthorsUpdateWorker')
    @patch('src.ui.main_window.QThread')
    def test_second_worker_uses_stored_credentials(self, mock_thread_class, mock_worker_class, main_window):
        """Test that second worker (actual update) uses stored credentials, not deleted worker."""
        # Set up stored credentials (as if first worker was created)
        main_window._authors_update_username = "stored_user"
        main_window._authors_update_password = "stored_pass"
        main_window._authors_update_csv_path = "/stored/path.csv"
        main_window._authors_update_use_test_api = False
        main_window._authors_update_credentials_are_new = False
        
        # Mock worker and thread
        mock_worker = Mock()
        mock_thread = Mock()
        mock_worker_class.return_value = mock_worker
        mock_thread_class.return_value = mock_thread
        
        # Call the method that starts actual update (second worker)
        main_window._start_actual_authors_update()
        
        # Verify worker was created with stored credentials
        mock_worker_class.assert_called_once_with(
            "stored_user", "stored_pass", "/stored/path.csv", False, dry_run_only=False, credentials_are_new=False
        )
    
    @patch('src.ui.main_window.CredentialsDialog')
    def test_authors_update_cancelled(self, mock_dialog_class, main_window):
        """Test that nothing happens when authors update dialog is cancelled."""
        # Mock dialog to return None (cancelled)
        mock_dialog = Mock()
        mock_dialog.get_credentials.return_value = None
        mock_dialog_class.return_value = mock_dialog
        
        # Call method
        main_window._on_update_authors_clicked()
        
        # Verify no credentials stored
        assert not hasattr(main_window, '_authors_update_username')


class TestMainWindowCleanup:
    """Test cleanup functionality."""
    
    def test_close_event_without_thread(self, main_window):
        """Test close event without active thread."""
        event = Mock()
        
        main_window.closeEvent(event)
        
        event.accept.assert_called_once()
