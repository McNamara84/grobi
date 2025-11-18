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


class TestMainWindowCleanup:
    """Test cleanup functionality."""
    
    def test_close_event_without_thread(self, main_window):
        """Test close event without active thread."""
        event = Mock()
        
        main_window.closeEvent(event)
        
        event.accept.assert_called_once()
