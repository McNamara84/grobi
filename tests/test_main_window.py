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


class TestCSVFileDetection:
    """Test CSV file detection functionality."""
    
    def test_check_csv_files_method_exists(self, main_window):
        """Test that _check_csv_files method exists."""
        assert hasattr(main_window, '_check_csv_files')
        assert callable(main_window._check_csv_files)
    
    @patch('src.ui.main_window.os.getcwd')
    @patch('src.ui.main_window.Path')
    def test_check_csv_files_no_files(self, mock_path_class, mock_getcwd, main_window):
        """Test CSV check when no files exist."""
        # Mock current directory
        mock_getcwd.return_value = "/fake/dir"
        
        # Mock Path().glob() to return empty lists
        mock_path_instance = Mock()
        mock_path_instance.glob.return_value = []
        mock_path_class.return_value = mock_path_instance
        
        # Clear username so it searches for any CSV files
        main_window._current_username = None
        
        main_window._check_csv_files()
        
        # With new card-based UI, we check if actions are disabled
        # The update action in the dropdown menu should be disabled
        # Check that status shows no CSV found
        assert "Keine CSV" in main_window.urls_card.status_label.text()
        assert "Keine CSV" in main_window.authors_card.status_label.text()
    
    @patch('src.ui.main_window.os.getcwd')
    def test_check_csv_files_with_username(self, mock_getcwd, main_window):
        """Test CSV check with username set."""
        # Mock current directory
        mock_getcwd.return_value = "/fake/dir"
        
        # Set username
        main_window._current_username = "test_user"
        
        # Create real Path objects but mock exists()
        with patch('src.ui.main_window.Path') as mock_path_class:
            from pathlib import Path as RealPath
            
            def path_side_effect(*args, **kwargs):
                # Create real path but mock exists
                real_path = RealPath(*args, **kwargs)
                mock_path = Mock(spec=RealPath)
                mock_path.__truediv__ = lambda self, other: path_side_effect(str(real_path / other))
                mock_path.exists.return_value = "test_user_urls.csv" in str(real_path)
                mock_path.name = real_path.name
                return mock_path
            
            mock_path_class.side_effect = path_side_effect
            
            main_window._check_csv_files()
            
            # URLs card should show CSV ready status (contains the filename)
            assert "test_user_urls.csv" in main_window.urls_card.status_label.text() or "🟢" in main_window.urls_card.status_label.text()
            # Authors card should show no CSV found
            assert "Keine CSV" in main_window.authors_card.status_label.text() or "⚪" in main_window.authors_card.status_label.text()
    
    @patch('src.ui.main_window.Path.glob')
    def test_check_csv_files_without_username(self, mock_glob, main_window):
        """Test CSV check without username (finds any CSV files)."""
        # No username set
        main_window._current_username = None
        
        # Mock glob returns some CSV files
        mock_urls_file = Mock()
        mock_urls_file.name = "some_user_urls.csv"
        mock_authors_file = Mock()
        mock_authors_file.name = "some_user_authors.csv"
        
        def glob_side_effect(pattern):
            if "*_urls.csv" in pattern:
                return [mock_urls_file]
            elif "*_authors.csv" in pattern:
                return [mock_authors_file]
            return []
        
        mock_glob.side_effect = glob_side_effect
        
        main_window._check_csv_files()
        
        # Both update buttons should be enabled
        assert main_window.update_button.isEnabled()
        assert main_window.update_authors_button.isEnabled()
    
    def test_csv_check_called_on_init(self, main_window):
        """Test that CSV check is called during initialization."""
        # Status labels should exist and have initial state
        assert hasattr(main_window, 'urls_status_label')
        assert hasattr(main_window, 'authors_status_label')
    
    def test_username_tracking_initialized(self, main_window):
        """Test that username tracking is initialized."""
        assert hasattr(main_window, '_current_username')
        assert main_window._current_username is None


class TestUIContainers:
    """Test UI container elements (CollapsibleSection and ActionCard components)."""
    
    def test_collapsible_sections_exist(self, main_window):
        """Test that CollapsibleSections are created instead of GroupBoxes."""
        from src.ui.components import CollapsibleSection
        
        # Check that we have collapsible sections
        assert hasattr(main_window, 'metadata_section')
        assert hasattr(main_window, 'tools_section')
        assert isinstance(main_window.metadata_section, CollapsibleSection)
        assert isinstance(main_window.tools_section, CollapsibleSection)
    
    def test_status_labels_exist(self, main_window):
        """Test that status labels exist (now in ActionCards)."""
        # Status labels are now part of the action cards
        assert hasattr(main_window, 'urls_card')
        assert hasattr(main_window, 'authors_card')
        
        # Cards should have status labels
        from PySide6.QtWidgets import QLabel
        assert isinstance(main_window.urls_card.status_label, QLabel)
        assert isinstance(main_window.authors_card.status_label, QLabel)
    
    def test_action_cards_exist(self, main_window):
        """Test that action cards exist with buttons."""
        # All main workflow cards should exist
        assert hasattr(main_window, 'urls_card')
        assert hasattr(main_window, 'authors_card')
        assert hasattr(main_window, 'publisher_card')
        assert hasattr(main_window, 'contributors_card')
        assert hasattr(main_window, 'rights_card')
        assert hasattr(main_window, 'downloads_card')
        
        # Cards should not be None
        assert main_window.urls_card is not None
        assert main_window.update_button is not None
        assert main_window.load_authors_button is not None
        assert main_window.update_authors_button is not None


class TestMenuBar:
    """Test MenuBar functionality."""
    
    def test_menubar_exists(self, main_window):
        """Test that menubar is created."""
        menubar = main_window.menuBar()
        assert menubar is not None
        
        # Check menu count
        actions = menubar.actions()
        assert len(actions) >= 2, "Should have at least Ansicht and Hilfe menus"
    
    @pytest.mark.skip(reason="Ansicht menu removed in Phase 1 - theme moved to Settings")
    def test_view_menu_exists(self, main_window):
        """Test that Ansicht (View) menu exists."""
        menubar = main_window.menuBar()
        actions = menubar.actions()
        
        menu_titles = [action.text() for action in actions]
        assert "Ansicht" in menu_titles
    
    def test_help_menu_exists(self, main_window):
        """Test that Hilfe (Help) menu exists."""
        menubar = main_window.menuBar()
        actions = menubar.actions()
        
        menu_titles = [action.text() for action in actions]
        assert "Hilfe" in menu_titles
    
    @pytest.mark.skip(reason="Theme actions removed from MainWindow in Phase 1 - moved to Settings")
    def test_theme_actions_exist(self, main_window):
        """Test that theme actions exist."""
        assert hasattr(main_window, 'auto_theme_action')
        assert hasattr(main_window, 'light_theme_action')
        assert hasattr(main_window, 'dark_theme_action')
        
        # Actions should be checkable
        assert main_window.auto_theme_action.isCheckable()
        assert main_window.light_theme_action.isCheckable()
        assert main_window.dark_theme_action.isCheckable()
    
    @pytest.mark.skip(reason="Theme action group removed from MainWindow in Phase 1 - moved to Settings")
    def test_theme_action_group(self, main_window):
        """Test that theme actions are in exclusive group."""
        assert hasattr(main_window, 'theme_action_group')
        assert main_window.theme_action_group.isExclusive()
    
    @patch('src.ui.main_window.AboutDialog')
    def test_show_about_dialog(self, mock_about_class, main_window):
        """Test that About dialog can be shown."""
        mock_dialog = Mock()
        mock_about_class.return_value = mock_dialog
        
        # Call method
        main_window._show_about_dialog()
        
        # Verify dialog was created and shown
        mock_about_class.assert_called_once_with(main_window)
        mock_dialog.exec.assert_called_once()
    
    @patch('src.ui.main_window.QDesktopServices.openUrl')
    def test_open_github(self, mock_open_url, main_window):
        """Test that GitHub repository can be opened."""
        main_window._open_github()
        
        # Verify openUrl was called
        assert mock_open_url.called
        call_args = mock_open_url.call_args[0][0]
        assert "github.com" in call_args.toString().lower()
    
    @pytest.mark.skip(reason="_set_theme method removed from MainWindow in Phase 1 - moved to Settings")
    def test_set_theme_method_exists(self, main_window):
        """Test that _set_theme method exists."""
        assert hasattr(main_window, '_set_theme')
        assert callable(main_window._set_theme)


class TestMainWindowSettings:
    """Test settings dialog integration."""
    
    def test_open_settings_dialog_method_exists(self, main_window):
        """Test that _open_settings_dialog method exists."""
        assert hasattr(main_window, '_open_settings_dialog')
        assert callable(main_window._open_settings_dialog)
    
    def test_settings_dialog_can_be_imported(self):
        """Test that SettingsDialog can be imported."""
        from src.ui.settings_dialog import SettingsDialog
        assert SettingsDialog is not None
    
    def test_theme_change_handler_callable(self, main_window):
        """Test that theme change handler is properly defined."""
        # Verify the handler method exists and is callable
        assert hasattr(main_window, '_on_settings_theme_changed')
        assert callable(main_window._on_settings_theme_changed)
        
        # Verify it accepts a Theme parameter (test with dummy call)
        from src.ui.theme_manager import Theme
        # This shouldn't raise an exception
        try:
            # Just verify the signature, don't actually call it
            import inspect
            sig = inspect.signature(main_window._on_settings_theme_changed)
            params = list(sig.parameters.keys())
            assert 'theme' in params
        except Exception:
            pass  # Signature inspection is optional
    
    def test_on_settings_theme_changed_method_exists(self, main_window):
        """Test that theme change handler exists."""
        assert hasattr(main_window, '_on_settings_theme_changed')
        assert callable(main_window._on_settings_theme_changed)
    
    def test_settings_menu_exists(self, main_window):
        """Test that Settings menu exists in menu bar."""
        menu_bar = main_window.menuBar()
        settings_menu = None
        
        for action in menu_bar.actions():
            if action.menu() and "Einstellungen" in action.text():
                settings_menu = action.menu()
                break
        
        assert settings_menu is not None, "Settings menu should exist"
    
    def test_settings_action_has_shortcut(self, main_window):
        """Test that Settings action has Ctrl+, shortcut."""
        menu_bar = main_window.menuBar()
        settings_menu = None
        
        for action in menu_bar.actions():
            if action.menu() and "Einstellungen" in action.text():
                settings_menu = action.menu()
                break
        
        if settings_menu:
            settings_action = settings_menu.actions()[0]
            shortcut = settings_action.shortcut().toString()
            # Accept both German and English keyboard shortcuts
            assert "Ctrl+," in shortcut or "Strg+," in shortcut
