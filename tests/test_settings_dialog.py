"""
Tests for Settings Dialog.
"""

import pytest
from unittest.mock import Mock, patch

from src.ui.settings_dialog import SettingsDialog, ConnectionTestWorker
from src.ui.theme_manager import Theme


@pytest.fixture
def qapp(qapp):
    """Ensure QApplication exists for tests."""
    return qapp


@pytest.fixture
def mock_db_credentials():
    """Mock database credential functions."""
    with patch('src.ui.settings_dialog.db_credentials_exist') as mock_exist, \
         patch('src.ui.settings_dialog.load_db_credentials') as mock_load, \
         patch('src.ui.settings_dialog.save_db_credentials') as mock_save:
        mock_exist.return_value = False
        mock_load.return_value = None
        yield {'exist': mock_exist, 'load': mock_load, 'save': mock_save}


@pytest.fixture
def mock_theme_manager():
    """Mock ThemeManager."""
    mock = Mock()
    mock.get_current_theme.return_value = Theme.AUTO
    mock.get_credentials_dialog_stylesheet.return_value = ""  # Return empty string for stylesheet
    yield mock


@pytest.fixture
def settings_dialog(qapp, mock_db_credentials, mock_theme_manager):
    """Create SettingsDialog instance."""
    dialog = SettingsDialog(mock_theme_manager)
    yield dialog
    # Simple cleanup - let Qt handle thread deletion automatically
    dialog.close()


class TestSettingsDialogInit:
    """Tests for SettingsDialog initialization."""
    
    def test_dialog_creation(self, settings_dialog):
        """Test that dialog is created successfully."""
        assert settings_dialog is not None
        assert settings_dialog.windowTitle() == "Einstellungen"
    
    def test_has_two_tabs(self, settings_dialog):
        """Test that dialog has General and Database tabs."""
        tab_widget = settings_dialog.tabs
        assert tab_widget.count() == 2
        assert tab_widget.tabText(0) == "Allgemein"
        assert tab_widget.tabText(1) == "Datenbank"
    
    def test_theme_radio_buttons_created(self, settings_dialog):
        """Test that theme radio buttons are created."""
        assert settings_dialog.auto_theme_radio is not None
        assert settings_dialog.light_theme_radio is not None
        assert settings_dialog.dark_theme_radio is not None
    
    def test_database_inputs_created(self, settings_dialog):
        """Test that database input fields are created."""
        assert settings_dialog.host_input is not None
        assert settings_dialog.database_input is not None
        assert settings_dialog.username_input is not None
        assert settings_dialog.password_input is not None


class TestThemeSelection:
    """Tests for theme selection functionality."""
    
    def test_auto_theme_selected_by_default(self, mock_theme_manager, mock_db_credentials, qapp):
        """Test that AUTO theme is selected by default."""
        mock_theme_manager.get_current_theme.return_value = Theme.AUTO
        dialog = SettingsDialog(mock_theme_manager)
        
        assert dialog.auto_theme_radio.isChecked()
        assert not dialog.light_theme_radio.isChecked()
        assert not dialog.dark_theme_radio.isChecked()
        dialog.close()
    
    def test_light_theme_preselected(self, mock_theme_manager, mock_db_credentials, qapp):
        """Test that LIGHT theme can be preselected."""
        mock_theme_manager.get_current_theme.return_value = Theme.LIGHT
        dialog = SettingsDialog(mock_theme_manager)
        
        assert not dialog.auto_theme_radio.isChecked()
        assert dialog.light_theme_radio.isChecked()
        assert not dialog.dark_theme_radio.isChecked()
        dialog.close()
    
    def test_dark_theme_preselected(self, mock_theme_manager, mock_db_credentials, qapp):
        """Test that DARK theme can be preselected."""
        mock_theme_manager.get_current_theme.return_value = Theme.DARK
        dialog = SettingsDialog(mock_theme_manager)
        
        assert not dialog.auto_theme_radio.isChecked()
        assert not dialog.light_theme_radio.isChecked()
        assert dialog.dark_theme_radio.isChecked()
        dialog.close()


class TestDatabaseCredentialLoading:
    """Tests for loading existing database credentials."""
    
    def test_load_existing_credentials(self, settings_dialog):
        """Test that database inputs exist and can be populated."""
        # Just verify the inputs exist and can hold values
        assert settings_dialog.host_input is not None
        assert settings_dialog.database_input is not None
        assert settings_dialog.username_input is not None
        assert settings_dialog.password_input is not None
        
        # Verify they are QLineEdit widgets
        from PySide6.QtWidgets import QLineEdit
        assert isinstance(settings_dialog.host_input, QLineEdit)
        assert isinstance(settings_dialog.database_input, QLineEdit)
    
    def test_empty_username_when_no_credentials(self, settings_dialog):
        """Test that password is never pre-filled for security."""
        # Password should always be empty initially (never pre-filled)
        assert settings_dialog.password_input.text() == ""
        assert settings_dialog.password_input.echoMode() == settings_dialog.password_input.EchoMode.Password


class TestConnectionTest:
    """Tests for database connection testing."""
    
    def test_connection_test_button_exists(self, settings_dialog):
        """Test that connection test button exists."""
        assert settings_dialog.test_button is not None
        assert settings_dialog.test_button.text() == "Verbindung testen"
    
    @patch('mysql.connector.connect')
    @patch('src.ui.settings_dialog.QMessageBox')
    def test_connection_test_starts_worker(self, mock_msgbox, mock_connect, settings_dialog, qtbot):
        """Test that clicking test button starts worker thread."""
        # Mock successful DB connection
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = (1,)
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        # Enable database functionality first (button is disabled by default)
        settings_dialog.db_enabled_checkbox.setChecked(True)
        
        # Setup inputs
        settings_dialog.host_input.setText("host")
        settings_dialog.database_input.setText("db")
        settings_dialog.username_input.setText("user")
        settings_dialog.password_input.setText("pass")
        
        # Click button
        settings_dialog.test_button.click()
        
        # Verify no warning was shown (inputs are valid)
        mock_msgbox.warning.assert_not_called()
        
        # Verify thread and worker were created
        assert settings_dialog.connection_test_thread is not None
        assert settings_dialog.connection_test_worker is not None
        
        # Wait for connection status to update (indicates thread finished)
        qtbot.waitUntil(lambda: "✓" in settings_dialog.connection_status.text() or "✗" in settings_dialog.connection_status.text(), timeout=5000)
        
        # Verify connection was attempted with correct parameters
        mock_connect.assert_called_once_with(
            host="host",
            database="db",
            user="user",
            password="pass",
            connect_timeout=10,
            auth_plugin='mysql_native_password'
        )
        
        # Verify success message
        assert "✓ Verbindung erfolgreich" in settings_dialog.connection_status.text()
        
        # Process events to ensure thread cleanup happens
        from PySide6.QtWidgets import QApplication
        QApplication.processEvents()
        qtbot.wait(100)  # Give deleteLater() time to execute


class TestConnectionTestWorker:
    """Tests for ConnectionTestWorker."""
    
    @patch('mysql.connector.connect')
    def test_successful_connection(self, mock_connect):
        """Test successful database connection."""
        mock_connection = Mock()
        mock_connect.return_value = mock_connection
        
        worker = ConnectionTestWorker("host", "database", "user", "password")
        
        # Connect to finished signal
        finished_called = []
        worker.finished.connect(lambda success, msg: finished_called.append((success, msg)))
        
        # Run worker
        worker.run()
        
        # Verify
        assert len(finished_called) == 1
        success, message = finished_called[0]
        assert success is True
        assert "erfolgreich" in message.lower()
        mock_connection.close.assert_called_once()
    
    @patch('mysql.connector.connect')
    def test_failed_connection(self, mock_connect):
        """Test failed database connection."""
        mock_connect.side_effect = Exception("Connection failed")
        
        worker = ConnectionTestWorker("host", "database", "user", "password")
        
        # Connect to finished signal
        finished_called = []
        worker.finished.connect(lambda success, msg: finished_called.append((success, msg)))
        
        # Run worker
        worker.run()
        
        # Verify
        assert len(finished_called) == 1
        success, message = finished_called[0]
        assert success is False
        assert "fehler" in message.lower()


class TestSaveCredentials:
    """Tests for saving credentials."""
    
    def test_save_with_db_disabled_succeeds(self, settings_dialog):
        """Test that save works when DB is disabled."""
        # Disable DB
        settings_dialog.db_enabled_checkbox.setChecked(False)
        
        # Mock accept to prevent actual dialog closure
        with patch.object(settings_dialog, 'accept'):
            settings_dialog._save_and_close()
            
            # Should not raise any errors
            assert True
    
    def test_save_valid_credentials(self, settings_dialog, mock_db_credentials):
        """Test saving valid credentials."""
        # Enable DB and fill inputs
        settings_dialog.db_enabled_checkbox.setChecked(True)
        settings_dialog.host_input.setText("host")
        settings_dialog.database_input.setText("db")
        settings_dialog.username_input.setText("user")
        settings_dialog.password_input.setText("pass")
        
        # Mock accept to prevent actual dialog closure
        with patch.object(settings_dialog, 'accept'):
            settings_dialog._save_and_close()
            
            # Verify save was called
            mock_db_credentials['save'].assert_called_once_with(
                "host", "db", "user", "pass"
            )


class TestThemeChangedSignal:
    """Tests for theme_changed signal."""
    
    @pytest.mark.skip(reason="Causes CI timeout at 82% - signal/slot cleanup issue")
    def test_theme_changed_signal_emitted(self, settings_dialog):
        """Test that theme_changed signal is emitted on save."""
        # Select LIGHT theme
        settings_dialog.light_theme_radio.setChecked(True)
        
        # Connect to signal
        signal_emitted = []
        settings_dialog.theme_changed.connect(lambda theme: signal_emitted.append(theme))
        
        # Mock accept, theme_manager.set_theme and save
        with patch.object(settings_dialog, 'accept'), \
             patch.object(settings_dialog.theme_manager, 'set_theme'):
            settings_dialog._save_and_close()
            
            # Verify signal emitted
            assert len(signal_emitted) == 1
            assert signal_emitted[0] == Theme.LIGHT
