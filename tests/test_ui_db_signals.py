"""Test UI signal connections for database synchronization.

Verifies that the new validation_update, database_update, and datacite_update
signals are properly connected to the main window handlers.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from PySide6.QtWidgets import QApplication
from PySide6.QtCore import QThread

from src.ui.main_window import MainWindow


@pytest.fixture
def app(qtbot):
    """Create QApplication instance."""
    return QApplication.instance() or QApplication([])


@pytest.fixture
def main_window(qtbot, app):
    """Create MainWindow instance for testing."""
    window = MainWindow()
    qtbot.addWidget(window)
    return window


class TestDatabaseSyncSignals:
    """Test database synchronization signal connections."""
    
    def test_validation_signal_connected(self, qtbot, main_window):
        """Test that validation_update signal is properly connected."""
        # Mock the CSV file existence
        with patch('src.ui.main_window.Path') as mock_path:
            mock_csv = Mock()
            mock_csv.exists.return_value = True
            mock_path.return_value = mock_csv
            
            # Set up authors CSV path
            main_window.authors_csv_path = "test.csv"
            
            # Mock AuthorsUpdateWorker
            with patch('src.ui.main_window.AuthorsUpdateWorker') as mock_worker_class:
                mock_worker = Mock()
                mock_worker.dry_run_only = True
                mock_worker_class.return_value = mock_worker
                
                # Mock credentials
                with patch('src.ui.main_window.CredentialsDialog') as mock_cred_dialog:
                    mock_dialog = Mock()
                    mock_dialog.exec.return_value = True
                    mock_dialog.username = "test"
                    mock_dialog.password = "test"
                    mock_dialog.use_test_api = True
                    mock_cred_dialog.return_value = mock_dialog
                    
                    # Trigger dry run
                    main_window._validate_authors_csv()
                    
                    # Verify validation_update signal was connected
                    mock_worker.validation_update.connect.assert_called()
    
    def test_database_and_datacite_signals_connected(self, qtbot, main_window):
        """Test that database_update and datacite_update signals are connected."""
        # Set up for actual update (not dry run)
        main_window.authors_csv_path = "test.csv"
        
        # Mock AuthorsUpdateWorker
        with patch('src.ui.main_window.AuthorsUpdateWorker') as mock_worker_class:
            mock_worker = Mock()
            mock_worker.dry_run_only = False
            mock_worker_class.return_value = mock_worker
            
            # Call the actual update method (bypassing dry run)
            main_window._perform_authors_update(
                username="test",
                password="test",
                csv_path="test.csv",
                use_test_api=True,
                credentials_are_new=False
            )
            
            # Verify new signals were connected
            mock_worker.validation_update.connect.assert_called()
            mock_worker.database_update.connect.assert_called()
            mock_worker.datacite_update.connect.assert_called()
    
    def test_validation_handler_logs_message(self, qtbot, main_window):
        """Test that validation handler logs messages."""
        # Clear log
        main_window.log_text.clear()
        
        # Call handler directly
        main_window._on_validation_update("⏳ Test validation message")
        
        # Verify message was logged
        log_content = main_window.log_text.toPlainText()
        assert "Test validation message" in log_content
    
    def test_database_handler_logs_message(self, qtbot, main_window):
        """Test that database handler logs messages."""
        # Clear log
        main_window.log_text.clear()
        
        # Call handler directly
        main_window._on_database_update("✓ Datenbank erfolgreich")
        
        # Verify message was logged
        log_content = main_window.log_text.toPlainText()
        assert "Datenbank erfolgreich" in log_content
    
    def test_datacite_handler_logs_message(self, qtbot, main_window):
        """Test that datacite handler logs messages."""
        # Clear log
        main_window.log_text.clear()
        
        # Call handler directly
        main_window._on_datacite_update("✓ DataCite erfolgreich")
        
        # Verify message was logged
        log_content = main_window.log_text.toPlainText()
        assert "DataCite erfolgreich" in log_content
    
    def test_doi_updated_handler_shows_success(self, qtbot, main_window):
        """Test that DOI updated handler shows success messages."""
        # Clear log
        main_window.log_text.clear()
        
        # Call handler with success
        main_window._on_author_doi_updated(
            "10.5880/test.001",
            True,
            "✓ Beide Systeme erfolgreich aktualisiert"
        )
        
        # Verify success was logged with [OK] prefix
        log_content = main_window.log_text.toPlainText()
        assert "[OK]" in log_content
        assert "10.5880/test.001" in log_content
        assert "Beide Systeme erfolgreich" in log_content
    
    def test_doi_updated_handler_shows_failure(self, qtbot, main_window):
        """Test that DOI updated handler shows failure messages."""
        # Clear log
        main_window.log_text.clear()
        
        # Call handler with failure
        main_window._on_author_doi_updated(
            "10.5880/test.002",
            False,
            "Datenbank-Update fehlgeschlagen"
        )
        
        # Verify error was logged with [FEHLER] prefix
        log_content = main_window.log_text.toPlainText()
        assert "[FEHLER]" in log_content
        assert "10.5880/test.002" in log_content
        assert "Datenbank-Update fehlgeschlagen" in log_content
    
    def test_log_file_includes_db_status(self, qtbot, main_window, tmp_path):
        """Test that log file includes database sync status."""
        # Mock QSettings to return DB enabled
        with patch('src.ui.main_window.QSettings') as mock_settings:
            settings_mock = Mock()
            settings_mock.value.return_value = True  # DB enabled
            mock_settings.return_value = settings_mock
            
            # Mock Path to use tmp_path
            with patch('src.ui.main_window.Path') as mock_path:
                log_file = tmp_path / "test_log.txt"
                mock_path.return_value = log_file
                
                # Mock os.getcwd to return tmp_path
                with patch('src.ui.main_window.os.getcwd', return_value=str(tmp_path)):
                    # Create log
                    error_list = ["10.5880/test.001: INKONSISTENZ - Datenbank erfolgreich, DataCite fehlgeschlagen"]
                    main_window._create_authors_update_log(
                        success_count=5,
                        error_count=1,
                        error_list=error_list
                    )
                    
                    # Read log file
                    log_files = list(tmp_path.glob("authors_update_log_*.txt"))
                    assert len(log_files) == 1
                    
                    log_content = log_files[0].read_text(encoding='utf-8')
                    
                    # Verify DB sync info is included
                    assert "Datenbank-Synchronisation: Aktiviert" in log_content
                    assert "KRITISCHE INKONSISTENZEN: 1" in log_content
                    assert "DATABASE-FIRST UPDATE PATTERN" in log_content
                    assert "INKONSISTENZ" in log_content
