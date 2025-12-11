"""Tests for DownloadURLFetchWorker."""

import pytest
from unittest.mock import MagicMock, patch
from PySide6.QtWidgets import QApplication

from src.workers.download_url_fetch_worker import DownloadURLFetchWorker
from src.db.sumariopmd_client import DatabaseError, ConnectionError as DBConnectionError


@pytest.fixture(scope="module")
def qapp():
    """Create QApplication instance for tests."""
    app = QApplication.instance() or QApplication([])
    yield app


class TestDownloadURLFetchWorker:
    """Tests for DownloadURLFetchWorker class."""
    
    def test_worker_initialization(self, qapp):
        """Test worker initialization with credentials."""
        worker = DownloadURLFetchWorker(
            db_host="test.host",
            db_name="test_db",
            db_user="test_user",
            db_password="test_pass"
        )
        
        assert worker.db_host == "test.host"
        assert worker.db_name == "test_db"
        assert worker.db_user == "test_user"
        assert worker.db_password == "test_pass"
    
    def test_worker_successful_fetch(self, qapp, qtbot):
        """Test successful DOI+download URL fetch."""
        worker = DownloadURLFetchWorker(
            db_host="test.host",
            db_name="test_db",
            db_user="test_user",
            db_password="test_pass"
        )
        
        mock_data = [
            ('10.5880/GFZ.1', 'data.zip', 'https://example.com/data.zip', 'Download data file', 'ZIP', 1024),
            ('10.5880/GFZ.2', 'file.nc', 'https://example.com/file.nc', 'Metadata file', 'NetCDF', 2048)
        ]
        
        with patch('src.workers.download_url_fetch_worker.SumarioPMDClient') as mock_client_class:
            mock_client = MagicMock()
            mock_client.test_connection.return_value = (True, "Connected")
            mock_client.fetch_all_dois_with_downloads.return_value = mock_data
            mock_client_class.return_value = mock_client
            
            # Connect signal spy
            with qtbot.waitSignal(worker.finished, timeout=2000) as blocker:
                worker.run()
            
            # Verify signal data - finished signal contains the list of tuples
            result_data = blocker.args[0]
            assert len(result_data) == 2
            assert result_data[0][0] == '10.5880/GFZ.1'
            assert result_data[1][0] == '10.5880/GFZ.2'
    
    def test_worker_connection_failure(self, qapp, qtbot):
        """Test handling of connection failure."""
        worker = DownloadURLFetchWorker(
            db_host="test.host",
            db_name="test_db",
            db_user="test_user",
            db_password="test_pass"
        )
        
        with patch('src.workers.download_url_fetch_worker.SumarioPMDClient') as mock_client_class:
            mock_client = MagicMock()
            mock_client.test_connection.return_value = (False, "Connection refused")
            mock_client_class.return_value = mock_client
            
            # Connect signal spy
            with qtbot.waitSignal(worker.error, timeout=2000) as blocker:
                worker.run()
            
            # Verify error message
            assert "Datenbankverbindung fehlgeschlagen" in blocker.args[0]
    
    def test_worker_no_data(self, qapp, qtbot):
        """Test handling when no DOIs with files are found."""
        worker = DownloadURLFetchWorker(
            db_host="test.host",
            db_name="test_db",
            db_user="test_user",
            db_password="test_pass"
        )
        
        with patch('src.workers.download_url_fetch_worker.SumarioPMDClient') as mock_client_class:
            mock_client = MagicMock()
            mock_client.test_connection.return_value = (True, "Connected")
            mock_client.fetch_all_dois_with_downloads.return_value = []
            mock_client_class.return_value = mock_client
            
            # Connect signal spy
            with qtbot.waitSignal(worker.error, timeout=2000) as blocker:
                worker.run()
            
            # Verify error message
            assert "Keine DOIs mit Download-URLs gefunden" in blocker.args[0]
    
    def test_worker_database_error(self, qapp, qtbot):
        """Test handling of database errors during fetch."""
        worker = DownloadURLFetchWorker(
            db_host="test.host",
            db_name="test_db",
            db_user="test_user",
            db_password="test_pass"
        )
        
        with patch('src.workers.download_url_fetch_worker.SumarioPMDClient') as mock_client_class:
            mock_client = MagicMock()
            mock_client.test_connection.return_value = (True, "Connected")
            mock_client.fetch_all_dois_with_downloads.side_effect = DatabaseError("Query failed")
            mock_client_class.return_value = mock_client
            
            # Connect signal spy
            with qtbot.waitSignal(worker.error, timeout=2000) as blocker:
                worker.run()
            
            # Verify error message
            assert "Datenbankfehler" in blocker.args[0]
    
    def test_worker_connection_error(self, qapp, qtbot):
        """Test handling of connection errors."""
        worker = DownloadURLFetchWorker(
            db_host="test.host",
            db_name="test_db",
            db_user="test_user",
            db_password="test_pass"
        )
        
        with patch('src.workers.download_url_fetch_worker.SumarioPMDClient') as mock_client_class:
            mock_client_class.side_effect = DBConnectionError("Cannot connect")
            
            # Connect signal spy
            with qtbot.waitSignal(worker.error, timeout=2000) as blocker:
                worker.run()
            
            # Verify error message
            assert "Datenbankverbindung fehlgeschlagen" in blocker.args[0]
    
    def test_worker_unexpected_error(self, qapp, qtbot):
        """Test handling of unexpected errors."""
        worker = DownloadURLFetchWorker(
            db_host="test.host",
            db_name="test_db",
            db_user="test_user",
            db_password="test_pass"
        )
        
        with patch('src.workers.download_url_fetch_worker.SumarioPMDClient') as mock_client_class:
            mock_client_class.side_effect = RuntimeError("Unexpected error")
            
            # Connect signal spy
            with qtbot.waitSignal(worker.error, timeout=2000) as blocker:
                worker.run()
            
            # Verify error message
            assert "Unerwarteter Fehler" in blocker.args[0]
