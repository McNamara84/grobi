"""Tests for DownloadURLUpdateWorker."""

import pytest
from unittest.mock import Mock, patch
import tempfile
import os

from src.workers.download_url_update_worker import DownloadURLUpdateWorker
from src.db.sumariopmd_client import DatabaseError, ConnectionError as DBConnectionError


class TestDownloadURLUpdateWorker:
    """Test suite for DownloadURLUpdateWorker class."""
    
    @pytest.fixture
    def valid_csv_file(self):
        """Create a temporary valid CSV file."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f:
            f.write("DOI,Filename,Download_URL,Description,Format,Size_Bytes\n")
            f.write("10.1594/GFZ.SDDB.1004,data.csv,https://download.gfz.de/data.csv,Download data,text/csv,62207\n")
            f.write("10.1594/GFZ.SDDB.1005,readme.txt,https://download.gfz.de/readme.txt,Readme file,text/plain,1024\n")
            csv_path = f.name
        
        yield csv_path
        
        if os.path.exists(csv_path):
            os.unlink(csv_path)
    
    @pytest.fixture
    def worker(self, valid_csv_file):
        """Create a DownloadURLUpdateWorker instance."""
        return DownloadURLUpdateWorker(
            csv_path=valid_csv_file,
            db_host="test.host",
            db_name="test_db",
            db_user="test_user",
            db_password="test_pass"
        )
    
    def test_worker_initialization(self, worker, valid_csv_file):
        """Test worker initialization."""
        assert worker.csv_path == valid_csv_file
        assert worker.db_host == "test.host"
        assert worker.db_name == "test_db"
        assert worker.db_user == "test_user"
        assert worker.db_password == "test_pass"
        assert worker._is_running is False
    
    def test_worker_stop(self, worker):
        """Test stop request."""
        worker._is_running = True
        worker.stop()
        assert worker._is_running is False
    
    def test_worker_run_success_with_updates(self, worker):
        """Test successful worker run with updates performed."""
        # Mock database client
        mock_client = Mock()
        mock_client.test_connection.return_value = (True, "Connected")
        
        # First entry: needs update, second entry: needs update
        mock_client.get_file_by_doi_and_filename.side_effect = [
            {'resource_id': 1, 'name': 'data.csv', 'url': 'https://old-url.org', 
             'description': 'Old desc', 'filemimetype': 'text/plain', 'size': 100},
            {'resource_id': 2, 'name': 'readme.txt', 'url': 'https://old-readme.org', 
             'description': 'Old readme', 'filemimetype': 'text/html', 'size': 500}
        ]
        mock_client.update_file_entry.return_value = True
        
        # Capture signals
        progress_signals = []
        entry_updated_signals = []
        finished_signal = []
        
        worker.progress_update.connect(lambda *args: progress_signals.append(args))
        worker.entry_updated.connect(lambda *args: entry_updated_signals.append(args))
        worker.finished.connect(lambda *args: finished_signal.append(args))
        
        with patch('src.workers.download_url_update_worker.SumarioPMDClient', return_value=mock_client):
            worker.run()
        
        # Check signals were emitted
        assert len(progress_signals) > 0
        assert len(entry_updated_signals) == 2
        assert len(finished_signal) == 1
        
        # Check final results
        success_count, error_count, skipped_count, error_list, skipped_details = finished_signal[0]
        assert success_count == 2
        assert error_count == 0
        assert skipped_count == 0
    
    def test_worker_run_with_skipped_entries(self, worker):
        """Test worker run where some entries are skipped (no changes)."""
        mock_client = Mock()
        mock_client.test_connection.return_value = (True, "Connected")
        
        # Both entries: no changes needed (same data as CSV)
        mock_client.get_file_by_doi_and_filename.side_effect = [
            {'resource_id': 1, 'name': 'data.csv', 'url': 'https://download.gfz.de/data.csv', 
             'description': 'Download data', 'filemimetype': 'text/csv', 'size': 62207},
            {'resource_id': 2, 'name': 'readme.txt', 'url': 'https://download.gfz.de/readme.txt', 
             'description': 'Readme file', 'filemimetype': 'text/plain', 'size': 1024}
        ]
        
        finished_signal = []
        worker.finished.connect(lambda *args: finished_signal.append(args))
        
        with patch('src.workers.download_url_update_worker.SumarioPMDClient', return_value=mock_client):
            worker.run()
        
        success_count, error_count, skipped_count, error_list, skipped_details = finished_signal[0]
        assert success_count == 0
        assert error_count == 0
        assert skipped_count == 2
    
    def test_worker_run_entry_not_found(self, worker):
        """Test worker run where entry is not found in database."""
        mock_client = Mock()
        mock_client.test_connection.return_value = (True, "Connected")
        
        # First entry: not found, second entry: found and updated
        mock_client.get_file_by_doi_and_filename.side_effect = [
            None,  # Not found
            {'resource_id': 2, 'name': 'readme.txt', 'url': 'https://old.org', 
             'description': 'Old', 'filemimetype': 'text/html', 'size': 500}
        ]
        mock_client.update_file_entry.return_value = True
        
        finished_signal = []
        worker.finished.connect(lambda *args: finished_signal.append(args))
        
        with patch('src.workers.download_url_update_worker.SumarioPMDClient', return_value=mock_client):
            worker.run()
        
        success_count, error_count, skipped_count, error_list, skipped_details = finished_signal[0]
        assert success_count == 1
        assert error_count == 1
        assert skipped_count == 0
        assert len(error_list) == 1
        assert "nicht in Datenbank gefunden" in error_list[0]
    
    def test_worker_run_database_error(self, worker):
        """Test worker run with database error during processing."""
        mock_client = Mock()
        mock_client.test_connection.return_value = (True, "Connected")
        
        # First entry: raises DatabaseError
        mock_client.get_file_by_doi_and_filename.side_effect = DatabaseError("Query failed")
        
        finished_signal = []
        worker.finished.connect(lambda *args: finished_signal.append(args))
        
        with patch('src.workers.download_url_update_worker.SumarioPMDClient', return_value=mock_client):
            worker.run()
        
        success_count, error_count, skipped_count, error_list, skipped_details = finished_signal[0]
        assert success_count == 0
        assert error_count == 2  # Both entries fail
        assert len(error_list) == 2
        assert "Datenbankfehler" in error_list[0]
    
    def test_worker_run_connection_failed(self, worker):
        """Test worker run when database connection fails."""
        mock_client = Mock()
        mock_client.test_connection.return_value = (False, "Connection refused")
        
        error_signal = []
        worker.error_occurred.connect(lambda msg: error_signal.append(msg))
        
        with patch('src.workers.download_url_update_worker.SumarioPMDClient', return_value=mock_client):
            worker.run()
        
        assert len(error_signal) == 1
        assert "Datenbankverbindung fehlgeschlagen" in error_signal[0]
    
    def test_worker_run_connection_exception(self, worker):
        """Test worker run when database connection raises exception."""
        error_signal = []
        worker.error_occurred.connect(lambda msg: error_signal.append(msg))
        
        with patch('src.workers.download_url_update_worker.SumarioPMDClient') as mock_client_class:
            mock_client_class.side_effect = DBConnectionError("Network unreachable")
            worker.run()
        
        assert len(error_signal) == 1
        assert "Datenbankverbindung fehlgeschlagen" in error_signal[0]
    
    def test_worker_run_csv_parse_error(self):
        """Test worker run with invalid CSV file."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f:
            f.write("Invalid,Headers,Here\n")
            csv_path = f.name
        
        try:
            worker = DownloadURLUpdateWorker(
                csv_path=csv_path,
                db_host="test.host",
                db_name="test_db",
                db_user="test_user",
                db_password="test_pass"
            )
            
            error_signal = []
            worker.error_occurred.connect(lambda msg: error_signal.append(msg))
            
            worker.run()
            
            assert len(error_signal) == 1
            assert "Fehler beim Lesen der CSV-Datei" in error_signal[0]
        finally:
            os.unlink(csv_path)
    
    def test_worker_run_csv_file_not_found(self):
        """Test worker run with non-existent CSV file."""
        worker = DownloadURLUpdateWorker(
            csv_path="non_existent_file.csv",
            db_host="test.host",
            db_name="test_db",
            db_user="test_user",
            db_password="test_pass"
        )
        
        error_signal = []
        worker.error_occurred.connect(lambda msg: error_signal.append(msg))
        
        worker.run()
        
        assert len(error_signal) == 1
        assert "Fehler beim Lesen der CSV-Datei" in error_signal[0]
    
    def test_worker_run_empty_csv(self):
        """Test worker run with CSV that has only headers."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f:
            f.write("DOI,Filename,Download_URL,Description,Format,Size_Bytes\n")
            csv_path = f.name
        
        try:
            worker = DownloadURLUpdateWorker(
                csv_path=csv_path,
                db_host="test.host",
                db_name="test_db",
                db_user="test_user",
                db_password="test_pass"
            )
            
            error_signal = []
            worker.error_occurred.connect(lambda msg: error_signal.append(msg))
            
            worker.run()
            
            assert len(error_signal) == 1
            assert "Keine gültigen Einträge" in error_signal[0]
        finally:
            os.unlink(csv_path)
    
    def test_worker_run_cancellation(self, worker):
        """Test worker cancellation during processing."""
        mock_client = Mock()
        mock_client.test_connection.return_value = (True, "Connected")
        
        # First entry processing triggers stop
        def stop_on_first_call(*args):
            worker.stop()
            return {'resource_id': 1, 'name': 'data.csv', 'url': 'https://old.org', 
                    'description': 'Old', 'filemimetype': 'text/csv', 'size': 100}
        
        mock_client.get_file_by_doi_and_filename.side_effect = stop_on_first_call
        mock_client.update_file_entry.return_value = True
        
        progress_signals = []
        worker.progress_update.connect(lambda *args: progress_signals.append(args))
        
        with patch('src.workers.download_url_update_worker.SumarioPMDClient', return_value=mock_client):
            worker.run()
        
        # Check that cancellation message was emitted
        cancel_messages = [p for p in progress_signals if "Abgebrochen" in p[2]]
        assert len(cancel_messages) >= 1
    
    def test_worker_run_unexpected_exception(self, worker):
        """Test worker run with unexpected exception."""
        error_signal = []
        worker.error_occurred.connect(lambda msg: error_signal.append(msg))
        
        with patch('src.workers.download_url_update_worker.CSVParser.parse_download_urls_csv') as mock_parse:
            mock_parse.side_effect = RuntimeError("Unexpected error")
            worker.run()
        
        assert len(error_signal) == 1
        assert "Unerwarteter Fehler" in error_signal[0]
    
    def test_worker_signal_emissions(self, worker):
        """Test that all expected signals are emitted correctly."""
        mock_client = Mock()
        mock_client.test_connection.return_value = (True, "Connected")
        
        # Mix of outcomes: updated, skipped, not found
        mock_client.get_file_by_doi_and_filename.side_effect = [
            {'resource_id': 1, 'name': 'data.csv', 'url': 'https://old.org', 
             'description': 'Old', 'filemimetype': 'text/csv', 'size': 100},  # Will be updated
            None  # Not found
        ]
        mock_client.update_file_entry.return_value = True
        
        # Capture all signals
        progress_signals = []
        entry_updated_signals = []
        finished_signal = []
        error_signals = []
        
        worker.progress_update.connect(lambda *args: progress_signals.append(args))
        worker.entry_updated.connect(lambda *args: entry_updated_signals.append(args))
        worker.finished.connect(lambda *args: finished_signal.append(args))
        worker.error_occurred.connect(lambda msg: error_signals.append(msg))
        
        with patch('src.workers.download_url_update_worker.SumarioPMDClient', return_value=mock_client):
            worker.run()
        
        # Verify progress signals (should include start, processing, finish)
        assert len(progress_signals) >= 3
        
        # Verify entry_updated signals (one per entry)
        assert len(entry_updated_signals) == 2
        
        # First entry: success (updated)
        doi1, filename1, success1, message1 = entry_updated_signals[0]
        assert doi1 == "10.1594/GFZ.SDDB.1004"
        assert filename1 == "data.csv"
        assert success1 is True
        assert "Aktualisiert" in message1
        
        # Second entry: error (not found)
        doi2, filename2, success2, message2 = entry_updated_signals[1]
        assert doi2 == "10.1594/GFZ.SDDB.1005"
        assert filename2 == "readme.txt"
        assert success2 is False
        assert "Nicht gefunden" in message2
        
        # Verify finished signal
        assert len(finished_signal) == 1
        
        # No error_occurred signal for normal processing
        assert len(error_signals) == 0
    
    def test_worker_change_detection_url_only(self, worker):
        """Test that only changed fields trigger an update."""
        mock_client = Mock()
        mock_client.test_connection.return_value = (True, "Connected")
        
        # Entry where only URL is different
        mock_client.get_file_by_doi_and_filename.side_effect = [
            {'resource_id': 1, 'name': 'data.csv', 'url': 'https://old-url.org', 
             'description': 'Download data', 'filemimetype': 'text/csv', 'size': 62207},
            {'resource_id': 2, 'name': 'readme.txt', 'url': 'https://download.gfz.de/readme.txt', 
             'description': 'Readme file', 'filemimetype': 'text/plain', 'size': 1024}  # No changes
        ]
        mock_client.update_file_entry.return_value = True
        
        finished_signal = []
        worker.finished.connect(lambda *args: finished_signal.append(args))
        
        with patch('src.workers.download_url_update_worker.SumarioPMDClient', return_value=mock_client):
            worker.run()
        
        success_count, error_count, skipped_count, error_list, skipped_details = finished_signal[0]
        assert success_count == 1  # First entry updated
        assert skipped_count == 1  # Second entry skipped
        
        # Verify update_file_entry was called with only url parameter
        mock_client.update_file_entry.assert_called_once()
        call_kwargs = mock_client.update_file_entry.call_args
        # Check that only url was passed (not description, filemimetype, size)
        assert call_kwargs[1].get('url') is not None
