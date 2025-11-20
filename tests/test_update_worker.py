"""Tests for UpdateWorker."""

import pytest
from unittest.mock import Mock, patch
import tempfile
import os

from src.workers.update_worker import UpdateWorker
from src.api.datacite_client import NetworkError


class TestUpdateWorker:
    """Test suite for UpdateWorker class."""
    
    @pytest.fixture
    def valid_csv_file(self):
        """Create a temporary valid CSV file."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f:
            f.write("DOI,Landing_Page_URL\n")
            f.write("10.5880/GFZ.1.1.2021.001,https://example.org/doi1\n")
            f.write("10.5880/GFZ.1.1.2021.002,https://example.org/doi2\n")
            csv_path = f.name
        
        yield csv_path
        
        # Cleanup
        if os.path.exists(csv_path):
            os.unlink(csv_path)
    
    @pytest.fixture
    def worker(self, valid_csv_file):
        """Create an UpdateWorker instance."""
        return UpdateWorker(
            username="test_user",
            password="test_pass",
            csv_path=valid_csv_file,
            use_test_api=True
        )
    
    def test_worker_initialization(self, worker, valid_csv_file):
        """Test worker initialization."""
        assert worker.username == "test_user"
        assert worker.password == "test_pass"
        assert worker.csv_path == valid_csv_file
        assert worker.use_test_api is True
        assert worker._is_running is False
    
    def test_worker_run_success(self, worker):
        """Test successful worker run."""
        # Mock DataCiteClient
        mock_client = Mock()
        mock_client.update_doi_url.return_value = (True, "Success")
        
        # Capture signals
        progress_signals = []
        doi_updated_signals = []
        finished_signal = []
        
        worker.progress_update.connect(lambda *args: progress_signals.append(args))
        worker.doi_updated.connect(lambda *args: doi_updated_signals.append(args))
        worker.finished.connect(lambda *args: finished_signal.append(args))
        
        with patch('src.workers.update_worker.DataCiteClient', return_value=mock_client):
            worker.run()
        
        # Check signals were emitted
        assert len(progress_signals) > 0
        assert len(doi_updated_signals) == 2  # Two DOIs in CSV
        assert len(finished_signal) == 1
        
        # Check final results
        success_count, error_count, error_list = finished_signal[0]
        assert success_count == 2
        assert error_count == 0
        assert len(error_list) == 0
    
    def test_worker_run_with_errors(self, worker):
        """Test worker run with some failed updates."""
        # Mock DataCiteClient - first DOI fails, second succeeds
        mock_client = Mock()
        mock_client.update_doi_url.side_effect = [
            (False, "DOI not found"),
            (True, "Success")
        ]
        
        finished_signal = []
        worker.finished.connect(lambda *args: finished_signal.append(args))
        
        with patch('src.workers.update_worker.DataCiteClient', return_value=mock_client):
            worker.run()
        
        # Check final results
        success_count, error_count, error_list = finished_signal[0]
        assert success_count == 1
        assert error_count == 1
        assert len(error_list) == 1
        assert "DOI not found" in error_list[0]
    
    def test_worker_run_csv_parse_error(self):
        """Test worker run with CSV parse error."""
        # Create worker with invalid CSV path
        worker = UpdateWorker(
            username="test_user",
            password="test_pass",
            csv_path="non_existent.csv",
            use_test_api=True
        )
        
        error_signal = []
        finished_signal = []
        
        worker.error_occurred.connect(lambda msg: error_signal.append(msg))
        worker.finished.connect(lambda *args: finished_signal.append(args))
        
        worker.run()
        
        # Check error was emitted
        assert len(error_signal) == 1
        assert "Fehler beim Lesen der CSV-Datei" in error_signal[0]
        
        # Check finished was called with zeros
        assert len(finished_signal) == 1
        assert finished_signal[0] == (0, 0, [])
    
    def test_worker_run_network_error(self, worker):
        """Test worker run with network error during update."""
        # Mock DataCiteClient to raise NetworkError
        mock_client = Mock()
        mock_client.update_doi_url.side_effect = NetworkError("Connection failed")
        
        error_signal = []
        
        worker.error_occurred.connect(lambda msg: error_signal.append(msg))
        
        with patch('src.workers.update_worker.DataCiteClient', return_value=mock_client):
            worker.run()
        
        # Check error was emitted
        assert len(error_signal) == 1
        assert "Netzwerkfehler" in error_signal[0]
    
    def test_worker_stop(self, worker):
        """Test worker stop functionality."""
        assert worker._is_running is False
        
        # Simulate running state
        worker._is_running = True
        
        worker.stop()
        
        assert worker._is_running is False
    
    def test_worker_signals_exist(self, worker):
        """Test that all required signals exist."""
        assert hasattr(worker, 'progress_update')
        assert hasattr(worker, 'doi_updated')
        assert hasattr(worker, 'finished')
        assert hasattr(worker, 'error_occurred')
    
    def test_worker_progress_updates(self, worker):
        """Test that progress updates are emitted correctly."""
        mock_client = Mock()
        mock_client.update_doi_url.return_value = (True, "Success")
        
        progress_signals = []
        worker.progress_update.connect(lambda *args: progress_signals.append(args))
        
        with patch('src.workers.update_worker.DataCiteClient', return_value=mock_client):
            worker.run()
        
        # Check we got progress updates for each DOI
        update_messages = [msg for _, _, msg in progress_signals if "Aktualisiere DOI" in msg]
        assert len(update_messages) == 2
    
    def test_worker_unexpected_error(self, worker):
        """Test worker handles unexpected errors gracefully."""
        # Mock DataCiteClient to raise unexpected exception
        mock_client = Mock()
        mock_client.update_doi_url.side_effect = Exception("Unexpected error")
        
        doi_updated_signals = []
        finished_signal = []
        
        worker.doi_updated.connect(lambda *args: doi_updated_signals.append(args))
        worker.finished.connect(lambda *args: finished_signal.append(args))
        
        with patch('src.workers.update_worker.DataCiteClient', return_value=mock_client):
            worker.run()
        
        # Check that worker continued despite errors
        assert len(doi_updated_signals) == 2
        
        # Both should have failed
        assert all(not success for _, success, _ in doi_updated_signals)
        
        # Check final results show all errors
        success_count, error_count, error_list = finished_signal[0]
        assert success_count == 0
        assert error_count == 2
