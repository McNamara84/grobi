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
        # Mock get_doi_metadata to return different URLs (so updates happen)
        mock_client.get_doi_metadata.side_effect = [
            {'data': {'attributes': {'url': 'https://old-url.org/doi1'}}},
            {'data': {'attributes': {'url': 'https://old-url.org/doi2'}}}
        ]
        
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
        
        # Check final results - now with skipped_count
        success_count, error_count, skipped_count, error_list = finished_signal[0]
        assert success_count == 2
        assert error_count == 0
        assert skipped_count == 0  # No skips - URLs are different
        assert len(error_list) == 0
    
    def test_worker_run_with_errors(self, worker):
        """Test worker run with some failed updates."""
        # Mock DataCiteClient - first DOI fails, second succeeds
        mock_client = Mock()
        mock_client.update_doi_url.side_effect = [
            (False, "DOI not found"),
            (True, "Success")
        ]
        # Mock get_doi_metadata to return different URLs
        mock_client.get_doi_metadata.side_effect = [
            {'data': {'attributes': {'url': 'https://old-url.org/doi1'}}},
            {'data': {'attributes': {'url': 'https://old-url.org/doi2'}}}
        ]
        
        finished_signal = []
        worker.finished.connect(lambda *args: finished_signal.append(args))
        
        with patch('src.workers.update_worker.DataCiteClient', return_value=mock_client):
            worker.run()
        
        # Check final results - now with skipped_count
        success_count, error_count, skipped_count, error_list = finished_signal[0]
        assert success_count == 1
        assert error_count == 1
        assert skipped_count == 0
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
        
        # Check finished was called with zeros (now with skipped_count)
        assert len(finished_signal) == 1
        assert finished_signal[0] == (0, 0, 0, [])
    
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
        # Mock get_doi_metadata to return different URLs
        mock_client.get_doi_metadata.side_effect = [
            {'data': {'attributes': {'url': 'https://old-url.org/doi1'}}},
            {'data': {'attributes': {'url': 'https://old-url.org/doi2'}}}
        ]
        
        progress_signals = []
        worker.progress_update.connect(lambda *args: progress_signals.append(args))
        
        with patch('src.workers.update_worker.DataCiteClient', return_value=mock_client):
            worker.run()
        
        # Check we got progress updates for each DOI (now says "Prüfe DOI")
        update_messages = [msg for _, _, msg in progress_signals if "Prüfe DOI" in msg]
        assert len(update_messages) == 2
    
    def test_worker_unexpected_error(self, worker):
        """Test worker handles unexpected errors gracefully."""
        # Mock DataCiteClient to raise unexpected exception
        mock_client = Mock()
        mock_client.get_doi_metadata.side_effect = Exception("Unexpected error")
        
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
        
        # Check final results show all errors (now with skipped_count)
        success_count, error_count, skipped_count, error_list = finished_signal[0]
        assert success_count == 0
        assert error_count == 2
        assert skipped_count == 0
    
    # Phase 1: NEW TESTS for Change Detection
    
    def test_url_change_detection_no_change(self, worker):
        """Test that unchanged URLs are skipped (Phase 1: Change Detection)."""
        # Mock DataCiteClient
        mock_client = Mock()
        # Mock get_doi_metadata to return SAME URLs as in CSV (no change)
        mock_client.get_doi_metadata.side_effect = [
            {'data': {'attributes': {'url': 'https://example.org/doi1'}}},  # Same as CSV
            {'data': {'attributes': {'url': 'https://example.org/doi2'}}}   # Same as CSV
        ]
        # update_doi_url should NOT be called for skipped DOIs
        mock_client.update_doi_url.return_value = (True, "Success")
        
        doi_updated_signals = []
        finished_signal = []
        
        worker.doi_updated.connect(lambda *args: doi_updated_signals.append(args))
        worker.finished.connect(lambda *args: finished_signal.append(args))
        
        with patch('src.workers.update_worker.DataCiteClient', return_value=mock_client):
            worker.run()
        
        # Check that both DOIs were marked as "skipped"
        assert len(doi_updated_signals) == 2
        for doi, success, message in doi_updated_signals:
            assert success is True
            assert "übersprungen" in message.lower() or "keine änderung" in message.lower()
        
        # Check final results
        success_count, error_count, skipped_count, error_list = finished_signal[0]
        assert success_count == 2  # Both counted as success (no change needed)
        assert skipped_count == 2   # Both were skipped
        assert error_count == 0
        
        # Verify update_doi_url was NEVER called (both skipped)
        assert mock_client.update_doi_url.call_count == 0
    
    def test_url_change_detection_with_change(self, worker):
        """Test that changed URLs are updated (Phase 1: Change Detection)."""
        # Mock DataCiteClient
        mock_client = Mock()
        # Mock get_doi_metadata to return DIFFERENT URLs than CSV (change detected)
        mock_client.get_doi_metadata.side_effect = [
            {'data': {'attributes': {'url': 'https://old-url.org/doi1'}}},  # Different
            {'data': {'attributes': {'url': 'https://old-url.org/doi2'}}}   # Different
        ]
        mock_client.update_doi_url.return_value = (True, "Success")
        
        finished_signal = []
        worker.finished.connect(lambda *args: finished_signal.append(args))
        
        with patch('src.workers.update_worker.DataCiteClient', return_value=mock_client):
            worker.run()
        
        # Check final results
        success_count, error_count, skipped_count, error_list = finished_signal[0]
        assert success_count == 2  # Both updated
        assert skipped_count == 0  # None skipped
        assert error_count == 0
        
        # Verify update_doi_url WAS called for both DOIs
        assert mock_client.update_doi_url.call_count == 2
    
    def test_url_change_detection_mixed(self):
        """Test mixed scenario: some changed, some unchanged."""
        # Create CSV with 3 DOIs
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f:
            f.write("DOI,Landing_Page_URL\n")
            f.write("10.5880/GFZ.1,https://example.org/doi1\n")
            f.write("10.5880/GFZ.2,https://example.org/doi2\n")
            f.write("10.5880/GFZ.3,https://example.org/doi3\n")
            csv_path = f.name
        
        worker = UpdateWorker(
            username="test_user",
            password="test_pass",
            csv_path=csv_path,
            use_test_api=True
        )
        
        # Mock DataCiteClient
        mock_client = Mock()
        # DOI 1: unchanged, DOI 2: changed, DOI 3: unchanged
        mock_client.get_doi_metadata.side_effect = [
            {'data': {'attributes': {'url': 'https://example.org/doi1'}}},      # Same
            {'data': {'attributes': {'url': 'https://old-url.org/doi2'}}},      # Different
            {'data': {'attributes': {'url': 'https://example.org/doi3'}}}       # Same
        ]
        mock_client.update_doi_url.return_value = (True, "Success")
        
        finished_signal = []
        worker.finished.connect(lambda *args: finished_signal.append(args))
        
        try:
            with patch('src.workers.update_worker.DataCiteClient', return_value=mock_client):
                worker.run()
            
            # Check final results
            success_count, error_count, skipped_count, error_list = finished_signal[0]
            assert success_count == 3  # All counted as success
            assert skipped_count == 2  # 2 were skipped (DOI 1 and 3)
            assert error_count == 0
            
            # Verify update_doi_url was called only ONCE (for DOI 2)
            assert mock_client.update_doi_url.call_count == 1
        finally:
            # Cleanup
            if os.path.exists(csv_path):
                os.unlink(csv_path)
    
    def test_url_change_detection_metadata_fetch_fails(self, worker):
        """Test that update proceeds if metadata fetch fails (Phase 1: fallback)."""
        # Mock DataCiteClient
        mock_client = Mock()
        # Mock get_doi_metadata to return None (fetch failed)
        mock_client.get_doi_metadata.return_value = None
        mock_client.update_doi_url.return_value = (True, "Success")
        
        finished_signal = []
        worker.finished.connect(lambda *args: finished_signal.append(args))
        
        with patch('src.workers.update_worker.DataCiteClient', return_value=mock_client):
            worker.run()
        
        # Check final results
        success_count, error_count, skipped_count, error_list = finished_signal[0]
        assert success_count == 2  # Both updated (fallback: couldn't check, so update anyway)
        assert skipped_count == 0  # None skipped
        assert error_count == 0
        
        # Verify update_doi_url WAS called (fallback behavior)
        assert mock_client.update_doi_url.call_count == 2
