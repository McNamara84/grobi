"""Tests for UpdateWorker skipped_details functionality (Phase 4)."""

import pytest
from unittest.mock import Mock, patch
from pathlib import Path
import tempfile

from src.workers.update_worker import UpdateWorker


class TestUpdateWorkerSkippedDetails:
    """Test skipped_details collection and reporting."""
    
    @pytest.fixture
    def temp_csv(self):
        """Create a temporary CSV file for testing."""
        content = "DOI,Landing_Page_URL\n10.5880/GFZ.1.1.2021.001,https://example.org/dataset1\n10.5880/GFZ.1.1.2021.002,https://example.org/dataset2"
        
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv', encoding='utf-8') as f:
            f.write(content)
            temp_path = f.name
        
        yield temp_path
        
        # Cleanup
        Path(temp_path).unlink(missing_ok=True)
    
    def test_skipped_details_empty_when_all_changed(self, qtbot, temp_csv):
        """Test that skipped_details is empty when all URLs changed."""
        worker = UpdateWorker("test_user", "test_pass", temp_csv, use_test_api=True)
        
        finished_signal = []
        worker.finished.connect(lambda *args: finished_signal.append(args))
        
        # Mock DataCiteClient with different URLs
        with patch('src.workers.update_worker.DataCiteClient') as MockClient:
            mock_client = Mock()
            MockClient.return_value = mock_client
            
            # URLs are different from CSV
            mock_client.get_doi_metadata.side_effect = [
                {'data': {'attributes': {'url': 'https://old-url.org/dataset1'}}},
                {'data': {'attributes': {'url': 'https://old-url.org/dataset2'}}}
            ]
            mock_client.update_doi_url.return_value = (True, "Success")
            
            worker.run()
            qtbot.wait(500)
        
        # Check finished signal
        success_count, error_count, skipped_count, error_list, skipped_details = finished_signal[0]
        
        # All URLs changed - no skips
        assert skipped_count == 0
        assert len(skipped_details) == 0
        assert success_count == 2
    
    def test_skipped_details_mixed_scenario(self, qtbot, temp_csv):
        """Test skipped_details with mixed changed/unchanged URLs."""
        worker = UpdateWorker("test_user", "test_pass", temp_csv, use_test_api=True)
        
        finished_signal = []
        worker.finished.connect(lambda *args: finished_signal.append(args))
        
        # Mock DataCiteClient: first unchanged, second changed
        with patch('src.workers.update_worker.DataCiteClient') as MockClient:
            mock_client = Mock()
            MockClient.return_value = mock_client
            
            mock_client.get_doi_metadata.side_effect = [
                {'data': {'attributes': {'url': 'https://example.org/dataset1'}}},  # Unchanged
                {'data': {'attributes': {'url': 'https://old-url.org/dataset2'}}}   # Changed
            ]
            mock_client.update_doi_url.return_value = (True, "Success")
            
            worker.run()
            qtbot.wait(500)
        
        # Check finished signal
        success_count, error_count, skipped_count, error_list, skipped_details = finished_signal[0]
        
        # One skipped, one updated
        assert skipped_count == 1
        assert success_count == 2  # Includes skipped (counted as success)
        assert len(skipped_details) == 1
        
        # Verify skipped DOI is the first one
        doi, reason = skipped_details[0]
        assert doi == "10.5880/GFZ.1.1.2021.001"
        assert "URL unverändert" in reason
    
    def test_skipped_details_on_csv_parse_error(self, qtbot):
        """Test that skipped_details is empty list on CSV parse error."""
        worker = UpdateWorker("test_user", "test_pass", "/nonexistent/file.csv", use_test_api=True)
        
        finished_signal = []
        worker.finished.connect(lambda *args: finished_signal.append(args))
        
        worker.run()
        qtbot.wait(50)
        
        # Check finished signal
        assert len(finished_signal) == 1
        success_count, error_count, skipped_count, error_list, skipped_details = finished_signal[0]
        
        assert success_count == 0
        assert skipped_count == 0
        assert len(skipped_details) == 0  # Empty list, not None
    
    def test_skipped_details_on_network_error(self, qtbot, temp_csv):
        """Test skipped_details is preserved on network error mid-update."""
        worker = UpdateWorker("test_user", "test_pass", temp_csv, use_test_api=True)
        
        finished_signal = []
        worker.finished.connect(lambda *args: finished_signal.append(args))
        
        # Mock DataCiteClient: first skipped, second causes network error
        with patch('src.workers.update_worker.DataCiteClient') as MockClient:
            mock_client = Mock()
            MockClient.return_value = mock_client
            
            from src.api.datacite_client import NetworkError
            
            mock_client.get_doi_metadata.side_effect = [
                {'data': {'attributes': {'url': 'https://example.org/dataset1'}}},  # Unchanged - will be skipped
                NetworkError("Connection lost")  # Second DOI causes error
            ]
            
            worker.run()
            qtbot.wait(500)
        
        # Check finished signal
        success_count, error_count, skipped_count, error_list, skipped_details = finished_signal[0]
        
        # First DOI was skipped before network error
        assert skipped_count == 1
        assert len(skipped_details) == 1
        doi, reason = skipped_details[0]
        assert doi == "10.5880/GFZ.1.1.2021.001"
    
    def test_skipped_details_logging(self, qtbot, temp_csv, caplog):
        """Test that skipped details are logged."""
        import logging
        caplog.set_level(logging.INFO)
        
        worker = UpdateWorker("test_user", "test_pass", temp_csv, use_test_api=True)
        
        finished_signal = []
        worker.finished.connect(lambda *args: finished_signal.append(args))
        
        # Mock DataCiteClient with unchanged URLs
        with patch('src.workers.update_worker.DataCiteClient') as MockClient:
            mock_client = Mock()
            MockClient.return_value = mock_client
            
            mock_client.get_doi_metadata.side_effect = [
                {'data': {'attributes': {'url': 'https://example.org/dataset1'}}},
                {'data': {'attributes': {'url': 'https://example.org/dataset2'}}}
            ]
            
            worker.run()
            qtbot.wait(500)
        
        # Check that skipped DOIs were logged
        assert "Skipped DOIs (first 5 of 2):" in caplog.text
        assert "10.5880/GFZ.1.1.2021.001: URL unverändert" in caplog.text
        assert "10.5880/GFZ.1.1.2021.002: URL unverändert" in caplog.text
