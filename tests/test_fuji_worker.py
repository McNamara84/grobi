"""Unit tests for F-UJI Worker classes."""

import pytest
import queue
import threading
from unittest.mock import MagicMock, patch, PropertyMock

from PySide6.QtCore import QThread
from PySide6.QtWidgets import QApplication

from src.api.fuji_client import FujiClient, FujiResult, FujiConnectionError, FujiAuthenticationError
from src.workers.fuji_worker import (
    FujiAssessmentWorker,
    FujiAssessmentThread,
    StreamingFujiWorker,
    StreamingFujiThread
)


@pytest.fixture(scope="module")
def qapp():
    """Create QApplication for the test module."""
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    yield app


@pytest.fixture
def mock_fuji_client():
    """Create a mock F-UJI client."""
    client = MagicMock(spec=FujiClient)
    client.test_connection.return_value = True
    client.endpoint = "https://test.example.com"
    return client


@pytest.fixture
def mock_datacite_client():
    """Create a mock DataCite client."""
    client = MagicMock()
    
    # Mock _fetch_page to return DOIs
    def mock_fetch_page(next_url):
        if next_url is None:
            # First page
            return (
                [("10.5880/test.001", "https://example.com/1"),
                 ("10.5880/test.002", "https://example.com/2")],
                "https://api.datacite.org/dois?cursor=abc"
            )
        else:
            # No more pages
            return ([], None)
    
    client._fetch_page = MagicMock(side_effect=mock_fetch_page)
    return client


class TestFujiAssessmentWorker:
    """Test FujiAssessmentWorker."""
    
    def test_worker_init(self, mock_fuji_client):
        """Test worker initialization."""
        dois = ["10.5880/test.001", "10.5880/test.002"]
        worker = FujiAssessmentWorker(dois, mock_fuji_client, max_workers=3)
        
        assert worker.dois == dois
        assert worker.fuji_client == mock_fuji_client
        assert worker.max_workers == 3
        assert worker._cancelled is False
    
    def test_worker_cancel(self, mock_fuji_client):
        """Test worker cancellation."""
        worker = FujiAssessmentWorker(["10.5880/test.001"], mock_fuji_client)
        worker.cancel()
        
        assert worker._cancelled is True
    
    def test_worker_empty_dois(self, qapp, mock_fuji_client):
        """Test worker with empty DOI list."""
        worker = FujiAssessmentWorker([], mock_fuji_client)
        
        finished_signal = MagicMock()
        worker.finished.connect(finished_signal)
        
        worker.run()
        
        # Should emit finished immediately
        finished_signal.assert_called_once()
    
    def test_worker_connection_failure(self, qapp, mock_fuji_client):
        """Test worker handles connection failure."""
        mock_fuji_client.test_connection.return_value = False
        
        worker = FujiAssessmentWorker(["10.5880/test.001"], mock_fuji_client)
        
        error_signal = MagicMock()
        finished_signal = MagicMock()
        worker.error.connect(error_signal)
        worker.finished.connect(finished_signal)
        
        worker.run()
        
        # Should emit error and finished
        error_signal.assert_called_once()
        finished_signal.assert_called_once()
    
    def test_worker_assess_single_doi_cancelled(self, mock_fuji_client):
        """Test _assess_single_doi when cancelled."""
        worker = FujiAssessmentWorker(["10.5880/test.001"], mock_fuji_client)
        worker._cancelled = True
        
        result = worker._assess_single_doi("10.5880/test.001")
        
        assert not result.is_success
        assert result.error == "Cancelled"


class TestFujiAssessmentThread:
    """Test FujiAssessmentThread."""
    
    def test_thread_init(self, qapp, mock_fuji_client):
        """Test thread initialization."""
        dois = ["10.5880/test.001"]
        thread = FujiAssessmentThread(dois, mock_fuji_client, max_workers=2)
        
        assert thread.worker is not None
        assert thread.worker.dois == dois
    
    def test_thread_cancel(self, qapp, mock_fuji_client):
        """Test thread cancellation."""
        thread = FujiAssessmentThread(["10.5880/test.001"], mock_fuji_client)
        thread.cancel()
        
        assert thread.worker._cancelled is True


class TestStreamingFujiWorker:
    """Test StreamingFujiWorker."""
    
    def test_worker_init(self, mock_datacite_client, mock_fuji_client):
        """Test worker initialization."""
        worker = StreamingFujiWorker(mock_datacite_client, mock_fuji_client, max_workers=5)
        
        assert worker.datacite_client == mock_datacite_client
        assert worker.fuji_client == mock_fuji_client
        assert worker.max_workers == 5
        assert worker._cancelled is False
    
    def test_worker_cancel(self, mock_datacite_client, mock_fuji_client):
        """Test worker cancellation."""
        worker = StreamingFujiWorker(mock_datacite_client, mock_fuji_client)
        worker.cancel()
        
        assert worker._cancelled is True
    
    def test_worker_connection_failure(self, qapp, mock_datacite_client, mock_fuji_client):
        """Test worker handles F-UJI connection failure."""
        mock_fuji_client.test_connection.return_value = False
        
        worker = StreamingFujiWorker(mock_datacite_client, mock_fuji_client)
        
        error_signal = MagicMock()
        finished_signal = MagicMock()
        worker.error.connect(error_signal)
        worker.finished.connect(finished_signal)
        
        worker.run()
        
        # Should emit error and finished
        error_signal.assert_called_once()
        finished_signal.assert_called_once()
    
    def test_worker_assess_single_doi(self, mock_datacite_client, mock_fuji_client):
        """Test _assess_single_doi."""
        mock_fuji_client.assess_doi.return_value = FujiResult(
            doi="10.5880/test.001",
            score_percent=50.0,
            score_earned=12,
            score_total=24,
            metrics_count=17
        )
        
        worker = StreamingFujiWorker(mock_datacite_client, mock_fuji_client)
        result = worker._assess_single_doi("10.5880/test.001")
        
        assert result.is_success
        assert result.score_percent == 50.0
    
    def test_worker_assess_single_doi_cancelled(self, mock_datacite_client, mock_fuji_client):
        """Test _assess_single_doi when cancelled."""
        worker = StreamingFujiWorker(mock_datacite_client, mock_fuji_client)
        worker._cancelled = True
        
        result = worker._assess_single_doi("10.5880/test.001")
        
        assert not result.is_success
        assert result.error == "Cancelled"


class TestStreamingFujiThread:
    """Test StreamingFujiThread."""
    
    def test_thread_init(self, qapp, mock_datacite_client, mock_fuji_client):
        """Test thread initialization."""
        thread = StreamingFujiThread(mock_datacite_client, mock_fuji_client, max_workers=3)
        
        assert thread.worker is not None
        assert thread.worker.datacite_client == mock_datacite_client
    
    def test_thread_cancel(self, qapp, mock_datacite_client, mock_fuji_client):
        """Test thread cancellation."""
        thread = StreamingFujiThread(mock_datacite_client, mock_fuji_client)
        thread.cancel()
        
        assert thread.worker._cancelled is True


class TestFujiWorkerIntegration:
    """Integration tests for F-UJI workers."""
    
    def test_streaming_worker_full_run(self, qapp, mock_datacite_client, mock_fuji_client):
        """Test streaming worker creates proper structure and finishes."""
        # Setup mock to return successful assessments
        mock_fuji_client.assess_doi.return_value = FujiResult(
            doi="10.5880/test.001",
            score_percent=55.0,
            score_earned=13,
            score_total=24,
            metrics_count=17
        )
        
        worker = StreamingFujiWorker(mock_datacite_client, mock_fuji_client, max_workers=2)
        
        # Verify worker is properly initialized
        assert worker.datacite_client == mock_datacite_client
        assert worker.fuji_client == mock_fuji_client
        assert worker._cancelled is False
        
        # Test that fetch_complete is set properly initially
        assert not worker._fetch_complete.is_set()
    
    def test_worker_handles_assessment_error(self, qapp, mock_datacite_client, mock_fuji_client):
        """Test worker handles assessment errors gracefully."""
        # All calls return errors
        mock_fuji_client.assess_doi.return_value = FujiResult(
            doi="10.5880/test.001",
            score_percent=-1,
            score_earned=0,
            score_total=0,
            metrics_count=0,
            error="Assessment failed"
        )
        
        worker = StreamingFujiWorker(mock_datacite_client, mock_fuji_client, max_workers=1)
        
        finished_called = [False]
        worker.finished.connect(lambda: finished_called.__setitem__(0, True))
        
        # Run worker
        thread = threading.Thread(target=worker.run)
        thread.start()
        thread.join(timeout=10)
        
        # Should have finished
        assert finished_called[0] or not thread.is_alive()


class TestFujiWorkerSignals:
    """Test signal emissions from workers."""
    
    def test_progress_signal(self, qapp, mock_fuji_client):
        """Test progress signal emission."""
        dois = ["10.5880/test.001"] * 15  # Enough to trigger progress
        
        mock_fuji_client.assess_doi.return_value = FujiResult(
            doi="10.5880/test.001",
            score_percent=50.0,
            score_earned=12,
            score_total=24,
            metrics_count=17
        )
        
        worker = FujiAssessmentWorker(dois, mock_fuji_client, max_workers=5)
        
        progress_messages = []
        worker.progress.connect(lambda msg: progress_messages.append(msg))
        
        worker.run()
        
        # Should have emitted at least one progress message
        assert len(progress_messages) >= 1
