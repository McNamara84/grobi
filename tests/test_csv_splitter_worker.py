"""Tests for CSV splitter worker."""

import pytest
from pathlib import Path
from unittest.mock import Mock, patch
from PySide6.QtCore import QThread

from src.workers.csv_splitter_worker import CSVSplitterWorker
from src.utils.csv_splitter import CSVSplitError


@pytest.fixture
def qapp(qtbot):
    """Provide QApplication instance."""
    from PySide6.QtWidgets import QApplication
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


class TestCSVSplitterWorker:
    """Tests for CSVSplitterWorker."""
    
    def test_worker_initialization(self, tmp_path):
        """Test worker initialization."""
        input_file = tmp_path / "test.csv"
        output_dir = tmp_path / "output"
        
        worker = CSVSplitterWorker(input_file, output_dir, prefix_level=2)
        
        assert worker.input_file == input_file
        assert worker.output_dir == output_dir
        assert worker.prefix_level == 2
    
    def test_worker_successful_split(self, qapp, tmp_path):
        """Test successful CSV splitting."""
        # Create test CSV
        input_file = tmp_path / "test.csv"
        input_file.write_text(
            "DOI,Landing_Page_URL\n"
            "10.5880/gfz.2011.100,http://example.com/1\n"
            "10.5880/gfz.2011.200,http://example.com/2\n"
        )
        
        output_dir = tmp_path / "output"
        
        worker = CSVSplitterWorker(input_file, output_dir, prefix_level=2)
        
        # Mock signals
        progress_mock = Mock()
        finished_mock = Mock()
        error_mock = Mock()
        
        worker.progress.connect(progress_mock)
        worker.finished.connect(finished_mock)
        worker.error.connect(error_mock)
        
        # Run worker
        worker.run()
        
        # Verify success
        assert finished_mock.call_count == 1
        assert error_mock.call_count == 0
        
        # Check finished signal arguments
        total_rows, prefix_counts = finished_mock.call_args[0]
        assert total_rows == 2
        assert len(prefix_counts) == 1
    
    def test_worker_error_handling(self, qapp, tmp_path):
        """Test worker error handling for nonexistent file."""
        input_file = tmp_path / "nonexistent.csv"
        output_dir = tmp_path / "output"
        
        worker = CSVSplitterWorker(input_file, output_dir, prefix_level=2)
        
        # Mock signals
        progress_mock = Mock()
        finished_mock = Mock()
        error_mock = Mock()
        
        worker.progress.connect(progress_mock)
        worker.finished.connect(finished_mock)
        worker.error.connect(error_mock)
        
        # Run worker
        worker.run()
        
        # Verify error
        assert error_mock.call_count == 1
        assert finished_mock.call_count == 0
        
        # Check error message
        error_message = error_mock.call_args[0][0]
        assert "Eingabedatei nicht gefunden" in error_message
    
    def test_worker_progress_updates(self, qapp, tmp_path):
        """Test that worker emits progress updates."""
        # Create test CSV
        input_file = tmp_path / "test.csv"
        input_file.write_text(
            "DOI,Landing_Page_URL\n"
            "10.5880/gfz.2011.100,http://example.com/1\n"
        )
        
        output_dir = tmp_path / "output"
        
        worker = CSVSplitterWorker(input_file, output_dir, prefix_level=2)
        
        # Mock progress signal
        progress_mock = Mock()
        worker.progress.connect(progress_mock)
        
        # Run worker
        worker.run()
        
        # Verify progress updates were emitted
        assert progress_mock.call_count > 0
        
        # Check for specific progress messages
        progress_messages = [call[0][0] for call in progress_mock.call_args_list]
        assert any("Starte CSV-Splitting" in msg for msg in progress_messages)
        assert any("[OK]" in msg for msg in progress_messages)
    
    def test_worker_with_invalid_csv_format(self, qapp, tmp_path):
        """Test worker handling of invalid CSV format."""
        # Create invalid CSV (no DOI column)
        input_file = tmp_path / "test.csv"
        input_file.write_text(
            "URL,Title\n"
            "http://example.com/1,Test\n"
        )
        
        output_dir = tmp_path / "output"
        
        worker = CSVSplitterWorker(input_file, output_dir, prefix_level=2)
        
        # Mock signals
        error_mock = Mock()
        finished_mock = Mock()
        
        worker.error.connect(error_mock)
        worker.finished.connect(finished_mock)
        
        # Run worker
        worker.run()
        
        # Verify error
        assert error_mock.call_count == 1
        assert finished_mock.call_count == 0
        
        error_message = error_mock.call_args[0][0]
        assert "DOI" in error_message
    
    def test_worker_with_different_prefix_levels(self, qapp, tmp_path):
        """Test worker with different prefix levels."""
        # Create test CSV
        input_file = tmp_path / "test.csv"
        input_file.write_text(
            "DOI,Landing_Page_URL\n"
            "10.5880/gfz.2011.100,http://example.com/1\n"
            "10.5880/gfz.2012.100,http://example.com/2\n"
        )
        
        output_dir = tmp_path / "output"
        
        # Test with level 1 (should group all 10.5880 together)
        worker = CSVSplitterWorker(input_file, output_dir, prefix_level=1)
        
        finished_mock = Mock()
        worker.finished.connect(finished_mock)
        
        worker.run()
        
        total_rows, prefix_counts = finished_mock.call_args[0]
        assert total_rows == 2
        assert len(prefix_counts) == 1  # All grouped under 10.5880
    
    def test_worker_exception_handling(self, qapp, tmp_path):
        """Test worker handling of unexpected exceptions."""
        input_file = tmp_path / "test.csv"
        input_file.write_text("DOI,URL\n10.5880/test,http://example.com\n")
        output_dir = tmp_path / "output"
        
        worker = CSVSplitterWorker(input_file, output_dir, prefix_level=2)
        
        error_mock = Mock()
        worker.error.connect(error_mock)
        
        # Mock split_csv_by_doi_prefix to raise unexpected exception
        with patch('src.workers.csv_splitter_worker.split_csv_by_doi_prefix') as mock_split:
            mock_split.side_effect = RuntimeError("Unexpected error")
            
            worker.run()
            
            # Should emit error signal
            assert error_mock.call_count == 1
            error_message = error_mock.call_args[0][0]
            assert "Unerwarteter Fehler" in error_message
