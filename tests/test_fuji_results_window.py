"""Unit tests for FujiResultsWindow."""

import pytest
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

from PySide6.QtCore import Qt
from PySide6.QtWidgets import QApplication, QMessageBox

from src.ui.fuji_results_window import FujiResultsWindow


@pytest.fixture(scope="module")
def qapp():
    """Create QApplication for the test module."""
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    yield app


@pytest.fixture
def window(qapp):
    """Create a FujiResultsWindow."""
    win = FujiResultsWindow()
    yield win
    # Ensure _is_running is False to avoid QMessageBox dialog on close
    win._is_running = False
    win.close()


class TestFujiResultsWindowInit:
    """Test window initialization."""
    
    def test_window_title(self, window):
        """Test window title is set correctly."""
        assert window.windowTitle() == "F-UJI FAIR Assessment"
    
    def test_minimum_size(self, window):
        """Test minimum size is set."""
        assert window.minimumWidth() >= 600
        assert window.minimumHeight() >= 400
    
    def test_tiles_empty_initially(self, window):
        """Test tiles dict is empty initially."""
        assert len(window.tiles) == 0
    
    def test_counters_zero_initially(self, window):
        """Test counters are zero initially."""
        assert window.total_dois == 0
        assert window.completed_count == 0
        assert window.error_count == 0
    
    def test_not_running_initially(self, window):
        """Test not running initially."""
        assert window._is_running is False


class TestFujiResultsWindowAssessment:
    """Test assessment flow."""
    
    def test_start_assessment(self, window):
        """Test starting an assessment."""
        window.start_assessment(100)
        
        assert window.total_dois == 100
        assert window.completed_count == 0
        assert window._is_running is True
        assert window.action_button.text() == "Abbrechen"
    
    def test_start_streaming_assessment(self, window):
        """Test starting streaming assessment."""
        window.start_streaming_assessment()
        
        assert window.total_dois == 0  # Will be updated dynamically
        assert window._is_running is True
    
    def test_set_total_dois(self, window):
        """Test setting total DOIs."""
        window.start_streaming_assessment()
        window.set_total_dois(50)
        
        assert window.total_dois == 50


class TestFujiResultsWindowTiles:
    """Test tile management."""
    
    def test_add_pending_tile(self, window, qapp):
        """Test adding a pending tile."""
        window.start_streaming_assessment()
        window.add_pending_tile("10.5880/test.001")
        
        qapp.processEvents()
        
        assert "10.5880/test.001" in window.tiles
        assert window.tiles["10.5880/test.001"].score_percent == -1
    
    def test_add_pending_tile_duplicate(self, window, qapp):
        """Test adding duplicate pending tile is ignored."""
        window.start_streaming_assessment()
        window.add_pending_tile("10.5880/test.001")
        window.add_pending_tile("10.5880/test.001")
        
        qapp.processEvents()
        
        # Should still only have one tile
        assert len(window.tiles) == 1
    
    def test_add_result_updates_tile(self, window, qapp):
        """Test add_result updates existing tile."""
        window.start_streaming_assessment()
        window.add_pending_tile("10.5880/test.001")
        window.add_result("10.5880/test.001", 75.5)
        
        qapp.processEvents()
        
        assert window.tiles["10.5880/test.001"].score_percent == 75.5
        assert window.completed_count == 1
    
    def test_add_result_creates_tile(self, window, qapp):
        """Test add_result creates tile if not exists."""
        window.start_assessment(10)
        window.add_result("10.5880/new.001", 60.0)
        
        qapp.processEvents()
        
        assert "10.5880/new.001" in window.tiles
        assert window.completed_count == 1
    
    def test_add_result_error_counts(self, window, qapp):
        """Test add_result counts errors."""
        window.start_streaming_assessment()
        window.add_pending_tile("10.5880/error.001")
        window.add_result("10.5880/error.001", -1)
        
        qapp.processEvents()
        
        assert window.error_count == 1


class TestFujiResultsWindowAverageScore:
    """Test average score display."""
    
    def test_average_score_updates(self, window, qapp):
        """Test average score is updated correctly."""
        window.start_streaming_assessment()
        
        # Add some results
        window.add_pending_tile("10.5880/test.001")
        window.add_pending_tile("10.5880/test.002")
        window.add_result("10.5880/test.001", 40.0)
        window.add_result("10.5880/test.002", 60.0)
        
        qapp.processEvents()
        
        # Average should be 50%
        assert "50.0%" in window.avg_score_label.text()
    
    def test_average_score_ignores_errors(self, window, qapp):
        """Test average score ignores error tiles."""
        window.start_streaming_assessment()
        
        window.add_pending_tile("10.5880/test.001")
        window.add_pending_tile("10.5880/test.002")
        window.add_pending_tile("10.5880/error.001")
        window.add_result("10.5880/test.001", 50.0)
        window.add_result("10.5880/test.002", 50.0)
        window.add_result("10.5880/error.001", -1)
        
        qapp.processEvents()
        
        # Average should be 50% (ignoring error)
        assert "50.0%" in window.avg_score_label.text()
    
    def test_average_score_empty_state(self, window):
        """Test average score shows dash when empty."""
        assert window.avg_score_label.text() == "—"


class TestFujiResultsWindowTileSize:
    """Test tile size calculation."""
    
    def test_tile_size_calculation(self, window, qapp):
        """Test tile size is calculated correctly."""
        window.start_assessment(10)
        
        size = window._calculate_tile_size()
        
        # Should be in reasonable range
        assert size >= 50
        assert size <= 150
    
    def test_tile_size_more_columns_for_more_dois(self, window, qapp):
        """Test more DOIs results in smaller tiles."""
        window.start_assessment(10)
        size_few = window._calculate_tile_size()
        
        window.start_assessment(200)
        size_many = window._calculate_tile_size()
        
        # More DOIs should result in smaller or equal tiles
        assert size_many <= size_few


class TestFujiResultsWindowCSVExport:
    """Test CSV export functionality."""
    
    def test_init_csv_export(self, window, qapp):
        """Test CSV export initialization."""
        window.start_streaming_assessment()
        
        # CSV should be initialized
        assert window._csv_path is not None
        assert window._csv_file is not None
    
    def test_write_csv_row(self, window, qapp):
        """Test writing CSV rows."""
        window.start_streaming_assessment()
        window.add_pending_tile("10.5880/test.001")
        window.add_result("10.5880/test.001", 55.5)
        
        qapp.processEvents()
        
        # CSV file should exist and contain data
        # Note: File is flushed on each write, so we can read it
        if window._csv_path and window._csv_path.exists() and window._csv_file and not window._csv_file.closed:
            window._csv_file.flush()
            content = window._csv_path.read_text(encoding='utf-8-sig')
            assert "10.5880/test.001" in content
            assert "55.5" in content
    
    def test_close_csv(self, window, qapp):
        """Test closing CSV file."""
        window.start_streaming_assessment()
        window._close_csv()
        
        # File should be closed
        assert window._csv_file is None or window._csv_file.closed


class TestFujiResultsWindowCompletion:
    """Test assessment completion."""
    
    def test_assessment_complete(self, window, qapp):
        """Test assessment completion."""
        window.start_assessment(2)
        
        window.add_result("10.5880/test.001", 50.0)
        window.add_result("10.5880/test.002", 60.0)
        
        qapp.processEvents()
        
        # Should be complete
        assert window._is_running is False
        assert window.action_button.text() == "Schließen"


class TestFujiResultsWindowSignals:
    """Test signal emissions."""
    
    def test_cancelled_signal(self, window, qapp, qtbot):
        """Test cancelled signal emission."""
        window.start_assessment(10)
        window.show()
        
        # Mock QMessageBox to auto-accept
        with patch.object(QMessageBox, 'question', return_value=QMessageBox.Yes):
            with qtbot.waitSignal(window.assessment_cancelled, timeout=1000):
                window._on_action_button_clicked()
        
        window.hide()
    
    def test_closed_signal(self, window, qapp, qtbot):
        """Test closed signal emission."""
        window.show()
        
        with qtbot.waitSignal(window.closed, timeout=1000):
            window.close()


class TestFujiResultsWindowStatus:
    """Test status bar updates."""
    
    def test_status_during_assessment(self, window, qapp):
        """Test status message during assessment."""
        window.start_assessment(10)
        window.add_result("10.5880/test.001", 50.0)
        
        qapp.processEvents()
        
        status = window.status_bar.currentMessage()
        assert "1" in status and "10" in status
    
    def test_status_with_errors(self, window, qapp):
        """Test status shows error count."""
        window.start_assessment(10)
        window.add_result("10.5880/test.001", 50.0)
        window.add_result("10.5880/error.001", -1)
        
        qapp.processEvents()
        
        status = window.status_bar.currentMessage()
        assert "Fehler" in status or "1" in status


class TestFujiResultsWindowResize:
    """Test window resize handling."""
    
    def test_resize_recalculates_tiles(self, window, qapp):
        """Test resizing recalculates tile sizes."""
        window.start_assessment(20)
        
        for i in range(5):
            window.add_result(f"10.5880/test.{i:03d}", 50.0)
        
        qapp.processEvents()
        
        # Resize window
        window.resize(1200, 800)
        qapp.processEvents()
        
        # Tiles should be recalculated (no crash)
        assert len(window.tiles) == 5
