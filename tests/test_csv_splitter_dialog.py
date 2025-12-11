"""Tests for CSV splitter dialog."""

import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock
from PySide6.QtCore import QThread

from src.ui.csv_splitter_dialog import CSVSplitterDialog


@pytest.fixture
def dialog(qtbot):
    """Create dialog instance and ensure cleanup after test."""
    dlg = CSVSplitterDialog()
    qtbot.addWidget(dlg)
    
    yield dlg
    
    # Cleanup: Ensure thread is None to prevent closeEvent blocking during teardown
    dlg.thread = None
    dlg.worker = None


class TestCSVSplitterDialog:
    """Tests for CSVSplitterDialog."""
    
    def test_dialog_initialization(self, dialog):
        """Test dialog is properly initialized."""
        assert dialog.windowTitle() == "CSV-Datei aufsplitten"
        assert dialog.isModal()
        assert dialog.worker is None
        assert dialog.thread is None
        assert dialog.input_file is None
        assert dialog.output_dir is None
    
    def test_initial_ui_state(self, dialog):
        """Test initial UI state."""
        assert dialog.start_button.isEnabled() is False
        assert dialog.output_button.isEnabled() is False
        assert dialog.browse_button.isEnabled() is True
        assert dialog.close_button.isEnabled() is True
        assert dialog.progress_bar.isVisible() is False
        assert dialog.prefix_spinbox.value() == 2
    
    def test_browse_file_selection(self, dialog, qtbot, tmp_path):
        """Test file selection via browse button."""
        test_file = tmp_path / "test.csv"
        test_file.write_text("DOI,URL\n10.5880/test,http://example.com\n")
        
        # Mock file dialog
        with patch('src.ui.csv_splitter_dialog.QFileDialog.getOpenFileName') as mock_dialog:
            mock_dialog.return_value = (str(test_file), "")
            
            dialog.browse_button.click()
            
            # Verify file is selected
            assert dialog.input_file == test_file
            assert dialog.file_label.text() == "test.csv"
            assert dialog.start_button.isEnabled() is True
            assert dialog.output_button.isEnabled() is True
            
            # Verify default output directory is set
            assert dialog.output_dir == test_file.parent / "split_output"
    
    def test_browse_file_cancelled(self, dialog, qtbot):
        """Test cancelling file selection."""
        with patch('src.ui.csv_splitter_dialog.QFileDialog.getOpenFileName') as mock_dialog:
            mock_dialog.return_value = ("", "")  # User cancelled
            
            dialog.browse_button.click()
            
            # State should remain unchanged
            assert dialog.input_file is None
            assert dialog.start_button.isEnabled() is False
    
    def test_browse_output_directory(self, dialog, qtbot, tmp_path):
        """Test output directory selection."""
        test_file = tmp_path / "test.csv"
        test_file.write_text("DOI,URL\n")
        
        # First select a file
        dialog.input_file = test_file
        dialog.output_dir = tmp_path / "default_output"
        dialog.output_button.setEnabled(True)
        
        # Then select custom output directory
        custom_output = tmp_path / "custom_output"
        
        with patch('src.ui.csv_splitter_dialog.QFileDialog.getExistingDirectory') as mock_dialog:
            mock_dialog.return_value = str(custom_output)
            
            dialog.output_button.click()
            
            assert dialog.output_dir == custom_output
    
    def test_prefix_level_spinbox(self, dialog):
        """Test prefix level spinbox configuration."""
        spinbox = dialog.prefix_spinbox
        
        assert spinbox.minimum() == 1
        assert spinbox.maximum() == 4
        assert spinbox.value() == 2
        
        # Test changing value
        spinbox.setValue(3)
        assert spinbox.value() == 3
    
    def test_start_splitting_no_file_warning(self, dialog, qtbot):
        """Test warning when starting without file selection."""
        with patch('src.ui.csv_splitter_dialog.QMessageBox.warning') as mock_warning:
            dialog._start_splitting()
            
            assert mock_warning.call_count == 1
            args = mock_warning.call_args[0]
            assert "gültige CSV-Datei" in args[2]
    
    def test_start_splitting_creates_worker(self, dialog, qtbot, tmp_path):
        """Test that starting splitting creates worker and thread."""
        test_file = tmp_path / "test.csv"
        test_file.write_text("DOI,URL\n10.5880/test,http://example.com\n")
        
        dialog.input_file = test_file
        dialog.output_dir = tmp_path / "output"
        dialog.start_button.setEnabled(True)
        
        # Mock QThread to prevent actual threading
        with patch('src.ui.csv_splitter_dialog.QThread') as mock_thread_class:
            mock_thread = MagicMock()
            mock_thread.isRunning.return_value = False  # Thread is not running
            mock_thread_class.return_value = mock_thread
            
            with patch('src.ui.csv_splitter_dialog.CSVSplitterWorker') as mock_worker_class:
                mock_worker = MagicMock()
                mock_worker_class.return_value = mock_worker
                
                dialog._start_splitting()
                
                # Verify worker was created with correct parameters
                assert mock_worker_class.call_count == 1
                call_args = mock_worker_class.call_args[0]
                assert call_args[0] == test_file
                assert call_args[1] == tmp_path / "output"
                assert call_args[2] == 2  # default prefix level
                
                # Verify thread was created and started
                assert mock_thread.start.call_count == 1
    
    def test_ui_disabled_during_processing(self, dialog, qtbot, tmp_path):
        """Test that UI controls are disabled during processing."""
        test_file = tmp_path / "test.csv"
        test_file.write_text("DOI,URL\n10.5880/test,http://example.com\n")
        
        dialog.input_file = test_file
        dialog.output_dir = tmp_path / "output"
        dialog.start_button.setEnabled(True)
        dialog.output_button.setEnabled(True)
        
        # Mock the thread start to prevent actual execution
        with patch.object(QThread, 'start'):
            with patch.object(dialog.progress_bar, 'setVisible') as mock_set_visible:
                dialog._start_splitting()
                
                # Verify controls are disabled
                assert dialog.start_button.isEnabled() is False
                assert dialog.browse_button.isEnabled() is False
                assert dialog.output_button.isEnabled() is False
                assert dialog.prefix_spinbox.isEnabled() is False
                # Verify setVisible(True) was called
                mock_set_visible.assert_called_with(True)
    
    def test_on_progress_updates_log(self, dialog, qtbot):
        """Test that progress updates are logged."""
        initial_text = dialog.log_text.toPlainText()
        
        dialog._on_progress("Test progress message")
        
        updated_text = dialog.log_text.toPlainText()
        assert "Test progress message" in updated_text
        assert len(updated_text) > len(initial_text)
    
    def test_on_finished_success(self, dialog, qtbot):
        """Test successful completion handling."""
        dialog.input_file = Path("test.csv")
        dialog.output_dir = Path("output")
        
        prefix_counts = {
            "10.5880/gfz.2011": 10,
            "10.1594/gfz.geofon": 5
        }
        
        with patch('src.ui.csv_splitter_dialog.QMessageBox.information') as mock_info:
            dialog._on_finished(15, prefix_counts)
            
            # Verify success message
            assert mock_info.call_count == 1
            args = mock_info.call_args[0]
            assert "erfolgreich" in args[2].lower()
            assert "15" in args[2]
            assert "2" in args[2]  # 2 files
            
            # Verify log contains summary
            log_text = dialog.log_text.toPlainText()
            assert "✅ ERFOLGREICH" in log_text
            assert "10.5880/gfz.2011: 10" in log_text
            assert "10.1594/gfz.geofon: 5" in log_text
            
            # Verify controls are re-enabled
            assert dialog.start_button.isEnabled() is True
            assert dialog.browse_button.isEnabled() is True
    
    def test_on_error_handling(self, dialog, qtbot):
        """Test error handling."""
        error_message = "Test error message"
        
        with patch('src.ui.csv_splitter_dialog.QMessageBox.critical') as mock_critical:
            dialog._on_error(error_message)
            
            # Verify error dialog
            assert mock_critical.call_count == 1
            args = mock_critical.call_args[0]
            assert "Fehler" in args[1]
            assert error_message in args[2]
            
            # Verify log contains error
            log_text = dialog.log_text.toPlainText()
            assert "❌ FEHLER" in log_text
            assert error_message in log_text
            
            # Verify progress bar is hidden
            assert dialog.progress_bar.isVisible() is False
            
            # Verify controls are re-enabled
            assert dialog.start_button.isEnabled() is True
    
    def test_close_event_while_processing(self, dialog, qtbot):
        """Test that dialog cannot be closed while processing."""
        # Simulate processing state
        dialog.thread = MagicMock()
        dialog.thread.isRunning.return_value = True
        
        # Create a mock event
        from PySide6.QtGui import QCloseEvent
        event = QCloseEvent()
        
        with patch('src.ui.csv_splitter_dialog.QMessageBox.warning') as mock_warning:
            dialog.closeEvent(event)
            
            # Verify close was prevented
            assert event.isAccepted() is False
            assert mock_warning.call_count == 1
        
        # Important: Reset thread to None to prevent pytest-qt teardown hanging
        dialog.thread = None
    
    def test_close_event_when_idle(self, dialog, qtbot):
        """Test that dialog can be closed when idle."""
        dialog.thread = None
        
        from PySide6.QtGui import QCloseEvent
        event = QCloseEvent()
        
        dialog.closeEvent(event)
        
        # Verify close was allowed
        assert event.isAccepted() is True
    
    def test_log_method_autoscrolls(self, dialog, qtbot):
        """Test that log method auto-scrolls to bottom."""
        # Add many log entries to force scrolling
        for i in range(100):
            dialog._log(f"Line {i}")
        
        # Verify scrollbar is at maximum (bottom) position
        scrollbar = dialog.log_text.verticalScrollBar()
        assert scrollbar.value() == scrollbar.maximum()
    
    def test_cleanup_thread(self, dialog, qtbot):
        """Test thread cleanup."""
        # Create mock thread and worker
        mock_thread = MagicMock()
        mock_worker = MagicMock()
        dialog.thread = mock_thread
        dialog.worker = mock_worker
        
        dialog._cleanup_thread()
        
        # Verify deleteLater was called on the mocks before they were cleared
        assert mock_thread.deleteLater.call_count == 1
        assert mock_worker.deleteLater.call_count == 1
        
        # Verify references are cleared
        assert dialog.thread is None
        assert dialog.worker is None
    
    def test_reset_controls(self, dialog):
        """Test control reset after processing."""
        # Disable controls (simulating processing state)
        dialog.start_button.setEnabled(False)
        dialog.browse_button.setEnabled(False)
        dialog.output_button.setEnabled(False)
        dialog.prefix_spinbox.setEnabled(False)
        
        # Reset
        dialog._reset_controls()
        
        # Verify controls are re-enabled
        assert dialog.start_button.isEnabled() is True
        assert dialog.browse_button.isEnabled() is True
        assert dialog.output_button.isEnabled() is True
        assert dialog.prefix_spinbox.isEnabled() is True
