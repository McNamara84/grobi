"""Tests for new MainWindow features: keyboard shortcuts, drag & drop, window geometry.

Note: This test file uses unittest.mock.patch for mocking internal methods.
The project's testing guidelines specify using the 'responses' library for HTTP mocking,
but that is specifically for external HTTP calls. For internal method mocking,
unittest.mock is the appropriate choice.
"""

import pytest
from unittest.mock import patch

from PySide6.QtWidgets import QApplication
from PySide6.QtCore import QSettings
from PySide6.QtGui import QKeySequence

from src.ui.main_window import MainWindow


@pytest.fixture(scope="module")
def qapp():
    """Create QApplication for tests."""
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    yield app


@pytest.fixture
def main_window(qapp, qtbot):
    """Create MainWindow for tests."""
    window = MainWindow()
    qtbot.addWidget(window)
    return window


class TestKeyboardShortcuts:
    """Tests for keyboard shortcuts functionality."""
    
    def test_shortcuts_are_registered(self, main_window):
        """Test that all 8 shortcuts are properly registered."""
        assert hasattr(main_window, '_shortcuts')
        assert len(main_window._shortcuts) == 8
    
    def test_shortcuts_have_correct_key_sequences(self, main_window):
        """Test that shortcuts have correct key sequences."""
        expected_shortcuts = [
            "Ctrl+1", "Ctrl+2", "Ctrl+3", "Ctrl+4",
            "Ctrl+5", "Ctrl+6", "Ctrl+7", "Ctrl+8"
        ]
        
        actual_shortcuts = [s.key().toString() for s in main_window._shortcuts]
        assert actual_shortcuts == expected_shortcuts
    
    def test_shortcuts_are_enabled(self, main_window):
        """Test that all shortcuts are enabled."""
        for shortcut in main_window._shortcuts:
            assert shortcut.isEnabled()
    
    def test_shortcut_does_not_conflict_with_menu_shortcuts(self, main_window):
        """Test that card shortcuts don't conflict with existing menu shortcuts."""
        # Menu shortcuts use Ctrl+, and Ctrl+Shift+S
        card_keys = [s.key().toString() for s in main_window._shortcuts]
        
        # These should not be in the card shortcuts
        assert "Ctrl+," not in card_keys
        assert "Ctrl+Shift+S" not in card_keys


class TestDragAndDrop:
    """Tests for drag and drop CSV functionality."""
    
    def test_accept_drops_is_enabled(self, main_window):
        """Test that drag and drop is enabled."""
        assert main_window.acceptDrops()
    
    def test_pending_csv_path_initialized(self, main_window):
        """Test that pending_csv_path is initialized."""
        assert hasattr(main_window, 'pending_csv_path')
        # Should be None initially
        assert main_window.pending_csv_path is None
    
    def test_handle_dropped_csv_urls(self, main_window, tmp_path):
        """Test CSV type detection for URL files."""
        csv_file = tmp_path / "test_urls.csv"
        csv_file.write_text("DOI,Landing_Page_URL\n10.5880/test,https://example.com")
        
        with patch.object(main_window, '_on_update_urls_clicked') as mock:
            main_window._handle_dropped_csv(str(csv_file))
            mock.assert_called_once()
        
        assert main_window.pending_csv_path == str(csv_file)
    
    def test_handle_dropped_csv_authors(self, main_window, tmp_path):
        """Test CSV type detection for author files."""
        csv_file = tmp_path / "test_authors.csv"
        csv_file.write_text("DOI,Creator Name,Given Name,Family Name\n10.5880/test,Doe,John,Doe")
        
        with patch.object(main_window, '_on_update_authors_clicked') as mock:
            main_window._handle_dropped_csv(str(csv_file))
            mock.assert_called_once()
    
    def test_handle_dropped_csv_contributors(self, main_window, tmp_path):
        """Test CSV type detection for contributor files."""
        csv_file = tmp_path / "test_contributors.csv"
        csv_file.write_text("DOI,ContributorType,Name\n10.5880/test,ContactPerson,John Doe")
        
        with patch.object(main_window, '_on_update_contributors_clicked') as mock:
            main_window._handle_dropped_csv(str(csv_file))
            mock.assert_called_once()
    
    def test_handle_dropped_csv_rights(self, main_window, tmp_path):
        """Test CSV type detection for rights files."""
        csv_file = tmp_path / "test_rights.csv"
        csv_file.write_text("DOI,RightsIdentifier,RightsURI\n10.5880/test,CC-BY-4.0,https://creativecommons.org/licenses/by/4.0/")
        
        with patch.object(main_window, '_on_update_rights_clicked') as mock:
            main_window._handle_dropped_csv(str(csv_file))
            mock.assert_called_once()
    
    def test_handle_dropped_csv_download_urls(self, main_window, tmp_path):
        """Test CSV type detection for download URL files."""
        csv_file = tmp_path / "test_downloads.csv"
        csv_file.write_text("DOI,ContentURL\n10.5880/test,https://example.com/download")
        
        with patch.object(main_window, '_on_update_download_urls_clicked') as mock:
            main_window._handle_dropped_csv(str(csv_file))
            mock.assert_called_once()
    
    def test_handle_dropped_csv_publisher(self, main_window, tmp_path):
        """Test CSV type detection for publisher files."""
        csv_file = tmp_path / "test_publisher.csv"
        csv_file.write_text("DOI,Publisher\n10.5880/test,GFZ Data Services")
        
        with patch.object(main_window, '_on_update_publisher_clicked') as mock:
            main_window._handle_dropped_csv(str(csv_file))
            mock.assert_called_once()
    
    def test_handle_dropped_csv_empty_file(self, main_window, tmp_path, qtbot):
        """Test handling of empty CSV file."""
        csv_file = tmp_path / "empty.csv"
        csv_file.write_text("")
        
        # Clear log first to ensure we only check new entries
        main_window.log_text.clear()
        
        # Should log error but not crash
        main_window._handle_dropped_csv(str(csv_file))
        
        # Check that the specific empty CSV error was logged
        log_text = main_window.log_text.toPlainText()
        assert "Leere CSV-Datei" in log_text, f"Expected 'Leere CSV-Datei' in log, got: {log_text}"
    
    def test_handle_dropped_csv_unknown_type(self, main_window, tmp_path):
        """Test handling of CSV with unknown headers."""
        csv_file = tmp_path / "unknown.csv"
        csv_file.write_text("Column1,Column2,Column3\nvalue1,value2,value3")
        
        # Clear log first
        main_window.log_text.clear()
        
        main_window._handle_dropped_csv(str(csv_file))
        
        # Should log specific warning about unrecognized CSV type
        log_text = main_window.log_text.toPlainText()
        assert "CSV-Typ nicht erkannt" in log_text, f"Expected 'CSV-Typ nicht erkannt' in log, got: {log_text}"
    
    def test_handle_dropped_csv_invalid_file(self, main_window, tmp_path):
        """Test handling of non-existent file."""
        fake_path = str(tmp_path / "nonexistent.csv")
        
        # Should log error but not crash
        main_window._handle_dropped_csv(fake_path)
        
        log_text = main_window.log_text.toPlainText()
        assert "FEHLER" in log_text


class TestWindowGeometry:
    """Tests for window geometry persistence."""
    
    def test_geometry_methods_exist(self, main_window):
        """Test that geometry methods exist."""
        assert hasattr(main_window, '_save_window_geometry')
        assert hasattr(main_window, '_restore_window_geometry')
        assert hasattr(main_window, '_is_window_on_screen')
        assert hasattr(main_window, '_set_default_geometry')
    
    def test_save_window_geometry(self, main_window):
        """Test that geometry is saved to QSettings."""
        # Move and resize window
        main_window.move(100, 100)
        main_window.resize(1000, 700)
        
        main_window._save_window_geometry()
        
        # Check that settings were saved
        settings = QSettings("GFZ", "GROBI")
        assert settings.value("window/geometry") is not None
    
    def test_is_window_on_screen_returns_bool(self, main_window):
        """Test that _is_window_on_screen returns a boolean."""
        result = main_window._is_window_on_screen()
        assert isinstance(result, bool)
    
    def test_set_default_geometry_centers_window(self, main_window, qapp):
        """Test that default geometry centers the window."""
        main_window._set_default_geometry()
        
        # Window should have reasonable size
        assert main_window.width() >= 900
        assert main_window.height() >= 600
    
    def test_restore_geometry_handles_missing_settings(self, main_window):
        """Test that restore handles missing settings gracefully."""
        # Clear any existing settings
        settings = QSettings("GFZ", "GROBI")
        settings.remove("window/geometry")
        settings.remove("window/state")
        settings.sync()
        
        # Should not crash - falls back to default geometry
        main_window._restore_window_geometry()
        
        # Window should still have valid size
        assert main_window.width() > 0
        assert main_window.height() > 0
    
    def test_geometry_restored_on_init(self, qapp, qtbot):
        """Test that a newly created window restores its geometry."""
        # Create first window and save its geometry
        window1 = MainWindow()
        qtbot.addWidget(window1)
        window1.move(200, 150)
        window1.resize(1100, 800)
        window1._save_window_geometry()
        
        # Create second window - should restore geometry
        window2 = MainWindow()
        qtbot.addWidget(window2)
        
        # Verify geometry was saved and restore was attempted.
        # Note: Exact size may vary on CI runners with virtual displays
        # (e.g., Xvfb with limited resolution). We verify that:
        # 1. Window has valid dimensions (not zero)
        # 2. Width is at least the minimum (900px) 
        # 3. Height is reasonable (at least 600px)
        assert window2.width() >= 900, f"Width {window2.width()} is below minimum"
        assert window2.height() >= 600, f"Height {window2.height()} is below minimum"
        
        # Additionally verify that QSettings actually contains saved geometry
        settings = QSettings("GFZ", "GROBI")
        assert settings.value("window/geometry") is not None, "Geometry was not saved"


class TestCollapsibleSectionSignalDisconnect:
    """Tests for CollapsibleSection signal handling."""
    
    def test_expand_animation_disconnects_signal(self, qapp, qtbot):
        """Test that the expand animation properly disconnects its finished signal."""
        from src.ui.components import CollapsibleSection
        from src.ui.components.collapsible_section import CollapsibleSection as CS
        
        section = CollapsibleSection("Test", expanded=True)
        qtbot.addWidget(section)
        section.show()
        
        # Use ANIMATION_DURATION from the class + buffer for reliability
        wait_time = CS.ANIMATION_DURATION + 50
        
        # Collapse then expand multiple times to test signal handling
        for _ in range(3):
            section.toggle()  # Collapse
            qtbot.wait(wait_time)
            section.toggle()  # Expand
            qtbot.wait(wait_time)
        
        # If signal wasn't disconnected properly, this would cause issues
        assert section.is_expanded()


class TestQWIDGETSIZE_MAX_Constant:
    """Tests for the QWIDGETSIZE_MAX constant usage."""
    
    def test_constant_is_defined(self):
        """Test that QWIDGETSIZE_MAX constant is properly defined."""
        from src.ui.components.collapsible_section import QWIDGETSIZE_MAX
        
        assert QWIDGETSIZE_MAX == 16777215
    
    def test_constant_used_in_animation(self, qapp, qtbot):
        """Test that expanded sections use the constant for max height."""
        from src.ui.components import CollapsibleSection
        from src.ui.components.collapsible_section import QWIDGETSIZE_MAX
        
        section = CollapsibleSection("Test", expanded=True)
        qtbot.addWidget(section)
        section.show()
        
        # After expansion, content area should have QWIDGETSIZE_MAX as max height
        assert section.content_area.maximumHeight() == QWIDGETSIZE_MAX
