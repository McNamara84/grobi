"""Tests for ThemeManager."""

import pytest
from PySide6.QtWidgets import QApplication
from PySide6.QtCore import QSettings

from src.ui.theme_manager import ThemeManager, Theme


@pytest.fixture(scope="module")
def qapp():
    """Create QApplication instance for GUI tests."""
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    yield app


@pytest.fixture
def theme_manager(qapp):
    """Create a ThemeManager instance for testing."""
    # Clear settings before each test
    settings = QSettings("GFZ", "GROBI")
    settings.clear()
    
    manager = ThemeManager()
    yield manager
    
    # Clear settings after test
    settings.clear()


class TestThemeManagerInit:
    """Test ThemeManager initialization."""
    
    def test_manager_creation(self, theme_manager):
        """Test that theme manager is created successfully."""
        assert theme_manager is not None
        assert isinstance(theme_manager, ThemeManager)
    
    def test_default_theme_is_auto(self, theme_manager):
        """Test that default theme is AUTO."""
        assert theme_manager.get_current_theme() == Theme.AUTO
    
    def test_theme_enum_values(self):
        """Test Theme enum values."""
        assert Theme.LIGHT.value == "light"
        assert Theme.DARK.value == "dark"
        assert Theme.AUTO.value == "auto"


class TestThemeManagerSetTheme:
    """Test theme setting functionality."""
    
    def test_set_theme_dark(self, theme_manager, qtbot):
        """Test setting dark theme."""
        # Connect signal spy
        with qtbot.waitSignal(theme_manager.theme_changed, timeout=1000) as blocker:
            theme_manager.set_theme(Theme.DARK)
        
        assert theme_manager.get_current_theme() == Theme.DARK
        assert blocker.args[0] == Theme.DARK
    
    def test_set_theme_light(self, theme_manager, qtbot):
        """Test setting light theme."""
        # First set to dark
        theme_manager.set_theme(Theme.DARK)
        
        # Then set to light
        with qtbot.waitSignal(theme_manager.theme_changed, timeout=1000) as blocker:
            theme_manager.set_theme(Theme.LIGHT)
        
        assert theme_manager.get_current_theme() == Theme.LIGHT
        assert blocker.args[0] == Theme.LIGHT
    
    def test_set_same_theme_no_signal(self, theme_manager, qtbot):
        """Test that setting the same theme doesn't emit signal."""
        # Set to auto (already auto by default)
        signal_emitted = False
        
        def on_theme_changed():
            nonlocal signal_emitted
            signal_emitted = True
        
        theme_manager.theme_changed.connect(on_theme_changed)
        theme_manager.set_theme(Theme.AUTO)
        
        # Process events
        QApplication.processEvents()
        
        assert not signal_emitted


class TestThemeManagerToggle:
    """Test theme toggle functionality."""
    
    def test_toggle_from_auto_to_light(self, theme_manager, qtbot):
        """Test toggling from auto to light."""
        assert theme_manager.get_current_theme() == Theme.AUTO
        
        with qtbot.waitSignal(theme_manager.theme_changed, timeout=1000):
            theme_manager.toggle_theme()
        
        assert theme_manager.get_current_theme() == Theme.LIGHT
    
    def test_toggle_from_light_to_dark(self, theme_manager, qtbot):
        """Test toggling from light to dark."""
        theme_manager.set_theme(Theme.LIGHT)
        
        with qtbot.waitSignal(theme_manager.theme_changed, timeout=1000):
            theme_manager.toggle_theme()
        
        assert theme_manager.get_current_theme() == Theme.DARK
    
    def test_toggle_from_dark_to_auto(self, theme_manager, qtbot):
        """Test toggling from dark to auto."""
        theme_manager.set_theme(Theme.DARK)
        
        with qtbot.waitSignal(theme_manager.theme_changed, timeout=1000):
            theme_manager.toggle_theme()
        
        assert theme_manager.get_current_theme() == Theme.AUTO
    
    def test_toggle_cycle_complete(self, theme_manager):
        """Test complete toggle cycle: AUTO -> LIGHT -> DARK -> AUTO."""
        assert theme_manager.get_current_theme() == Theme.AUTO
        
        theme_manager.toggle_theme()
        assert theme_manager.get_current_theme() == Theme.LIGHT
        
        theme_manager.toggle_theme()
        assert theme_manager.get_current_theme() == Theme.DARK
        
        theme_manager.toggle_theme()
        assert theme_manager.get_current_theme() == Theme.AUTO


class TestThemeManagerSystemDetection:
    """Test system dark mode detection."""
    
    def test_is_system_dark_mode_returns_bool(self, theme_manager):
        """Test that is_system_dark_mode returns a boolean."""
        result = theme_manager.is_system_dark_mode()
        assert isinstance(result, bool)
    
    def test_get_effective_theme_with_auto(self, theme_manager):
        """Test get_effective_theme with AUTO mode."""
        theme_manager.set_theme(Theme.AUTO)
        effective = theme_manager.get_effective_theme()
        assert effective in [Theme.LIGHT, Theme.DARK]
        assert effective != Theme.AUTO
    
    def test_get_effective_theme_with_light(self, theme_manager):
        """Test get_effective_theme with LIGHT mode."""
        theme_manager.set_theme(Theme.LIGHT)
        effective = theme_manager.get_effective_theme()
        assert effective == Theme.LIGHT
    
    def test_get_effective_theme_with_dark(self, theme_manager):
        """Test get_effective_theme with DARK mode."""
        theme_manager.set_theme(Theme.DARK)
        effective = theme_manager.get_effective_theme()
        assert effective == Theme.DARK


class TestThemeManagerPersistence:
    """Test theme persistence."""
    
    def test_theme_saved_to_settings(self, qapp):
        """Test that theme is saved to QSettings."""
        settings = QSettings("GFZ", "GROBI")
        settings.clear()
        
        manager = ThemeManager()
        manager.set_theme(Theme.DARK)
        
        # Check settings
        assert settings.value("theme") == Theme.DARK.value
        
        settings.clear()
    
    def test_theme_loaded_from_settings(self, qapp):
        """Test that theme is loaded from QSettings."""
        settings = QSettings("GFZ", "GROBI")
        settings.clear()
        
        # Save dark theme
        settings.setValue("theme", Theme.DARK.value)
        
        # Create new manager - should load dark theme
        manager = ThemeManager()
        assert manager.get_current_theme() == Theme.DARK
        
        settings.clear()
    
    def test_auto_theme_persistence(self, qapp):
        """Test that AUTO theme can be saved and loaded."""
        settings = QSettings("GFZ", "GROBI")
        settings.clear()
        
        # Save auto theme
        settings.setValue("theme", Theme.AUTO.value)
        
        # Create new manager - should load auto theme
        manager = ThemeManager()
        assert manager.get_current_theme() == Theme.AUTO
        
        settings.clear()
    
    def test_invalid_theme_in_settings(self, qapp):
        """Test handling of invalid theme in settings."""
        settings = QSettings("GFZ", "GROBI")
        settings.clear()
        
        # Save invalid theme
        settings.setValue("theme", "invalid")
        
        # Create new manager - should fall back to AUTO
        manager = ThemeManager()
        assert manager.get_current_theme() == Theme.AUTO
        
        settings.clear()


class TestThemeManagerStylesheets:
    """Test stylesheet generation."""
    
    def test_main_window_light_stylesheet(self, theme_manager):
        """Test main window light theme stylesheet."""
        theme_manager.set_theme(Theme.LIGHT)
        stylesheet = theme_manager.get_main_window_stylesheet()
        
        assert isinstance(stylesheet, str)
        assert len(stylesheet) > 0
        assert "#f5f5f5" in stylesheet  # Light background
        assert "QMainWindow" in stylesheet
    
    def test_main_window_dark_stylesheet(self, theme_manager):
        """Test main window dark theme stylesheet."""
        theme_manager.set_theme(Theme.DARK)
        stylesheet = theme_manager.get_main_window_stylesheet()
        
        assert isinstance(stylesheet, str)
        assert len(stylesheet) > 0
        assert "#1e1e1e" in stylesheet  # Dark background
        assert "QMainWindow" in stylesheet
    
    def test_credentials_dialog_light_stylesheet(self, theme_manager):
        """Test credentials dialog light theme stylesheet."""
        theme_manager.set_theme(Theme.LIGHT)
        stylesheet = theme_manager.get_credentials_dialog_stylesheet()
        
        assert isinstance(stylesheet, str)
        assert len(stylesheet) > 0
        assert "QDialog" in stylesheet
        assert "#f5f5f5" in stylesheet  # Light background
    
    def test_credentials_dialog_dark_stylesheet(self, theme_manager):
        """Test credentials dialog dark theme stylesheet."""
        theme_manager.set_theme(Theme.DARK)
        stylesheet = theme_manager.get_credentials_dialog_stylesheet()
        
        assert isinstance(stylesheet, str)
        assert len(stylesheet) > 0
        assert "QDialog" in stylesheet
        assert "#1e1e1e" in stylesheet  # Dark background
    
    def test_stylesheets_contain_required_elements(self, theme_manager):
        """Test that stylesheets contain all required UI elements."""
        # Test light theme
        theme_manager.set_theme(Theme.LIGHT)
        main_stylesheet = theme_manager.get_main_window_stylesheet()
        dialog_stylesheet = theme_manager.get_credentials_dialog_stylesheet()
        
        # Check main window elements
        assert "QPushButton" in main_stylesheet
        assert "QTextEdit" in main_stylesheet
        assert "QProgressBar" in main_stylesheet
        assert "QLabel" in main_stylesheet
        
        # Check dialog elements
        assert "QLineEdit" in dialog_stylesheet
        assert "QCheckBox" in dialog_stylesheet
        assert "QPushButton" in dialog_stylesheet
        
        # Test dark theme
        theme_manager.set_theme(Theme.DARK)
        main_stylesheet = theme_manager.get_main_window_stylesheet()
        dialog_stylesheet = theme_manager.get_credentials_dialog_stylesheet()
        
        # Check main window elements
        assert "QPushButton" in main_stylesheet
        assert "QTextEdit" in main_stylesheet
        assert "QProgressBar" in main_stylesheet
        assert "QLabel" in main_stylesheet
        
        # Check dialog elements
        assert "QLineEdit" in dialog_stylesheet
        assert "QCheckBox" in dialog_stylesheet
        assert "QPushButton" in dialog_stylesheet


class TestThemeManagerSignals:
    """Test theme manager signals."""
    
    def test_theme_changed_signal_exists(self, theme_manager):
        """Test that theme_changed signal exists."""
        assert hasattr(theme_manager, 'theme_changed')
    
    def test_theme_changed_signal_emits_correct_theme(self, theme_manager, qtbot):
        """Test that theme_changed signal emits the correct theme."""
        with qtbot.waitSignal(theme_manager.theme_changed, timeout=1000) as blocker:
            theme_manager.set_theme(Theme.DARK)
        
        assert blocker.args[0] == Theme.DARK
        
        with qtbot.waitSignal(theme_manager.theme_changed, timeout=1000) as blocker:
            theme_manager.set_theme(Theme.LIGHT)
        
        assert blocker.args[0] == Theme.LIGHT
