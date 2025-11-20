"""Theme Manager for GROBI application."""

from enum import Enum
from PySide6.QtCore import QObject, Signal, QSettings
from PySide6.QtWidgets import QApplication
from PySide6.QtGui import QPalette


class Theme(Enum):
    """Available themes."""
    LIGHT = "light"
    DARK = "dark"
    AUTO = "auto"


class ThemeManager(QObject):
    """Manages application theme and provides stylesheets."""
    
    theme_changed = Signal(Theme)
    
    def __init__(self):
        """Initialize the theme manager."""
        super().__init__()
        self.settings = QSettings("GFZ", "GROBI")
        self._current_theme = self._load_theme()
    
    def _load_theme(self) -> Theme:
        """
        Load theme preference from settings.
        
        Returns:
            Theme: Saved theme or default (AUTO)
        """
        theme_str = self.settings.value("theme", Theme.AUTO.value)
        try:
            return Theme(theme_str)
        except ValueError:
            return Theme.AUTO
    
    def _save_theme(self, theme: Theme):
        """
        Save theme preference to settings.
        
        Args:
            theme: Theme to save
        """
        self.settings.setValue("theme", theme.value)
    
    def get_current_theme(self) -> Theme:
        """
        Get current theme.
        
        Returns:
            Theme: Current theme
        """
        return self._current_theme
    
    def set_theme(self, theme: Theme):
        """
        Set application theme.
        
        Args:
            theme: Theme to apply
        """
        if theme != self._current_theme:
            self._current_theme = theme
            self._save_theme(theme)
            self.theme_changed.emit(theme)
    
    def toggle_theme(self):
        """Cycle through themes: AUTO -> LIGHT -> DARK -> AUTO."""
        if self._current_theme == Theme.AUTO:
            new_theme = Theme.LIGHT
        elif self._current_theme == Theme.LIGHT:
            new_theme = Theme.DARK
        else:
            new_theme = Theme.AUTO
        self.set_theme(new_theme)
    
    def is_system_dark_mode(self) -> bool:
        """
        Detect if system is using dark mode.
        
        Returns:
            bool: True if system is in dark mode
        """
        app = QApplication.instance()
        if app:
            palette = app.palette()
            # Check if window background is darker than text color
            window_color = palette.color(QPalette.Window)
            text_color = palette.color(QPalette.WindowText)
            # Calculate relative luminance
            window_luminance = (0.299 * window_color.red() + 
                              0.587 * window_color.green() + 
                              0.114 * window_color.blue())
            text_luminance = (0.299 * text_color.red() + 
                            0.587 * text_color.green() + 
                            0.114 * text_color.blue())
            return window_luminance < text_luminance
        return False
    
    def get_effective_theme(self) -> Theme:
        """
        Get the effective theme (resolves AUTO to LIGHT or DARK).
        
        Returns:
            Theme: LIGHT or DARK (never AUTO)
        """
        if self._current_theme == Theme.AUTO:
            return Theme.DARK if self.is_system_dark_mode() else Theme.LIGHT
        return self._current_theme
    
    def get_main_window_stylesheet(self) -> str:
        """
        Get stylesheet for main window.
        
        Returns:
            str: CSS stylesheet
        """
        effective_theme = self.get_effective_theme()
        if effective_theme == Theme.DARK:
            return self._get_dark_main_window_stylesheet()
        else:
            return self._get_light_main_window_stylesheet()
    
    def get_credentials_dialog_stylesheet(self) -> str:
        """
        Get stylesheet for credentials dialog.
        
        Returns:
            str: CSS stylesheet
        """
        effective_theme = self.get_effective_theme()
        if effective_theme == Theme.DARK:
            return self._get_dark_credentials_dialog_stylesheet()
        else:
            return self._get_light_credentials_dialog_stylesheet()
    
    def _get_light_main_window_stylesheet(self) -> str:
        """Get light theme stylesheet for main window."""
        return """
            QMainWindow {
                background-color: #f5f5f5;
            }
            QTextEdit {
                background-color: white;
                color: #333;
                border: 1px solid #ccc;
                border-radius: 4px;
                padding: 8px;
                font-family: 'Segoe UI', Tahoma, sans-serif;
            }
            QPushButton {
                background-color: #0078d4;
                color: white;
                border: none;
                padding: 10px 20px;
                border-radius: 4px;
                font-size: 14px;
                min-width: 120px;
            }
            QPushButton:hover {
                background-color: #106ebe;
            }
            QPushButton:pressed {
                background-color: #005a9e;
            }
            QPushButton:disabled {
                background-color: #cccccc;
                color: #666666;
            }
            QProgressBar {
                border: 1px solid #ccc;
                border-radius: 4px;
                text-align: center;
                background-color: #e0e0e0;
            }
            QProgressBar::chunk {
                background-color: #0078d4;
                border-radius: 3px;
            }
            QLabel {
                color: #333;
            }
        """
    
    def _get_dark_main_window_stylesheet(self) -> str:
        """Get dark theme stylesheet for main window."""
        return """
            QMainWindow {
                background-color: #1e1e1e;
            }
            QTextEdit {
                background-color: #2d2d2d;
                color: #d4d4d4;
                border: 1px solid #3e3e3e;
                border-radius: 4px;
                padding: 8px;
                font-family: 'Segoe UI', Tahoma, sans-serif;
            }
            QPushButton {
                background-color: #0e639c;
                color: #ffffff;
                border: none;
                padding: 10px 20px;
                border-radius: 4px;
                font-size: 14px;
                min-width: 120px;
            }
            QPushButton:hover {
                background-color: #1177bb;
            }
            QPushButton:pressed {
                background-color: #0d5a8c;
            }
            QPushButton:disabled {
                background-color: #3e3e3e;
                color: #808080;
            }
            QProgressBar {
                border: 1px solid #3e3e3e;
                border-radius: 4px;
                text-align: center;
                background-color: #2d2d2d;
                color: #d4d4d4;
            }
            QProgressBar::chunk {
                background-color: #0e639c;
                border-radius: 3px;
            }
            QLabel {
                color: #d4d4d4;
            }
        """
    
    def _get_light_credentials_dialog_stylesheet(self) -> str:
        """Get light theme stylesheet for credentials dialog."""
        return """
            QDialog {
                background-color: #f5f5f5;
            }
            QLabel {
                color: #333;
            }
            QLineEdit {
                padding: 8px;
                border: 1px solid #ccc;
                border-radius: 4px;
                background-color: white;
                color: #333;
            }
            QLineEdit:focus {
                border: 2px solid #0078d4;
            }
            QCheckBox {
                color: #333;
                spacing: 8px;
            }
            QCheckBox::indicator {
                width: 18px;
                height: 18px;
            }
            QPushButton {
                padding: 8px 16px;
                border-radius: 4px;
                background-color: #0078d4;
                color: white;
                border: none;
                min-width: 80px;
            }
            QPushButton:hover {
                background-color: #106ebe;
            }
            QPushButton:pressed {
                background-color: #005a9e;
            }
            QPushButton:disabled {
                background-color: #cccccc;
                color: #666666;
            }
        """
    
    def _get_dark_credentials_dialog_stylesheet(self) -> str:
        """Get dark theme stylesheet for credentials dialog."""
        return """
            QDialog {
                background-color: #1e1e1e;
            }
            QLabel {
                color: #d4d4d4;
            }
            QLineEdit {
                padding: 8px;
                border: 1px solid #3e3e3e;
                border-radius: 4px;
                background-color: #2d2d2d;
                color: #d4d4d4;
            }
            QLineEdit:focus {
                border: 2px solid #0e639c;
            }
            QCheckBox {
                color: #d4d4d4;
                spacing: 8px;
            }
            QCheckBox::indicator {
                width: 18px;
                height: 18px;
                background-color: #2d2d2d;
                border: 1px solid #3e3e3e;
            }
            QCheckBox::indicator:checked {
                background-color: #0e639c;
                border: 1px solid #0e639c;
            }
            QPushButton {
                padding: 8px 16px;
                border-radius: 4px;
                background-color: #0e639c;
                color: white;
                border: none;
                min-width: 80px;
            }
            QPushButton:hover {
                background-color: #1177bb;
            }
            QPushButton:pressed {
                background-color: #0d5a8c;
            }
            QPushButton:disabled {
                background-color: #3e3e3e;
                color: #808080;
            }
        """
