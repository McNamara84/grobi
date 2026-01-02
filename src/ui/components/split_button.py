"""Split Button - A button with primary action and dropdown menu."""

from PySide6.QtWidgets import (
    QWidget, QHBoxLayout, QPushButton, QToolButton, QMenu, QSizePolicy
)
from PySide6.QtCore import Signal, Qt
from PySide6.QtGui import QAction


class SplitButton(QWidget):
    """
    A button with a primary action and a dropdown menu for secondary actions.
    
    The button is split into two parts:
    - Left part: Primary action button (clicking triggers `clicked` signal)
    - Right part: Dropdown arrow that opens a menu with additional actions
    
    Signals:
        clicked: Emitted when the primary button is clicked
        action_triggered(str): Emitted when a menu action is triggered, with action ID
    
    Example:
        >>> split_btn = SplitButton("📥 Exportieren")
        >>> split_btn.add_action("🔄 Aus CSV aktualisieren", "update")
        >>> split_btn.clicked.connect(self.on_export)
        >>> split_btn.action_triggered.connect(self.on_action)  # receives action_id str
    """
    
    clicked = Signal()
    action_triggered = Signal(str)
    
    def __init__(self, text: str = "", icon: str = "", parent: QWidget = None):
        """
        Initialize the split button.
        
        Args:
            text: Button text (can include emoji as icon prefix)
            icon: Optional icon (currently supports emoji strings)
            parent: Parent widget
        """
        super().__init__(parent)
        
        self._text = text
        self._icon = icon
        self._enabled = True
        
        self._setup_ui()
    
    def _setup_ui(self):
        """Set up the UI components."""
        layout = QHBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)
        
        # Primary button (left part)
        self._primary_button = QPushButton()
        self._update_primary_text()
        self._primary_button.setMinimumHeight(40)
        self._primary_button.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self._primary_button.clicked.connect(self.clicked.emit)
        self._primary_button.setObjectName("splitButtonPrimary")
        layout.addWidget(self._primary_button)
        
        # Dropdown button (right part)
        self._dropdown_button = QToolButton()
        self._dropdown_button.setArrowType(Qt.DownArrow)
        self._dropdown_button.setMinimumHeight(40)
        self._dropdown_button.setFixedWidth(30)
        self._dropdown_button.setPopupMode(QToolButton.InstantPopup)
        self._dropdown_button.setObjectName("splitButtonDropdown")
        
        # Create menu
        self._menu = QMenu(self)
        self._dropdown_button.setMenu(self._menu)
        self._menu.triggered.connect(self._on_menu_triggered)
        
        layout.addWidget(self._dropdown_button)
        
        # Initially hide dropdown if no actions
        self._dropdown_button.setVisible(False)
    
    def _update_primary_text(self):
        """Update the primary button text with optional icon."""
        display_text = f"{self._icon} {self._text}".strip() if self._icon else self._text
        self._primary_button.setText(display_text)
    
    def _on_menu_triggered(self, action: QAction):
        """Handle menu action trigger."""
        action_id = action.data() or action.text()
        self.action_triggered.emit(action_id)
    
    def add_action(self, text: str, action_id: str = "", icon: str = "") -> QAction:
        """
        Add an action to the dropdown menu.
        
        Args:
            text: Action text (can include emoji)
            action_id: Unique identifier for the action (stored in action.data())
            icon: Optional icon (emoji string)
            
        Returns:
            QAction: The created action
        """
        display_text = f"{icon} {text}".strip() if icon else text
        action = self._menu.addAction(display_text)
        action.setData(action_id if action_id else text)
        
        # Show dropdown button when we have actions
        self._dropdown_button.setVisible(True)
        
        return action
    
    def add_separator(self):
        """Add a separator to the dropdown menu."""
        self._menu.addSeparator()
    
    def clear_actions(self):
        """Remove all actions from the dropdown menu."""
        self._menu.clear()
        self._dropdown_button.setVisible(False)
    
    def set_text(self, text: str):
        """
        Set the primary button text.
        
        Args:
            text: New button text
        """
        self._text = text
        self._update_primary_text()
    
    def text(self) -> str:
        """
        Get the primary button text.
        
        Returns:
            str: Current button text
        """
        return self._text
    
    def set_icon(self, icon: str):
        """
        Set the button icon (emoji).
        
        Args:
            icon: Icon string (emoji)
        """
        self._icon = icon
        self._update_primary_text()
    
    def setEnabled(self, enabled: bool):
        """
        Enable or disable the entire split button.
        
        Args:
            enabled: True to enable, False to disable
        """
        self._enabled = enabled
        self._primary_button.setEnabled(enabled)
        self._dropdown_button.setEnabled(enabled)
        super().setEnabled(enabled)
    
    def isEnabled(self) -> bool:
        """
        Check if the button is enabled.
        
        Returns:
            bool: True if enabled
        """
        return self._enabled
    
    def set_primary_enabled(self, enabled: bool):
        """
        Enable or disable only the primary button.
        
        Useful when the export is always available but update requires a CSV file.
        
        Args:
            enabled: True to enable, False to disable
        """
        self._primary_button.setEnabled(enabled)
    
    def set_dropdown_enabled(self, enabled: bool):
        """
        Enable or disable only the dropdown button.
        
        Args:
            enabled: True to enable, False to disable
        """
        self._dropdown_button.setEnabled(enabled)
    
    def set_action_enabled(self, action_id: str, enabled: bool):
        """
        Enable or disable a specific menu action.
        
        Args:
            action_id: The action ID to modify
            enabled: True to enable, False to disable
        """
        for action in self._menu.actions():
            if action.data() == action_id:
                action.setEnabled(enabled)
                break

    def is_action_enabled(self, action_id: str) -> bool:
        """
        Check if a specific menu action is enabled.
        
        Args:
            action_id: The action ID to check
            
        Returns:
            bool: True if the action is enabled, False if disabled or not found
        """
        for action in self._menu.actions():
            if action.data() == action_id:
                return action.isEnabled()
        return False

    def setToolTip(self, tooltip: str):
        """
        Set tooltip for the primary button.
        
        Args:
            tooltip: Tooltip text
        """
        self._primary_button.setToolTip(tooltip)
    
    def setAccessibleName(self, name: str):
        """
        Set accessible name for screen readers.
        
        Args:
            name: Accessible name
        """
        self._primary_button.setAccessibleName(name)
        super().setAccessibleName(name)
    
    @property
    def primary_button(self) -> QPushButton:
        """Get the primary button widget (for advanced styling)."""
        return self._primary_button
    
    @property
    def dropdown_button(self) -> QToolButton:
        """Get the dropdown button widget (for advanced styling)."""
        return self._dropdown_button
    
    @property
    def menu(self) -> QMenu:
        """Get the dropdown menu (for advanced customization)."""
        return self._menu
