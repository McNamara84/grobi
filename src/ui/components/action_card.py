"""Action Card - A card widget for workflow actions with split button."""

from PySide6.QtWidgets import (
    QFrame, QVBoxLayout, QHBoxLayout, QLabel, QSizePolicy, QGraphicsDropShadowEffect
)
from PySide6.QtCore import Signal, Qt
from PySide6.QtGui import QFont, QColor

from src.ui.components.split_button import SplitButton


class ActionCard(QFrame):
    """
    A card widget representing a single workflow action.
    
    Each card contains:
    - Icon and title
    - Optional description
    - Status indicator
    - Split button for primary/secondary actions
    
    Signals:
        primary_clicked: Emitted when the primary button is clicked
        action_triggered(str): Emitted with action ID from dropdown menu
    
    Example:
        >>> card = ActionCard(
        ...     icon="🔗",
        ...     title="Landing Page URLs",
        ...     description="DOI URLs verwalten"
        ... )
        >>> card.set_status("🟢 CSV bereit: TIB.GFZ_urls.csv")
        >>> card.add_action("🔄 Aus CSV aktualisieren", "update")
        >>> card.primary_clicked.connect(self.on_export)
    """
    
    primary_clicked = Signal()
    action_triggered = Signal(str)
    
    # Card size constraints
    CARD_MIN_WIDTH = 260
    CARD_MAX_WIDTH = 320
    CARD_MIN_HEIGHT = 160
    
    def __init__(
        self,
        icon: str = "",
        title: str = "",
        description: str = "",
        primary_text: str = "📥 Exportieren",
        parent = None
    ):
        """
        Initialize the action card.
        
        Args:
            icon: Icon (emoji string) for the card
            title: Card title
            description: Optional description text
            primary_text: Text for the primary button
            parent: Parent widget
        """
        super().__init__(parent)
        
        self._icon = icon
        self._title = title
        self._description = description
        self._primary_text = primary_text
        self._status_text = ""
        
        self._setup_ui()
        self._setup_shadow()
    
    def _setup_ui(self):
        """Set up the UI components."""
        self.setObjectName("actionCard")
        self.setFrameShape(QFrame.StyledPanel)
        self.setFrameShadow(QFrame.Raised)
        
        # Size policy
        self.setSizePolicy(QSizePolicy.Preferred, QSizePolicy.Fixed)
        self.setMinimumWidth(self.CARD_MIN_WIDTH)
        self.setMaximumWidth(self.CARD_MAX_WIDTH)
        self.setMinimumHeight(self.CARD_MIN_HEIGHT)
        
        # Main layout
        layout = QVBoxLayout(self)
        layout.setContentsMargins(16, 16, 16, 16)
        layout.setSpacing(8)
        
        # Header row (icon + title)
        header_layout = QHBoxLayout()
        header_layout.setSpacing(8)
        
        # Icon label
        self._icon_label = QLabel(self._icon)
        self._icon_label.setObjectName("cardIcon")
        icon_font = QFont()
        icon_font.setPointSize(20)
        self._icon_label.setFont(icon_font)
        self._icon_label.setFixedWidth(32)
        self._icon_label.setAlignment(Qt.AlignCenter)
        header_layout.addWidget(self._icon_label)
        
        # Title label
        self._title_label = QLabel(self._title)
        self._title_label.setObjectName("cardTitle")
        title_font = QFont()
        title_font.setPointSize(14)
        title_font.setBold(True)
        self._title_label.setFont(title_font)
        self._title_label.setWordWrap(True)
        header_layout.addWidget(self._title_label, 1)
        
        layout.addLayout(header_layout)
        
        # Description label (optional)
        self._description_label = QLabel(self._description)
        self._description_label.setObjectName("cardDescription")
        self._description_label.setWordWrap(True)
        desc_font = QFont()
        desc_font.setPointSize(11)
        self._description_label.setFont(desc_font)
        if not self._description:
            self._description_label.setVisible(False)
        layout.addWidget(self._description_label)
        
        # Status label
        self._status_label = QLabel("⚪ Keine CSV-Datei gefunden")
        self._status_label.setObjectName("cardStatus")
        status_font = QFont()
        status_font.setPointSize(11)
        self._status_label.setFont(status_font)
        layout.addWidget(self._status_label)
        
        # Spacer to push button to bottom
        layout.addStretch()
        
        # Split button
        self._split_button = SplitButton(self._primary_text)
        self._split_button.clicked.connect(self.primary_clicked.emit)
        self._split_button.action_triggered.connect(self._on_action_triggered)
        layout.addWidget(self._split_button)
    
    def _setup_shadow(self):
        """Set up the drop shadow effect."""
        shadow = QGraphicsDropShadowEffect(self)
        shadow.setBlurRadius(12)
        shadow.setXOffset(0)
        shadow.setYOffset(2)
        shadow.setColor(QColor(0, 0, 0, 30))
        self.setGraphicsEffect(shadow)
    
    def _on_action_triggered(self, action):
        """Handle action trigger from split button."""
        action_id = action.data()
        self.action_triggered.emit(action_id)
    
    def set_icon(self, icon: str):
        """
        Set the card icon.
        
        Args:
            icon: Icon (emoji string)
        """
        self._icon = icon
        self._icon_label.setText(icon)
    
    def set_title(self, title: str):
        """
        Set the card title.
        
        Args:
            title: Title text
        """
        self._title = title
        self._title_label.setText(title)
    
    def set_description(self, description: str):
        """
        Set the card description.
        
        Args:
            description: Description text (empty to hide)
        """
        self._description = description
        self._description_label.setText(description)
        self._description_label.setVisible(bool(description))
    
    def set_status(self, status: str, is_ready: bool = None):
        """
        Set the status text.
        
        Args:
            status: Status text (can include emoji indicators)
            is_ready: Optional boolean to auto-format status
                     If provided, will use 🟢/⚪ prefix
        """
        if is_ready is not None:
            prefix = "🟢" if is_ready else "⚪"
            self._status_text = f"{prefix} {status}"
        else:
            self._status_text = status
        
        self._status_label.setText(self._status_text)
    
    def set_primary_text(self, text: str):
        """
        Set the primary button text.
        
        Args:
            text: Button text
        """
        self._primary_text = text
        self._split_button.set_text(text)
    
    def add_action(self, text: str, action_id: str = "", icon: str = ""):
        """
        Add an action to the dropdown menu.
        
        Args:
            text: Action text
            action_id: Unique identifier for the action
            icon: Optional icon (emoji)
        """
        return self._split_button.add_action(text, action_id, icon)
    
    def add_separator(self):
        """Add a separator to the dropdown menu."""
        self._split_button.add_separator()
    
    def clear_actions(self):
        """Remove all dropdown actions."""
        self._split_button.clear_actions()
    
    def setEnabled(self, enabled: bool):
        """
        Enable or disable the entire card.
        
        Args:
            enabled: True to enable, False to disable
        """
        super().setEnabled(enabled)
        self._split_button.setEnabled(enabled)
    
    def set_primary_enabled(self, enabled: bool):
        """
        Enable or disable only the primary button.
        
        Args:
            enabled: True to enable
        """
        self._split_button.set_primary_enabled(enabled)
    
    def set_action_enabled(self, action_id: str, enabled: bool):
        """
        Enable or disable a specific action.
        
        Args:
            action_id: Action identifier
            enabled: True to enable
        """
        self._split_button.set_action_enabled(action_id, enabled)
    
    def setToolTip(self, tooltip: str):
        """
        Set tooltip for the card.
        
        Args:
            tooltip: Tooltip text
        """
        super().setToolTip(tooltip)
        self._split_button.setToolTip(tooltip)
    
    @property
    def split_button(self) -> SplitButton:
        """Get the split button widget (for advanced customization)."""
        return self._split_button
    
    @property
    def icon_label(self) -> QLabel:
        """Get the icon label widget."""
        return self._icon_label
    
    @property
    def title_label(self) -> QLabel:
        """Get the title label widget."""
        return self._title_label
    
    @property
    def description_label(self) -> QLabel:
        """Get the description label widget."""
        return self._description_label
    
    @property
    def status_label(self) -> QLabel:
        """Get the status label widget."""
        return self._status_label
    
    def enterEvent(self, event):
        """Handle mouse enter for hover effect."""
        # Update shadow on hover
        shadow = self.graphicsEffect()
        if shadow and isinstance(shadow, QGraphicsDropShadowEffect):
            shadow.setBlurRadius(20)
            shadow.setColor(QColor(0, 0, 0, 50))
        super().enterEvent(event)
    
    def leaveEvent(self, event):
        """Handle mouse leave to reset hover effect."""
        # Reset shadow
        shadow = self.graphicsEffect()
        if shadow and isinstance(shadow, QGraphicsDropShadowEffect):
            shadow.setBlurRadius(12)
            shadow.setColor(QColor(0, 0, 0, 30))
        super().leaveEvent(event)
