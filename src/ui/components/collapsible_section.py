"""Collapsible Section - An expandable/collapsible container widget."""

from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QToolButton, QLabel, 
    QFrame, QSizePolicy
)
from PySide6.QtCore import Signal, Qt, QPropertyAnimation, QEasingCurve
from PySide6.QtGui import QFont


# Qt maximum widget size constant for removing height constraints.
# This is 2^24 - 1, Qt's internal limit for widget dimensions.
# This value is consistent across Qt versions and platforms.
QWIDGETSIZE_MAX = 16777215


class CollapsibleSection(QWidget):
    """
    A collapsible section with animated expand/collapse.
    
    The section has a clickable header that toggles visibility of its content.
    The expand/collapse is animated smoothly.
    
    Signals:
        toggled(bool): Emitted when section is expanded (True) or collapsed (False)
    
    Example:
        >>> section = CollapsibleSection("Metadaten-Verwaltung")
        >>> flow_layout = FlowLayout()
        >>> flow_layout.addWidget(card1)
        >>> flow_layout.addWidget(card2)
        >>> section.set_content_layout(flow_layout)
    """
    
    toggled = Signal(bool)
    
    # Animation duration in milliseconds
    ANIMATION_DURATION = 200
    
    def __init__(
        self, 
        title: str = "", 
        expanded: bool = True,
        parent: QWidget = None
    ):
        """
        Initialize the collapsible section.
        
        Args:
            title: Section header title
            expanded: Initial state (True = expanded)
            parent: Parent widget
        """
        super().__init__(parent)
        
        self._title = title
        self._expanded = expanded
        
        self._setup_ui()
        
        # Set initial state without animation
        if not expanded:
            self._content_area.setMaximumHeight(0)
    
    def _setup_ui(self):
        """Set up the UI components."""
        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(0, 0, 0, 0)
        main_layout.setSpacing(0)
        
        # Header frame
        self._header_frame = QFrame()
        self._header_frame.setObjectName("collapsibleHeader")
        self._header_frame.setCursor(Qt.PointingHandCursor)
        self._header_frame.setFrameShape(QFrame.NoFrame)
        
        header_layout = QHBoxLayout(self._header_frame)
        header_layout.setContentsMargins(8, 8, 8, 8)
        header_layout.setSpacing(8)
        
        # Toggle button (arrow indicator)
        self._toggle_button = QToolButton()
        self._toggle_button.setObjectName("collapsibleToggle")
        self._toggle_button.setArrowType(
            Qt.DownArrow if self._expanded else Qt.RightArrow
        )
        self._toggle_button.setAutoRaise(True)
        self._toggle_button.setFixedSize(20, 20)
        self._toggle_button.clicked.connect(self.toggle)
        header_layout.addWidget(self._toggle_button)
        
        # Title label
        self._title_label = QLabel(self._title)
        self._title_label.setObjectName("collapsibleTitle")
        title_font = QFont()
        title_font.setPointSize(12)
        title_font.setBold(True)
        self._title_label.setFont(title_font)
        header_layout.addWidget(self._title_label)
        
        # Stretch to push content to the left
        header_layout.addStretch()
        
        main_layout.addWidget(self._header_frame)
        
        # Content area (what gets collapsed)
        self._content_area = QWidget()
        self._content_area.setObjectName("collapsibleContent")
        self._content_area.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        
        # Content layout will be set by set_content_layout()
        self._content_layout = QVBoxLayout(self._content_area)
        self._content_layout.setContentsMargins(0, 8, 0, 8)
        self._content_layout.setSpacing(0)
        
        main_layout.addWidget(self._content_area)
        
        # Make header clickable
        self._header_frame.mousePressEvent = self._on_header_clicked
    
    def _on_header_clicked(self, event):
        """Handle header click to toggle section."""
        self.toggle()
    
    def toggle(self):
        """Toggle the expanded/collapsed state with animation."""
        self._expanded = not self._expanded
        self._animate_toggle()
        self.toggled.emit(self._expanded)
    
    def _animate_toggle(self):
        """Animate the expand/collapse transition."""
        # Stop any running animation to prevent visual glitches on rapid toggles
        if hasattr(self, '_animation') and self._animation is not None:
            if self._animation.state() == QPropertyAnimation.State.Running:
                self._animation.stop()
                # Only try to disconnect if we previously connected (avoid Qt warning)
                if getattr(self, '_expand_signal_connected', False):
                    try:
                        self._animation.finished.disconnect(self._on_expand_finished)
                    except RuntimeError:
                        pass  # Already disconnected
                    finally:
                        self._expand_signal_connected = False
        
        # Update arrow direction
        self._toggle_button.setArrowType(
            Qt.DownArrow if self._expanded else Qt.RightArrow
        )
        
        # Calculate content height
        if self._expanded:
            # Expanding: calculate full content height
            self._content_area.setMaximumHeight(QWIDGETSIZE_MAX)
            self._content_area.adjustSize()
            target_height = self._content_area.sizeHint().height()
            start_height = 0
        else:
            # Collapsing
            target_height = 0
            start_height = self._content_area.height()
        
        # Create animation
        self._animation = QPropertyAnimation(self._content_area, b"maximumHeight")
        self._animation.setDuration(self.ANIMATION_DURATION)
        self._animation.setStartValue(start_height)
        self._animation.setEndValue(target_height)
        self._animation.setEasingCurve(QEasingCurve.InOutQuad)
        
        if self._expanded:
            # When expanding, remove max height constraint at the end
            # Set flag before connecting to ensure consistency even if connect fails
            self._expand_signal_connected = True
            try:
                self._animation.finished.connect(self._on_expand_finished)
            except Exception:
                self._expand_signal_connected = False
                raise
        
        self._animation.start()
    
    def _on_expand_finished(self):
        """Called when expand animation finishes."""
        # Use flag + try-finally to ensure flag is always updated correctly
        if getattr(self, '_expand_signal_connected', False):
            try:
                self._animation.finished.disconnect(self._on_expand_finished)
            except RuntimeError:
                pass  # Already disconnected
            finally:
                self._expand_signal_connected = False
        
        # Remove height constraint so content can resize naturally
        self._content_area.setMaximumHeight(QWIDGETSIZE_MAX)
    
    def set_content_layout(self, layout):
        """
        Set the layout for the content area.
        
        This replaces any existing content layout.
        
        Args:
            layout: QLayout to use for content
        """
        # Remove existing layout items
        while self._content_layout.count():
            item = self._content_layout.takeAt(0)
            if item.widget():
                item.widget().setParent(None)
        
        # Add new layout as a widget container
        container = QWidget()
        container.setLayout(layout)
        self._content_layout.addWidget(container)
    
    def add_widget(self, widget: QWidget):
        """
        Add a widget directly to the content area.
        
        Args:
            widget: Widget to add
        """
        self._content_layout.addWidget(widget)
    
    def set_expanded(self, expanded: bool, animate: bool = True):
        """
        Set the expanded state.
        
        Args:
            expanded: True to expand, False to collapse
            animate: Whether to animate the transition
        """
        if expanded != self._expanded:
            if animate:
                self.toggle()
            else:
                self._expanded = expanded
                self._toggle_button.setArrowType(
                    Qt.DownArrow if expanded else Qt.RightArrow
                )
                if expanded:
                    self._content_area.setMaximumHeight(QWIDGETSIZE_MAX)
                else:
                    self._content_area.setMaximumHeight(0)
                self.toggled.emit(expanded)
    
    def is_expanded(self) -> bool:
        """
        Check if the section is expanded.
        
        Returns:
            bool: True if expanded
        """
        return self._expanded
    
    def set_title(self, title: str):
        """
        Set the section title.
        
        Args:
            title: New title text
        """
        self._title = title
        self._title_label.setText(title)
    
    def title(self) -> str:
        """
        Get the section title.
        
        Returns:
            str: Current title
        """
        return self._title
    
    @property
    def header_frame(self) -> QFrame:
        """Get the header frame widget (for advanced styling)."""
        return self._header_frame
    
    @property
    def content_area(self) -> QWidget:
        """Get the content area widget (for advanced styling)."""
        return self._content_area
    
    @property
    def title_label(self) -> QLabel:
        """Get the title label widget (for advanced styling)."""
        return self._title_label
