"""FAIR Assessment Tile Widget for displaying DOI scores."""

from PySide6.QtWidgets import QWidget, QSizePolicy
from PySide6.QtCore import Qt, Signal, QSize
from PySide6.QtGui import QColor, QPainter, QBrush, QPen, QFont, QFontMetrics


class FujiTile(QWidget):
    """
    A tile widget displaying a DOI's FAIR assessment score.
    
    The tile shows:
    - The DOI identifier
    - The FAIR score as percentage
    - Background color based on score (red -> yellow -> green)
    """
    
    # Signal emitted when tile is clicked (for future use)
    clicked = Signal(str)  # DOI
    
    # Color constants
    COLOR_ERROR = QColor(128, 128, 128)      # Gray for errors
    COLOR_RED = QColor(139, 0, 0)            # Dark red for 0%
    COLOR_YELLOW = QColor(255, 255, 0)       # Yellow for 50%
    COLOR_GREEN = QColor(0, 100, 0)          # Dark green for 100%
    
    def __init__(self, doi: str, score_percent: float = -1, parent=None):
        """
        Initialize the tile.
        
        Args:
            doi: The DOI identifier
            score_percent: FAIR score as percentage (0-100), or -1 for error/pending
            parent: Parent widget
        """
        super().__init__(parent)
        
        self.doi = doi
        self.score_percent = score_percent
        self._size = 100  # Default tile size
        
        self.setMinimumSize(50, 50)
        self.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
        self.setCursor(Qt.PointingHandCursor)
        
        # Tooltip with full DOI
        self._update_tooltip()
    
    def set_score(self, score_percent: float):
        """
        Update the score and repaint.
        
        Args:
            score_percent: New score (0-100) or -1 for error
        """
        self.score_percent = score_percent
        self._update_tooltip()
        self.update()
    
    def set_tile_size(self, size: int):
        """
        Set the tile size.
        
        Args:
            size: Size in pixels (width = height)
        """
        self._size = max(50, min(200, size))
        self.setFixedSize(self._size, self._size)
        self.update()
    
    def _update_tooltip(self):
        """Update the tooltip text."""
        if self.score_percent < 0:
            self.setToolTip(f"{self.doi}\n\nStatus: Fehler oder ausstehend")
        else:
            self.setToolTip(f"{self.doi}\n\nFAIR Score: {self.score_percent:.1f}%")
    
    def sizeHint(self) -> QSize:
        """Return the preferred size."""
        return QSize(self._size, self._size)
    
    def minimumSizeHint(self) -> QSize:
        """Return the minimum size."""
        return QSize(50, 50)
    
    def _calculate_background_color(self) -> QColor:
        """
        Calculate background color based on score.
        
        Returns:
            QColor based on score (red -> yellow -> green gradient)
        """
        if self.score_percent < 0:
            return self.COLOR_ERROR
        
        percent = max(0, min(100, self.score_percent))
        
        if percent <= 50:
            # Red to Yellow gradient
            ratio = percent / 50
            r = int(self.COLOR_RED.red() + (self.COLOR_YELLOW.red() - self.COLOR_RED.red()) * ratio)
            g = int(self.COLOR_RED.green() + (self.COLOR_YELLOW.green() - self.COLOR_RED.green()) * ratio)
            b = int(self.COLOR_RED.blue() + (self.COLOR_YELLOW.blue() - self.COLOR_RED.blue()) * ratio)
        else:
            # Yellow to Green gradient
            ratio = (percent - 50) / 50
            r = int(self.COLOR_YELLOW.red() + (self.COLOR_GREEN.red() - self.COLOR_YELLOW.red()) * ratio)
            g = int(self.COLOR_YELLOW.green() + (self.COLOR_GREEN.green() - self.COLOR_YELLOW.green()) * ratio)
            b = int(self.COLOR_YELLOW.blue() + (self.COLOR_GREEN.blue() - self.COLOR_YELLOW.blue()) * ratio)
        
        return QColor(r, g, b)
    
    def _get_text_color(self) -> QColor:
        """
        Get contrasting text color based on background.
        
        Returns:
            White or black depending on background brightness
        """
        bg = self._calculate_background_color()
        # Calculate perceived brightness
        brightness = (bg.red() * 299 + bg.green() * 587 + bg.blue() * 114) / 1000
        return QColor(Qt.black) if brightness > 128 else QColor(Qt.white)
    
    def _get_doi_suffix(self) -> str:
        """
        Get a shortened DOI for display.
        
        Returns:
            The DOI suffix (after the last /)
        """
        if '/' in self.doi:
            return self.doi.split('/')[-1]
        return self.doi
    
    def _calculate_font_size(self, text: str, available_width: int, max_size: int = 12) -> int:
        """
        Calculate font size to fit text in available width.
        
        Args:
            text: Text to fit
            available_width: Available width in pixels
            max_size: Maximum font size
            
        Returns:
            Font size that fits
        """
        for size in range(max_size, 6, -1):
            font = QFont()
            font.setPointSize(size)
            metrics = QFontMetrics(font)
            if metrics.horizontalAdvance(text) <= available_width:
                return size
        return 6
    
    def paintEvent(self, event):
        """Paint the tile."""
        painter = QPainter(self)
        painter.setRenderHint(QPainter.Antialiasing)
        
        # Background
        bg_color = self._calculate_background_color()
        painter.setBrush(QBrush(bg_color))
        painter.setPen(QPen(Qt.black, 1))
        painter.drawRoundedRect(self.rect().adjusted(1, 1, -1, -1), 5, 5)
        
        # Text color
        text_color = self._get_text_color()
        painter.setPen(text_color)
        
        available_width = self._size - 10  # Padding
        
        # Score text (large, centered)
        if self.score_percent < 0:
            score_text = "—"
        else:
            score_text = f"{self.score_percent:.0f}%"
        
        score_font_size = min(self._size // 3, 24)
        score_font = QFont()
        score_font.setPointSize(score_font_size)
        score_font.setBold(True)
        painter.setFont(score_font)
        
        score_rect = self.rect().adjusted(0, 5, 0, -self._size // 3)
        painter.drawText(score_rect, Qt.AlignCenter, score_text)
        
        # DOI text (smaller, at bottom)
        doi_display = self._get_doi_suffix()
        doi_font_size = self._calculate_font_size(doi_display, available_width, max_size=10)
        doi_font = QFont()
        doi_font.setPointSize(doi_font_size)
        painter.setFont(doi_font)
        
        doi_rect = self.rect().adjusted(5, self._size // 2, -5, -5)
        
        # Elide text if still too long
        metrics = QFontMetrics(doi_font)
        elided_text = metrics.elidedText(doi_display, Qt.ElideMiddle, available_width)
        painter.drawText(doi_rect, Qt.AlignCenter | Qt.TextWordWrap, elided_text)
    
    def mousePressEvent(self, event):
        """Handle mouse press (for future click functionality)."""
        if event.button() == Qt.LeftButton:
            self.clicked.emit(self.doi)
        super().mousePressEvent(event)
