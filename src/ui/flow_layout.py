"""Flow Layout - A layout that arranges widgets like flowing text."""

from PySide6.QtWidgets import QLayout, QSizePolicy, QStyle
from PySide6.QtCore import Qt, QRect, QSize, QPoint


class FlowLayout(QLayout):
    """
    A layout that arranges widgets in a flowing manner, like text.
    
    Widgets are placed left-to-right, top-to-bottom, wrapping to the next
    row when there's not enough horizontal space.
    """
    
    def __init__(self, parent=None, margin: int = -1, h_spacing: int = -1, v_spacing: int = -1):
        """
        Initialize the flow layout.
        
        Args:
            parent: Parent widget
            margin: Layout margin (uses style default if -1)
            h_spacing: Horizontal spacing between items (uses style default if -1)
            v_spacing: Vertical spacing between items (uses style default if -1)
        """
        super().__init__(parent)
        
        self._item_list = []
        self._h_space = h_spacing
        self._v_space = v_spacing
        
        if margin >= 0:
            self.setContentsMargins(margin, margin, margin, margin)
    
    def __del__(self):
        """Clean up layout items.
        
        Note: We rely on Qt's parent-child ownership model for widget cleanup.
        Explicitly setting parent to None releases ownership before removal.
        """
        while self._item_list:
            item = self._item_list.pop(0)
            if item:
                widget = item.widget()
                if widget:
                    # Release from parent ownership for proper cleanup
                    widget.setParent(None)
    
    def addItem(self, item):
        """Add an item to the layout."""
        self._item_list.append(item)
    
    def horizontalSpacing(self) -> int:
        """Return the horizontal spacing between items."""
        if self._h_space >= 0:
            return self._h_space
        return self._smart_spacing(QStyle.PM_LayoutHorizontalSpacing)
    
    def verticalSpacing(self) -> int:
        """Return the vertical spacing between items."""
        if self._v_space >= 0:
            return self._v_space
        return self._smart_spacing(QStyle.PM_LayoutVerticalSpacing)
    
    def setHorizontalSpacing(self, spacing: int):
        """Set the horizontal spacing between items."""
        self._h_space = spacing
    
    def setVerticalSpacing(self, spacing: int):
        """Set the vertical spacing between items."""
        self._v_space = spacing
    
    def count(self) -> int:
        """Return the number of items in the layout."""
        return len(self._item_list)
    
    def itemAt(self, index: int):
        """Return the item at the given index."""
        if 0 <= index < len(self._item_list):
            return self._item_list[index]
        return None
    
    def takeAt(self, index: int):
        """Remove and return the item at the given index."""
        if 0 <= index < len(self._item_list):
            return self._item_list.pop(index)
        return None
    
    def expandingDirections(self) -> Qt.Orientations:
        """Return the directions in which the layout can expand."""
        return Qt.Orientations(Qt.Orientation(0))
    
    def hasHeightForWidth(self) -> bool:
        """Return True because height depends on width."""
        return True
    
    def heightForWidth(self, width: int) -> int:
        """Return the height required for the given width."""
        height = self._do_layout(QRect(0, 0, width, 0), True)
        return height
    
    def setGeometry(self, rect: QRect):
        """Set the geometry of the layout."""
        super().setGeometry(rect)
        self._do_layout(rect, False)
    
    def sizeHint(self) -> QSize:
        """Return the preferred size of the layout."""
        return self.minimumSize()
    
    def minimumSize(self) -> QSize:
        """Return the minimum size of the layout."""
        size = QSize()
        
        for item in self._item_list:
            size = size.expandedTo(item.minimumSize())
        
        margins = self.contentsMargins()
        size += QSize(margins.left() + margins.right(), 
                      margins.top() + margins.bottom())
        return size
    
    def _do_layout(self, rect: QRect, test_only: bool) -> int:
        """
        Perform the layout calculation.
        
        Args:
            rect: The available rectangle
            test_only: If True, only calculate without moving widgets
            
        Returns:
            The height of the layout
        """
        left, top, right, bottom = self.getContentsMargins()
        effective_rect = rect.adjusted(left, top, -right, -bottom)
        x = effective_rect.x()
        y = effective_rect.y()
        line_height = 0
        
        for item in self._item_list:
            widget = item.widget()
            if widget is None:
                continue
                
            h_space = self.horizontalSpacing()
            if h_space == -1:
                h_space = widget.style().layoutSpacing(
                    QSizePolicy.PushButton, QSizePolicy.PushButton, Qt.Horizontal
                )
            
            v_space = self.verticalSpacing()
            if v_space == -1:
                v_space = widget.style().layoutSpacing(
                    QSizePolicy.PushButton, QSizePolicy.PushButton, Qt.Vertical
                )
            
            next_x = x + item.sizeHint().width() + h_space
            
            if next_x - h_space > effective_rect.right() and line_height > 0:
                x = effective_rect.x()
                y = y + line_height + v_space
                next_x = x + item.sizeHint().width() + h_space
                line_height = 0
            
            if not test_only:
                item.setGeometry(QRect(QPoint(x, y), item.sizeHint()))
            
            x = next_x
            line_height = max(line_height, item.sizeHint().height())
        
        return y + line_height - rect.y() + bottom
    
    def _smart_spacing(self, pm: QStyle.PixelMetric) -> int:
        """
        Get smart spacing based on parent widget.
        
        Args:
            pm: The pixel metric to query
            
        Returns:
            The spacing value
        """
        parent = self.parent()
        if parent is None:
            return -1
        elif parent.isWidgetType():
            return parent.style().pixelMetric(pm, None, parent)
        else:
            return parent.spacing()
    
    def clear(self):
        """Remove all items from the layout."""
        while self.count():
            item = self.takeAt(0)
            if item.widget():
                item.widget().deleteLater()
