"""Unit tests for FlowLayout."""

import pytest
from PySide6.QtCore import QRect, QSize, Qt
from PySide6.QtWidgets import QApplication, QWidget, QPushButton, QVBoxLayout, QStyle

from src.ui.flow_layout import FlowLayout


@pytest.fixture(scope="module")
def qapp():
    """Create QApplication for the test module."""
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    yield app


@pytest.fixture
def container(qapp):
    """Create a container widget with FlowLayout."""
    widget = QWidget()
    widget.setFixedSize(400, 300)
    layout = FlowLayout(widget, margin=10, h_spacing=8, v_spacing=8)
    widget.setLayout(layout)
    return widget


@pytest.fixture
def layout(container):
    """Get the FlowLayout from the container."""
    return container.layout()


class TestFlowLayoutInit:
    """Test FlowLayout initialization."""
    
    def test_empty_layout(self, layout):
        """Test empty layout has no items."""
        assert layout.count() == 0
    
    def test_horizontal_spacing(self, layout):
        """Test horizontal spacing is set."""
        assert layout.horizontalSpacing() == 8
    
    def test_vertical_spacing(self, layout):
        """Test vertical spacing is set."""
        assert layout.verticalSpacing() == 8
    
    def test_set_horizontal_spacing(self, layout):
        """Test setting horizontal spacing."""
        layout.setHorizontalSpacing(12)
        assert layout.horizontalSpacing() == 12
    
    def test_set_vertical_spacing(self, layout):
        """Test setting vertical spacing."""
        layout.setVerticalSpacing(15)
        assert layout.verticalSpacing() == 15
    
    def test_default_spacing_fallback(self, qapp):
        """Test default spacing when not specified."""
        widget = QWidget()
        layout = FlowLayout(widget)  # No explicit spacing
        # Should use smart spacing (returns -1 or style-based value)
        h_space = layout.horizontalSpacing()
        v_space = layout.verticalSpacing()
        # Either -1 (using smart spacing) or a reasonable value
        assert h_space >= -1
        assert v_space >= -1


class TestFlowLayoutItems:
    """Test adding and removing items."""
    
    def test_add_widget(self, layout, qapp):
        """Test adding a widget."""
        btn = QPushButton("Test")
        layout.addWidget(btn)
        assert layout.count() == 1
    
    def test_add_multiple_widgets(self, layout, qapp):
        """Test adding multiple widgets."""
        for i in range(5):
            btn = QPushButton(f"Button {i}")
            layout.addWidget(btn)
        assert layout.count() == 5
    
    def test_item_at(self, layout, qapp):
        """Test itemAt returns correct item."""
        btn1 = QPushButton("First")
        btn2 = QPushButton("Second")
        layout.addWidget(btn1)
        layout.addWidget(btn2)
        
        assert layout.itemAt(0).widget() == btn1
        assert layout.itemAt(1).widget() == btn2
    
    def test_item_at_invalid_index(self, layout):
        """Test itemAt returns None for invalid index."""
        assert layout.itemAt(-1) is None
        assert layout.itemAt(100) is None
    
    def test_take_at(self, layout, qapp):
        """Test takeAt removes and returns item."""
        btn = QPushButton("Test")
        layout.addWidget(btn)
        
        assert layout.count() == 1
        item = layout.takeAt(0)
        assert item.widget() == btn
        assert layout.count() == 0
    
    def test_take_at_invalid_index(self, layout):
        """Test takeAt returns None for invalid index."""
        assert layout.takeAt(-1) is None
        assert layout.takeAt(100) is None
    
    def test_clear(self, layout, qapp):
        """Test clearing all items."""
        for i in range(5):
            btn = QPushButton(f"Button {i}")
            layout.addWidget(btn)
        
        assert layout.count() == 5
        layout.clear()
        qapp.processEvents()
        assert layout.count() == 0


class TestFlowLayoutSizing:
    """Test size calculations."""
    
    def test_minimum_size(self, layout, qapp):
        """Test minimum size calculation."""
        btn = QPushButton("Test Button")
        layout.addWidget(btn)
        
        min_size = layout.minimumSize()
        assert min_size.width() > 0
        assert min_size.height() > 0
    
    def test_size_hint(self, layout, qapp):
        """Test size hint equals minimum size."""
        btn = QPushButton("Test")
        layout.addWidget(btn)
        
        assert layout.sizeHint() == layout.minimumSize()
    
    def test_has_height_for_width(self, layout):
        """Test layout has height-for-width dependency."""
        assert layout.hasHeightForWidth() is True
    
    def test_height_for_width(self, layout, qapp):
        """Test height calculation for given width."""
        for i in range(10):
            btn = QPushButton(f"Button {i}")
            btn.setFixedSize(80, 30)
            layout.addWidget(btn)
        
        # Narrow width should require more height
        height_narrow = layout.heightForWidth(100)
        height_wide = layout.heightForWidth(500)
        
        # Narrow layout needs more height (more rows)
        assert height_narrow >= height_wide
    
    def test_expanding_directions(self, layout):
        """Test layout doesn't expand."""
        directions = layout.expandingDirections()
        assert directions == Qt.Orientations(Qt.Orientation(0))


class TestFlowLayoutGeometry:
    """Test geometry calculations."""
    
    def test_set_geometry(self, container, layout, qapp):
        """Test setGeometry arranges widgets."""
        for i in range(5):
            btn = QPushButton(f"Btn{i}")
            btn.setFixedSize(80, 30)
            layout.addWidget(btn)
        
        container.show()
        qapp.processEvents()
        
        # Widgets should be positioned
        for i in range(layout.count()):
            item = layout.itemAt(i)
            widget = item.widget()
            # Widget should have valid geometry
            assert widget.geometry().width() > 0
            assert widget.geometry().height() > 0
        
        container.hide()
    
    def test_widgets_wrap_to_next_row(self, container, layout, qapp):
        """Test widgets wrap when width exceeded."""
        # Add widgets that should wrap
        for i in range(10):
            btn = QPushButton(f"Button{i}")
            btn.setFixedSize(100, 30)
            layout.addWidget(btn)
        
        container.show()
        container.resize(350, 300)  # Not wide enough for all buttons
        qapp.processEvents()
        
        # Check that not all widgets are on same row
        first_y = layout.itemAt(0).widget().y()
        last_y = layout.itemAt(9).widget().y()
        assert last_y > first_y  # Last button should be on a different row
        
        container.hide()


class TestFlowLayoutSmartSpacing:
    """Test smart spacing functionality."""
    
    def test_smart_spacing_no_parent(self, qapp):
        """Test smart spacing without parent widget."""
        layout = FlowLayout()  # No parent
        # Should return -1 for smart spacing when no parent
        result = layout._smart_spacing(QStyle.PM_LayoutHorizontalSpacing)
        assert result == -1
    
    def test_smart_spacing_with_widget_parent(self, container, layout):
        """Test smart spacing with widget parent."""
        from PySide6.QtWidgets import QStyle
        # Layout has widget parent, should return style value
        result = layout._smart_spacing(QStyle.PM_LayoutHorizontalSpacing)
        # Should return a valid spacing value from style
        assert isinstance(result, int)


class TestFlowLayoutEdgeCases:
    """Test edge cases."""
    
    def test_empty_layout_geometry(self, layout, qapp):
        """Test geometry with empty layout."""
        layout.setGeometry(QRect(0, 0, 100, 100))
        # Should not crash
    
    def test_single_widget_layout(self, container, layout, qapp):
        """Test layout with single widget."""
        btn = QPushButton("Single")
        btn.setFixedSize(80, 30)
        layout.addWidget(btn)
        
        container.show()
        qapp.processEvents()
        
        # Widget should be positioned correctly
        widget = layout.itemAt(0).widget()
        assert widget.x() >= 10  # Margin
        assert widget.y() >= 10  # Margin
        
        container.hide()
    
    def test_widgets_with_different_sizes(self, container, layout, qapp):
        """Test layout with different sized widgets."""
        sizes = [(50, 50), (100, 30), (80, 40), (60, 60)]
        
        for w, h in sizes:
            btn = QPushButton(f"{w}x{h}")
            btn.setFixedSize(w, h)
            layout.addWidget(btn)
        
        container.show()
        qapp.processEvents()
        
        # All widgets should have valid positions
        for i in range(layout.count()):
            widget = layout.itemAt(i).widget()
            assert widget.x() >= 0
            assert widget.y() >= 0
        
        container.hide()
