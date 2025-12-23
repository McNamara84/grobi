"""Unit tests for FujiTile widget."""

import pytest
from PySide6.QtCore import Qt, QSize
from PySide6.QtGui import QColor
from PySide6.QtWidgets import QApplication

from src.ui.fuji_tile import FujiTile


@pytest.fixture(scope="module")
def qapp():
    """Create QApplication for the test module."""
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    yield app


@pytest.fixture
def tile(qapp):
    """Create a basic FujiTile."""
    return FujiTile("10.5880/GFZ.1.1.2021.001", 54.17)


@pytest.fixture
def error_tile(qapp):
    """Create a FujiTile with error state."""
    return FujiTile("10.5880/GFZ.error", -1)


@pytest.fixture
def pending_tile(qapp):
    """Create a pending FujiTile."""
    return FujiTile("10.5880/GFZ.pending", -1)


class TestFujiTileInit:
    """Test FujiTile initialization."""
    
    def test_doi_stored(self, tile):
        """Test that DOI is stored correctly."""
        assert tile.doi == "10.5880/GFZ.1.1.2021.001"
    
    def test_score_stored(self, tile):
        """Test that score is stored correctly."""
        assert tile.score_percent == 54.17
    
    def test_default_size(self, tile):
        """Test default tile size."""
        assert tile._size == 100
    
    def test_minimum_size(self, tile):
        """Test minimum size is set."""
        min_size = tile.minimumSize()
        assert min_size.width() >= 50
        assert min_size.height() >= 50
    
    def test_cursor_pointing_hand(self, tile):
        """Test cursor is pointing hand."""
        assert tile.cursor().shape() == Qt.PointingHandCursor


class TestFujiTileScore:
    """Test score-related functionality."""
    
    def test_set_score(self, tile):
        """Test setting a new score."""
        tile.set_score(75.0)
        assert tile.score_percent == 75.0
    
    def test_set_error_score(self, tile):
        """Test setting error score."""
        tile.set_score(-1)
        assert tile.score_percent == -1
    
    def test_score_update_triggers_repaint(self, tile, qapp):
        """Test that score update triggers widget update."""
        # Just verify no exception is raised
        tile.set_score(80.0)
        qapp.processEvents()


class TestFujiTileSize:
    """Test tile sizing."""
    
    def test_set_tile_size(self, tile):
        """Test setting tile size."""
        tile.set_tile_size(120)
        assert tile._size == 120
        assert tile.width() == 120
        assert tile.height() == 120
    
    def test_size_clamped_minimum(self, tile):
        """Test size is clamped to minimum."""
        tile.set_tile_size(30)  # Below minimum
        assert tile._size >= 50
    
    def test_size_clamped_maximum(self, tile):
        """Test size is clamped to maximum."""
        tile.set_tile_size(300)  # Above maximum
        assert tile._size <= 200
    
    def test_size_hint(self, tile):
        """Test sizeHint returns correct size."""
        tile.set_tile_size(100)
        hint = tile.sizeHint()
        assert hint == QSize(100, 100)
    
    def test_minimum_size_hint(self, tile):
        """Test minimumSizeHint returns minimum size."""
        hint = tile.minimumSizeHint()
        assert hint == QSize(50, 50)


class TestFujiTileColors:
    """Test color calculations."""
    
    def test_error_color_gray(self, error_tile):
        """Test error tile has gray background."""
        color = error_tile._calculate_background_color()
        assert color == FujiTile.COLOR_ERROR
    
    def test_zero_percent_red(self, qapp):
        """Test 0% score is red."""
        tile = FujiTile("test", 0)
        color = tile._calculate_background_color()
        # Should be close to dark red
        assert color.red() > color.green()
        assert color.red() > color.blue()
    
    def test_fifty_percent_yellow(self, qapp):
        """Test 50% score is yellow."""
        tile = FujiTile("test", 50)
        color = tile._calculate_background_color()
        # Should be yellow-ish
        assert color.red() > 200
        assert color.green() > 200
    
    def test_hundred_percent_green(self, qapp):
        """Test 100% score is green."""
        tile = FujiTile("test", 100)
        color = tile._calculate_background_color()
        # Should be close to dark green
        assert color.green() > color.red()
    
    def test_negative_score_clamped(self, qapp):
        """Test negative score returns error color."""
        tile = FujiTile("test", -5)
        color = tile._calculate_background_color()
        assert color == FujiTile.COLOR_ERROR
    
    def test_over_hundred_clamped(self, qapp):
        """Test score over 100 is clamped."""
        tile = FujiTile("test", 150)
        color = tile._calculate_background_color()
        # Should be same as 100%
        tile100 = FujiTile("test100", 100)
        color100 = tile100._calculate_background_color()
        assert color == color100


class TestFujiTileTextColor:
    """Test text color contrast."""
    
    def test_dark_background_white_text(self, qapp):
        """Test dark background gets white text."""
        tile = FujiTile("test", 0)  # Dark red background
        text_color = tile._get_text_color()
        assert text_color == QColor(Qt.white)
    
    def test_light_background_black_text(self, qapp):
        """Test light background gets black text."""
        tile = FujiTile("test", 50)  # Yellow background (bright)
        text_color = tile._get_text_color()
        assert text_color == QColor(Qt.black)


class TestFujiTileDoiDisplay:
    """Test DOI display formatting."""
    
    def test_doi_suffix_extracted(self, tile):
        """Test DOI suffix is extracted correctly."""
        suffix = tile._get_doi_suffix()
        assert suffix == "GFZ.1.1.2021.001"
    
    def test_doi_suffix_no_slash(self, qapp):
        """Test DOI without slash returns full DOI."""
        tile = FujiTile("simple-doi", 50)
        suffix = tile._get_doi_suffix()
        assert suffix == "simple-doi"
    
    def test_doi_suffix_multiple_slashes(self, qapp):
        """Test DOI with multiple slashes returns last part."""
        tile = FujiTile("10.5880/GFZ/subdir/item", 50)
        suffix = tile._get_doi_suffix()
        assert suffix == "item"


class TestFujiTileTooltip:
    """Test tooltip functionality."""
    
    def test_tooltip_with_score(self, tile):
        """Test tooltip shows score."""
        tooltip = tile.toolTip()
        assert "10.5880/GFZ.1.1.2021.001" in tooltip
        assert "54.2%" in tooltip or "54.17%" in tooltip
    
    def test_tooltip_with_error(self, error_tile):
        """Test tooltip shows error status."""
        tooltip = error_tile.toolTip()
        assert "10.5880/GFZ.error" in tooltip
        assert "Fehler" in tooltip or "ausstehend" in tooltip


class TestFujiTileFontSize:
    """Test font size calculation."""
    
    def test_font_size_for_short_text(self, tile):
        """Test font size for short text."""
        size = tile._calculate_font_size("short", 100)
        assert size >= 6
        assert size <= 12
    
    def test_font_size_for_long_text(self, tile):
        """Test font size for long text."""
        size = tile._calculate_font_size("very_long_doi_suffix_text", 50)
        # Should return smaller size for long text
        assert size >= 6
    
    def test_minimum_font_size(self, tile):
        """Test minimum font size is 6."""
        size = tile._calculate_font_size("x" * 100, 10)
        assert size >= 6


class TestFujiTilePainting:
    """Test painting functionality."""
    
    def test_paint_event_no_crash(self, tile, qapp):
        """Test paintEvent doesn't crash."""
        # Force a repaint
        tile.show()
        tile.repaint()
        qapp.processEvents()
        tile.hide()
    
    def test_paint_error_tile(self, error_tile, qapp):
        """Test painting error tile doesn't crash."""
        error_tile.show()
        error_tile.repaint()
        qapp.processEvents()
        error_tile.hide()


class TestFujiTileSignals:
    """Test signal emission."""
    
    def test_click_emits_signal(self, tile, qapp, qtbot):
        """Test clicking tile emits clicked signal."""
        with qtbot.waitSignal(tile.clicked, timeout=1000) as blocker:
            qtbot.mouseClick(tile, Qt.LeftButton)
        
        assert blocker.args[0] == "10.5880/GFZ.1.1.2021.001"
