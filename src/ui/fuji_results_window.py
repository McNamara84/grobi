"""FAIR Assessment Results Window displaying DOI tiles."""

import logging
from typing import Dict

from PySide6.QtWidgets import (
    QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, QScrollArea,
    QLabel, QPushButton, QStatusBar, QFrame, QMessageBox
)
from PySide6.QtCore import Qt, Signal, Slot
from PySide6.QtGui import QFont, QCloseEvent

from src.ui.flow_layout import FlowLayout
from src.ui.fuji_tile import FujiTile


logger = logging.getLogger(__name__)


class FujiResultsWindow(QMainWindow):
    """
    Window for displaying FAIR assessment results as colored tiles.
    
    Features:
    - Responsive tile grid that adjusts to window size
    - Dynamic tile sizing based on DOI count
    - Real-time updates as assessments complete
    - Status bar showing progress
    """
    
    # Signals
    closed = Signal()  # Emitted when window is closed
    assessment_cancelled = Signal()  # Emitted when user cancels
    
    def __init__(self, parent=None, theme_manager=None):
        """
        Initialize the results window.
        
        Args:
            parent: Parent widget
            theme_manager: Optional theme manager for styling
        """
        super().__init__(parent)
        
        self.theme_manager = theme_manager
        self.tiles: Dict[str, FujiTile] = {}
        self.total_dois = 0
        self.completed_count = 0
        self.error_count = 0
        self._is_running = False
        
        self._setup_ui()
        self._apply_styles()
        
        # Window settings
        self.setWindowTitle("F-UJI FAIR Assessment")
        self.setMinimumSize(600, 400)
        self.resize(900, 700)
    
    def _setup_ui(self):
        """Set up the user interface."""
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        
        main_layout = QVBoxLayout(central_widget)
        main_layout.setSpacing(10)
        main_layout.setContentsMargins(10, 10, 10, 10)
        
        # Header
        header_layout = QHBoxLayout()
        
        self.title_label = QLabel("FAIR Assessment Ergebnisse")
        title_font = QFont()
        title_font.setPointSize(14)
        title_font.setBold(True)
        self.title_label.setFont(title_font)
        header_layout.addWidget(self.title_label)
        
        header_layout.addStretch()
        
        # Cancel/Close button
        self.action_button = QPushButton("Abbrechen")
        self.action_button.clicked.connect(self._on_action_button_clicked)
        header_layout.addWidget(self.action_button)
        
        main_layout.addLayout(header_layout)
        
        # Info label
        self.info_label = QLabel("Starte FAIR Assessment...")
        self.info_label.setWordWrap(True)
        main_layout.addWidget(self.info_label)
        
        # Separator
        separator = QFrame()
        separator.setFrameShape(QFrame.HLine)
        separator.setFrameShadow(QFrame.Sunken)
        main_layout.addWidget(separator)
        
        # Scroll area for tiles
        self.scroll_area = QScrollArea()
        self.scroll_area.setWidgetResizable(True)
        self.scroll_area.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self.scroll_area.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        
        # Container widget for tiles
        self.tiles_container = QWidget()
        self.flow_layout = FlowLayout(self.tiles_container, margin=10, h_spacing=8, v_spacing=8)
        self.tiles_container.setLayout(self.flow_layout)
        
        self.scroll_area.setWidget(self.tiles_container)
        main_layout.addWidget(self.scroll_area, 1)  # Stretch factor 1
        
        # Status bar
        self.status_bar = QStatusBar()
        self.setStatusBar(self.status_bar)
        self.status_bar.showMessage("Bereit")
    
    def _apply_styles(self):
        """Apply theme-aware styles."""
        if self.theme_manager:
            # Theme manager will handle styling
            pass
        
        # Default styling for scroll area
        self.scroll_area.setStyleSheet("""
            QScrollArea {
                border: 1px solid #ccc;
                border-radius: 5px;
            }
        """)
    
    def start_assessment(self, total_dois: int):
        """
        Start a new assessment run.
        
        Args:
            total_dois: Total number of DOIs to assess
        """
        self.total_dois = total_dois
        self.completed_count = 0
        self.error_count = 0
        self._is_running = True
        
        # Clear existing tiles
        self.flow_layout.clear()
        self.tiles.clear()
        
        self.action_button.setText("Abbrechen")
        self.info_label.setText(f"Bewerte {total_dois} DOIs nach FAIR-Kriterien...")
        self._update_status()
        
        logger.info(f"Starting FAIR assessment for {total_dois} DOIs")
    
    @Slot(str, float)
    def add_result(self, doi: str, score_percent: float):
        """
        Add or update a DOI result.
        
        Args:
            doi: The DOI identifier
            score_percent: FAIR score (0-100) or -1 for error
        """
        if doi in self.tiles:
            # Update existing tile
            self.tiles[doi].set_score(score_percent)
        else:
            # Create new tile
            tile = FujiTile(doi, score_percent)
            tile.clicked.connect(self._on_tile_clicked)
            
            # Calculate tile size based on count
            tile_size = self._calculate_tile_size()
            tile.set_tile_size(tile_size)
            
            self.tiles[doi] = tile
            self.flow_layout.addWidget(tile)
        
        # Update counters
        self.completed_count += 1
        if score_percent < 0:
            self.error_count += 1
        
        self._update_status()
        self._recalculate_tile_sizes()
        
        # Check if complete
        if self.completed_count >= self.total_dois:
            self._on_assessment_complete()
    
    def _calculate_tile_size(self) -> int:
        """
        Calculate optimal tile size based on window width and DOI count.
        
        Returns:
            Tile size in pixels
        """
        # Get available width
        available_width = self.scroll_area.viewport().width() - 30  # Account for margins
        
        # Determine minimum columns based on DOI count
        tile_count = max(self.total_dois, len(self.tiles), 1)
        
        if tile_count <= 20:
            min_columns = 4
        elif tile_count <= 50:
            min_columns = 6
        elif tile_count <= 100:
            min_columns = 8
        else:
            min_columns = 10
        
        # Calculate tile size
        tile_size = (available_width - (min_columns - 1) * 8) // min_columns
        
        # Clamp to reasonable range
        tile_size = max(50, min(150, tile_size))
        
        return tile_size
    
    def _recalculate_tile_sizes(self):
        """Recalculate and update all tile sizes."""
        tile_size = self._calculate_tile_size()
        
        for tile in self.tiles.values():
            tile.set_tile_size(tile_size)
    
    def _update_status(self):
        """Update the status bar."""
        if self._is_running:
            msg = f"{self.completed_count} von {self.total_dois} DOIs bewertet"
            if self.error_count > 0:
                msg += f" ({self.error_count} Fehler)"
            self.status_bar.showMessage(msg)
        else:
            self.status_bar.showMessage("Fertig")
    
    def _on_assessment_complete(self):
        """Called when all assessments are complete."""
        self._is_running = False
        self.action_button.setText("Schließen")
        
        success_count = self.completed_count - self.error_count
        
        # Calculate average score
        scores = [t.score_percent for t in self.tiles.values() if t.score_percent >= 0]
        avg_score = sum(scores) / len(scores) if scores else 0
        
        self.info_label.setText(
            f"Assessment abgeschlossen: {success_count} von {self.total_dois} DOIs erfolgreich bewertet. "
            f"Durchschnittlicher FAIR-Score: {avg_score:.1f}%"
        )
        
        self.status_bar.showMessage(
            f"Fertig - {success_count} erfolgreich, {self.error_count} Fehler"
        )
        
        logger.info(f"FAIR assessment complete: {success_count} success, {self.error_count} errors, avg score: {avg_score:.1f}%")
    
    def _on_action_button_clicked(self):
        """Handle action button click."""
        if self._is_running:
            # Confirm cancellation
            reply = QMessageBox.question(
                self,
                "Assessment abbrechen",
                "Möchtest du das laufende Assessment wirklich abbrechen?",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No
            )
            if reply == QMessageBox.Yes:
                self._is_running = False
                self.assessment_cancelled.emit()
                self.action_button.setText("Schließen")
                self.info_label.setText("Assessment abgebrochen.")
                self.status_bar.showMessage("Abgebrochen")
        else:
            self.close()
    
    def _on_tile_clicked(self, doi: str):
        """Handle tile click (placeholder for future functionality)."""
        logger.debug(f"Tile clicked: {doi}")
        # Future: Show detailed results dialog
    
    def resizeEvent(self, event):
        """Handle window resize."""
        super().resizeEvent(event)
        # Recalculate tile sizes on resize
        self._recalculate_tile_sizes()
    
    def closeEvent(self, event: QCloseEvent):
        """Handle window close."""
        if self._is_running:
            reply = QMessageBox.question(
                self,
                "Fenster schließen",
                "Das Assessment läuft noch. Möchtest du es abbrechen und das Fenster schließen?",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No
            )
            if reply == QMessageBox.No:
                event.ignore()
                return
            
            self.assessment_cancelled.emit()
        
        self.closed.emit()
        event.accept()

    def _on_cancel_during_close(self):
        """Handle cancel during close (placeholder for cleanup)."""
        _ = self.completed_count - self.error_count  # Used in _on_action_button_clicked
