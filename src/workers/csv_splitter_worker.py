"""Worker for CSV splitting operations."""

import logging
from pathlib import Path

from PySide6.QtCore import QObject, Signal

from src.utils.csv_splitter import split_csv_by_doi_prefix, CSVSplitError

logger = logging.getLogger(__name__)


class CSVSplitterWorker(QObject):
    """Worker for splitting CSV files by DOI prefix in a separate thread."""
    
    # Signals
    progress = Signal(str)  # Progress message
    finished = Signal(int, dict)  # total_rows, prefix_counts
    error = Signal(str)  # Error message
    
    def __init__(self, input_file: Path, output_dir: Path, prefix_level: int = 2):
        """
        Initialize the worker.
        
        Args:
            input_file: Path to input CSV file
            output_dir: Directory to write output files
            prefix_level: Level of DOI prefix to use for splitting (1-4)
        
        Raises:
            ValueError: If prefix_level is not between 1 and 4
        """
        super().__init__()
        
        if not 1 <= prefix_level <= 4:
            raise ValueError(f"prefix_level muss zwischen 1 und 4 liegen, erhalten: {prefix_level}")
        
        self.input_file = input_file
        self.output_dir = output_dir
        self.prefix_level = prefix_level
        self._is_running = True
    
    def run(self):
        """Execute CSV splitting operation."""
        try:
            self.progress.emit("Starte CSV-Splitting...")
            
            total_rows, prefix_counts = split_csv_by_doi_prefix(
                self.input_file,
                self.output_dir,
                self.prefix_level,
                progress_callback=self._on_progress,
                should_stop=lambda: not self._is_running
            )
            
            self.progress.emit("[OK] CSV-Splitting erfolgreich abgeschlossen")
            self.finished.emit(total_rows, prefix_counts)
            
        except CSVSplitError as e:
            self.error.emit(str(e))
        except Exception as e:
            logger.exception("Unexpected error in CSV splitter worker")
            self.error.emit(f"Unerwarteter Fehler ({type(e).__name__}): {str(e)}")
    
    def _on_progress(self, message: str):
        """Forward progress messages."""
        self.progress.emit(message)
    
    def stop(self):
        """Request graceful stop of the operation."""
        self._is_running = False
        logger.info("CSV Splitter worker stop requested")
