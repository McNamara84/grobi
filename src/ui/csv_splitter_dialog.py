"""Dialog for splitting CSV files by DOI prefix."""

import logging
from pathlib import Path

from PySide6.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QPushButton, QLabel,
    QFileDialog, QTextEdit, QProgressBar, QGroupBox, QSpinBox, QMessageBox
)
from PySide6.QtCore import QThread, Qt
from PySide6.QtGui import QFont

from src.workers.csv_splitter_worker import CSVSplitterWorker

logger = logging.getLogger(__name__)

# Log formatting constant
LOG_SEPARATOR = "=" * 60


class CSVSplitterDialog(QDialog):
    """Dialog for splitting CSV files by DOI prefix."""
    
    def __init__(self, parent=None):
        """Initialize the dialog."""
        super().__init__(parent)
        self.setWindowTitle("CSV-Datei aufsplitten")
        self.setModal(True)
        self.setMinimumSize(700, 500)
        
        # Worker and thread
        self.worker = None
        self.thread = None
        
        # Selected file
        self.input_file = None
        self.output_dir = None
        
        self._setup_ui()
        
        logger.info("CSV Splitter Dialog initialized")
    
    def _setup_ui(self):
        """Set up the user interface."""
        layout = QVBoxLayout(self)
        layout.setSpacing(15)
        layout.setContentsMargins(20, 20, 20, 20)
        
        # Description
        desc_label = QLabel(
            "Dieses Tool splittet große CSV-Dateien in kleinere Dateien basierend auf DOI-Präfixen.\n"
            "Beispiel: DOIs wie 10.5880/gfz.2011.100 werden gruppiert nach 10.5880/gfz.2011"
        )
        desc_label.setWordWrap(True)
        layout.addWidget(desc_label)
        
        # File selection group
        file_group = QGroupBox("Dateiauswahl")
        file_layout = QVBoxLayout()
        
        # Input file selection
        input_layout = QHBoxLayout()
        input_layout.addWidget(QLabel("CSV-Datei:"))
        self.file_label = QLabel("Keine Datei ausgewählt")
        self.file_label.setStyleSheet("font-style: italic;")
        input_layout.addWidget(self.file_label, 1)
        
        self.browse_button = QPushButton("Durchsuchen...")
        self.browse_button.clicked.connect(self._browse_file)
        input_layout.addWidget(self.browse_button)
        
        file_layout.addLayout(input_layout)
        
        # Output directory selection
        output_layout = QHBoxLayout()
        output_layout.addWidget(QLabel("Ausgabe-Ordner:"))
        self.output_label = QLabel("Automatisch (gleicher Ordner wie Eingabe)")
        self.output_label.setStyleSheet("font-style: italic;")
        output_layout.addWidget(self.output_label, 1)
        
        self.output_button = QPushButton("Ändern...")
        self.output_button.clicked.connect(self._browse_output_dir)
        self.output_button.setEnabled(False)
        output_layout.addWidget(self.output_button)
        
        file_layout.addLayout(output_layout)
        
        # Prefix level selection
        prefix_layout = QHBoxLayout()
        prefix_layout.addWidget(QLabel("Präfix-Level:"))
        
        self.prefix_spinbox = QSpinBox()
        self.prefix_spinbox.setMinimum(1)
        self.prefix_spinbox.setMaximum(4)
        self.prefix_spinbox.setValue(2)
        self.prefix_spinbox.setToolTip(
            "Level 1: 10.5880\n"
            "Level 2: 10.5880/gfz.2011 (empfohlen)\n"
            "Level 3: 10.5880/gfz.2011.100"
        )
        prefix_layout.addWidget(self.prefix_spinbox)
        
        prefix_info = QLabel("(Level 2 empfohlen für optimale Gruppierung)")
        prefix_info.setStyleSheet("font-size: 10px; color: gray;")
        prefix_layout.addWidget(prefix_info)
        prefix_layout.addStretch()
        
        file_layout.addLayout(prefix_layout)
        
        file_group.setLayout(file_layout)
        layout.addWidget(file_group)
        
        # Progress section
        progress_group = QGroupBox("Fortschritt")
        progress_layout = QVBoxLayout()
        
        self.log_text = QTextEdit()
        self.log_text.setReadOnly(True)
        self.log_text.setMaximumHeight(200)
        log_font = QFont("Courier New")
        log_font.setPointSize(9)
        self.log_text.setFont(log_font)
        progress_layout.addWidget(self.log_text)
        
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        progress_layout.addWidget(self.progress_bar)
        
        progress_group.setLayout(progress_layout)
        layout.addWidget(progress_group, 1)
        
        # Buttons
        button_layout = QHBoxLayout()
        button_layout.addStretch()
        
        self.start_button = QPushButton("Splitting starten")
        self.start_button.setEnabled(False)
        self.start_button.setMinimumWidth(150)
        self.start_button.clicked.connect(self._start_splitting)
        button_layout.addWidget(self.start_button)
        
        self.close_button = QPushButton("Schließen")
        self.close_button.setMinimumWidth(100)
        self.close_button.clicked.connect(self.close)
        button_layout.addWidget(self.close_button)
        
        layout.addLayout(button_layout)
    
    def _browse_file(self):
        """Open file browser to select CSV file."""
        file_path, _ = QFileDialog.getOpenFileName(
            self,
            "CSV-Datei auswählen",
            str(Path.home()),
            "CSV Files (*.csv);;All Files (*.*)"
        )
        
        if file_path:
            self.input_file = Path(file_path)
            self.file_label.setText(self.input_file.name)
            self.file_label.setStyleSheet("font-weight: bold;")
            
            # Set default output directory to same as input
            self.output_dir = self.input_file.parent / "split_output"
            self.output_label.setText(str(self.output_dir))
            self.output_label.setStyleSheet("font-weight: normal;")
            
            self.start_button.setEnabled(True)
            self.output_button.setEnabled(True)
            
            self._log(f"Datei ausgewählt: {self.input_file}")
    
    def _browse_output_dir(self):
        """Open directory browser to select output directory."""
        dir_path = QFileDialog.getExistingDirectory(
            self,
            "Ausgabe-Ordner auswählen",
            str(self.output_dir or Path.home())
        )
        
        if dir_path:
            self.output_dir = Path(dir_path)
            self.output_label.setText(str(self.output_dir))
            self._log(f"Ausgabe-Ordner: {self.output_dir}")
    
    def _start_splitting(self):
        """Start the CSV splitting operation."""
        if not self.input_file or not self.input_file.exists():
            QMessageBox.warning(self, "Fehler", "Bitte wählen Sie eine gültige CSV-Datei aus.")
            return
        
        # Disable controls during processing
        self.start_button.setEnabled(False)
        self.browse_button.setEnabled(False)
        self.output_button.setEnabled(False)
        self.prefix_spinbox.setEnabled(False)
        self.progress_bar.setVisible(True)
        self.progress_bar.setRange(0, 0)  # Indeterminate
        
        self._log(LOG_SEPARATOR)
        self._log(f"Starte Splitting von: {self.input_file.name}")
        self._log(f"Ausgabe-Ordner: {self.output_dir}")
        self._log(f"Präfix-Level: {self.prefix_spinbox.value()}")
        self._log(LOG_SEPARATOR)
        
        # Create worker and thread
        self.worker = CSVSplitterWorker(
            self.input_file,
            self.output_dir,
            self.prefix_spinbox.value()
        )
        self.thread = QThread()
        self.worker.moveToThread(self.thread)
        
        # Connect signals
        self.thread.started.connect(self.worker.run)
        self.worker.progress.connect(self._on_progress)
        self.worker.finished.connect(self._on_finished)
        self.worker.error.connect(self._on_error)
        self.worker.finished.connect(self.thread.quit)
        self.worker.error.connect(self.thread.quit)
        self.thread.finished.connect(self._cleanup_thread)
        
        # Start processing
        self.thread.start()
    
    def _on_progress(self, message: str):
        """Handle progress updates."""
        self._log(message)
    
    def _on_finished(self, total_rows: int, prefix_counts: dict):
        """Handle successful completion."""
        self.progress_bar.setRange(0, 1)
        self.progress_bar.setValue(1)
        self.progress_bar.setVisible(False)
        
        self._log(LOG_SEPARATOR)
        self._log(f"✅ ERFOLGREICH: {total_rows} DOIs in {len(prefix_counts)} Dateien aufgeteilt")
        self._log(LOG_SEPARATOR)
        self._log("\nÜbersicht:")
        
        for prefix, count in sorted(prefix_counts.items()):
            self._log(f"  {prefix}: {count} DOIs")
        
        self._log(f"\nAusgabe-Ordner: {self.output_dir}")
        
        QMessageBox.information(
            self,
            "Erfolgreich",
            f"CSV-Datei erfolgreich aufgeteilt!\n\n"
            f"{total_rows} DOIs wurden in {len(prefix_counts)} Dateien aufgeteilt.\n\n"
            f"Ausgabe: {self.output_dir}"
        )
        
        self._reset_controls()
    
    def _on_error(self, error_message: str):
        """Handle errors."""
        self.progress_bar.setVisible(False)
        self._log(f"❌ FEHLER: {error_message}")
        
        QMessageBox.critical(
            self,
            "Fehler",
            f"Fehler beim Aufsplitten der CSV-Datei:\n\n{error_message}"
        )
        
        self._reset_controls()
    
    def _cleanup_thread(self):
        """Clean up thread and worker."""
        if self.thread:
            if self.thread.isRunning():
                self.thread.wait()
            self.thread.deleteLater()
            self.thread = None
        if self.worker:
            self.worker.deleteLater()
            self.worker = None
    
    def _reset_controls(self):
        """Re-enable controls after processing."""
        self.start_button.setEnabled(True)
        self.browse_button.setEnabled(True)
        self.output_button.setEnabled(True)
        self.prefix_spinbox.setEnabled(True)
    
    def _log(self, message: str):
        """Add message to log."""
        self.log_text.append(message)
        # Auto-scroll to bottom
        cursor = self.log_text.textCursor()
        cursor.movePosition(cursor.MoveOperation.End)
        self.log_text.setTextCursor(cursor)
    
    def closeEvent(self, event):
        """Handle dialog close event."""
        # Don't allow closing while processing
        if self.thread and self.thread.isRunning():
            QMessageBox.warning(
                self,
                "Vorgang läuft",
                "Bitte warten Sie, bis das Splitting abgeschlossen ist."
            )
            event.ignore()
        else:
            event.accept()
