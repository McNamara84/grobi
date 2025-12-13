"""Dialog for previewing and confirming schema upgrades."""

import logging
from typing import List
from pathlib import Path
from datetime import datetime

from PySide6.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QLabel, QPushButton,
    QTableWidget, QTableWidgetItem, QHeaderView, QGroupBox,
    QCheckBox, QMessageBox, QSplitter, QTabWidget, QWidget,
    QProgressBar, QTextEdit, QFileDialog
)
from PySide6.QtCore import Qt, Signal
from PySide6.QtGui import QColor, QFont

from src.workers.schema_upgrade_worker import DOIUpgradeInfo, UpgradeStatus


logger = logging.getLogger(__name__)


class SchemaUpgradePreviewDialog(QDialog):
    """Dialog for previewing and confirming schema upgrades."""
    
    # Signal emitted when user confirms upgrade
    upgrade_confirmed = Signal(list)  # List of DOIUpgradeInfo to upgrade
    
    def __init__(self, 
                 upgradeable: List[DOIUpgradeInfo],
                 not_upgradeable: List[DOIUpgradeInfo],
                 already_current: List[DOIUpgradeInfo],
                 parent=None):
        """
        Initialize the preview dialog.
        
        Args:
            upgradeable: List of DOIs that can be upgraded
            not_upgradeable: List of DOIs that cannot be upgraded (with reasons)
            already_current: List of DOIs already on current schema
            parent: Parent widget
        """
        super().__init__(parent)
        self.upgradeable = upgradeable
        self.not_upgradeable = not_upgradeable
        self.already_current = already_current
        
        self.setWindowTitle("Schema-Upgrade Vorschau")
        self.setMinimumSize(900, 600)
        self.setModal(True)
        
        self._setup_ui()
    
    def _setup_ui(self):
        """Set up the dialog UI."""
        layout = QVBoxLayout(self)
        layout.setSpacing(15)
        
        # Summary header
        summary = self._create_summary_section()
        layout.addWidget(summary)
        
        # Tab widget for different categories
        tabs = QTabWidget()
        
        # Tab 1: Upgradeable DOIs
        upgrade_tab = self._create_upgradeable_tab()
        tabs.addTab(upgrade_tab, f"✅ Upgrade-fähig ({len(self.upgradeable)})")
        
        # Tab 2: Not upgradeable DOIs
        not_upgrade_tab = self._create_not_upgradeable_tab()
        tabs.addTab(not_upgrade_tab, f"❌ Nicht upgrade-fähig ({len(self.not_upgradeable)})")
        
        # Tab 3: Already current DOIs
        current_tab = self._create_already_current_tab()
        tabs.addTab(current_tab, f"ℹ️ Bereits aktuell ({len(self.already_current)})")
        
        layout.addWidget(tabs)
        
        # Options section
        options = self._create_options_section()
        layout.addWidget(options)
        
        # Button section
        buttons = self._create_button_section()
        layout.addLayout(buttons)
    
    def _create_summary_section(self) -> QGroupBox:
        """Create the summary statistics section."""
        group = QGroupBox("Zusammenfassung")
        layout = QVBoxLayout()
        
        total = len(self.upgradeable) + len(self.not_upgradeable) + len(self.already_current)
        
        # Statistics
        stats_layout = QHBoxLayout()
        
        # Total DOIs
        total_label = QLabel(f"<b>Gesamt:</b> {total} DOIs")
        stats_layout.addWidget(total_label)
        
        stats_layout.addSpacing(30)
        
        # Upgradeable
        upgrade_label = QLabel(f"<span style='color: green;'><b>Upgrade-fähig:</b> {len(self.upgradeable)}</span>")
        stats_layout.addWidget(upgrade_label)
        
        stats_layout.addSpacing(30)
        
        # Not upgradeable
        not_upgrade_label = QLabel(f"<span style='color: red;'><b>Nicht upgrade-fähig:</b> {len(self.not_upgradeable)}</span>")
        stats_layout.addWidget(not_upgrade_label)
        
        stats_layout.addSpacing(30)
        
        # Already current
        current_label = QLabel(f"<span style='color: gray;'><b>Bereits aktuell:</b> {len(self.already_current)}</span>")
        stats_layout.addWidget(current_label)
        
        stats_layout.addStretch()
        layout.addLayout(stats_layout)
        
        # Count DOIs with Funder migration
        funder_count = sum(1 for d in self.upgradeable if d.has_funder_contributors)
        if funder_count > 0:
            funder_label = QLabel(
                f"<span style='color: orange;'>⚠️ {funder_count} DOI(s) haben 'Funder'-Contributors, "
                f"die zu fundingReferences migriert werden</span>"
            )
            layout.addWidget(funder_label)
        
        group.setLayout(layout)
        return group
    
    def _create_upgradeable_tab(self) -> QWidget:
        """Create the tab for upgradeable DOIs."""
        widget = QWidget()
        layout = QVBoxLayout(widget)
        
        if not self.upgradeable:
            label = QLabel("Keine DOIs zum Upgrade verfügbar.")
            label.setAlignment(Qt.AlignCenter)
            layout.addWidget(label)
            return widget
        
        # Info label
        info = QLabel(
            "Diese DOIs können auf Schema 4.6 aktualisiert werden. "
            "Alle erforderlichen Pflichtfelder sind vorhanden."
        )
        info.setWordWrap(True)
        layout.addWidget(info)
        
        # Table
        table = QTableWidget()
        table.setColumnCount(4)
        table.setHorizontalHeaderLabels(["DOI", "Aktuelles Schema", "Funder-Migration", "Details"])
        table.horizontalHeader().setSectionResizeMode(0, QHeaderView.Stretch)
        table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeToContents)
        table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeToContents)
        table.horizontalHeader().setSectionResizeMode(3, QHeaderView.Stretch)
        table.setRowCount(len(self.upgradeable))
        
        for row, info in enumerate(self.upgradeable):
            # DOI
            table.setItem(row, 0, QTableWidgetItem(info.doi))
            
            # Current schema
            table.setItem(row, 1, QTableWidgetItem(info.current_schema))
            
            # Funder migration
            if info.has_funder_contributors:
                funder_item = QTableWidgetItem(f"Ja ({info.funder_count})")
                funder_item.setForeground(QColor("orange"))
            else:
                funder_item = QTableWidgetItem("Nein")
            table.setItem(row, 2, funder_item)
            
            # Details
            table.setItem(row, 3, QTableWidgetItem(info.reason))
        
        layout.addWidget(table)
        return widget
    
    def _create_not_upgradeable_tab(self) -> QWidget:
        """Create the tab for non-upgradeable DOIs."""
        widget = QWidget()
        layout = QVBoxLayout(widget)
        
        if not self.not_upgradeable:
            label = QLabel("Alle DOIs können aktualisiert werden! 🎉")
            label.setAlignment(Qt.AlignCenter)
            layout.addWidget(label)
            return widget
        
        # Warning label
        warning = QLabel(
            "⚠️ Diese DOIs können NICHT automatisch aktualisiert werden, "
            "da erforderliche Pflichtfelder fehlen. "
            "Bitte manuell in DataCite Fabrica bearbeiten."
        )
        warning.setWordWrap(True)
        warning.setStyleSheet("color: red;")
        layout.addWidget(warning)
        
        # Export button
        export_btn = QPushButton("📥 Liste als CSV exportieren")
        export_btn.clicked.connect(self._export_not_upgradeable)
        layout.addWidget(export_btn)
        
        # Table
        table = QTableWidget()
        table.setColumnCount(3)
        table.setHorizontalHeaderLabels(["DOI", "Aktuelles Schema", "Grund (fehlende Felder)"])
        table.horizontalHeader().setSectionResizeMode(0, QHeaderView.Stretch)
        table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeToContents)
        table.horizontalHeader().setSectionResizeMode(2, QHeaderView.Stretch)
        table.setRowCount(len(self.not_upgradeable))
        
        for row, info in enumerate(self.not_upgradeable):
            # DOI
            doi_item = QTableWidgetItem(info.doi)
            table.setItem(row, 0, doi_item)
            
            # Current schema
            table.setItem(row, 1, QTableWidgetItem(info.current_schema))
            
            # Reason (detailed!)
            reason_item = QTableWidgetItem(info.reason)
            reason_item.setForeground(QColor("red"))
            table.setItem(row, 2, reason_item)
        
        layout.addWidget(table)
        return widget
    
    def _create_already_current_tab(self) -> QWidget:
        """Create the tab for DOIs already on current schema."""
        widget = QWidget()
        layout = QVBoxLayout(widget)
        
        if not self.already_current:
            label = QLabel("Keine DOIs sind bereits auf dem aktuellen Schema.")
            label.setAlignment(Qt.AlignCenter)
            layout.addWidget(label)
            return widget
        
        # Info label
        info = QLabel(
            "Diese DOIs verwenden bereits Schema 4.6 und benötigen kein Upgrade."
        )
        info.setWordWrap(True)
        layout.addWidget(info)
        
        # Table
        table = QTableWidget()
        table.setColumnCount(2)
        table.setHorizontalHeaderLabels(["DOI", "Schema Version"])
        table.horizontalHeader().setSectionResizeMode(0, QHeaderView.Stretch)
        table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeToContents)
        table.setRowCount(len(self.already_current))
        
        for row, info in enumerate(self.already_current):
            table.setItem(row, 0, QTableWidgetItem(info.doi))
            table.setItem(row, 1, QTableWidgetItem(info.current_schema))
        
        layout.addWidget(table)
        return widget
    
    def _create_options_section(self) -> QGroupBox:
        """Create the options section."""
        group = QGroupBox("Optionen")
        layout = QVBoxLayout()
        
        # Confirm all checkbox
        self.confirm_all_checkbox = QCheckBox(
            "Alle Upgrades ohne weitere Nachfragen durchführen"
        )
        self.confirm_all_checkbox.setToolTip(
            "Wenn aktiviert, werden alle upgrade-fähigen DOIs ohne "
            "einzelne Bestätigungen aktualisiert."
        )
        layout.addWidget(self.confirm_all_checkbox)
        
        group.setLayout(layout)
        return group
    
    def _create_button_section(self) -> QHBoxLayout:
        """Create the button section."""
        layout = QHBoxLayout()
        
        # Cancel button
        cancel_btn = QPushButton("Abbrechen")
        cancel_btn.clicked.connect(self.reject)
        layout.addWidget(cancel_btn)
        
        layout.addStretch()
        
        # Start upgrade button
        self.upgrade_btn = QPushButton(f"🚀 Upgrade starten ({len(self.upgradeable)} DOIs)")
        self.upgrade_btn.setMinimumHeight(40)
        self.upgrade_btn.setEnabled(len(self.upgradeable) > 0)
        self.upgrade_btn.clicked.connect(self._on_upgrade_clicked)
        layout.addWidget(self.upgrade_btn)
        
        return layout
    
    def _on_upgrade_clicked(self):
        """Handle upgrade button click."""
        if not self.upgradeable:
            QMessageBox.information(
                self,
                "Keine DOIs",
                "Es gibt keine DOIs zum Aktualisieren."
            )
            return
        
        # Confirm if not "confirm all" checked
        if not self.confirm_all_checkbox.isChecked():
            reply = QMessageBox.question(
                self,
                "Upgrade bestätigen",
                f"Möchtest du wirklich {len(self.upgradeable)} DOI(s) auf Schema 4.6 aktualisieren?\n\n"
                f"Diese Aktion kann nicht rückgängig gemacht werden.",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No
            )
            
            if reply != QMessageBox.Yes:
                return
        
        # Emit signal with DOIs to upgrade
        self.upgrade_confirmed.emit(self.upgradeable)
        self.accept()
    
    def _export_not_upgradeable(self):
        """Export non-upgradeable DOIs to CSV."""
        if not self.not_upgradeable:
            QMessageBox.information(
                self,
                "Keine Daten",
                "Es gibt keine nicht-upgrade-fähigen DOIs zum Exportieren."
            )
            return
        
        # Ask for save location
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        default_filename = f"nicht_upgradefaehig_{timestamp}.csv"
        
        filepath, _ = QFileDialog.getSaveFileName(
            self,
            "Nicht-upgrade-fähige DOIs exportieren",
            default_filename,
            "CSV Files (*.csv)"
        )
        
        if not filepath:
            return
        
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                # Header
                f.write("DOI,Aktuelles_Schema,Grund\n")
                
                # Data
                for info in self.not_upgradeable:
                    # Escape quotes in reason
                    reason = info.reason.replace('"', '""')
                    f.write(f'{info.doi},{info.current_schema},"{reason}"\n')
            
            QMessageBox.information(
                self,
                "Export erfolgreich",
                f"Die Liste wurde gespeichert:\n{filepath}"
            )
            
        except Exception as e:
            QMessageBox.critical(
                self,
                "Export fehlgeschlagen",
                f"Fehler beim Speichern:\n{str(e)}"
            )
    
    def is_confirm_all_checked(self) -> bool:
        """Check if 'confirm all' option is selected."""
        return self.confirm_all_checkbox.isChecked()
