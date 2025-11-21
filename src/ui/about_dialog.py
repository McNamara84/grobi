"""About dialog for GROBI application."""

import logging
from pathlib import Path

from PySide6.QtWidgets import (
    QDialog, QVBoxLayout, QLabel, QPushButton, QWidget
)
from PySide6.QtCore import Qt, QUrl
from PySide6.QtGui import QPixmap, QDesktopServices

from src.__version__ import (
    __version__, __author__, __organization__, __license__, __url__, __description__
)


logger = logging.getLogger(__name__)


class AboutDialog(QDialog):
    """About dialog showing application information."""
    
    def __init__(self, parent=None):
        """
        Initialize the About dialog.
        
        Args:
            parent: Parent widget
        """
        super().__init__(parent)
        self.setWindowTitle("Über GROBI")
        self.setFixedSize(450, 550)
        self.setModal(True)
        
        self._setup_ui()
        logger.info("About dialog initialized")
    
    def _setup_ui(self):
        """Set up the user interface."""
        layout = QVBoxLayout(self)
        layout.setSpacing(15)
        layout.setContentsMargins(30, 30, 30, 30)
        
        # Logo
        logo_label = QLabel()
        logo_label.setAlignment(Qt.AlignCenter)
        logo_path = Path(__file__).parent / "GROBI-Logo.ico"
        if logo_path.exists():
            pixmap = QPixmap(str(logo_path))
            # Scale to 128x128 for prominent display
            pixmap = pixmap.scaled(128, 128, Qt.KeepAspectRatio, Qt.SmoothTransformation)
            logo_label.setPixmap(pixmap)
        layout.addWidget(logo_label)
        
        # Spacing after logo
        layout.addSpacing(10)
        
        # Title: GROBI
        title_label = QLabel("GROBI")
        title_label.setAlignment(Qt.AlignCenter)
        title_label.setStyleSheet("font-size: 24pt; font-weight: bold;")
        layout.addWidget(title_label)
        
        # Version
        version_label = QLabel(f"Version {__version__}")
        version_label.setAlignment(Qt.AlignCenter)
        version_label.setStyleSheet("font-size: 12pt;")
        layout.addWidget(version_label)
        
        # Spacing
        layout.addSpacing(10)
        
        # Description
        description_label = QLabel(__description__)
        description_label.setAlignment(Qt.AlignCenter)
        description_label.setWordWrap(True)
        description_label.setStyleSheet("font-size: 10pt;")
        layout.addWidget(description_label)
        
        # Short info
        info_label = QLabel(
            "A modern GUI tool for GFZ Data Services\n"
            "to manage DataCite DOIs"
        )
        info_label.setAlignment(Qt.AlignCenter)
        info_label.setWordWrap(True)
        info_label.setStyleSheet("font-size: 9pt; color: #666;")
        layout.addWidget(info_label)
        
        # Spacing
        layout.addSpacing(15)
        
        # Author information
        author_container = QWidget()
        author_layout = QVBoxLayout(author_container)
        author_layout.setSpacing(5)
        author_layout.setContentsMargins(0, 0, 0, 0)
        
        copyright_label = QLabel(f"© 2025 {__author__}")
        copyright_label.setAlignment(Qt.AlignCenter)
        copyright_label.setStyleSheet("font-size: 9pt;")
        author_layout.addWidget(copyright_label)
        
        org_label = QLabel(__organization__)
        org_label.setAlignment(Qt.AlignCenter)
        org_label.setWordWrap(True)
        org_label.setStyleSheet("font-size: 9pt;")
        author_layout.addWidget(org_label)
        
        layout.addWidget(author_container)
        
        # Spacing
        layout.addSpacing(15)
        
        # License
        license_label = QLabel(f"Lizenz: {__license__}")
        license_label.setAlignment(Qt.AlignCenter)
        license_label.setStyleSheet("font-size: 9pt;")
        layout.addWidget(license_label)
        
        # Link buttons
        button_container = QWidget()
        button_layout = QVBoxLayout(button_container)
        button_layout.setSpacing(8)
        button_layout.setContentsMargins(0, 0, 0, 0)
        
        # GitHub button
        github_button = QPushButton("🐙 GitHub-Repository öffnen")
        github_button.setMinimumHeight(35)
        github_button.clicked.connect(self._open_github)
        github_button.setCursor(Qt.PointingHandCursor)
        button_layout.addWidget(github_button)
        
        # Changelog button
        changelog_button = QPushButton("📝 Changelog anzeigen")
        changelog_button.setMinimumHeight(35)
        changelog_button.clicked.connect(self._open_changelog)
        changelog_button.setCursor(Qt.PointingHandCursor)
        button_layout.addWidget(changelog_button)
        
        # License button
        license_button = QPushButton("📄 Lizenz anzeigen")
        license_button.setMinimumHeight(35)
        license_button.clicked.connect(self._open_license)
        license_button.setCursor(Qt.PointingHandCursor)
        button_layout.addWidget(license_button)
        
        layout.addWidget(button_container)
        
        # Spacing
        layout.addSpacing(10)
        
        # OK button
        ok_button = QPushButton("OK")
        ok_button.setMinimumHeight(40)
        ok_button.setDefault(True)
        ok_button.clicked.connect(self.accept)
        layout.addWidget(ok_button)
    
    def _open_github(self):
        """Open GitHub repository in browser."""
        try:
            QDesktopServices.openUrl(QUrl(__url__))
            logger.info(f"Opened GitHub repository: {__url__}")
        except Exception as e:
            logger.error(f"Failed to open GitHub URL: {e}")
    
    def _open_changelog(self):
        """Open CHANGELOG.md in default application."""
        try:
            # Try to find CHANGELOG.md in project root
            changelog_path = Path(__file__).parent.parent.parent / "CHANGELOG.md"
            
            if changelog_path.exists():
                # Open with default application
                url = QUrl.fromLocalFile(str(changelog_path.resolve()))
                QDesktopServices.openUrl(url)
                logger.info(f"Opened CHANGELOG.md: {changelog_path}")
            else:
                # Fallback: Open GitHub Releases page
                releases_url = f"{__url__}/releases"
                QDesktopServices.openUrl(QUrl(releases_url))
                logger.info(f"CHANGELOG.md not found, opened releases page: {releases_url}")
        except Exception as e:
            logger.error(f"Failed to open changelog: {e}")
            # Last resort: Try GitHub releases
            try:
                releases_url = f"{__url__}/releases"
                QDesktopServices.openUrl(QUrl(releases_url))
            except Exception as e2:
                logger.error(f"Failed to open releases page: {e2}")
    
    def _open_license(self):
        """Open LICENSE file in default application."""
        try:
            # Try to find LICENSE in project root
            license_path = Path(__file__).parent.parent.parent / "LICENSE"
            
            if license_path.exists():
                # Open with default application
                url = QUrl.fromLocalFile(str(license_path.resolve()))
                QDesktopServices.openUrl(url)
                logger.info(f"Opened LICENSE: {license_path}")
            else:
                # Fallback: Open GitHub license page
                license_url = f"{__url__}/blob/main/LICENSE"
                QDesktopServices.openUrl(QUrl(license_url))
                logger.info(f"LICENSE not found, opened GitHub license page: {license_url}")
        except Exception as e:
            logger.error(f"Failed to open license: {e}")
            # Last resort: Try GitHub license page
            try:
                license_url = f"{__url__}/blob/main/LICENSE"
                QDesktopServices.openUrl(QUrl(license_url))
            except Exception as e2:
                logger.error(f"Failed to open GitHub license page: {e2}")
