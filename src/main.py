"""Main entry point for GROBI application."""

import sys
import logging
from PySide6.QtWidgets import QApplication
from PySide6.QtCore import Qt

from src.ui.main_window import MainWindow

# CRITICAL: Force PyMySQL inclusion in Nuitka build
# Without this explicit import AND USAGE, Nuitka won't include pymysql in the frozen executable
import pymysql  # noqa: F401 - Required for Nuitka packaging
_ = pymysql.__version__  # Force Nuitka to keep pymysql module


def setup_logging():
    """Configure logging for the application."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('grobi.log', encoding='utf-8'),
            logging.StreamHandler()
        ]
    )


def main():
    """Main entry point for the application."""
    # Set up logging
    setup_logging()
    logger = logging.getLogger(__name__)
    logger.info("Starting GROBI application")
    
    # Enable High DPI support
    QApplication.setHighDpiScaleFactorRoundingPolicy(
        Qt.HighDpiScaleFactorRoundingPolicy.PassThrough
    )
    
    # Create application
    app = QApplication(sys.argv)
    app.setApplicationName("GROBI")
    app.setOrganizationName("GFZ Data Services")
    
    # Create and show main window
    window = MainWindow()
    window.show()
    
    logger.info("Main window displayed")
    
    # Run event loop
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
