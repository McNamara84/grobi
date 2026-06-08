"""Unit tests for Main application entry point."""

from unittest.mock import Mock, patch

from PySide6.QtWidgets import QApplication


class TestMainModule:
    """Test main module functionality."""
    
    def test_main_module_imports(self):
        """Test that main module can be imported."""
        from src.main import main, setup_logging
        assert callable(main)
        assert callable(setup_logging)
    
    def test_setup_logging_callable(self):
        """Test that setup_logging function can be called without error."""
        from src.main import setup_logging
        
        # If setup_logging raises an exception, the test will fail
        # which is the desired behavior
        setup_logging()
    
    def test_qapplication_attributes(self):
        """Test that QApplication has required attributes for High DPI."""
        # This verifies Qt environment is properly set up
        assert hasattr(QApplication, 'setAttribute')
        assert hasattr(QApplication, 'instance')

    @patch('src.main.sys.exit')
    @patch('src.main.MainWindow')
    @patch('src.main.QApplication')
    def test_main_creates_application_and_window(
        self,
        mock_qapplication,
        mock_main_window_class,
        mock_exit
    ):
        """Test main() orchestration without starting a real Qt event loop."""
        from src.main import main

        mock_app = Mock()
        mock_app.exec.return_value = 0
        mock_qapplication.return_value = mock_app
        mock_window = Mock()
        mock_main_window_class.return_value = mock_window

        main()

        mock_qapplication.setHighDpiScaleFactorRoundingPolicy.assert_called_once()
        mock_qapplication.assert_called_once()
        mock_app.setApplicationName.assert_called_once_with("GROBI")
        mock_app.setOrganizationName.assert_called_once_with("GFZ Data Services")
        mock_window.show.assert_called_once()
        mock_exit.assert_called_once_with(0)
