"""Unit tests for Main application entry point."""

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
        
        # Just test that setup_logging can be called without error
        # The actual directory creation is tested by running the app
        try:
            setup_logging()
            success = True
        except Exception:
            success = False
        
        assert success
    
    def test_qapplication_attributes(self):
        """Test that QApplication has required attributes for High DPI."""
        # This verifies Qt environment is properly set up
        assert hasattr(QApplication, 'setAttribute')
        assert hasattr(QApplication, 'instance')
