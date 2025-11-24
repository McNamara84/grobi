"""Tests for log file creation with skipped_details (Phase 4)."""

from unittest.mock import patch

from src.ui.main_window import MainWindow


class TestLogFilesWithSkippedDetails:
    """Test log file creation includes skipped_details sections."""
    
    def test_update_log_includes_skipped_details_section(self, qtbot, tmp_path):
        """Test that URL update log includes ÜBERSPRUNGENE DOIs section."""
        main_window = MainWindow()
        qtbot.addWidget(main_window)
        
        # Mock os.getcwd to return tmp_path
        with patch('src.ui.main_window.os.getcwd', return_value=str(tmp_path)):
            error_list = ["10.5880/test.001: DOI nicht gefunden"]
            skipped_details = [
                ("10.5880/test.002", "URL unverändert: https://example.org/data1"),
                ("10.5880/test.003", "URL unverändert: https://example.org/data2")
            ]
            
            main_window._create_update_log(
                success_count=5,
                skipped_count=2,
                error_count=1,
                error_list=error_list,
                skipped_details=skipped_details
            )
            
            # Find log file
            log_files = list(tmp_path.glob("update_log_*.txt"))
            assert len(log_files) == 1
            
            log_content = log_files[0].read_text(encoding='utf-8')
            
            # Verify summary section
            assert "Gesamt: 8 DOIs" in log_content
            assert "Erfolgreich aktualisiert: 5" in log_content
            assert "Übersprungen (keine Änderungen): 2" in log_content
            assert "Fehlgeschlagen: 1" in log_content
            
            # Verify efficiency section
            assert "EFFIZIENZ:" in log_content
            assert "API-Calls vermieden: 2/8 (25.0%)" in log_content
            
            # Verify skipped details section
            assert "ÜBERSPRUNGENE DOIs (keine Änderungen):" in log_content
            assert "10.5880/test.002" in log_content
            assert "Grund: URL unverändert: https://example.org/data1" in log_content
            assert "10.5880/test.003" in log_content
            assert "Grund: URL unverändert: https://example.org/data2" in log_content
            
            # Verify error section still exists
            assert "FEHLER:" in log_content
            assert "10.5880/test.001: DOI nicht gefunden" in log_content
    
    def test_update_log_without_skipped_details(self, qtbot, tmp_path):
        """Test URL update log when no DOIs were skipped."""
        main_window = MainWindow()
        qtbot.addWidget(main_window)
        
        with patch('src.ui.main_window.os.getcwd', return_value=str(tmp_path)):
            main_window._create_update_log(
                success_count=5,
                skipped_count=0,
                error_count=0,
                error_list=[],
                skipped_details=[]
            )
            
            log_files = list(tmp_path.glob("update_log_*.txt"))
            assert len(log_files) == 1
            
            log_content = log_files[0].read_text(encoding='utf-8')
            
            # Should NOT have skipped details section when list is empty
            assert "ÜBERSPRUNGENE DOIs" not in log_content
            assert "Erfolgreich aktualisiert: 5" in log_content
            assert "Übersprungen (keine Änderungen): 0" in log_content
    
    def test_authors_log_includes_skipped_details_section(self, qtbot, tmp_path):
        """Test that authors update log includes skipped details section."""
        main_window = MainWindow()
        qtbot.addWidget(main_window)
        
        with patch('src.ui.main_window.os.getcwd', return_value=str(tmp_path)):
            error_list = []
            skipped_details = [
                ("10.5880/test.001", "Keine Änderungen in Creator-Metadaten"),
                ("10.5880/test.002", "Keine Änderungen in Creator-Metadaten"),
                ("10.5880/test.003", "Creator-Anzahl identisch (3 creators)")
            ]
            
            main_window._create_authors_update_log(
                success_count=3,
                skipped_count=3,
                error_count=0,
                error_list=error_list,
                skipped_details=skipped_details
            )
            
            # Find log file
            log_files = list(tmp_path.glob("authors_update_log_*.txt"))
            assert len(log_files) == 1
            
            log_content = log_files[0].read_text(encoding='utf-8')
            
            # Verify summary
            assert "Gesamt: 6 DOIs" in log_content
            assert "Erfolgreich aktualisiert: 3" in log_content
            assert "Übersprungen (keine Änderungen): 3" in log_content
            
            # Verify efficiency
            assert "EFFIZIENZ:" in log_content
            assert "API-Calls vermieden: 3/6 (50.0%)" in log_content
            
            # Verify skipped details section
            assert "ÜBERSPRUNGENE DOIs (keine Änderungen):" in log_content
            assert "10.5880/test.001" in log_content
            assert "Grund: Keine Änderungen in Creator-Metadaten" in log_content
            assert "10.5880/test.002" in log_content
            assert "10.5880/test.003" in log_content
            assert "Grund: Creator-Anzahl identisch (3 creators)" in log_content
    
    def test_authors_log_with_database_and_skipped_details(self, qtbot, tmp_path):
        """Test authors log with both database sync and skipped details."""
        main_window = MainWindow()
        qtbot.addWidget(main_window)
        
        # Enable database in settings
        from PySide6.QtCore import QSettings
        settings = QSettings("GFZ", "GROBI")
        settings.setValue("database/enabled", True)
        
        try:
            with patch('src.ui.main_window.os.getcwd', return_value=str(tmp_path)):
                error_list = ["10.5880/test.005: INKONSISTENZ - Datenbank erfolgreich, DataCite fehlgeschlagen"]
                skipped_details = [
                    ("10.5880/test.001", "Keine Änderungen in Creator-Metadaten"),
                    ("10.5880/test.002", "Keine Änderungen in Creator-Metadaten")
                ]
                
                main_window._create_authors_update_log(
                    success_count=2,
                    skipped_count=2,
                    error_count=1,
                    error_list=error_list,
                    skipped_details=skipped_details
                )
                
                log_files = list(tmp_path.glob("authors_update_log_*.txt"))
                assert len(log_files) == 1
                
                log_content = log_files[0].read_text(encoding='utf-8')
                
                # Verify database sync info
                assert "Datenbank-Synchronisation: Aktiviert" in log_content
                assert "KRITISCHE INKONSISTENZEN: 1" in log_content
                assert "DATABASE-FIRST UPDATE PATTERN" in log_content
                
                # Verify skipped details section exists
                assert "ÜBERSPRUNGENE DOIs (keine Änderungen):" in log_content
                assert "10.5880/test.001" in log_content
                assert "10.5880/test.002" in log_content
                
                # Verify error section
                assert "FEHLER:" in log_content
                assert "10.5880/test.005: INKONSISTENZ" in log_content
        finally:
            # Cleanup settings
            settings.setValue("database/enabled", False)
    
    def test_log_efficiency_calculation_with_skips(self, qtbot, tmp_path):
        """Test that efficiency percentage is calculated correctly."""
        main_window = MainWindow()
        qtbot.addWidget(main_window)
        
        with patch('src.ui.main_window.os.getcwd', return_value=str(tmp_path)):
            # 95 skipped out of 100 total = 95% efficiency
            skipped_details = [(f"10.5880/test.{i:03d}", "URL unverändert") for i in range(95)]
            
            main_window._create_update_log(
                success_count=5,
                skipped_count=95,
                error_count=0,
                error_list=[],
                skipped_details=skipped_details
            )
            
            log_files = list(tmp_path.glob("update_log_*.txt"))
            log_content = log_files[0].read_text(encoding='utf-8')
            
            # Verify 95% efficiency
            assert "API-Calls vermieden: 95/100 (95.0%)" in log_content
            assert "Gesamt: 100 DOIs" in log_content
    
    def test_log_with_many_skipped_details(self, qtbot, tmp_path):
        """Test log file can handle large number of skipped DOIs."""
        main_window = MainWindow()
        qtbot.addWidget(main_window)
        
        with patch('src.ui.main_window.os.getcwd', return_value=str(tmp_path)):
            # Create 500 skipped DOIs
            skipped_details = [
                (f"10.5880/test.{i:04d}", f"URL unverändert: https://example.org/data{i}")
                for i in range(500)
            ]
            
            main_window._create_update_log(
                success_count=5,
                skipped_count=500,
                error_count=0,
                error_list=[],
                skipped_details=skipped_details
            )
            
            log_files = list(tmp_path.glob("update_log_*.txt"))
            log_content = log_files[0].read_text(encoding='utf-8')
            
            # Verify all 500 DOIs are in log
            assert "ÜBERSPRUNGENE DOIs (keine Änderungen):" in log_content
            assert "10.5880/test.0000" in log_content
            assert "10.5880/test.0499" in log_content
            
            # Count occurrences (each DOI appears once)
            assert log_content.count("10.5880/test.") >= 500
    
    def test_log_file_creation_error_handling(self, qtbot):
        """Test that log creation errors are handled gracefully."""
        main_window = MainWindow()
        qtbot.addWidget(main_window)
        
        # Use invalid path that will cause error
        with patch('src.ui.main_window.os.getcwd', return_value="/nonexistent/path"):
            # Should not raise exception
            main_window._create_update_log(
                success_count=5,
                skipped_count=2,
                error_count=0,
                error_list=[],
                skipped_details=[("10.5880/test.001", "Test reason")]
            )
            
            # Log message should be created
            # (Check through main_window._log method call would require more mocking)
