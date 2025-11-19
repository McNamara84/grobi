"""Unit tests for CSV Exporter."""

import csv
import os
import pytest
import tempfile
from pathlib import Path
from unittest.mock import patch

from src.utils.csv_exporter import export_dois_to_csv, validate_csv_format, CSVExportError


@pytest.fixture
def temp_dir():
    """Create a temporary directory for testing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


@pytest.fixture
def sample_dois():
    """Sample DOI data for testing."""
    return [
        ("10.5880/GFZ.1.1.2021.001", "https://dataservices.gfz-potsdam.de/panmetaworks/showshort.php?id=1234"),
        ("10.5880/GFZ.1.1.2021.002", "https://dataservices.gfz-potsdam.de/panmetaworks/showshort.php?id=5678"),
        ("10.5880/GFZ.1.1.2021.003", "https://dataservices.gfz-potsdam.de/panmetaworks/showshort.php?id=9012"),
    ]


class TestExportDOIsToCSV:
    """Test CSV export functionality."""
    
    def test_successful_export(self, temp_dir, sample_dois):
        """Test successful export of DOIs to CSV."""
        username = "TIB.GFZ"
        
        filepath = export_dois_to_csv(sample_dois, username, temp_dir)
        
        # Check that file was created
        assert os.path.exists(filepath)
        assert filepath.endswith("TIB.GFZ.csv")
        
        # Read and verify content
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            rows = list(reader)
        
        # Check header
        assert rows[0] == ['DOI', 'Landing_Page_URL']
        
        # Check data rows
        assert len(rows) == 4  # Header + 3 data rows
        assert rows[1][0] == "10.5880/GFZ.1.1.2021.001"
        assert rows[1][1] == "https://dataservices.gfz-potsdam.de/panmetaworks/showshort.php?id=1234"
        assert rows[2][0] == "10.5880/GFZ.1.1.2021.002"
        assert rows[3][0] == "10.5880/GFZ.1.1.2021.003"
    
    def test_empty_list_export(self, temp_dir):
        """Test export with empty DOI list."""
        username = "EMPTY.USER"
        
        filepath = export_dois_to_csv([], username, temp_dir)
        
        # File should still be created with header
        assert os.path.exists(filepath)
        
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            rows = list(reader)
        
        # Only header should be present
        assert len(rows) == 1
        assert rows[0] == ['DOI', 'Landing_Page_URL']
    
    def test_default_output_dir(self, sample_dois):
        """Test export to current working directory when output_dir is None."""
        username = "TEST.USER"
        
        # Export to current directory
        filepath = export_dois_to_csv(sample_dois, username, output_dir=None)
        
        try:
            # Should create file in current directory
            assert os.path.exists(filepath)
            assert Path(filepath).parent == Path.cwd()
        finally:
            # Clean up
            if os.path.exists(filepath):
                os.remove(filepath)
    
    def test_username_sanitization(self, temp_dir, sample_dois):
        """Test that problematic characters in username are sanitized."""
        username = "TIB/GFZ:TEST*FILE?"
        
        filepath = export_dois_to_csv(sample_dois, username, temp_dir)
        
        # Special characters should be replaced with underscores
        assert os.path.exists(filepath)
        filename = os.path.basename(filepath)
        assert "/" not in filename
        assert ":" not in filename
        assert "*" not in filename
        assert "?" not in filename
    
    def test_utf8_encoding(self, temp_dir):
        """Test that CSV is properly encoded in UTF-8."""
        # DOIs with special characters
        dois_with_special_chars = [
            ("10.5880/GFZ.Ö.Ä.Ü", "https://example.com/ümlaut"),
            ("10.5880/GFZ.中文", "https://example.com/chinese"),
        ]
        
        filepath = export_dois_to_csv(dois_with_special_chars, "UTF8.TEST", temp_dir)
        
        # Read with UTF-8 encoding
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
            assert "Ö" in content or "中文" in content
    
    def test_creates_output_directory(self, sample_dois):
        """Test that non-existent output directory is created."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = os.path.join(tmpdir, "subdir", "another")
            
            filepath = export_dois_to_csv(sample_dois, "TEST.USER", output_dir)
            
            assert os.path.exists(filepath)
            assert os.path.exists(output_dir)
    
    def test_permission_error_directory(self, sample_dois):
        """Test handling of permission errors when creating directory."""
        with patch('pathlib.Path.mkdir', side_effect=PermissionError("No permission")):
            with pytest.raises(CSVExportError) as exc_info:
                export_dois_to_csv(sample_dois, "TEST.USER", "/invalid/path")
            
            assert "Berechtigung" in str(exc_info.value)
    
    def test_permission_error_file(self, temp_dir, sample_dois):
        """Test handling of permission errors when writing file."""
        with patch('builtins.open', side_effect=PermissionError("No write permission")):
            with pytest.raises(CSVExportError) as exc_info:
                export_dois_to_csv(sample_dois, "TEST.USER", temp_dir)
            
            assert "Berechtigung" in str(exc_info.value) or "Schreiben" in str(exc_info.value)
    
    def test_os_error_file(self, temp_dir, sample_dois):
        """Test handling of OS errors when writing file."""
        with patch('builtins.open', side_effect=OSError("Disk full")):
            with pytest.raises(CSVExportError) as exc_info:
                export_dois_to_csv(sample_dois, "TEST.USER", temp_dir)
            
            assert "CSV-Datei konnte nicht gespeichert werden" in str(exc_info.value)
    
    def test_returns_filepath(self, temp_dir, sample_dois):
        """Test that function returns the filepath."""
        username = "RETURN.TEST"
        
        filepath = export_dois_to_csv(sample_dois, username, temp_dir)
        
        assert isinstance(filepath, str)
        assert filepath.endswith(f"{username}.csv")
        assert os.path.isabs(filepath)


class TestValidateCSVFormat:
    """Test CSV format validation."""
    
    def test_valid_csv(self, temp_dir, sample_dois):
        """Test validation of a valid CSV file."""
        filepath = export_dois_to_csv(sample_dois, "VALID.TEST", temp_dir)
        
        assert validate_csv_format(filepath) is True
    
    def test_invalid_header(self, temp_dir):
        """Test validation fails with invalid header."""
        filepath = os.path.join(temp_dir, "invalid_header.csv")
        
        with open(filepath, 'w', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['Wrong', 'Header'])
            writer.writerow(['10.5880/GFZ.1', 'https://example.com'])
        
        assert validate_csv_format(filepath) is False
    
    def test_invalid_row_length(self, temp_dir):
        """Test validation fails with wrong number of columns."""
        filepath = os.path.join(temp_dir, "invalid_rows.csv")
        
        with open(filepath, 'w', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['DOI', 'Landing_Page_URL'])
            writer.writerow(['10.5880/GFZ.1'])  # Missing column
            writer.writerow(['10.5880/GFZ.2', 'https://example.com', 'extra'])  # Extra column
        
        assert validate_csv_format(filepath) is False
    
    def test_empty_csv(self, temp_dir):
        """Test validation of empty CSV file."""
        filepath = os.path.join(temp_dir, "empty.csv")
        
        # Create empty file
        with open(filepath, 'w', encoding='utf-8'):
            pass
        
        assert validate_csv_format(filepath) is False
    
    def test_header_only_csv(self, temp_dir):
        """Test validation of CSV with only header."""
        filepath = os.path.join(temp_dir, "header_only.csv")
        
        with open(filepath, 'w', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['DOI', 'Landing_Page_URL'])
        
        # Header-only CSV is valid (represents empty data)
        assert validate_csv_format(filepath) is True
    
    def test_nonexistent_file(self):
        """Test validation of non-existent file."""
        assert validate_csv_format("/nonexistent/file.csv") is False


class TestErrorMessages:
    """Test that error messages are user-friendly and in German."""
    
    def test_permission_error_message_german(self, sample_dois):
        """Test permission error message is in German."""
        with patch('pathlib.Path.mkdir', side_effect=PermissionError()):
            with pytest.raises(CSVExportError) as exc_info:
                export_dois_to_csv(sample_dois, "TEST", "/invalid")
            
            error_message = str(exc_info.value)
            assert "Berechtigung" in error_message or "Verzeichnis" in error_message
    
    def test_os_error_message_german(self, temp_dir, sample_dois):
        """Test OS error message is in German."""
        with patch('builtins.open', side_effect=OSError("Mock error")):
            with pytest.raises(CSVExportError) as exc_info:
                export_dois_to_csv(sample_dois, "TEST", temp_dir)
            
            error_message = str(exc_info.value)
            assert "CSV-Datei" in error_message or "gespeichert" in error_message
