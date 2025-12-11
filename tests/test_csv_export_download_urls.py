"""Tests for CSV export of DOIs with download URLs."""

import pytest
import csv
from pathlib import Path

from src.utils.csv_exporter import export_dois_download_urls, CSVExportError


class TestExportDOIsDownloadURLs:
    """Tests for export_dois_download_urls function."""
    
    def test_export_success(self, tmp_path):
        """Test successful export of DOIs with download URLs."""
        data = [
            ("10.5880/GFZ.1", "data.zip", "https://download.gfz.de/data.zip", "Download data and description", "ZIP", 1048576),
            ("10.5880/GFZ.1", "metadata.xml", "https://download.gfz.de/meta.xml", "Metadata file", "XML", 2048),
            ("10.5880/GFZ.2", "dataset.nc", "https://download.gfz.de/data.nc", "NetCDF dataset", "NetCDF", 5242880),
        ]
        
        filepath = tmp_path / "dois_downloads.csv"
        export_dois_download_urls(data, str(filepath))
        
        # Verify file exists
        assert filepath.exists()
        
        # Verify content
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            rows = list(reader)
        
        # Check header
        assert rows[0] == ['DOI', 'Filename', 'Download_URL', 'Description', 'Format', 'Size_Bytes']
        
        # Check data rows
        assert len(rows) == 4  # Header + 3 data rows
        assert rows[1] == ["10.5880/GFZ.1", "data.zip", "https://download.gfz.de/data.zip", "Download data and description", "ZIP", "1048576"]
        assert rows[2] == ["10.5880/GFZ.1", "metadata.xml", "https://download.gfz.de/meta.xml", "Metadata file", "XML", "2048"]
        assert rows[3] == ["10.5880/GFZ.2", "dataset.nc", "https://download.gfz.de/data.nc", "NetCDF dataset", "NetCDF", "5242880"]
    
    def test_export_empty_list(self, tmp_path):
        """Test export with empty data list."""
        filepath = tmp_path / "empty.csv"
        export_dois_download_urls([], str(filepath))
        
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            rows = list(reader)
        
        # Only header
        assert len(rows) == 1
        assert rows[0] == ['DOI', 'Filename', 'Download_URL', 'Description', 'Format', 'Size_Bytes']
    
    def test_export_special_characters(self, tmp_path):
        """Test export with special characters in filenames and URLs."""
        data = [
            ("10.5880/GFZ.ü.ä.ö", "dätä file.zip", "https://example.com/file?param=val&other=123", "Special chars test", "ZIP", 1024),
        ]
        
        filepath = tmp_path / "special.csv"
        export_dois_download_urls(data, str(filepath))
        
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            rows = list(reader)
        
        assert rows[1] == [
            "10.5880/GFZ.ü.ä.ö",
            "dätä file.zip",
            "https://example.com/file?param=val&other=123",
            "Special chars test",
            "ZIP",
            "1024"
        ]
    
    def test_export_large_file_sizes(self, tmp_path):
        """Test export with large file sizes."""
        data = [
            ("10.5880/GFZ.1", "huge.tar.gz", "https://example.com/huge.tar.gz", "Large file", "TAR.GZ", 10737418240),  # 10 GB
        ]
        
        filepath = tmp_path / "large.csv"
        export_dois_download_urls(data, str(filepath))
        
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            rows = list(reader)
        
        assert rows[1][5] == "10737418240"
    
    def test_export_multiple_files_per_doi(self, tmp_path):
        """Test export where DOIs have multiple files (realistic scenario)."""
        data = [
            ("10.5880/GFZ.1", "file1.zip", "https://example.com/file1.zip", "First file", "ZIP", 1024),
            ("10.5880/GFZ.1", "file2.zip", "https://example.com/file2.zip", "Second file", "ZIP", 2048),
            ("10.5880/GFZ.1", "file3.xml", "https://example.com/file3.xml", "Metadata", "XML", 512),
            ("10.5880/GFZ.2", "data.nc", "https://example.com/data.nc", "Dataset", "NetCDF", 4096),
        ]
        
        filepath = tmp_path / "multiple.csv"
        export_dois_download_urls(data, str(filepath))
        
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            rows = list(reader)
        
        # 4 data rows + 1 header
        assert len(rows) == 5
        
        # First DOI appears 3 times
        doi_1_count = sum(1 for row in rows[1:] if row[0] == "10.5880/GFZ.1")
        assert doi_1_count == 3
        
        # Second DOI appears once
        doi_2_count = sum(1 for row in rows[1:] if row[0] == "10.5880/GFZ.2")
        assert doi_2_count == 1
    
    def test_export_permission_error(self, tmp_path):
        """Test handling of permission errors."""
        import sys
        import os
        
        # Skip on Windows as file permissions work differently
        if sys.platform == "win32":
            pytest.skip("Permission tests are unreliable on Windows")
        
        # Create a read-only directory
        readonly_dir = tmp_path / "readonly"
        readonly_dir.mkdir()
        os.chmod(readonly_dir, 0o444)
        
        filepath = readonly_dir / "test.csv"
        data = [("10.5880/GFZ.1", "file.zip", "https://example.com/file.zip", "Test file", "ZIP", 1024)]
        
        try:
            with pytest.raises(CSVExportError, match="Keine Berechtigung|konnte nicht gespeichert werden"):
                export_dois_download_urls(data, str(filepath))
        finally:
            # Restore write permissions for cleanup
            os.chmod(readonly_dir, 0o755)
    
    def test_export_invalid_path(self):
        """Test handling of invalid file paths."""
        data = [("10.5880/GFZ.1", "file.zip", "https://example.com/file.zip", "Test file", "ZIP", 1024)]
        
        # Invalid path (assuming this doesn't exist on any system)
        invalid_path = "/nonexistent/path/with/many/levels/file.csv"
        
        with pytest.raises(CSVExportError):
            export_dois_download_urls(data, invalid_path)
    
    def test_export_utf8_encoding(self, tmp_path):
        """Test that files are properly encoded in UTF-8."""
        data = [
            ("10.5880/GFZ.1", "日本語.zip", "https://example.com/日本語.zip", "Japanese file", "ZIP", 1024),
            ("10.5880/GFZ.2", "Файл.tar", "https://example.com/Файл.tar", "Russian file", "TAR", 2048),
        ]
        
        filepath = tmp_path / "utf8.csv"
        export_dois_download_urls(data, str(filepath))
        
        # Verify UTF-8 encoding
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
        
        assert "日本語" in content
        assert "Файл" in content
