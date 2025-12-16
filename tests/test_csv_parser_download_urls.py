"""Tests for CSVParser.parse_download_urls_csv method."""

import pytest
import tempfile
import os

from src.utils.csv_parser import CSVParser, CSVParseError


class TestParseDownloadUrlsCsv:
    """Test suite for CSVParser.parse_download_urls_csv method."""
    
    def test_parse_download_urls_csv_valid(self):
        """Test parsing a valid download URLs CSV file."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f:
            f.write("DOI,Filename,Download_URL,Description,Format,Size_Bytes\n")
            f.write("10.1594/GFZ.SDDB.1004,data.csv,https://download.gfz.de/data.csv,Download data,text/csv,62207\n")
            f.write("10.5880/GFZ.1.1.2021.001,readme.txt,https://example.org/readme.txt,Read me file,text/plain,1024\n")
            csv_path = f.name
        
        try:
            result = CSVParser.parse_download_urls_csv(csv_path)
            
            assert len(result) == 2
            
            assert result[0]['doi'] == '10.1594/GFZ.SDDB.1004'
            assert result[0]['filename'] == 'data.csv'
            assert result[0]['download_url'] == 'https://download.gfz.de/data.csv'
            assert result[0]['description'] == 'Download data'
            assert result[0]['format'] == 'text/csv'
            assert result[0]['size_bytes'] == 62207
            
            assert result[1]['doi'] == '10.5880/GFZ.1.1.2021.001'
            assert result[1]['filename'] == 'readme.txt'
            assert result[1]['size_bytes'] == 1024
        finally:
            os.unlink(csv_path)
    
    def test_parse_download_urls_csv_missing_file(self):
        """Test parsing with non-existent file."""
        with pytest.raises(FileNotFoundError, match="CSV-Datei nicht gefunden"):
            CSVParser.parse_download_urls_csv("non_existent_file.csv")
    
    def test_parse_download_urls_csv_missing_headers(self):
        """Test parsing CSV with missing required headers."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f:
            f.write("DOI,Filename,URL\n")  # Missing Download_URL, Description, Format, Size_Bytes
            f.write("10.1594/GFZ.SDDB.1004,data.csv,https://example.org\n")
            csv_path = f.name
        
        try:
            with pytest.raises(CSVParseError, match="In der CSV-Datei fehlen erforderliche Header"):
                CSVParser.parse_download_urls_csv(csv_path)
        finally:
            os.unlink(csv_path)
    
    def test_parse_download_urls_csv_invalid_headers(self):
        """Test parsing CSV with wrong header names."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f:
            f.write("doi,file_name,url,desc,format,size\n")  # Wrong case/names
            f.write("10.1594/GFZ.SDDB.1004,data.csv,https://example.org,desc,csv,1000\n")
            csv_path = f.name
        
        try:
            with pytest.raises(CSVParseError, match="In der CSV-Datei fehlen erforderliche Header"):
                CSVParser.parse_download_urls_csv(csv_path)
        finally:
            os.unlink(csv_path)
    
    def test_parse_download_urls_csv_invalid_doi_format(self):
        """Test parsing CSV with invalid DOI format (rows are skipped)."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f:
            f.write("DOI,Filename,Download_URL,Description,Format,Size_Bytes\n")
            f.write("invalid-doi,data.csv,https://example.org/data.csv,Data,text/csv,1000\n")
            f.write("10.1594/GFZ.SDDB.1004,valid.csv,https://example.org/valid.csv,Valid,text/csv,2000\n")
            csv_path = f.name
        
        try:
            result = CSVParser.parse_download_urls_csv(csv_path)
            
            # Invalid DOI row should be skipped
            assert len(result) == 1
            assert result[0]['doi'] == '10.1594/GFZ.SDDB.1004'
        finally:
            os.unlink(csv_path)
    
    def test_parse_download_urls_csv_missing_filename(self):
        """Test parsing CSV with missing filename (row is skipped)."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f:
            f.write("DOI,Filename,Download_URL,Description,Format,Size_Bytes\n")
            f.write("10.1594/GFZ.SDDB.1004,,https://example.org/data.csv,Data,text/csv,1000\n")
            f.write("10.5880/GFZ.1,valid.csv,https://example.org/valid.csv,Valid,text/csv,2000\n")
            csv_path = f.name
        
        try:
            result = CSVParser.parse_download_urls_csv(csv_path)
            
            # Row with missing filename should be skipped
            assert len(result) == 1
            assert result[0]['filename'] == 'valid.csv'
        finally:
            os.unlink(csv_path)
    
    def test_parse_download_urls_csv_empty_file(self):
        """Test parsing an empty CSV file."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f:
            f.write("")
            csv_path = f.name
        
        try:
            with pytest.raises(CSVParseError, match="CSV-Datei ist leer oder hat keine Header-Zeile"):
                CSVParser.parse_download_urls_csv(csv_path)
        finally:
            os.unlink(csv_path)
    
    def test_parse_download_urls_csv_only_header(self):
        """Test parsing CSV with only header row, no data."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f:
            f.write("DOI,Filename,Download_URL,Description,Format,Size_Bytes\n")
            csv_path = f.name
        
        try:
            result = CSVParser.parse_download_urls_csv(csv_path)
            
            # Should return empty list
            assert result == []
        finally:
            os.unlink(csv_path)
    
    def test_parse_download_urls_csv_invalid_size_bytes(self):
        """Test parsing CSV with invalid size_bytes values (should default to 0)."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f:
            f.write("DOI,Filename,Download_URL,Description,Format,Size_Bytes\n")
            f.write("10.1594/GFZ.SDDB.1004,data1.csv,https://example.org/1,Desc,text/csv,not_a_number\n")
            f.write("10.1594/GFZ.SDDB.1005,data2.csv,https://example.org/2,Desc,text/csv,\n")
            f.write("10.1594/GFZ.SDDB.1006,data3.csv,https://example.org/3,Desc,text/csv,abc123\n")
            csv_path = f.name
        
        try:
            result = CSVParser.parse_download_urls_csv(csv_path)
            
            assert len(result) == 3
            # All invalid sizes should default to 0
            assert result[0]['size_bytes'] == 0
            assert result[1]['size_bytes'] == 0
            assert result[2]['size_bytes'] == 0
        finally:
            os.unlink(csv_path)
    
    def test_parse_download_urls_csv_negative_size(self):
        """Test parsing CSV with negative size_bytes (should be corrected to 0)."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f:
            f.write("DOI,Filename,Download_URL,Description,Format,Size_Bytes\n")
            f.write("10.1594/GFZ.SDDB.1004,data.csv,https://example.org/data.csv,Desc,text/csv,-500\n")
            csv_path = f.name
        
        try:
            result = CSVParser.parse_download_urls_csv(csv_path)
            
            assert len(result) == 1
            assert result[0]['size_bytes'] == 0  # Negative corrected to 0
        finally:
            os.unlink(csv_path)
    
    def test_parse_download_urls_csv_utf8_encoding_error(self):
        """Test parsing CSV with non-UTF-8 encoding."""
        # Create file with actual invalid UTF-8 bytes
        with tempfile.NamedTemporaryFile(mode='wb', suffix='.csv', delete=False) as f:
            f.write(b"DOI,Filename,Download_URL,Description,Format,Size_Bytes\n")
            f.write(b"10.1594/GFZ.SDDB.1004,data.csv,https://example.org/data.csv,Beschreibung\x80\x81,text/csv,1000\n")
            csv_path = f.name
        
        try:
            with pytest.raises(CSVParseError, match="nicht UTF-8 kodiert"):
                CSVParser.parse_download_urls_csv(csv_path)
        finally:
            os.unlink(csv_path)
    
    def test_parse_download_urls_csv_whitespace_trimming(self):
        """Test that whitespace is trimmed from values."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f:
            f.write("DOI,Filename,Download_URL,Description,Format,Size_Bytes\n")
            f.write("  10.1594/GFZ.SDDB.1004  ,  data.csv  ,  https://example.org  ,  Desc  ,  text/csv  ,  1000  \n")
            csv_path = f.name
        
        try:
            result = CSVParser.parse_download_urls_csv(csv_path)
            
            assert len(result) == 1
            assert result[0]['doi'] == '10.1594/GFZ.SDDB.1004'
            assert result[0]['filename'] == 'data.csv'
            assert result[0]['download_url'] == 'https://example.org'
            assert result[0]['description'] == 'Desc'
            assert result[0]['format'] == 'text/csv'
            assert result[0]['size_bytes'] == 1000
        finally:
            os.unlink(csv_path)
    
    def test_parse_download_urls_csv_empty_optional_fields(self):
        """Test parsing CSV with empty optional fields (description, format, url)."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f:
            f.write("DOI,Filename,Download_URL,Description,Format,Size_Bytes\n")
            f.write("10.1594/GFZ.SDDB.1004,data.csv,,,,\n")
            csv_path = f.name
        
        try:
            result = CSVParser.parse_download_urls_csv(csv_path)
            
            assert len(result) == 1
            assert result[0]['doi'] == '10.1594/GFZ.SDDB.1004'
            assert result[0]['filename'] == 'data.csv'
            assert result[0]['download_url'] == ''
            assert result[0]['description'] == ''
            assert result[0]['format'] == ''
            assert result[0]['size_bytes'] == 0
        finally:
            os.unlink(csv_path)
    
    def test_parse_download_urls_csv_skips_empty_rows(self):
        """Test that completely empty rows are skipped."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f:
            f.write("DOI,Filename,Download_URL,Description,Format,Size_Bytes\n")
            f.write("10.1594/GFZ.SDDB.1004,data.csv,https://example.org,Desc,text/csv,1000\n")
            f.write(",,,,,\n")  # Empty row
            f.write("10.1594/GFZ.SDDB.1005,data2.csv,https://example.org/2,Desc2,text/csv,2000\n")
            csv_path = f.name
        
        try:
            result = CSVParser.parse_download_urls_csv(csv_path)
            
            assert len(result) == 2
            assert result[0]['doi'] == '10.1594/GFZ.SDDB.1004'
            assert result[1]['doi'] == '10.1594/GFZ.SDDB.1005'
        finally:
            os.unlink(csv_path)
    
    def test_parse_download_urls_csv_path_is_directory(self):
        """Test parsing when path is a directory, not a file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            with pytest.raises(CSVParseError, match="Pfad ist keine Datei"):
                CSVParser.parse_download_urls_csv(tmpdir)
