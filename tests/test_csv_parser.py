"""Tests for CSV Parser."""

import pytest
import tempfile
import os

from src.utils.csv_parser import CSVParser, CSVParseError


class TestCSVParser:
    """Test suite for CSVParser class."""
    
    def test_validate_doi_format_valid(self):
        """Test DOI format validation with valid DOIs."""
        valid_dois = [
            "10.5880/GFZ.1.1.2021.001",
            "10.1234/example",
            "10.12345/test-doi",
            "10.1000/182"
        ]
        
        for doi in valid_dois:
            assert CSVParser.validate_doi_format(doi), f"DOI should be valid: {doi}"
    
    def test_validate_doi_format_invalid(self):
        """Test DOI format validation with invalid DOIs."""
        invalid_dois = [
            "",
            "11.5880/test",  # Wrong prefix
            "10.123/test",  # Registrant code too short
            "10/5880/test",  # Missing dot after registrant
            "not-a-doi",
            None
        ]
        
        for doi in invalid_dois:
            assert not CSVParser.validate_doi_format(doi), f"DOI should be invalid: {doi}"
    
    def test_validate_url_format_valid(self):
        """Test URL format validation with valid URLs."""
        valid_urls = [
            "https://example.org",
            "https://example.org/path/to/resource",
            "http://example.com",
            "https://example.org:8080/path",
            "http://localhost:3000",
            "https://sub.domain.example.org/page.html"
        ]
        
        for url in valid_urls:
            assert CSVParser.validate_url_format(url), f"URL should be valid: {url}"
    
    def test_validate_url_format_invalid(self):
        """Test URL format validation with invalid URLs."""
        invalid_urls = [
            "",
            "example.org",  # Missing protocol
            "ftp://example.org",  # Wrong protocol
            "//example.org",  # Missing protocol
            "not-a-url",
            None
        ]
        
        for url in invalid_urls:
            assert not CSVParser.validate_url_format(url), f"URL should be invalid: {url}"
    
    def test_parse_update_csv_valid(self):
        """Test parsing a valid CSV file."""
        # Create temporary CSV file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f:
            f.write("DOI,Landing_Page_URL\n")
            f.write("10.5880/GFZ.1.1.2021.001,https://example.org/doi1\n")
            f.write("10.5880/GFZ.1.1.2021.002,https://example.org/doi2\n")
            csv_path = f.name
        
        try:
            result = CSVParser.parse_update_csv(csv_path)
            
            assert len(result) == 2
            assert result[0] == ("10.5880/GFZ.1.1.2021.001", "https://example.org/doi1")
            assert result[1] == ("10.5880/GFZ.1.1.2021.002", "https://example.org/doi2")
        
        finally:
            os.unlink(csv_path)
    
    def test_parse_update_csv_missing_file(self):
        """Test parsing with non-existent file."""
        with pytest.raises(FileNotFoundError):
            CSVParser.parse_update_csv("non_existent_file.csv")
    
    def test_parse_update_csv_missing_headers(self):
        """Test parsing CSV with missing headers."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f:
            f.write("DOI,URL\n")  # Wrong header name
            f.write("10.5880/GFZ.1.1.2021.001,https://example.org/doi1\n")
            csv_path = f.name
        
        try:
            with pytest.raises(CSVParseError) as exc_info:
                CSVParser.parse_update_csv(csv_path)
            
            assert "Header" in str(exc_info.value)
        
        finally:
            os.unlink(csv_path)
    
    def test_parse_update_csv_missing_url(self):
        """Test parsing CSV with missing URL."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f:
            f.write("DOI,Landing_Page_URL\n")
            f.write("10.5880/GFZ.1.1.2021.001,\n")  # Empty URL
            csv_path = f.name
        
        try:
            with pytest.raises(CSVParseError) as exc_info:
                CSVParser.parse_update_csv(csv_path)
            
            assert "Landing Page URL fehlt" in str(exc_info.value)
        
        finally:
            os.unlink(csv_path)
    
    def test_parse_update_csv_invalid_doi_format(self):
        """Test parsing CSV with invalid DOI format."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f:
            f.write("DOI,Landing_Page_URL\n")
            f.write("invalid-doi,https://example.org/doi1\n")
            csv_path = f.name
        
        try:
            with pytest.raises(CSVParseError) as exc_info:
                CSVParser.parse_update_csv(csv_path)
            
            assert "Ungültiges DOI-Format" in str(exc_info.value)
        
        finally:
            os.unlink(csv_path)
    
    def test_parse_update_csv_invalid_url_format(self):
        """Test parsing CSV with invalid URL format."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f:
            f.write("DOI,Landing_Page_URL\n")
            f.write("10.5880/GFZ.1.1.2021.001,not-a-url\n")
            csv_path = f.name
        
        try:
            with pytest.raises(CSVParseError) as exc_info:
                CSVParser.parse_update_csv(csv_path)
            
            assert "Ungültige URL" in str(exc_info.value)
        
        finally:
            os.unlink(csv_path)
    
    def test_parse_update_csv_empty_file(self):
        """Test parsing empty CSV file."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f:
            f.write("DOI,Landing_Page_URL\n")  # Only header, no data
            csv_path = f.name
        
        try:
            with pytest.raises(CSVParseError) as exc_info:
                CSVParser.parse_update_csv(csv_path)
            
            assert "Keine gültigen DOI/URL-Paare" in str(exc_info.value)
        
        finally:
            os.unlink(csv_path)
    
    def test_parse_update_csv_skip_missing_doi(self):
        """Test that rows with missing DOI are skipped with warning."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f:
            f.write("DOI,Landing_Page_URL\n")
            f.write(",https://example.org/doi1\n")  # Missing DOI
            f.write("10.5880/GFZ.1.1.2021.002,https://example.org/doi2\n")
            csv_path = f.name
        
        try:
            result = CSVParser.parse_update_csv(csv_path)
            
            # Should only return the valid row
            assert len(result) == 1
            assert result[0] == ("10.5880/GFZ.1.1.2021.002", "https://example.org/doi2")
        
        finally:
            os.unlink(csv_path)
    
    def test_parse_update_csv_with_whitespace(self):
        """Test parsing CSV with whitespace in values."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f:
            f.write("DOI,Landing_Page_URL\n")
            f.write("  10.5880/GFZ.1.1.2021.001  ,  https://example.org/doi1  \n")
            csv_path = f.name
        
        try:
            result = CSVParser.parse_update_csv(csv_path)
            
            # Should strip whitespace
            assert len(result) == 1
            assert result[0] == ("10.5880/GFZ.1.1.2021.001", "https://example.org/doi1")
        
        finally:
            os.unlink(csv_path)
