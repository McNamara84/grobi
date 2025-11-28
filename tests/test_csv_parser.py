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
            "10.1000/182",
            "10.123/test",  # 3-digit registrant code (now valid)
            "10.1/test",    # 1-digit registrant code (now valid)
            "10.12/test"    # 2-digit registrant code (now valid)
        ]
        
        for doi in valid_dois:
            assert CSVParser.validate_doi_format(doi), f"DOI should be valid: {doi}"
    
    def test_validate_doi_format_invalid(self):
        """Test DOI format validation with invalid DOIs."""
        invalid_dois = [
            "",
            "11.5880/test",  # Wrong prefix
            "10/5880/test",  # Missing dot after prefix
            "10./test",      # Missing registrant code
            "10.123/",       # Empty suffix
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
    
    # Tests for parse_authors_update_csv()
    
    def test_validate_orcid_format_valid(self):
        """Test ORCID format validation with valid ORCIDs."""
        valid_orcids = [
            "0000-0001-5000-0007",
            "0000-0002-1825-0097",
            "0000-0001-5109-3700",
            "0000-0002-1694-233X",  # X checksum
            "https://orcid.org/0000-0001-5000-0007",
            "https://orcid.org/0000-0002-1694-233X",
        ]
        
        for orcid in valid_orcids:
            assert CSVParser.validate_orcid_format(orcid), f"ORCID should be valid: {orcid}"
    
    def test_validate_orcid_format_invalid(self):
        """Test ORCID format validation with invalid ORCIDs."""
        invalid_orcids = [
            "0000-0001-5000",  # Too short
            "0000-0001-5000-00",  # Too short
            "0000-0001-5000-00077",  # Too long
            "1234-5678-9012-3456",  # Doesn't start with 0000
            "not-an-orcid",
            "https://example.org/0000-0001-5000-0007",  # Wrong domain
        ]
        
        for orcid in invalid_orcids:
            assert not CSVParser.validate_orcid_format(orcid), f"ORCID should be invalid: {orcid}"
    
    def test_parse_authors_update_csv_valid(self):
        """Test parsing a valid authors CSV file."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8', newline='') as f:
            f.write("DOI,Creator Name,Name Type,Given Name,Family Name,Name Identifier,Name Identifier Scheme,Scheme URI\n")
            f.write('10.5880/GFZ.1.1.2021.001,"Smith, John",Personal,John,Smith,0000-0001-5000-0007,ORCID,https://orcid.org\n')
            f.write('10.5880/GFZ.1.1.2021.001,"Doe, Jane",Personal,Jane,Doe,0000-0002-1825-0097,ORCID,https://orcid.org\n')
            f.write("10.5880/GFZ.1.1.2021.002,Example Org,Organizational,,,,,\n")
            csv_path = f.name
        
        try:
            creators_by_doi, warnings = CSVParser.parse_authors_update_csv(csv_path)
            
            assert len(creators_by_doi) == 2
            assert "10.5880/GFZ.1.1.2021.001" in creators_by_doi
            assert "10.5880/GFZ.1.1.2021.002" in creators_by_doi
            
            # First DOI has 2 creators
            creators_doi1 = creators_by_doi["10.5880/GFZ.1.1.2021.001"]
            assert len(creators_doi1) == 2
            assert creators_doi1[0]["name"] == "Smith, John"
            assert creators_doi1[0]["nameType"] == "Personal"
            assert creators_doi1[0]["givenName"] == "John"
            assert creators_doi1[0]["familyName"] == "Smith"
            assert creators_doi1[0]["nameIdentifier"] == "0000-0001-5000-0007"
            
            # Second DOI has 1 creator (organizational)
            creators_doi2 = creators_by_doi["10.5880/GFZ.1.1.2021.002"]
            assert len(creators_doi2) == 1
            assert creators_doi2[0]["name"] == "Example Org"
            assert creators_doi2[0]["nameType"] == "Organizational"
            assert creators_doi2[0]["givenName"] == ""
            assert creators_doi2[0]["familyName"] == ""
            
            # Should have no warnings for valid data
            assert len(warnings) == 0
        
        finally:
            os.unlink(csv_path)
    
    def test_parse_authors_update_csv_missing_headers(self):
        """Test parsing CSV with missing required headers."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f:
            f.write("DOI,Creator_Name,Name_Type\n")  # Missing other columns
            f.write("10.5880/GFZ.1.1.2021.001,Smith, John,Personal\n")
            csv_path = f.name
        
        try:
            with pytest.raises(CSVParseError) as exc_info:
                CSVParser.parse_authors_update_csv(csv_path)
            
            assert "fehlen folgende Header" in str(exc_info.value)
        
        finally:
            os.unlink(csv_path)
    
    def test_parse_authors_update_csv_invalid_doi_format(self):
        """Test parsing CSV with invalid DOI format."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8', newline='') as f:
            f.write("DOI,Creator Name,Name Type,Given Name,Family Name,Name Identifier,Name Identifier Scheme,Scheme URI\n")
            f.write('invalid-doi,"Smith, John",Personal,John,Smith,,,\n')
            csv_path = f.name
        
        try:
            with pytest.raises(CSVParseError) as exc_info:
                CSVParser.parse_authors_update_csv(csv_path)
            
            assert "Ungültiges DOI-Format" in str(exc_info.value)
        
        finally:
            os.unlink(csv_path)
    
    def test_parse_authors_update_csv_missing_creator_name(self):
        """Test parsing CSV with missing creator name."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f:
            f.write("DOI,Creator Name,Name Type,Given Name,Family Name,Name Identifier,Name Identifier Scheme,Scheme URI\n")
            f.write("10.5880/GFZ.1.1.2021.001,,Personal,John,Smith,,,\n")
            csv_path = f.name
        
        try:
            with pytest.raises(CSVParseError) as exc_info:
                CSVParser.parse_authors_update_csv(csv_path)
            
            assert "Creator Name fehlt" in str(exc_info.value)
        
        finally:
            os.unlink(csv_path)
    
    def test_parse_authors_update_csv_invalid_name_type(self):
        """Test parsing CSV with invalid name type."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8', newline='') as f:
            f.write("DOI,Creator Name,Name Type,Given Name,Family Name,Name Identifier,Name Identifier Scheme,Scheme URI\n")
            f.write('10.5880/GFZ.1.1.2021.001,"Smith, John",InvalidType,John,Smith,,,\n')
            csv_path = f.name
        
        try:
            with pytest.raises(CSVParseError) as exc_info:
                CSVParser.parse_authors_update_csv(csv_path)
            
            assert "Ungültiger Name Type" in str(exc_info.value) or "Name Type" in str(exc_info.value)
        
        finally:
            os.unlink(csv_path)
    
    def test_parse_authors_update_csv_organizational_with_names(self):
        """Test parsing CSV with organizational creator that has given/family names."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f:
            f.write("DOI,Creator Name,Name Type,Given Name,Family Name,Name Identifier,Name Identifier Scheme,Scheme URI\n")
            f.write("10.5880/GFZ.1.1.2021.001,Example Org,Organizational,John,Smith,,,\n")
            csv_path = f.name
        
        try:
            with pytest.raises(CSVParseError) as exc_info:
                CSVParser.parse_authors_update_csv(csv_path)
            
            assert "Organizational" in str(exc_info.value)
            assert "Given Name oder Family Name" in str(exc_info.value)
        
        finally:
            os.unlink(csv_path)
    
    def test_parse_authors_update_csv_personal_missing_names(self):
        """Test parsing CSV with personal creator missing both given and family names."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8', newline='') as f:
            f.write("DOI,Creator Name,Name Type,Given Name,Family Name,Name Identifier,Name Identifier Scheme,Scheme URI\n")
            f.write("10.5880/GFZ.1.1.2021.001,Smith,Personal,,,,,\n")
            csv_path = f.name
        
        try:
            # This should pass - Personal creators don't require given/family names
            # The name field is sufficient
            creators_by_doi, warnings = CSVParser.parse_authors_update_csv(csv_path)
            assert len(creators_by_doi) == 1
            assert creators_by_doi["10.5880/GFZ.1.1.2021.001"][0]["name"] == "Smith"
        
        finally:
            os.unlink(csv_path)
    
    def test_parse_authors_update_csv_invalid_orcid_warning(self):
        """Test that invalid ORCID format generates warning but doesn't fail."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8', newline='') as f:
            f.write("DOI,Creator Name,Name Type,Given Name,Family Name,Name Identifier,Name Identifier Scheme,Scheme URI\n")
            f.write('10.5880/GFZ.1.1.2021.001,"Smith, John",Personal,John,Smith,invalid-orcid,ORCID,https://orcid.org\n')
            csv_path = f.name
        
        try:
            creators_by_doi, warnings = CSVParser.parse_authors_update_csv(csv_path)
            
            # Should parse successfully
            assert len(creators_by_doi) == 1
            
            # But should have a warning about invalid ORCID
            assert len(warnings) > 0
            assert any("ORCID" in w and "invalid-orcid" in w for w in warnings)
        
        finally:
            os.unlink(csv_path)
    
    def test_parse_authors_update_csv_preserves_order(self):
        """Test that creator order is preserved within each DOI."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f:
            f.write("DOI,Creator Name,Name Type,Given Name,Family Name,Name Identifier,Name Identifier Scheme,Scheme URI\n")
            f.write("10.5880/GFZ.1.1.2021.001,First Author,Personal,First,Author,,,\n")
            f.write("10.5880/GFZ.1.1.2021.001,Second Author,Personal,Second,Author,,,\n")
            f.write("10.5880/GFZ.1.1.2021.001,Third Author,Personal,Third,Author,,,\n")
            csv_path = f.name
        
        try:
            creators_by_doi, warnings = CSVParser.parse_authors_update_csv(csv_path)
            
            creators = creators_by_doi["10.5880/GFZ.1.1.2021.001"]
            assert len(creators) == 3
            
            # Check order is preserved
            assert creators[0]["name"] == "First Author"
            assert creators[1]["name"] == "Second Author"
            assert creators[2]["name"] == "Third Author"
        
        finally:
            os.unlink(csv_path)
    
    def test_parse_authors_update_csv_empty_file(self):
        """Test parsing empty CSV file."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f:
            f.write("DOI,Creator Name,Name Type,Given Name,Family Name,Name Identifier,Name Identifier Scheme,Scheme URI\n")
            csv_path = f.name
        
        try:
            with pytest.raises(CSVParseError) as exc_info:
                CSVParser.parse_authors_update_csv(csv_path)
            
            assert "Keine gültigen Creator-Daten" in str(exc_info.value)
        
        finally:
            os.unlink(csv_path)
    
    def test_parse_authors_update_csv_with_whitespace(self):
        """Test parsing CSV with whitespace in values."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8', newline='') as f:
            f.write("DOI,Creator Name,Name Type,Given Name,Family Name,Name Identifier,Name Identifier Scheme,Scheme URI\n")
            f.write('  10.5880/GFZ.1.1.2021.001  ,"  Smith, John  ",  Personal  ,  John  ,  Smith  ,,,\n')
            csv_path = f.name
        
        try:
            creators_by_doi, warnings = CSVParser.parse_authors_update_csv(csv_path)
            
            # Should strip whitespace
            assert len(creators_by_doi) == 1
            creators = creators_by_doi["10.5880/GFZ.1.1.2021.001"]
            assert creators[0]["name"] == "Smith, John"
            assert creators[0]["nameType"] == "Personal"
            assert creators[0]["givenName"] == "John"
            assert creators[0]["familyName"] == "Smith"
        
        finally:
            os.unlink(csv_path)


class TestParsePublisherUpdateCSV:
    """Test suite for Publisher Update CSV parsing."""
    
    def test_parse_publisher_update_csv_valid(self):
        """Test parsing a valid publisher update CSV file."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8', newline='') as f:
            f.write("DOI,Publisher Name,Publisher Identifier,Publisher Identifier Scheme,Scheme URI,Language\n")
            f.write("10.5880/GFZ.1.1.2021.001,GFZ German Research Centre for Geosciences,https://ror.org/04z8jg394,ROR,https://ror.org,en\n")
            f.write("10.5880/GFZ.1.1.2021.002,Helmholtz Centre Potsdam,,,,de\n")
            csv_path = f.name
        
        try:
            publisher_by_doi, warnings = CSVParser.parse_publisher_update_csv(csv_path)
            
            assert len(publisher_by_doi) == 2
            assert len(warnings) == 0
            
            # Check first entry with all fields
            pub1 = publisher_by_doi["10.5880/GFZ.1.1.2021.001"]
            assert pub1["name"] == "GFZ German Research Centre for Geosciences"
            assert pub1["publisherIdentifier"] == "https://ror.org/04z8jg394"
            assert pub1["publisherIdentifierScheme"] == "ROR"
            assert pub1["schemeUri"] == "https://ror.org"
            assert pub1["lang"] == "en"
            
            # Check second entry with minimal fields
            pub2 = publisher_by_doi["10.5880/GFZ.1.1.2021.002"]
            assert pub2["name"] == "Helmholtz Centre Potsdam"
            assert pub2["publisherIdentifier"] == ""
            assert pub2["publisherIdentifierScheme"] == ""
            assert pub2["schemeUri"] == ""
            assert pub2["lang"] == "de"
        
        finally:
            os.unlink(csv_path)
    
    def test_parse_publisher_update_csv_missing_name(self):
        """Test parsing CSV with missing publisher name raises error."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8', newline='') as f:
            f.write("DOI,Publisher Name,Publisher Identifier,Publisher Identifier Scheme,Scheme URI,Language\n")
            f.write("10.5880/GFZ.1.1.2021.001,,https://ror.org/04z8jg394,ROR,https://ror.org,en\n")
            csv_path = f.name
        
        try:
            with pytest.raises(CSVParseError) as exc_info:
                CSVParser.parse_publisher_update_csv(csv_path)
            
            # Should raise error since publisher name is required
            assert "Publisher Name fehlt" in str(exc_info.value)
        
        finally:
            os.unlink(csv_path)
    
    def test_parse_publisher_update_csv_invalid_doi(self):
        """Test parsing CSV with invalid DOI format raises error."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8', newline='') as f:
            f.write("DOI,Publisher Name,Publisher Identifier,Publisher Identifier Scheme,Scheme URI,Language\n")
            f.write("invalid-doi,Test Publisher,,,,\n")
            csv_path = f.name
        
        try:
            with pytest.raises(CSVParseError) as exc_info:
                CSVParser.parse_publisher_update_csv(csv_path)
            
            assert "DOI" in str(exc_info.value)
        
        finally:
            os.unlink(csv_path)
    
    def test_parse_publisher_update_csv_wrong_header(self):
        """Test parsing CSV with wrong header raises error."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8', newline='') as f:
            f.write("DOI,Name,URL\n")
            f.write("10.5880/GFZ.1.1.2021.001,Test,https://example.com\n")
            csv_path = f.name
        
        try:
            with pytest.raises(CSVParseError) as exc_info:
                CSVParser.parse_publisher_update_csv(csv_path)
            
            assert "Header" in str(exc_info.value) or "Spalte" in str(exc_info.value)
        
        finally:
            os.unlink(csv_path)
    
    def test_parse_publisher_update_csv_duplicate_doi(self):
        """Test parsing CSV with duplicate DOI raises error."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8', newline='') as f:
            f.write("DOI,Publisher Name,Publisher Identifier,Publisher Identifier Scheme,Scheme URI,Language\n")
            f.write("10.5880/GFZ.1.1.2021.001,First Publisher,,,,en\n")
            f.write("10.5880/GFZ.1.1.2021.001,Second Publisher,,,,de\n")
            csv_path = f.name
        
        try:
            with pytest.raises(CSVParseError) as exc_info:
                CSVParser.parse_publisher_update_csv(csv_path)
            
            # Should raise error about duplicate DOI
            assert "mehrfach" in str(exc_info.value) or "doppelt" in str(exc_info.value).lower()
        
        finally:
            os.unlink(csv_path)
    
    def test_parse_publisher_update_csv_empty_file(self):
        """Test parsing empty CSV file raises error."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f:
            f.write("DOI,Publisher Name,Publisher Identifier,Publisher Identifier Scheme,Scheme URI,Language\n")
            csv_path = f.name
        
        try:
            with pytest.raises(CSVParseError) as exc_info:
                CSVParser.parse_publisher_update_csv(csv_path)
            
            assert "Keine gültigen" in str(exc_info.value) or "leer" in str(exc_info.value).lower()
        
        finally:
            os.unlink(csv_path)
    
    def test_parse_publisher_update_csv_with_whitespace(self):
        """Test parsing CSV with whitespace in values."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8', newline='') as f:
            f.write("DOI,Publisher Name,Publisher Identifier,Publisher Identifier Scheme,Scheme URI,Language\n")
            f.write("  10.5880/GFZ.1.1.2021.001  ,  Test Publisher  ,  https://ror.org/123  ,  ROR  ,  https://ror.org  ,  en  \n")
            csv_path = f.name
        
        try:
            publisher_by_doi, warnings = CSVParser.parse_publisher_update_csv(csv_path)
            
            # Should strip whitespace
            assert len(publisher_by_doi) == 1
            pub = publisher_by_doi["10.5880/GFZ.1.1.2021.001"]
            assert pub["name"] == "Test Publisher"
            assert pub["publisherIdentifier"] == "https://ror.org/123"
            assert pub["publisherIdentifierScheme"] == "ROR"
            assert pub["schemeUri"] == "https://ror.org"
            assert pub["lang"] == "en"
        
        finally:
            os.unlink(csv_path)
    
    def test_parse_publisher_update_csv_optional_fields_empty(self):
        """Test parsing CSV where optional fields are empty."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8', newline='') as f:
            f.write("DOI,Publisher Name,Publisher Identifier,Publisher Identifier Scheme,Scheme URI,Language\n")
            f.write("10.5880/GFZ.1.1.2021.001,Minimal Publisher,,,,\n")
            csv_path = f.name
        
        try:
            publisher_by_doi, warnings = CSVParser.parse_publisher_update_csv(csv_path)
            
            assert len(publisher_by_doi) == 1
            pub = publisher_by_doi["10.5880/GFZ.1.1.2021.001"]
            assert pub["name"] == "Minimal Publisher"
            assert pub["publisherIdentifier"] == ""
            assert pub["publisherIdentifierScheme"] == ""
            assert pub["schemeUri"] == ""
            # lang may be empty or stripped
        
        finally:
            os.unlink(csv_path)
