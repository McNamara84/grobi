"""Unit tests for CSV Exporter."""

import csv
import os
import pytest
import tempfile
from pathlib import Path
from unittest.mock import patch

from src.utils.csv_exporter import (
    export_dois_to_csv,
    export_dois_with_creators_to_csv,
    export_dois_with_publisher_to_csv,
    export_dead_links_to_csv,
    validate_csv_format,
    CSVExportError
)


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


@pytest.fixture
def sample_creator_data():
    """Sample creator data for testing."""
    return [
        (
            "10.5880/GFZ.1.1.2021.001",
            "Miller, Elizabeth",
            "Personal",
            "Elizabeth",
            "Miller",
            "https://orcid.org/0000-0001-5000-0007",
            "ORCID",
            "https://orcid.org"
        ),
        (
            "10.5880/GFZ.1.1.2021.001",
            "Smith, John",
            "Personal",
            "John",
            "Smith",
            "",
            "",
            ""
        ),
        (
            "10.5880/GFZ.1.1.2021.002",
            "GFZ Data Services",
            "Organizational",
            "",
            "",
            "",
            "",
            ""
        ),
    ]


@pytest.fixture
def sample_publisher_data():
    """Sample publisher data for testing."""
    return [
        (
            "10.5880/GFZ.1.1.2021.001",
            "GFZ German Research Centre for Geosciences",
            "https://ror.org/04z8jg394",
            "ROR",
            "https://ror.org",
            "en"
        ),
        (
            "10.5880/GFZ.1.1.2021.002",
            "Helmholtz Centre Potsdam",
            "",
            "",
            "",
            ""
        ),
        (
            "10.5880/GFZ.1.1.2021.003",
            "Example Publisher",
            "https://ror.org/12345",
            "ROR",
            "https://ror.org",
            "de"
        ),
    ]


class TestExportDOIsToCSV:
    """Test CSV export functionality."""
    
    def test_successful_export(self, temp_dir, sample_dois):
        """Test successful export of DOIs to CSV."""
        username = "TIB.GFZ"
        
        filepath = export_dois_to_csv(sample_dois, username, temp_dir)
        
        # Check that file was created
        assert os.path.exists(filepath)
        assert filepath.endswith("TIB.GFZ_urls.csv")
        
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
        assert filepath.endswith(f"{username}_urls.csv")
        assert os.path.isabs(filepath)


class TestExportDeadLinksToCSV:
    """Test CSV export for dead links."""

    def test_export_dead_links_success(self, temp_dir):
        """Test successful export of dead links to CSV."""
        data = [
            ("10.5880/GFZ.1", "https://example.org/a"),
            ("10.5880/GFZ.2", "https://example.org/b")
        ]

        filepath = Path(temp_dir) / "dead_links.csv"
        export_dead_links_to_csv(data, str(filepath))

        assert filepath.exists()

        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            rows = list(reader)

        assert rows[0] == ["DOI", "URL"]
        assert rows[1] == ["10.5880/GFZ.1", "https://example.org/a"]
        assert rows[2] == ["10.5880/GFZ.2", "https://example.org/b"]

    def test_export_dead_links_empty(self, temp_dir):
        """Test export with empty dead links list."""
        filepath = Path(temp_dir) / "dead_links.csv"
        export_dead_links_to_csv([], str(filepath))

        assert filepath.exists()

        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            rows = list(reader)

        assert rows[0] == ["DOI", "URL"]
        assert len(rows) == 1


class TestExportDOIsWithCreatorsToCSV:
    """Test CSV export functionality for DOIs with creators."""
    
    def test_successful_export_with_creators(self, temp_dir, sample_creator_data):
        """Test successful export of DOIs with creator information to CSV."""
        username = "TIB.GFZ"
        
        filepath = export_dois_with_creators_to_csv(sample_creator_data, username, temp_dir)
        
        # Check that file was created
        assert os.path.exists(filepath)
        assert filepath.endswith("TIB.GFZ_authors.csv")
        
        # Read and verify content
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            rows = list(reader)
        
        # Check header
        expected_header = [
            'DOI',
            'Creator Name',
            'Name Type',
            'Given Name',
            'Family Name',
            'Name Identifier',
            'Name Identifier Scheme',
            'Scheme URI'
        ]
        assert rows[0] == expected_header
        
        # Check data rows
        assert len(rows) == 4  # Header + 3 data rows
        
        # First creator with ORCID
        assert rows[1][0] == "10.5880/GFZ.1.1.2021.001"
        assert rows[1][1] == "Miller, Elizabeth"
        assert rows[1][2] == "Personal"
        assert rows[1][3] == "Elizabeth"
        assert rows[1][4] == "Miller"
        assert rows[1][5] == "https://orcid.org/0000-0001-5000-0007"
        assert rows[1][6] == "ORCID"
        assert rows[1][7] == "https://orcid.org"
        
        # Second creator without ORCID
        assert rows[2][0] == "10.5880/GFZ.1.1.2021.001"
        assert rows[2][1] == "Smith, John"
        assert rows[2][5] == ""  # No ORCID
        
        # Organizational creator
        assert rows[3][0] == "10.5880/GFZ.1.1.2021.002"
        assert rows[3][1] == "GFZ Data Services"
        assert rows[3][2] == "Organizational"
        assert rows[3][3] == ""  # No given name
        assert rows[3][4] == ""  # No family name
    
    def test_empty_creator_list_export(self, temp_dir):
        """Test export with empty creator list."""
        username = "EMPTY.USER"
        
        filepath = export_dois_with_creators_to_csv([], username, temp_dir)
        
        # File should still be created with header
        assert os.path.exists(filepath)
        
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            rows = list(reader)
        
        # Only header should be present
        assert len(rows) == 1
        assert len(rows[0]) == 8  # 8 columns
    
    def test_organizational_creators(self, temp_dir):
        """Test export of organizational creators with empty fields."""
        creator_data = [
            (
                "10.5880/GFZ.1.1.2021.001",
                "GFZ Data Services",
                "Organizational",
                "",
                "",
                "",
                "",
                ""
            )
        ]
        
        filepath = export_dois_with_creators_to_csv(creator_data, "ORG.TEST", temp_dir)
        
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            rows = list(reader)
        
        assert len(rows) == 2  # Header + 1 data row
        assert rows[1][2] == "Organizational"
        assert rows[1][3] == ""  # Empty given name
        assert rows[1][4] == ""  # Empty family name
    
    def test_creators_without_orcid(self, temp_dir):
        """Test export of creators without ORCID identifiers."""
        creator_data = [
            (
                "10.5880/GFZ.1.1.2021.001",
                "Smith, John",
                "Personal",
                "John",
                "Smith",
                "",  # No ORCID
                "",
                ""
            )
        ]
        
        filepath = export_dois_with_creators_to_csv(creator_data, "NO_ORCID.TEST", temp_dir)
        
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            rows = list(reader)
        
        assert len(rows) == 2
        assert rows[1][5] == ""  # Empty name identifier
        assert rows[1][6] == ""  # Empty scheme
        assert rows[1][7] == ""  # Empty scheme URI
    
    def test_utf8_encoding_creators(self, temp_dir):
        """Test that CSV with creators is properly encoded in UTF-8."""
        creator_data = [
            (
                "10.5880/GFZ.Ö.Ä.Ü",
                "Müller, Hans",
                "Personal",
                "Hans",
                "Müller",
                "https://orcid.org/0000-0001-2345-6789",
                "ORCID",
                "https://orcid.org"
            )
        ]
        
        filepath = export_dois_with_creators_to_csv(creator_data, "UTF8.TEST", temp_dir)
        
        # Read with UTF-8 encoding
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
            assert "Müller" in content
    
    def test_returns_filepath_creators(self, temp_dir, sample_creator_data):
        """Test that function returns the filepath."""
        username = "RETURN.TEST"
        
        filepath = export_dois_with_creators_to_csv(sample_creator_data, username, temp_dir)
        
        assert isinstance(filepath, str)
        assert filepath.endswith(f"{username}_authors.csv")
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


class TestExportDOIsWithPublisherToCSV:
    """Test CSV export functionality for DOIs with publisher data."""
    
    def test_successful_export_with_publisher(self, temp_dir, sample_publisher_data):
        """Test successful export of DOIs with publisher information to CSV."""
        username = "TIB.GFZ"
        
        filepath, warnings_count = export_dois_with_publisher_to_csv(
            sample_publisher_data, username, temp_dir
        )
        
        # Check that file was created
        assert os.path.exists(filepath)
        assert filepath.endswith("TIB.GFZ_publishers.csv")
        
        # Read and verify content
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            rows = list(reader)
        
        # Check header
        expected_header = [
            'DOI',
            'Publisher Name',
            'Publisher Identifier',
            'Publisher Identifier Scheme',
            'Scheme URI',
            'Language'
        ]
        assert rows[0] == expected_header
        
        # Check data rows
        assert len(rows) == 4  # Header + 3 data rows
        
        # First publisher with full data
        assert rows[1][0] == "10.5880/GFZ.1.1.2021.001"
        assert rows[1][1] == "GFZ German Research Centre for Geosciences"
        assert rows[1][2] == "https://ror.org/04z8jg394"
        assert rows[1][3] == "ROR"
        assert rows[1][4] == "https://ror.org"
        assert rows[1][5] == "en"
        
        # Second publisher without identifier
        assert rows[2][0] == "10.5880/GFZ.1.1.2021.002"
        assert rows[2][1] == "Helmholtz Centre Potsdam"
        assert rows[2][2] == ""  # No identifier
    
    def test_empty_publisher_list_export(self, temp_dir):
        """Test export with empty publisher list."""
        username = "TIB.GFZ"
        
        filepath, warnings_count = export_dois_with_publisher_to_csv([], username, temp_dir)
        
        # File should still be created with just header
        assert os.path.exists(filepath)
        
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            rows = list(reader)
        
        assert len(rows) == 1  # Only header
        assert warnings_count == 0
    
    def test_publisher_without_identifier_warning(self, temp_dir):
        """Test that DOIs without publisher identifier generate warnings."""
        data = [
            (
                "10.5880/GFZ.1",
                "Publisher Without ID",
                "",  # No identifier
                "",
                "",
                ""
            ),
            (
                "10.5880/GFZ.2",
                "Publisher With ID",
                "https://ror.org/12345",
                "ROR",
                "https://ror.org",
                "en"
            ),
        ]
        
        filepath, warnings_count = export_dois_with_publisher_to_csv(
            data, "TEST", temp_dir
        )
        
        assert warnings_count == 1  # One DOI without identifier
    
    def test_utf8_encoding_publisher(self, temp_dir):
        """Test UTF-8 encoding with special characters in publisher names."""
        data = [
            (
                "10.5880/GFZ.1.1.2021.001",
                "Müller Förschungszentrum für Geowissenschaften",
                "https://ror.org/12345",
                "ROR",
                "https://ror.org",
                "de"
            ),
        ]
        
        filepath, warnings_count = export_dois_with_publisher_to_csv(
            data, "TEST", temp_dir
        )
        
        # Read and verify encoding
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
        
        assert "Müller" in content
        assert "Förschungszentrum" in content
    
    def test_returns_filepath_and_warnings_tuple(self, temp_dir, sample_publisher_data):
        """Test that function returns (filepath, warnings_count) tuple."""
        result = export_dois_with_publisher_to_csv(
            sample_publisher_data, "TEST", temp_dir
        )
        
        assert isinstance(result, tuple)
        assert len(result) == 2
        
        filepath, warnings_count = result
        assert isinstance(filepath, str)
        assert isinstance(warnings_count, int)
        assert filepath.endswith("TEST_publishers.csv")
    
    def test_username_sanitization_publisher(self, temp_dir, sample_publisher_data):
        """Test that username is sanitized in filename."""
        username = "TIB/GFZ:Test"  # Contains invalid chars
        
        filepath, _ = export_dois_with_publisher_to_csv(
            sample_publisher_data, username, temp_dir
        )
        
        # Filename should not contain invalid characters
        filename = os.path.basename(filepath)
        assert "/" not in filename
        assert ":" not in filename
    
    def test_creates_output_directory_publisher(self, temp_dir, sample_publisher_data):
        """Test that output directory is created if it doesn't exist."""
        new_dir = os.path.join(temp_dir, "new_subdir")
        assert not os.path.exists(new_dir)
        
        filepath, _ = export_dois_with_publisher_to_csv(
            sample_publisher_data, "TEST", new_dir
        )
        
        assert os.path.exists(new_dir)
        assert os.path.exists(filepath)
