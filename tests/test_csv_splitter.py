"""Tests for CSV splitter functionality."""

import pytest
import csv
from pathlib import Path
from unittest.mock import Mock

from src.utils.csv_splitter import (
    extract_doi_prefix,
    split_csv_by_doi_prefix,
    CSVSplitError
)


class TestExtractDOIPrefix:
    """Tests for DOI prefix extraction."""
    
    def test_extract_prefix_level2_simple(self):
        """Test extracting level 2 prefix from simple DOI."""
        doi = "10.5880/gfz.2011.100"
        prefix = extract_doi_prefix(doi, level=2)
        assert prefix == "10.5880/gfz.2011"
    
    def test_extract_prefix_level2_complex(self):
        """Test extracting level 2 prefix from complex DOI."""
        doi = "10.1594/gfz.geofon.gfz2008ewsv"
        prefix = extract_doi_prefix(doi, level=2)
        assert prefix == "10.1594/gfz.geofon"
    
    def test_extract_prefix_level1(self):
        """Test extracting level 1 prefix (registrant only)."""
        doi = "10.5880/gfz.2011.100"
        prefix = extract_doi_prefix(doi, level=1)
        assert prefix == "10.5880"
    
    def test_extract_prefix_level3(self):
        """Test extracting level 3 prefix."""
        doi = "10.5880/gfz.2011.100"
        prefix = extract_doi_prefix(doi, level=3)
        assert prefix == "10.5880/gfz.2011.100"
    
    def test_extract_prefix_isdc_format(self):
        """Test extracting prefix from ISDC DOI format."""
        doi = "10.1594/gfz.isdc.champ/ch-me-2-asc-boom"
        prefix = extract_doi_prefix(doi, level=2)
        assert prefix == "10.1594/gfz.isdc"
    
    def test_extract_prefix_short_doi(self):
        """Test extracting prefix from DOI with few parts."""
        doi = "10.5880/simple"
        prefix = extract_doi_prefix(doi, level=2)
        # Should return what's available
        assert prefix == "10.5880/simple"
    
    def test_extract_prefix_invalid_no_slash(self):
        """Test error handling for DOI without slash."""
        with pytest.raises(CSVSplitError, match="Ungültiges DOI-Format"):
            extract_doi_prefix("10.5880.invalid", level=2)
    
    def test_extract_prefix_invalid_empty(self):
        """Test error handling for empty DOI."""
        with pytest.raises(CSVSplitError, match="Ungültiges DOI-Format"):
            extract_doi_prefix("", level=2)
    
    def test_extract_prefix_invalid_none(self):
        """Test error handling for None DOI."""
        with pytest.raises(CSVSplitError, match="Ungültiges DOI-Format"):
            extract_doi_prefix(None, level=2)
    
    def test_extract_prefix_consistency(self):
        """Test that similar DOIs get same prefix."""
        doi1 = "10.5880/gfz.2011.100"
        doi2 = "10.5880/gfz.2011.200"
        doi3 = "10.5880/gfz.2011.999"
        
        prefix1 = extract_doi_prefix(doi1, level=2)
        prefix2 = extract_doi_prefix(doi2, level=2)
        prefix3 = extract_doi_prefix(doi3, level=2)
        
        assert prefix1 == prefix2 == prefix3 == "10.5880/gfz.2011"
    
    def test_extract_prefix_invalid_level(self):
        """Test error handling for invalid prefix level."""
        doi = "10.5880/gfz.2011.100"
        
        # Test level < 1
        with pytest.raises(CSVSplitError, match="Ungültiger prefix_level.*zwischen 1 und 4"):
            extract_doi_prefix(doi, level=0)
        
        # Test level > 4
        with pytest.raises(CSVSplitError, match="Ungültiger prefix_level.*zwischen 1 und 4"):
            extract_doi_prefix(doi, level=5)
        
        # Test negative level
        with pytest.raises(CSVSplitError, match="Ungültiger prefix_level.*zwischen 1 und 4"):
            extract_doi_prefix(doi, level=-1)
    
    def test_extract_prefix_doi_ends_with_slash(self):
        """Test error handling for DOI ending with slash."""
        # DOI ending with / should raise error
        with pytest.raises(CSVSplitError, match="Ungültiges DOI-Format: 10.5880/"):
            extract_doi_prefix("10.5880/", level=2)


class TestSplitCSVByDOIPrefix:
    """Tests for CSV splitting by DOI prefix."""
    
    def test_split_basic_urls_csv(self, tmp_path):
        """Test splitting a basic URLs CSV file."""
        # Create test input file
        input_file = tmp_path / "test_input.csv"
        with open(input_file, 'w', encoding='utf-8-sig', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['DOI', 'Landing_Page_URL'])
            writer.writerow(['10.5880/gfz.2011.100', 'http://example.com/1'])
            writer.writerow(['10.5880/gfz.2011.200', 'http://example.com/2'])
            writer.writerow(['10.1594/gfz.geofon.test1', 'http://example.com/3'])
            writer.writerow(['10.1594/gfz.geofon.test2', 'http://example.com/4'])
        
        output_dir = tmp_path / "output"
        
        total_rows, prefix_counts = split_csv_by_doi_prefix(
            input_file,
            output_dir,
            prefix_level=2
        )
        
        # Check totals
        assert total_rows == 4
        assert len(prefix_counts) == 2
        assert prefix_counts['10.5880/gfz.2011'] == 2
        assert prefix_counts['10.1594/gfz.geofon'] == 2
        
        # Check output files exist
        assert (output_dir / "test_input_10.5880_gfz.2011.csv").exists()
        assert (output_dir / "test_input_10.1594_gfz.geofon.csv").exists()
        
        # Verify content of first output file
        with open(output_dir / "test_input_10.5880_gfz.2011.csv", 'r', encoding='utf-8-sig') as f:
            reader = csv.reader(f)
            rows = list(reader)
            assert len(rows) == 3  # Header + 2 data rows
            assert rows[0] == ['DOI', 'Landing_Page_URL']
            assert rows[1][0] == '10.5880/gfz.2011.100'
            assert rows[2][0] == '10.5880/gfz.2011.200'
    
    def test_split_authors_csv(self, tmp_path):
        """Test splitting an authors CSV file."""
        input_file = tmp_path / "test_authors.csv"
        with open(input_file, 'w', encoding='utf-8-sig', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['DOI', 'Creator Name', 'Name Type', 'Given Name', 'Family Name', 
                           'Name Identifier', 'Name Identifier Scheme', 'Scheme URI'])
            writer.writerow(['10.5880/gfz.2011.100', 'John Doe', 'Personal', 'John', 'Doe', 
                           '0000-0001-2345-6789', 'ORCID', 'https://orcid.org'])
            writer.writerow(['10.5880/gfz.2011.200', 'Jane Smith', 'Personal', 'Jane', 'Smith', 
                           '', '', ''])
            writer.writerow(['10.1594/gfz.test.001', 'GFZ', 'Organizational', '', '', 
                           '', '', ''])
        
        output_dir = tmp_path / "output"
        
        total_rows, prefix_counts = split_csv_by_doi_prefix(
            input_file,
            output_dir,
            prefix_level=2
        )
        
        assert total_rows == 3
        assert len(prefix_counts) == 2
        assert prefix_counts['10.5880/gfz.2011'] == 2
        assert prefix_counts['10.1594/gfz.test'] == 1
    
    def test_split_with_prefix_level1(self, tmp_path):
        """Test splitting with level 1 prefix (registrant only)."""
        input_file = tmp_path / "test_input.csv"
        with open(input_file, 'w', encoding='utf-8-sig', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['DOI', 'Landing_Page_URL'])
            writer.writerow(['10.5880/gfz.2011.100', 'http://example.com/1'])
            writer.writerow(['10.5880/gfz.2012.100', 'http://example.com/2'])
            writer.writerow(['10.1594/gfz.test.001', 'http://example.com/3'])
        
        output_dir = tmp_path / "output"
        
        total_rows, prefix_counts = split_csv_by_doi_prefix(
            input_file,
            output_dir,
            prefix_level=1
        )
        
        # With level 1, all 10.5880 should be grouped together
        assert total_rows == 3
        assert len(prefix_counts) == 2
        assert prefix_counts['10.5880'] == 2
        assert prefix_counts['10.1594'] == 1
    
    def test_split_with_progress_callback(self, tmp_path):
        """Test that progress callback is called."""
        input_file = tmp_path / "test_input.csv"
        with open(input_file, 'w', encoding='utf-8-sig', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['DOI', 'Landing_Page_URL'])
            writer.writerow(['10.5880/gfz.2011.100', 'http://example.com/1'])
            writer.writerow(['10.1594/gfz.test.001', 'http://example.com/2'])
        
        output_dir = tmp_path / "output"
        
        # Mock callback
        progress_callback = Mock()
        
        split_csv_by_doi_prefix(
            input_file,
            output_dir,
            prefix_level=2,
            progress_callback=progress_callback
        )
        
        # Verify callback was called
        assert progress_callback.call_count > 0
        # Check for expected messages
        calls = [str(call) for call in progress_callback.call_args_list]
        assert any('Lese CSV-Datei' in str(call) for call in calls)
    
    def test_split_skips_invalid_dois(self, tmp_path, caplog):
        """Test that invalid DOIs are skipped gracefully and logged."""
        import logging
        
        input_file = tmp_path / "test_input.csv"
        with open(input_file, 'w', encoding='utf-8-sig', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['DOI', 'Landing_Page_URL'])
            writer.writerow(['10.5880/gfz.2011.100', 'http://example.com/1'])
            writer.writerow(['invalid-doi', 'http://example.com/2'])  # Invalid
            writer.writerow(['', 'http://example.com/3'])  # Empty
            writer.writerow(['10.5880/gfz.2011.200', 'http://example.com/4'])
        
        output_dir = tmp_path / "output"
        
        with caplog.at_level(logging.WARNING):
            total_rows, prefix_counts = split_csv_by_doi_prefix(
                input_file,
                output_dir,
                prefix_level=2
            )
        
        # Should only count valid DOIs
        assert total_rows == 2
        assert prefix_counts['10.5880/gfz.2011'] == 2
        
        # Verify warnings were logged for invalid DOIs
        assert any('Überspringe ungültigen DOI: invalid-doi' in record.message 
                   for record in caplog.records if record.levelno == logging.WARNING)
    
    def test_split_creates_output_directory(self, tmp_path):
        """Test that output directory is created if it doesn't exist."""
        input_file = tmp_path / "test_input.csv"
        with open(input_file, 'w', encoding='utf-8-sig', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['DOI', 'Landing_Page_URL'])
            writer.writerow(['10.5880/gfz.2011.100', 'http://example.com/1'])
        
        output_dir = tmp_path / "deep" / "nested" / "output"
        assert not output_dir.exists()
        
        split_csv_by_doi_prefix(input_file, output_dir, prefix_level=2)
        
        assert output_dir.exists()
        assert output_dir.is_dir()
    
    def test_split_nonexistent_file_error(self, tmp_path):
        """Test error when input file doesn't exist."""
        input_file = tmp_path / "nonexistent.csv"
        output_dir = tmp_path / "output"
        
        with pytest.raises(CSVSplitError, match="Eingabedatei nicht gefunden"):
            split_csv_by_doi_prefix(input_file, output_dir, prefix_level=2)
    
    def test_split_invalid_csv_format_error(self, tmp_path):
        """Test error when CSV doesn't have DOI as first column."""
        input_file = tmp_path / "test_input.csv"
        with open(input_file, 'w', encoding='utf-8-sig', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['URL', 'Title'])  # Wrong header
            writer.writerow(['http://example.com/1', 'Test'])
        
        output_dir = tmp_path / "output"
        
        with pytest.raises(CSVSplitError, match="CSV-Datei muss 'DOI' als erste Spalte haben"):
            split_csv_by_doi_prefix(input_file, output_dir, prefix_level=2)
    
    def test_split_single_prefix_group(self, tmp_path):
        """Test splitting when all DOIs have same prefix."""
        input_file = tmp_path / "test_input.csv"
        with open(input_file, 'w', encoding='utf-8-sig', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['DOI', 'Landing_Page_URL'])
            writer.writerow(['10.5880/gfz.2011.100', 'http://example.com/1'])
            writer.writerow(['10.5880/gfz.2011.200', 'http://example.com/2'])
            writer.writerow(['10.5880/gfz.2011.300', 'http://example.com/3'])
        
        output_dir = tmp_path / "output"
        
        total_rows, prefix_counts = split_csv_by_doi_prefix(
            input_file,
            output_dir,
            prefix_level=2
        )
        
        assert total_rows == 3
        assert len(prefix_counts) == 1
        assert prefix_counts['10.5880/gfz.2011'] == 3
        
        # Should create single output file
        output_files = list(output_dir.glob("*.csv"))
        assert len(output_files) == 1
    
    def test_split_many_prefix_groups(self, tmp_path):
        """Test splitting with many different prefix groups."""
        input_file = tmp_path / "test_input.csv"
        with open(input_file, 'w', encoding='utf-8-sig', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['DOI', 'Landing_Page_URL'])
            for i in range(10):
                writer.writerow([f'10.5880/gfz.{2000+i}.100', f'http://example.com/{i}'])
        
        output_dir = tmp_path / "output"
        
        total_rows, prefix_counts = split_csv_by_doi_prefix(
            input_file,
            output_dir,
            prefix_level=2
        )
        
        assert total_rows == 10
        assert len(prefix_counts) == 10  # Each year is a different prefix
        
        # Should create 10 output files
        output_files = list(output_dir.glob("*.csv"))
        assert len(output_files) == 10
    
    def test_split_preserves_csv_encoding(self, tmp_path):
        """Test that UTF-8 BOM encoding is preserved."""
        input_file = tmp_path / "test_input.csv"
        with open(input_file, 'w', encoding='utf-8-sig', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['DOI', 'Landing_Page_URL'])
            writer.writerow(['10.5880/gfz.2011.100', 'http://example.com/ä'])
        
        output_dir = tmp_path / "output"
        
        split_csv_by_doi_prefix(input_file, output_dir, prefix_level=2)
        
        # Verify UTF-8 BOM is used in output
        output_file = output_dir / "test_input_10.5880_gfz.2011.csv"
        with open(output_file, 'r', encoding='utf-8-sig') as f:
            content = f.read()
            assert 'ä' in content
    
    def test_split_filename_sanitization(self, tmp_path):
        """Test that slashes in prefix are replaced in filename."""
        input_file = tmp_path / "test_input.csv"
        with open(input_file, 'w', encoding='utf-8-sig', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['DOI', 'Landing_Page_URL'])
            writer.writerow(['10.5880/gfz.2011.100', 'http://example.com/1'])
        
        output_dir = tmp_path / "output"
        
        split_csv_by_doi_prefix(input_file, output_dir, prefix_level=2)
        
        # Verify slash is replaced with underscore
        expected_file = output_dir / "test_input_10.5880_gfz.2011.csv"
        assert expected_file.exists()
        assert '/' not in expected_file.name
    
    def test_split_empty_csv_with_header_only(self, tmp_path):
        """Test splitting CSV with only header row (no data)."""
        input_file = tmp_path / "test_input.csv"
        with open(input_file, 'w', encoding='utf-8-sig', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['DOI', 'Landing_Page_URL'])
            # No data rows
        
        output_dir = tmp_path / "output"
        
        total_rows, prefix_counts = split_csv_by_doi_prefix(
            input_file,
            output_dir,
            prefix_level=2
        )
        
        # Should return 0 rows and empty dict
        assert total_rows == 0
        assert prefix_counts == {}
        
        # Output directory should exist but contain no CSV files
        assert output_dir.exists()
        output_files = list(output_dir.glob("*.csv"))
        assert len(output_files) == 0
    
    def test_split_completely_empty_csv(self, tmp_path):
        """Test splitting completely empty CSV file (no rows at all)."""
        input_file = tmp_path / "test_input.csv"
        # Create empty file
        input_file.touch()
        
        output_dir = tmp_path / "output"
        
        with pytest.raises(CSVSplitError, match="CSV-Datei ist leer"):
            split_csv_by_doi_prefix(input_file, output_dir, prefix_level=2)
    
    def test_split_invalid_prefix_level(self, tmp_path):
        """Test error handling for invalid prefix_level."""
        input_file = tmp_path / "test_input.csv"
        input_file.write_text("DOI,URL\n10.5880/test,http://example.com\n")
        
        output_dir = tmp_path / "output"
        
        # Test level < 1
        with pytest.raises(CSVSplitError, match="Ungültiger Prefix-Level.*zwischen 1 und 4"):
            split_csv_by_doi_prefix(input_file, output_dir, prefix_level=0)
        
        # Test level > 4
        with pytest.raises(CSVSplitError, match="Ungültiger Prefix-Level.*zwischen 1 und 4"):
            split_csv_by_doi_prefix(input_file, output_dir, prefix_level=5)
