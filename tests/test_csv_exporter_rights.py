"""Unit tests for CSV Exporter - Rights functionality."""

import csv
import os
import pytest
import tempfile
from pathlib import Path

from src.utils.csv_exporter import (
    export_dois_with_rights_to_csv,
    CSVExportError
)


@pytest.fixture
def temp_dir():
    """Create a temporary directory for testing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


@pytest.fixture
def sample_rights_data():
    """Sample rights data for testing."""
    return [
        (
            "10.5880/GFZ.1.1.2021.001",
            "Creative Commons Attribution 4.0 International",
            "https://creativecommons.org/licenses/by/4.0/legalcode",
            "https://spdx.org/licenses/",
            "cc-by-4.0",
            "SPDX",
            "en"
        ),
        (
            "10.5880/GFZ.1.1.2021.002",
            "",  # No rights
            "",
            "",
            "",
            "",
            ""
        ),
        (
            "10.5880/GFZ.1.1.2021.003",
            "CC BY 4.0",
            "http://creativecommons.org/licenses/by/4.0",
            "",
            "",
            "",
            ""
        ),
    ]


@pytest.fixture
def sample_rights_data_multiple_per_doi():
    """Sample rights data with multiple rights per DOI."""
    return [
        (
            "10.5880/GFZ.1.1.2021.001",
            "License 1",
            "https://example.org/license1",
            "",
            "CC-BY-4.0",
            "SPDX",
            "en"
        ),
        (
            "10.5880/GFZ.1.1.2021.001",
            "License 2",
            "https://example.org/license2",
            "",
            "",
            "",
            "de"
        ),
        (
            "10.5880/GFZ.1.1.2021.002",
            "Single License",
            "",
            "",
            "",
            "",
            ""
        ),
    ]


class TestExportDOIsWithRightsToCSV:
    """Test exporting DOIs with rights to CSV."""

    def test_export_basic(self, temp_dir, sample_rights_data):
        """Test basic export functionality."""
        filepath = export_dois_with_rights_to_csv(
            sample_rights_data,
            "TIB.GFZ",
            temp_dir
        )

        assert Path(filepath).exists()
        assert Path(filepath).name == "TIB.GFZ_rights.csv"

        # Verify content
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        assert len(rows) == 3

        # Check first row (full rights data)
        assert rows[0]['DOI'] == "10.5880/GFZ.1.1.2021.001"
        assert rows[0]['rights'] == "Creative Commons Attribution 4.0 International"
        assert rows[0]['rightsUri'] == "https://creativecommons.org/licenses/by/4.0/legalcode"
        assert rows[0]['schemeUri'] == "https://spdx.org/licenses/"
        assert rows[0]['rightsIdentifier'] == "cc-by-4.0"
        assert rows[0]['rightsIdentifierScheme'] == "SPDX"
        assert rows[0]['lang'] == "en"

        # Check second row (empty rights)
        assert rows[1]['DOI'] == "10.5880/GFZ.1.1.2021.002"
        assert rows[1]['rights'] == ""
        assert rows[1]['rightsIdentifier'] == ""

    def test_export_headers(self, temp_dir, sample_rights_data):
        """Test that exported CSV has correct headers."""
        filepath = export_dois_with_rights_to_csv(
            sample_rights_data,
            "TIB.GFZ",
            temp_dir
        )

        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            headers = next(reader)

        expected_headers = [
            'DOI', 'rights', 'rightsUri', 'schemeUri',
            'rightsIdentifier', 'rightsIdentifierScheme', 'lang'
        ]
        assert headers == expected_headers

    def test_export_multiple_rights_per_doi(self, temp_dir, sample_rights_data_multiple_per_doi):
        """Test export with multiple rights entries for same DOI."""
        filepath = export_dois_with_rights_to_csv(
            sample_rights_data_multiple_per_doi,
            "TIB.GFZ",
            temp_dir
        )

        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        assert len(rows) == 3

        # First DOI should appear twice
        doi_001_rows = [r for r in rows if r['DOI'] == "10.5880/GFZ.1.1.2021.001"]
        assert len(doi_001_rows) == 2
        assert doi_001_rows[0]['rights'] == "License 1"
        assert doi_001_rows[1]['rights'] == "License 2"

    def test_export_empty_data(self, temp_dir):
        """Test export with empty data list."""
        filepath = export_dois_with_rights_to_csv(
            [],
            "TIB.GFZ",
            temp_dir
        )

        assert Path(filepath).exists()

        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        assert len(rows) == 0

    def test_export_special_characters(self, temp_dir):
        """Test export with special characters in rights text."""
        special_data = [
            (
                "10.5880/GFZ.1.1.2021.001",
                "License with \"quotes\" and, commas",
                "https://example.org",
                "",
                "",
                "",
                ""
            ),
            (
                "10.5880/GFZ.1.1.2021.002",
                "Lizenz mit Umlauten: äöüß",
                "",
                "",
                "",
                "",
                "de"
            ),
        ]

        filepath = export_dois_with_rights_to_csv(
            special_data,
            "TIB.GFZ",
            temp_dir
        )

        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        assert rows[0]['rights'] == 'License with "quotes" and, commas'
        assert rows[1]['rights'] == "Lizenz mit Umlauten: äöüß"

    def test_export_filename_format(self, temp_dir, sample_rights_data):
        """Test that filename follows expected format."""
        filepath = export_dois_with_rights_to_csv(
            sample_rights_data,
            "XUVM.KDVJHQ",
            temp_dir
        )

        assert Path(filepath).name == "XUVM.KDVJHQ_rights.csv"

    def test_export_utf8_encoding(self, temp_dir):
        """Test that file is exported with UTF-8 encoding."""
        unicode_data = [
            (
                "10.5880/GFZ.1.1.2021.001",
                "日本語ライセンス",
                "",
                "",
                "",
                "",
                "ja"
            ),
        ]

        filepath = export_dois_with_rights_to_csv(
            unicode_data,
            "TIB.GFZ",
            temp_dir
        )

        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()

        assert "日本語ライセンス" in content

    def test_export_returns_filepath(self, temp_dir, sample_rights_data):
        """Test that export returns the correct filepath."""
        filepath = export_dois_with_rights_to_csv(
            sample_rights_data,
            "TIB.GFZ",
            temp_dir
        )

        expected_path = os.path.join(temp_dir, "TIB.GFZ_rights.csv")
        assert filepath == expected_path

    def test_export_invalid_directory(self, sample_rights_data):
        """Test export to invalid directory raises error (only runs on non-Windows)."""
        import sys
        if sys.platform == 'win32':
            # On Windows, use an invalid path with illegal characters
            with pytest.raises((CSVExportError, OSError, ValueError)):
                export_dois_with_rights_to_csv(
                    sample_rights_data,
                    "TIB.GFZ",
                    "Z:\\<>|*?\"nonexistent"  # Invalid characters on Windows
                )
        else:
            # On Unix-like systems, use a permission-denied scenario
            with pytest.raises((CSVExportError, OSError, FileNotFoundError)):
                export_dois_with_rights_to_csv(
                    sample_rights_data,
                    "TIB.GFZ",
                    "/non/existent/directory"
                )

    def test_export_preserves_order(self, temp_dir):
        """Test that export preserves order of entries."""
        ordered_data = [
            ("10.5880/GFZ.001", "First", "", "", "", "", ""),
            ("10.5880/GFZ.002", "Second", "", "", "", "", ""),
            ("10.5880/GFZ.003", "Third", "", "", "", "", ""),
            ("10.5880/GFZ.004", "Fourth", "", "", "", "", ""),
        ]

        filepath = export_dois_with_rights_to_csv(
            ordered_data,
            "TIB.GFZ",
            temp_dir
        )

        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        assert rows[0]['rights'] == "First"
        assert rows[1]['rights'] == "Second"
        assert rows[2]['rights'] == "Third"
        assert rows[3]['rights'] == "Fourth"

    def test_export_all_fields_empty_except_doi(self, temp_dir):
        """Test export where only DOI is populated (no rights)."""
        data = [
            ("10.5880/GFZ.1.1.2021.001", "", "", "", "", "", ""),
            ("10.5880/GFZ.1.1.2021.002", "", "", "", "", "", ""),
        ]

        filepath = export_dois_with_rights_to_csv(
            data,
            "TIB.GFZ",
            temp_dir
        )

        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        assert len(rows) == 2
        for row in rows:
            assert row['DOI'].startswith("10.5880/")
            assert row['rights'] == ""
            assert row['rightsUri'] == ""
            assert row['rightsIdentifier'] == ""
