"""Unit tests for CSV Parser - Rights functionality."""

import pytest
import tempfile
import os

from src.utils.csv_parser import CSVParser, CSVParseError, SPDXValidationError, LanguageCodeError


class TestSPDXValidation:
    """Test SPDX license identifier validation."""

    def test_valid_spdx_identifiers_uppercase(self):
        """Test validation of valid SPDX identifiers in uppercase."""
        valid_identifiers = [
            "CC-BY-4.0",
            "CC-BY-SA-4.0",
            "CC-BY-NC-4.0",
            "CC-BY-NC-SA-4.0",
            "CC-BY-ND-4.0",
            "CC-BY-NC-ND-4.0",
            "CC0-1.0",
            "MIT",
            "Apache-2.0",
            "GPL-3.0-only",
        ]
        for identifier in valid_identifiers:
            assert CSVParser.validate_spdx_identifier(identifier), \
                f"SPDX identifier should be valid: {identifier}"

    def test_valid_spdx_identifiers_lowercase(self):
        """Test validation of valid SPDX identifiers in lowercase (DataCite normalizes to lowercase)."""
        valid_identifiers = [
            "cc-by-4.0",
            "cc-by-sa-4.0",
            "cc-by-nc-4.0",
            "cc0-1.0",
            "mit",
            "apache-2.0",
        ]
        for identifier in valid_identifiers:
            assert CSVParser.validate_spdx_identifier(identifier), \
                f"SPDX identifier should be valid (case-insensitive): {identifier}"

    def test_valid_spdx_identifiers_mixed_case(self):
        """Test validation of valid SPDX identifiers in mixed case."""
        valid_identifiers = [
            "Cc-By-4.0",
            "CC-by-SA-4.0",
            "cc-BY-4.0",
        ]
        for identifier in valid_identifiers:
            assert CSVParser.validate_spdx_identifier(identifier), \
                f"SPDX identifier should be valid (case-insensitive): {identifier}"

    def test_invalid_spdx_identifiers(self):
        """Test validation rejects invalid SPDX identifiers."""
        invalid_identifiers = [
            "CC-BY",  # Missing version
            "CC-BY-5.0",  # Non-existent version
            "INVALID-LICENSE",
            "CC BY 4.0",  # Spaces instead of dashes
            "creative-commons",
            "public-domain",
        ]
        for identifier in invalid_identifiers:
            assert not CSVParser.validate_spdx_identifier(identifier), \
                f"SPDX identifier should be invalid: {identifier}"

    def test_empty_spdx_identifier_is_valid(self):
        """Test that empty identifier is considered valid (optional field)."""
        assert CSVParser.validate_spdx_identifier("") is True
        assert CSVParser.validate_spdx_identifier(None) is True


class TestLanguageCodeValidation:
    """Test ISO 639-1 language code validation."""

    def test_valid_language_codes(self):
        """Test validation of valid language codes."""
        valid_codes = ["en", "de", "fr", "es", "it", "pt", "nl", "ja", "zh", "ko"]
        for code in valid_codes:
            assert CSVParser.validate_language_code(code), \
                f"Language code should be valid: {code}"

    def test_valid_language_codes_uppercase(self):
        """Test that uppercase codes are normalized and validated."""
        valid_codes = ["EN", "DE", "FR"]
        for code in valid_codes:
            assert CSVParser.validate_language_code(code), \
                f"Language code should be valid (case-insensitive): {code}"

    def test_invalid_language_codes(self):
        """Test validation rejects invalid language codes."""
        invalid_codes = [
            "english",  # Full word
            "eng",  # ISO 639-2 (3-letter)
            "xx",  # Non-existent
            "123",  # Numbers
            "e",  # Too short
        ]
        for code in invalid_codes:
            assert not CSVParser.validate_language_code(code), \
                f"Language code should be invalid: {code}"

    def test_empty_language_code_is_valid(self):
        """Test that empty code is considered valid (optional field)."""
        assert CSVParser.validate_language_code("") is True
        assert CSVParser.validate_language_code(None) is True


class TestParseRightsUpdateCSV:
    """Test parsing of rights update CSV files."""

    def test_parse_valid_csv(self):
        """Test parsing a valid rights CSV file."""
        with tempfile.NamedTemporaryFile(
            mode='w', suffix='.csv', delete=False, encoding='utf-8'
        ) as f:
            f.write("DOI,rights,rightsUri,schemeUri,rightsIdentifier,rightsIdentifierScheme,lang\n")
            f.write("10.5880/GFZ.1.1.2021.001,Creative Commons Attribution 4.0,https://creativecommons.org/licenses/by/4.0/legalcode,https://spdx.org/licenses/,CC-BY-4.0,SPDX,en\n")
            csv_path = f.name

        try:
            rights_by_doi, warnings = CSVParser.parse_rights_update_csv(csv_path)

            assert "10.5880/GFZ.1.1.2021.001" in rights_by_doi
            assert len(rights_by_doi["10.5880/GFZ.1.1.2021.001"]) == 1
            
            rights = rights_by_doi["10.5880/GFZ.1.1.2021.001"][0]
            assert rights["rights"] == "Creative Commons Attribution 4.0"
            assert rights["rightsUri"] == "https://creativecommons.org/licenses/by/4.0/legalcode"
            assert rights["schemeUri"] == "https://spdx.org/licenses/"
            assert rights["rightsIdentifier"] == "CC-BY-4.0"
            assert rights["rightsIdentifierScheme"] == "SPDX"
            assert rights["lang"] == "en"
            assert len(warnings) == 0
        finally:
            os.unlink(csv_path)

    def test_parse_csv_with_lowercase_spdx(self):
        """Test parsing CSV with lowercase SPDX identifier (as returned by DataCite)."""
        with tempfile.NamedTemporaryFile(
            mode='w', suffix='.csv', delete=False, encoding='utf-8'
        ) as f:
            f.write("DOI,rights,rightsUri,schemeUri,rightsIdentifier,rightsIdentifierScheme,lang\n")
            f.write("10.5880/GFZ.1.1.2021.001,CC BY 4.0,,,cc-by-4.0,SPDX,en\n")
            csv_path = f.name

        try:
            rights_by_doi, warnings = CSVParser.parse_rights_update_csv(csv_path)

            assert "10.5880/GFZ.1.1.2021.001" in rights_by_doi
            rights = rights_by_doi["10.5880/GFZ.1.1.2021.001"][0]
            assert rights["rightsIdentifier"] == "cc-by-4.0"
            assert len(warnings) == 0  # Should not raise error for lowercase
        finally:
            os.unlink(csv_path)

    def test_parse_csv_with_empty_rights(self):
        """Test parsing CSV with DOI that has no rights (empty fields)."""
        with tempfile.NamedTemporaryFile(
            mode='w', suffix='.csv', delete=False, encoding='utf-8'
        ) as f:
            f.write("DOI,rights,rightsUri,schemeUri,rightsIdentifier,rightsIdentifierScheme,lang\n")
            f.write("10.5880/GFZ.1.1.2021.001,,,,,,\n")
            csv_path = f.name

        try:
            rights_by_doi, warnings = CSVParser.parse_rights_update_csv(csv_path)

            assert "10.5880/GFZ.1.1.2021.001" in rights_by_doi
            # Empty rights should result in empty list (remove all rights)
            assert rights_by_doi["10.5880/GFZ.1.1.2021.001"] == []
        finally:
            os.unlink(csv_path)

    def test_parse_csv_with_multiple_rights_per_doi(self):
        """Test parsing CSV with multiple rights entries for same DOI."""
        with tempfile.NamedTemporaryFile(
            mode='w', suffix='.csv', delete=False, encoding='utf-8'
        ) as f:
            f.write("DOI,rights,rightsUri,schemeUri,rightsIdentifier,rightsIdentifierScheme,lang\n")
            f.write("10.5880/GFZ.1.1.2021.001,License 1,https://example.org/1,,,,en\n")
            f.write("10.5880/GFZ.1.1.2021.001,License 2,https://example.org/2,,,,de\n")
            csv_path = f.name

        try:
            rights_by_doi, warnings = CSVParser.parse_rights_update_csv(csv_path)

            assert len(rights_by_doi["10.5880/GFZ.1.1.2021.001"]) == 2
            assert rights_by_doi["10.5880/GFZ.1.1.2021.001"][0]["rights"] == "License 1"
            assert rights_by_doi["10.5880/GFZ.1.1.2021.001"][1]["rights"] == "License 2"
        finally:
            os.unlink(csv_path)

    def test_parse_csv_invalid_spdx(self):
        """Test that invalid SPDX identifier raises error."""
        with tempfile.NamedTemporaryFile(
            mode='w', suffix='.csv', delete=False, encoding='utf-8'
        ) as f:
            f.write("DOI,rights,rightsUri,schemeUri,rightsIdentifier,rightsIdentifierScheme,lang\n")
            f.write("10.5880/GFZ.1.1.2021.001,Test,,,INVALID-LICENSE,SPDX,en\n")
            csv_path = f.name

        try:
            with pytest.raises(SPDXValidationError) as exc_info:
                CSVParser.parse_rights_update_csv(csv_path)
            assert "SPDX" in str(exc_info.value)
            assert "INVALID-LICENSE" in str(exc_info.value)
        finally:
            os.unlink(csv_path)

    def test_parse_csv_invalid_language_code(self):
        """Test that invalid language code raises error."""
        with tempfile.NamedTemporaryFile(
            mode='w', suffix='.csv', delete=False, encoding='utf-8'
        ) as f:
            f.write("DOI,rights,rightsUri,schemeUri,rightsIdentifier,rightsIdentifierScheme,lang\n")
            f.write("10.5880/GFZ.1.1.2021.001,Test,,,,SPDX,invalid\n")
            csv_path = f.name

        try:
            with pytest.raises(LanguageCodeError) as exc_info:
                CSVParser.parse_rights_update_csv(csv_path)
            assert "Sprachcode" in str(exc_info.value) or "lang" in str(exc_info.value).lower()
        finally:
            os.unlink(csv_path)

    def test_parse_csv_missing_doi(self):
        """Test that rows without DOI are skipped with warning."""
        with tempfile.NamedTemporaryFile(
            mode='w', suffix='.csv', delete=False, encoding='utf-8'
        ) as f:
            f.write("DOI,rights,rightsUri,schemeUri,rightsIdentifier,rightsIdentifierScheme,lang\n")
            f.write(",Test License,https://example.org,,,,en\n")
            f.write("10.5880/GFZ.1.1.2021.001,Valid License,,,,,\n")
            csv_path = f.name

        try:
            rights_by_doi, warnings = CSVParser.parse_rights_update_csv(csv_path)

            assert len(rights_by_doi) == 1
            assert "10.5880/GFZ.1.1.2021.001" in rights_by_doi
            assert len(warnings) == 1
            assert "DOI fehlt" in warnings[0]
        finally:
            os.unlink(csv_path)

    def test_parse_csv_invalid_doi_format(self):
        """Test that invalid DOI format raises error."""
        with tempfile.NamedTemporaryFile(
            mode='w', suffix='.csv', delete=False, encoding='utf-8'
        ) as f:
            f.write("DOI,rights,rightsUri,schemeUri,rightsIdentifier,rightsIdentifierScheme,lang\n")
            f.write("not-a-valid-doi,Test,,,,SPDX,en\n")
            csv_path = f.name

        try:
            with pytest.raises(CSVParseError) as exc_info:
                CSVParser.parse_rights_update_csv(csv_path)
            assert "DOI-Format" in str(exc_info.value)
        finally:
            os.unlink(csv_path)

    def test_parse_csv_missing_headers(self):
        """Test that missing required headers raises error."""
        with tempfile.NamedTemporaryFile(
            mode='w', suffix='.csv', delete=False, encoding='utf-8'
        ) as f:
            f.write("DOI,rights,rightsUri\n")  # Missing schemeUri, rightsIdentifier, etc.
            f.write("10.5880/GFZ.1.1.2021.001,Test,https://example.org\n")
            csv_path = f.name

        try:
            with pytest.raises(CSVParseError) as exc_info:
                CSVParser.parse_rights_update_csv(csv_path)
            assert "Header" in str(exc_info.value) or "fehlen" in str(exc_info.value)
        finally:
            os.unlink(csv_path)

    def test_parse_csv_file_not_found(self):
        """Test handling of non-existent file."""
        with pytest.raises(FileNotFoundError):
            CSVParser.parse_rights_update_csv("/non/existent/file.csv")

    def test_parse_csv_non_spdx_identifier_scheme(self):
        """Test that non-SPDX identifier schemes are not validated against SPDX list."""
        with tempfile.NamedTemporaryFile(
            mode='w', suffix='.csv', delete=False, encoding='utf-8'
        ) as f:
            f.write("DOI,rights,rightsUri,schemeUri,rightsIdentifier,rightsIdentifierScheme,lang\n")
            f.write("10.5880/GFZ.1.1.2021.001,Custom License,,,CUSTOM-ID,CustomScheme,en\n")
            csv_path = f.name

        try:
            rights_by_doi, warnings = CSVParser.parse_rights_update_csv(csv_path)

            # Should not raise error - CUSTOM-ID is not validated against SPDX
            assert "10.5880/GFZ.1.1.2021.001" in rights_by_doi
            rights = rights_by_doi["10.5880/GFZ.1.1.2021.001"][0]
            assert rights["rightsIdentifier"] == "CUSTOM-ID"
            assert rights["rightsIdentifierScheme"] == "CustomScheme"
        finally:
            os.unlink(csv_path)

    def test_parse_csv_multiple_dois(self):
        """Test parsing CSV with multiple different DOIs."""
        with tempfile.NamedTemporaryFile(
            mode='w', suffix='.csv', delete=False, encoding='utf-8'
        ) as f:
            f.write("DOI,rights,rightsUri,schemeUri,rightsIdentifier,rightsIdentifierScheme,lang\n")
            f.write("10.5880/GFZ.1.1.2021.001,License A,,,CC-BY-4.0,SPDX,en\n")
            f.write("10.5880/GFZ.1.1.2021.002,,,,,,\n")
            f.write("10.5880/GFZ.1.1.2021.003,License B,https://example.org,,,,de\n")
            csv_path = f.name

        try:
            rights_by_doi, warnings = CSVParser.parse_rights_update_csv(csv_path)

            assert len(rights_by_doi) == 3
            assert len(rights_by_doi["10.5880/GFZ.1.1.2021.001"]) == 1
            assert rights_by_doi["10.5880/GFZ.1.1.2021.002"] == []  # Empty rights
            assert len(rights_by_doi["10.5880/GFZ.1.1.2021.003"]) == 1
        finally:
            os.unlink(csv_path)

    def test_parse_csv_whitespace_trimming(self):
        """Test that whitespace is properly trimmed from values."""
        with tempfile.NamedTemporaryFile(
            mode='w', suffix='.csv', delete=False, encoding='utf-8'
        ) as f:
            f.write("DOI,rights,rightsUri,schemeUri,rightsIdentifier,rightsIdentifierScheme,lang\n")
            f.write("  10.5880/GFZ.1.1.2021.001  ,  License A  ,  https://example.org  ,,  CC-BY-4.0  ,  SPDX  ,  en  \n")
            csv_path = f.name

        try:
            rights_by_doi, warnings = CSVParser.parse_rights_update_csv(csv_path)

            assert "10.5880/GFZ.1.1.2021.001" in rights_by_doi
            rights = rights_by_doi["10.5880/GFZ.1.1.2021.001"][0]
            assert rights["rights"] == "License A"
            assert rights["rightsUri"] == "https://example.org"
            assert rights["rightsIdentifier"] == "CC-BY-4.0"
            assert rights["lang"] == "en"
        finally:
            os.unlink(csv_path)
