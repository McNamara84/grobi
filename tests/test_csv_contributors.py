"""Tests for CSV exporter and parser contributor functionality."""

import csv
import os
import pytest

from src.utils.csv_exporter import export_dois_with_contributors_to_csv
from src.utils.csv_parser import CSVParser, CSVParseError


class TestExportContributorsCSV:
    """Tests for export_dois_with_contributors_to_csv function."""
    
    def test_export_creates_file(self, tmp_path):
        """Test that export creates a CSV file."""
        data = [
            (
                '10.5880/GFZ.1.1.2021.001',
                'Müller, Hans',
                'Personal',
                'Hans',
                'Müller',
                '0000-0001-2345-6789',
                'ORCID',
                'https://orcid.org',
                'ContactPerson, DataManager',
                'GFZ Potsdam',
                'https://ror.org/04z8jg394',
                'hans.mueller@gfz.de',
                'https://www.gfz.de',
                'Senior Scientist'
            )
        ]
        
        filepath = export_dois_with_contributors_to_csv(
            data, 'TIB.GFZ', str(tmp_path)
        )
        
        assert os.path.exists(filepath)
        assert filepath.endswith('TIB.GFZ_contributors.csv')
    
    def test_export_correct_headers(self, tmp_path):
        """Test that exported CSV has correct headers."""
        data = [
            (
                '10.5880/GFZ.1.1.2021.001',
                'Test Name', 'Personal', 'Test', 'Name',
                '', '', '', 'Researcher',
                '', '', '', '', ''
            )
        ]
        
        filepath = export_dois_with_contributors_to_csv(
            data, 'TIB.GFZ', str(tmp_path)
        )
        
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            headers = next(reader)
        
        expected_headers = [
            'DOI', 'Contributor Name', 'Name Type', 'Given Name', 'Family Name',
            'Name Identifier', 'Name Identifier Scheme', 'Scheme URI',
            'Contributor Types', 'Affiliation', 'Affiliation Identifier',
            'Email', 'Website', 'Position'
        ]
        
        assert headers == expected_headers
    
    def test_export_multiple_contributors(self, tmp_path):
        """Test exporting multiple contributors for same DOI."""
        data = [
            (
                '10.5880/GFZ.1.1.2021.001',
                'Müller, Hans', 'Personal', 'Hans', 'Müller',
                '0000-0001-2345-6789', 'ORCID', 'https://orcid.org',
                'ContactPerson', '', '',
                'hans@gfz.de', '', ''
            ),
            (
                '10.5880/GFZ.1.1.2021.001',
                'GFZ Data Services', 'Organizational', '', '',
                'https://ror.org/04z8jg394', 'ROR', 'https://ror.org',
                'HostingInstitution', '', '',
                '', '', ''
            )
        ]
        
        filepath = export_dois_with_contributors_to_csv(
            data, 'TIB.GFZ', str(tmp_path)
        )
        
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader)  # Skip header
            rows = list(reader)
        
        assert len(rows) == 2
        assert rows[0][1] == 'Müller, Hans'
        assert rows[1][1] == 'GFZ Data Services'
    
    def test_export_comma_separated_contributor_types(self, tmp_path):
        """Test that multiple ContributorTypes are preserved."""
        data = [
            (
                '10.5880/GFZ.1.1.2021.001',
                'Multi Role', 'Personal', 'Multi', 'Role',
                '', '', '',
                'ContactPerson, DataManager, Researcher',  # Multiple types
                '', '', '', '', ''
            )
        ]
        
        filepath = export_dois_with_contributors_to_csv(
            data, 'TIB.GFZ', str(tmp_path)
        )
        
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader)  # Skip header
            row = next(reader)
        
        # ContributorTypes is column 8 (0-indexed)
        assert row[8] == 'ContactPerson, DataManager, Researcher'
    
    def test_export_unicode_characters(self, tmp_path):
        """Test that Unicode characters are properly handled."""
        data = [
            (
                '10.5880/GFZ.1.1.2021.001',
                'Müller-Schäfer, Jürgen',  # German umlauts
                'Personal', 'Jürgen', 'Müller-Schäfer',
                '', '', '', 'Researcher',
                'Universität Würzburg',  # More umlauts
                '', '', '', ''
            )
        ]
        
        filepath = export_dois_with_contributors_to_csv(
            data, 'TIB.GFZ', str(tmp_path)
        )
        
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader)
            row = next(reader)
        
        assert row[1] == 'Müller-Schäfer, Jürgen'
        assert row[9] == 'Universität Würzburg'
    
    def test_export_sanitizes_username(self, tmp_path):
        """Test that username is sanitized for filename."""
        data = [
            ('10.5880/GFZ.1', 'Test', 'Personal', '', '', '', '', '',
             'Researcher', '', '', '', '', '')
        ]
        
        filepath = export_dois_with_contributors_to_csv(
            data, 'TIB/GFZ:Test', str(tmp_path)
        )
        
        assert 'TIB_GFZ_Test_contributors.csv' in filepath
    
    def test_export_creates_directory(self, tmp_path):
        """Test that export creates missing directories."""
        new_dir = tmp_path / 'subdir' / 'nested'
        data = [
            ('10.5880/GFZ.1', 'Test', 'Personal', '', '', '', '', '',
             'Researcher', '', '', '', '', '')
        ]
        
        filepath = export_dois_with_contributors_to_csv(
            data, 'TIB.GFZ', str(new_dir)
        )
        
        assert os.path.exists(filepath)
        assert 'subdir' in filepath and 'nested' in filepath


class TestParseContributorsCSV:
    """Tests for CSVParser.parse_contributors_update_csv method."""
    
    def create_csv(self, tmp_path, rows, headers=None):
        """Helper to create a test CSV file."""
        if headers is None:
            headers = [
                'DOI', 'Contributor Name', 'Name Type', 'Given Name', 'Family Name',
                'Name Identifier', 'Name Identifier Scheme', 'Scheme URI',
                'Contributor Types', 'Affiliation', 'Affiliation Identifier',
                'Email', 'Website', 'Position'
            ]
        
        filepath = tmp_path / 'test_contributors.csv'
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            for row in rows:
                writer.writerow(row)
        
        return str(filepath)
    
    def test_parse_single_contributor(self, tmp_path):
        """Test parsing a CSV with a single contributor."""
        rows = [
            [
                '10.5880/GFZ.1.1.2021.001',
                'Müller, Hans', 'Personal', 'Hans', 'Müller',
                '0000-0001-2345-6789', 'ORCID', 'https://orcid.org',
                'ContactPerson', 'GFZ Potsdam', 'https://ror.org/04z8jg394',
                'hans@gfz.de', 'https://gfz.de', 'Scientist'
            ]
        ]
        filepath = self.create_csv(tmp_path, rows)
        
        result, warnings = CSVParser.parse_contributors_update_csv(filepath)
        
        assert len(result) == 1
        assert '10.5880/GFZ.1.1.2021.001' in result
        
        contributor = result['10.5880/GFZ.1.1.2021.001'][0]
        assert contributor['name'] == 'Müller, Hans'
        assert contributor['nameType'] == 'Personal'
        assert contributor['givenName'] == 'Hans'
        assert contributor['familyName'] == 'Müller'
        assert contributor['contributorTypes'] == ['ContactPerson']
        assert contributor['email'] == 'hans@gfz.de'
    
    def test_parse_multiple_contributors_same_doi(self, tmp_path):
        """Test parsing multiple contributors for the same DOI."""
        rows = [
            [
                '10.5880/GFZ.1.1.2021.001',
                'First Contributor', 'Personal', 'First', 'Contributor',
                '', '', '', 'Researcher', '', '', '', '', ''
            ],
            [
                '10.5880/GFZ.1.1.2021.001',
                'Second Contributor', 'Personal', 'Second', 'Contributor',
                '', '', '', 'DataManager', '', '', '', '', ''
            ]
        ]
        filepath = self.create_csv(tmp_path, rows)
        
        result, warnings = CSVParser.parse_contributors_update_csv(filepath)
        
        assert len(result['10.5880/GFZ.1.1.2021.001']) == 2
        assert result['10.5880/GFZ.1.1.2021.001'][0]['name'] == 'First Contributor'
        assert result['10.5880/GFZ.1.1.2021.001'][1]['name'] == 'Second Contributor'
    
    def test_parse_multiple_contributor_types(self, tmp_path):
        """Test parsing comma-separated ContributorTypes."""
        rows = [
            [
                '10.5880/GFZ.1.1.2021.001',
                'Multi Role', 'Personal', 'Multi', 'Role',
                '', '', '',
                'ContactPerson, DataManager, Researcher',
                '', '', '', '', ''
            ]
        ]
        filepath = self.create_csv(tmp_path, rows)
        
        result, warnings = CSVParser.parse_contributors_update_csv(filepath)
        
        contributor = result['10.5880/GFZ.1.1.2021.001'][0]
        assert contributor['contributorTypes'] == ['ContactPerson', 'DataManager', 'Researcher']
    
    def test_parse_organizational_contributor(self, tmp_path):
        """Test parsing an organizational contributor."""
        rows = [
            [
                '10.5880/GFZ.1.1.2021.001',
                'GFZ Data Services', 'Organizational', '', '',
                'https://ror.org/04z8jg394', 'ROR', 'https://ror.org',
                'HostingInstitution', '', '', '', '', ''
            ]
        ]
        filepath = self.create_csv(tmp_path, rows)
        
        result, warnings = CSVParser.parse_contributors_update_csv(filepath)
        
        contributor = result['10.5880/GFZ.1.1.2021.001'][0]
        assert contributor['nameType'] == 'Organizational'
        assert contributor['givenName'] == ''
        assert contributor['familyName'] == ''
    
    def test_parse_missing_contributor_types_error(self, tmp_path):
        """Test that missing ContributorTypes raises an error."""
        rows = [
            [
                '10.5880/GFZ.1.1.2021.001',
                'Test', 'Personal', 'Test', 'Name',
                '', '', '',
                '',  # Missing ContributorTypes
                '', '', '', '', ''
            ]
        ]
        filepath = self.create_csv(tmp_path, rows)
        
        with pytest.raises(CSVParseError) as exc_info:
            CSVParser.parse_contributors_update_csv(filepath)
        
        assert "Contributor Types fehlt" in str(exc_info.value)
    
    def test_parse_missing_name_error(self, tmp_path):
        """Test that missing contributor name raises an error."""
        rows = [
            [
                '10.5880/GFZ.1.1.2021.001',
                '',  # Missing name
                'Personal', '', '',
                '', '', '', 'Researcher', '', '', '', '', ''
            ]
        ]
        filepath = self.create_csv(tmp_path, rows)
        
        with pytest.raises(CSVParseError) as exc_info:
            CSVParser.parse_contributors_update_csv(filepath)
        
        assert "Contributor Name fehlt" in str(exc_info.value)
    
    def test_parse_organizational_with_given_name_error(self, tmp_path):
        """Test that organizational contributor with given name raises error."""
        rows = [
            [
                '10.5880/GFZ.1.1.2021.001',
                'Some Org', 'Organizational', 'ShouldNotHave', '',
                '', '', '', 'HostingInstitution', '', '', '', '', ''
            ]
        ]
        filepath = self.create_csv(tmp_path, rows)
        
        with pytest.raises(CSVParseError) as exc_info:
            CSVParser.parse_contributors_update_csv(filepath)
        
        assert "darf keine Given Name" in str(exc_info.value)
    
    def test_parse_invalid_doi_format_error(self, tmp_path):
        """Test that invalid DOI format raises an error."""
        rows = [
            [
                'invalid-doi-format',  # Invalid DOI
                'Test', 'Personal', '', '',
                '', '', '', 'Researcher', '', '', '', '', ''
            ]
        ]
        filepath = self.create_csv(tmp_path, rows)
        
        with pytest.raises(CSVParseError) as exc_info:
            CSVParser.parse_contributors_update_csv(filepath)
        
        assert "Ungültiges DOI-Format" in str(exc_info.value)
    
    def test_parse_unknown_contributor_type_warning(self, tmp_path):
        """Test that unknown ContributorType generates warning."""
        rows = [
            [
                '10.5880/GFZ.1.1.2021.001',
                'Test', 'Personal', '', '',
                '', '', '',
                'UnknownType',  # Invalid type
                '', '', '', '', ''
            ]
        ]
        filepath = self.create_csv(tmp_path, rows)
        
        result, warnings = CSVParser.parse_contributors_update_csv(filepath)
        
        assert len(warnings) == 1
        assert 'UnknownType' in warnings[0]
    
    def test_parse_contactinfo_without_contactperson_warning(self, tmp_path):
        """Test warning when ContactInfo provided but not ContactPerson."""
        rows = [
            [
                '10.5880/GFZ.1.1.2021.001',
                'Test', 'Personal', '', '',
                '', '', '',
                'Researcher',  # Not ContactPerson
                '', '',
                'test@example.com', '', ''  # But has email
            ]
        ]
        filepath = self.create_csv(tmp_path, rows)
        
        result, warnings = CSVParser.parse_contributors_update_csv(filepath)
        
        assert any('ContactPerson' in w for w in warnings)
    
    def test_parse_invalid_email_warning(self, tmp_path):
        """Test warning for invalid email format."""
        rows = [
            [
                '10.5880/GFZ.1.1.2021.001',
                'Test', 'Personal', '', '',
                '', '', '',
                'ContactPerson', '', '',
                'not-an-email',  # Invalid email
                '', ''
            ]
        ]
        filepath = self.create_csv(tmp_path, rows)
        
        result, warnings = CSVParser.parse_contributors_update_csv(filepath)
        
        assert any('Email-Format' in w for w in warnings)
    
    def test_parse_invalid_website_warning(self, tmp_path):
        """Test warning for invalid website URL."""
        rows = [
            [
                '10.5880/GFZ.1.1.2021.001',
                'Test', 'Personal', '', '',
                '', '', '',
                'ContactPerson', '', '',
                '',
                'not-a-url',  # Invalid URL
                ''
            ]
        ]
        filepath = self.create_csv(tmp_path, rows)
        
        result, warnings = CSVParser.parse_contributors_update_csv(filepath)
        
        assert any('Website-URL' in w for w in warnings)
    
    def test_parse_missing_headers_error(self, tmp_path):
        """Test that missing headers raises an error."""
        # Create CSV with wrong headers
        filepath = tmp_path / 'test.csv'
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['DOI', 'Name', 'Type'])  # Wrong headers
            writer.writerow(['10.5880/GFZ.1', 'Test', 'Personal'])
        
        with pytest.raises(CSVParseError) as exc_info:
            CSVParser.parse_contributors_update_csv(str(filepath))
        
        assert "fehlen folgende Header" in str(exc_info.value)
    
    def test_parse_file_not_found(self, tmp_path):
        """Test that missing file raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError):
            CSVParser.parse_contributors_update_csv(str(tmp_path / 'nonexistent.csv'))
    
    def test_parse_empty_file_error(self, tmp_path):
        """Test that empty file raises error."""
        filepath = tmp_path / 'empty.csv'
        filepath.touch()
        
        with pytest.raises(CSVParseError) as exc_info:
            CSVParser.parse_contributors_update_csv(str(filepath))
        
        assert "keine Header-Zeile" in str(exc_info.value)
    
    def test_parse_preserves_doi_order(self, tmp_path):
        """Test that DOI order is preserved."""
        rows = [
            ['10.5880/GFZ.3', 'Third', 'Personal', '', '', '', '', '',
             'Researcher', '', '', '', '', ''],
            ['10.5880/GFZ.1', 'First', 'Personal', '', '', '', '', '',
             'Researcher', '', '', '', '', ''],
            ['10.5880/GFZ.2', 'Second', 'Personal', '', '', '', '', '',
             'Researcher', '', '', '', '', ''],
        ]
        filepath = self.create_csv(tmp_path, rows)
        
        result, _ = CSVParser.parse_contributors_update_csv(filepath)
        
        dois = list(result.keys())
        assert dois == ['10.5880/GFZ.3', '10.5880/GFZ.1', '10.5880/GFZ.2']
    
    def test_parse_default_name_type(self, tmp_path):
        """Test that empty Name Type defaults to Personal."""
        rows = [
            [
                '10.5880/GFZ.1.1.2021.001',
                'Test Name', '',  # Empty Name Type
                'Test', 'Name',
                '', '', '', 'Researcher', '', '', '', '', ''
            ]
        ]
        filepath = self.create_csv(tmp_path, rows)
        
        result, _ = CSVParser.parse_contributors_update_csv(filepath)
        
        contributor = result['10.5880/GFZ.1.1.2021.001'][0]
        assert contributor['nameType'] == 'Personal'
    
    def test_parse_orcid_validation_warning(self, tmp_path):
        """Test warning for invalid ORCID format."""
        rows = [
            [
                '10.5880/GFZ.1.1.2021.001',
                'Test', 'Personal', '', '',
                'invalid-orcid', 'ORCID', 'https://orcid.org',
                'Researcher', '', '', '', '', ''
            ]
        ]
        filepath = self.create_csv(tmp_path, rows)
        
        result, warnings = CSVParser.parse_contributors_update_csv(filepath)
        
        assert any('ORCID-Format' in w for w in warnings)


class TestEmailValidation:
    """Tests for the _validate_email_format helper method."""
    
    def test_valid_email(self):
        """Test valid email formats."""
        assert CSVParser._validate_email_format('test@example.com') is True
        assert CSVParser._validate_email_format('user.name@domain.org') is True
        assert CSVParser._validate_email_format('user+tag@sub.domain.co.uk') is True
    
    def test_invalid_email(self):
        """Test invalid email formats."""
        assert CSVParser._validate_email_format('') is False
        assert CSVParser._validate_email_format('not-an-email') is False
        assert CSVParser._validate_email_format('@domain.com') is False
        assert CSVParser._validate_email_format('user@') is False
        assert CSVParser._validate_email_format('user@domain') is False


class TestValidContributorTypes:
    """Tests for VALID_CONTRIBUTOR_TYPES constant."""
    
    def test_all_datacite_types_included(self):
        """Test that all DataCite ContributorTypes are included."""
        expected = [
            'ContactPerson', 'DataCollector', 'DataCurator', 'DataManager',
            'Distributor', 'Editor', 'HostingInstitution', 'Producer',
            'ProjectLeader', 'ProjectManager', 'ProjectMember',
            'RegistrationAgency', 'RegistrationAuthority', 'RelatedPerson',
            'Researcher', 'ResearchGroup', 'RightsHolder', 'Sponsor',
            'Supervisor', 'WorkPackageLeader', 'Other'
        ]
        for ct in expected:
            assert ct in CSVParser.VALID_CONTRIBUTOR_TYPES
    
    def test_gfz_internal_type_included(self):
        """Test that GFZ-internal pointOfContact is included."""
        assert 'pointOfContact' in CSVParser.VALID_CONTRIBUTOR_TYPES
