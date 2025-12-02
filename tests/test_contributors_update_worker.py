"""Tests for ContributorsUpdateWorker."""

import csv
import pytest
from unittest.mock import MagicMock, patch, PropertyMock

from PySide6.QtCore import QSettings

from src.workers.contributors_update_worker import ContributorsUpdateWorker


@pytest.fixture
def mock_settings():
    """Mock QSettings to control database enabled state."""
    with patch.object(QSettings, 'value') as mock_value:
        mock_value.return_value = False  # DB disabled by default
        yield mock_value


@pytest.fixture
def sample_csv(tmp_path):
    """Create a sample contributors CSV file."""
    csv_path = tmp_path / "contributors.csv"
    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            'DOI', 'Contributor Name', 'Name Type', 'Given Name', 'Family Name',
            'Name Identifier', 'Name Identifier Scheme', 'Scheme URI',
            'Contributor Types', 'Affiliation', 'Affiliation Identifier',
            'Email', 'Website', 'Position'
        ])
        writer.writerow([
            '10.5880/GFZ.1.1.2021.001',
            'Müller, Hans', 'Personal', 'Hans', 'Müller',
            '0000-0001-2345-6789', 'ORCID', 'https://orcid.org',
            'ContactPerson, DataManager', 'GFZ Potsdam', '',
            'hans@gfz.de', 'https://gfz.de', 'Scientist'
        ])
        writer.writerow([
            '10.5880/GFZ.1.1.2021.001',
            'GFZ Data Services', 'Organizational', '', '',
            'https://ror.org/04z8jg394', 'ROR', 'https://ror.org',
            'HostingInstitution', '', '',
            '', '', ''
        ])
    return str(csv_path)


class TestContributorsUpdateWorkerInit:
    """Tests for ContributorsUpdateWorker initialization."""
    
    def test_init_basic(self, sample_csv):
        """Test basic initialization."""
        worker = ContributorsUpdateWorker(
            username='TIB.GFZ',
            password='secret',
            csv_path=sample_csv,
            use_test_api=True,
            dry_run_only=True
        )
        
        assert worker.username == 'TIB.GFZ'
        assert worker.password == 'secret'
        assert worker.csv_path == sample_csv
        assert worker.use_test_api is True
        assert worker.dry_run_only is True
        assert worker._is_running is False
    
    def test_init_credentials_are_new(self, sample_csv):
        """Test initialization with new credentials flag."""
        worker = ContributorsUpdateWorker(
            username='TIB.GFZ',
            password='secret',
            csv_path=sample_csv,
            credentials_are_new=True
        )
        
        assert worker.credentials_are_new is True
        assert worker._first_success is False


class TestContributorChangeDetection:
    """Tests for contributor change detection methods."""
    
    def test_detect_changes_partial_update(self, sample_csv):
        """Test change detection with partial update (fewer CSV contributors)."""
        worker = ContributorsUpdateWorker(
            username='TIB.GFZ',
            password='secret',
            csv_path=sample_csv
        )
        
        current_metadata = {
            'data': {
                'attributes': {
                    'contributors': [
                        {'name': 'First', 'contributorType': 'Researcher'},
                        {'name': 'Second', 'contributorType': 'DataManager'}
                    ]
                }
            }
        }
        
        # Only update First (partial update)
        csv_contributors = [
            {'name': 'First', 'contributorTypes': ['ContactPerson'], 'email': 'test@example.com'}
        ]
        
        has_changes, description = worker._detect_contributor_changes(
            current_metadata, csv_contributors
        )
        
        # Should detect ContributorType change and email DB field
        assert has_changes is True
        assert 'ContributorType' in description or 'E-Mail' in description
    
    def test_detect_changes_no_contributors(self, sample_csv):
        """Test when no contributors exist in both."""
        worker = ContributorsUpdateWorker(
            username='TIB.GFZ',
            password='secret',
            csv_path=sample_csv
        )
        
        current_metadata = {
            'data': {'attributes': {'contributors': []}}
        }
        
        has_changes, description = worker._detect_contributor_changes(
            current_metadata, []
        )
        
        assert has_changes is False
        assert 'Keine Contributors' in description
    
    def test_detect_changes_unmatched_contributor(self, sample_csv):
        """Test detection when CSV contributor not found in DataCite."""
        worker = ContributorsUpdateWorker(
            username='TIB.GFZ',
            password='secret',
            csv_path=sample_csv
        )
        
        current_metadata = {
            'data': {
                'attributes': {
                    'contributors': [
                        {'name': 'Existing Person', 'contributorType': 'Researcher', 'nameType': 'Personal'}
                    ]
                }
            }
        }
        
        csv_contributors = [
            {'name': 'Unknown Person', 'nameType': 'Personal', 'contributorTypes': ['Researcher']}
        ]
        
        has_changes, description = worker._detect_contributor_changes(
            current_metadata, csv_contributors
        )
        
        assert has_changes is True
        assert 'nicht in DataCite gefunden' in description
    
    def test_detect_changes_contributor_type_changed(self, sample_csv):
        """Test detection of ContributorType change."""
        worker = ContributorsUpdateWorker(
            username='TIB.GFZ',
            password='secret',
            csv_path=sample_csv
        )
        
        current_metadata = {
            'data': {
                'attributes': {
                    'contributors': [
                        {'name': 'Test', 'contributorType': 'Researcher', 'nameType': 'Personal'}
                    ]
                }
            }
        }
        
        csv_contributors = [
            {'name': 'Test', 'nameType': 'Personal', 'contributorTypes': ['DataManager']}
        ]
        
        has_changes, description = worker._detect_contributor_changes(
            current_metadata, csv_contributors
        )
        
        assert has_changes is True
        assert 'ContributorType geändert' in description
    
    def test_detect_changes_no_changes(self, sample_csv):
        """Test when contributors are identical."""
        worker = ContributorsUpdateWorker(
            username='TIB.GFZ',
            password='secret',
            csv_path=sample_csv
        )
        
        current_metadata = {
            'data': {
                'attributes': {
                    'contributors': [
                        {
                            'name': 'Test Person',
                            'nameType': 'Personal',
                            'givenName': 'Test',
                            'familyName': 'Person',
                            'contributorType': 'Researcher',
                            'nameIdentifiers': []
                        }
                    ]
                }
            }
        }
        
        csv_contributors = [
            {
                'name': 'Test Person',
                'nameType': 'Personal',
                'givenName': 'Test',
                'familyName': 'Person',
                'contributorTypes': ['Researcher'],
                'nameIdentifier': ''
            }
        ]
        
        has_changes, description = worker._detect_contributor_changes(
            current_metadata, csv_contributors
        )
        
        assert has_changes is False
        assert 'Keine Änderungen' in description
    
    def test_detect_changes_orcid_changed(self, sample_csv):
        """Test detection of ORCID change."""
        worker = ContributorsUpdateWorker(
            username='TIB.GFZ',
            password='secret',
            csv_path=sample_csv
        )
        
        current_metadata = {
            'data': {
                'attributes': {
                    'contributors': [
                        {
                            'name': 'Test',
                            'nameType': 'Personal',
                            'contributorType': 'Researcher',
                            'nameIdentifiers': [
                                {'nameIdentifier': '0000-0001-0000-0001', 'nameIdentifierScheme': 'ORCID'}
                            ]
                        }
                    ]
                }
            }
        }
        
        csv_contributors = [
            {
                'name': 'Test',
                'nameType': 'Personal',
                'contributorTypes': ['Researcher'],
                'nameIdentifier': '0000-0001-0000-0002'  # Different ORCID
            }
        ]
        
        has_changes, description = worker._detect_contributor_changes(
            current_metadata, csv_contributors
        )
        
        assert has_changes is True
        assert 'ORCID geändert' in description


class TestOrcidNormalization:
    """Tests for ORCID normalization."""
    
    def test_normalize_orcid_full_url(self, sample_csv):
        """Test normalizing full ORCID URL."""
        worker = ContributorsUpdateWorker(
            username='TIB.GFZ',
            password='secret',
            csv_path=sample_csv
        )
        
        result = worker._normalize_orcid('https://orcid.org/0000-0001-2345-6789')
        assert result == '0000-0001-2345-6789'
    
    def test_normalize_orcid_http_url(self, sample_csv):
        """Test normalizing HTTP ORCID URL."""
        worker = ContributorsUpdateWorker(
            username='TIB.GFZ',
            password='secret',
            csv_path=sample_csv
        )
        
        result = worker._normalize_orcid('http://orcid.org/0000-0001-2345-6789')
        assert result == '0000-0001-2345-6789'
    
    def test_normalize_orcid_already_normalized(self, sample_csv):
        """Test that already normalized ORCID is unchanged."""
        worker = ContributorsUpdateWorker(
            username='TIB.GFZ',
            password='secret',
            csv_path=sample_csv
        )
        
        result = worker._normalize_orcid('0000-0001-2345-6789')
        assert result == '0000-0001-2345-6789'
    
    def test_normalize_orcid_empty(self, sample_csv):
        """Test normalizing empty ORCID."""
        worker = ContributorsUpdateWorker(
            username='TIB.GFZ',
            password='secret',
            csv_path=sample_csv
        )
        
        result = worker._normalize_orcid('')
        assert result == ''


class TestExtractOrcid:
    """Tests for extracting ORCID from contributor."""
    
    def test_extract_orcid_found(self, sample_csv):
        """Test extracting existing ORCID."""
        worker = ContributorsUpdateWorker(
            username='TIB.GFZ',
            password='secret',
            csv_path=sample_csv
        )
        
        contributor = {
            'nameIdentifiers': [
                {
                    'nameIdentifier': 'https://orcid.org/0000-0001-2345-6789',
                    'nameIdentifierScheme': 'ORCID'
                }
            ]
        }
        
        result = worker._extract_orcid(contributor)
        assert result == '0000-0001-2345-6789'
    
    def test_extract_orcid_not_found(self, sample_csv):
        """Test extracting ORCID when none exists."""
        worker = ContributorsUpdateWorker(
            username='TIB.GFZ',
            password='secret',
            csv_path=sample_csv
        )
        
        contributor = {
            'nameIdentifiers': [
                {'nameIdentifier': 'https://ror.org/04z8jg394', 'nameIdentifierScheme': 'ROR'}
            ]
        }
        
        result = worker._extract_orcid(contributor)
        assert result == ''
    
    def test_extract_orcid_empty_identifiers(self, sample_csv):
        """Test extracting ORCID with empty identifiers."""
        worker = ContributorsUpdateWorker(
            username='TIB.GFZ',
            password='secret',
            csv_path=sample_csv
        )
        
        contributor = {'nameIdentifiers': []}
        
        result = worker._extract_orcid(contributor)
        assert result == ''


class TestPrepareContributorsForDB:
    """Tests for preparing contributor data for database."""
    
    def test_prepare_basic_contributor(self, sample_csv):
        """Test preparing a basic contributor."""
        worker = ContributorsUpdateWorker(
            username='TIB.GFZ',
            password='secret',
            csv_path=sample_csv
        )
        
        csv_contributors = [
            {
                'name': 'Müller, Hans',
                'givenName': 'Hans',
                'familyName': 'Müller',
                'nameType': 'Personal',
                'nameIdentifier': '0000-0001-2345-6789',
                'contributorTypes': ['Researcher']
            }
        ]
        
        result = worker._prepare_contributors_for_db(csv_contributors)
        
        assert len(result) == 1
        assert result[0]['firstname'] == 'Hans'
        assert result[0]['lastname'] == 'Müller'
        assert result[0]['orcid'] == '0000-0001-2345-6789'
        assert result[0]['contributorTypes'] == 'Researcher'
    
    def test_prepare_multiple_contributor_types(self, sample_csv):
        """Test preparing contributor with multiple types."""
        worker = ContributorsUpdateWorker(
            username='TIB.GFZ',
            password='secret',
            csv_path=sample_csv
        )
        
        csv_contributors = [
            {
                'name': 'Test',
                'familyName': 'Test',
                'nameType': 'Personal',
                'contributorTypes': ['ContactPerson', 'DataManager', 'Researcher']
            }
        ]
        
        result = worker._prepare_contributors_for_db(csv_contributors)
        
        assert result[0]['contributorTypes'] == 'ContactPerson, DataManager, Researcher'
    
    def test_prepare_contact_person_with_info(self, sample_csv):
        """Test preparing ContactPerson with email/website/position."""
        worker = ContributorsUpdateWorker(
            username='TIB.GFZ',
            password='secret',
            csv_path=sample_csv
        )
        
        csv_contributors = [
            {
                'name': 'Contact Person',
                'familyName': 'Person',
                'givenName': 'Contact',
                'nameType': 'Personal',
                'contributorTypes': ['ContactPerson'],
                'email': 'contact@example.com',
                'website': 'https://example.com',
                'position': 'Manager'
            }
        ]
        
        result = worker._prepare_contributors_for_db(csv_contributors)
        
        assert result[0]['email'] == 'contact@example.com'
        assert result[0]['website'] == 'https://example.com'
        assert result[0]['position'] == 'Manager'
    
    def test_prepare_non_contact_person_no_contactinfo(self, sample_csv):
        """Test that non-ContactPerson doesn't get ContactInfo."""
        worker = ContributorsUpdateWorker(
            username='TIB.GFZ',
            password='secret',
            csv_path=sample_csv
        )
        
        csv_contributors = [
            {
                'name': 'Researcher',
                'familyName': 'Researcher',
                'nameType': 'Personal',
                'contributorTypes': ['Researcher'],  # Not ContactPerson
                'email': 'ignored@example.com'  # Should not be included
            }
        ]
        
        result = worker._prepare_contributors_for_db(csv_contributors)
        
        assert 'email' not in result[0]
        assert 'website' not in result[0]
        assert 'position' not in result[0]


class TestWorkerSignals:
    """Tests for worker signal definitions."""
    
    def test_has_required_signals(self, sample_csv):
        """Test that worker has all required signals."""
        worker = ContributorsUpdateWorker(
            username='TIB.GFZ',
            password='secret',
            csv_path=sample_csv
        )
        
        # Check main signals
        assert hasattr(worker, 'progress_update')
        assert hasattr(worker, 'dry_run_complete')
        assert hasattr(worker, 'doi_updated')
        assert hasattr(worker, 'finished')
        assert hasattr(worker, 'error_occurred')
        assert hasattr(worker, 'request_save_credentials')
        
        # Check Database-First Pattern signals
        assert hasattr(worker, 'validation_update')
        assert hasattr(worker, 'datacite_update')
        assert hasattr(worker, 'database_update')


class TestWorkerStop:
    """Tests for worker stop functionality."""
    
    def test_stop_sets_flag(self, sample_csv):
        """Test that stop() sets _is_running to False."""
        worker = ContributorsUpdateWorker(
            username='TIB.GFZ',
            password='secret',
            csv_path=sample_csv
        )
        
        worker._is_running = True
        worker.stop()
        
        assert worker._is_running is False
