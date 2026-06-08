"""Tests for ContributorsUpdateWorker."""

import csv
import pytest
from unittest.mock import Mock, patch

from PySide6.QtCore import QSettings

from src.api.datacite_client import NetworkError
from src.db.sumariopmd_client import DatabaseError
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


class TestContributorsUpdateWorkerRun:
    """Tests for ContributorsUpdateWorker.run with mocked external systems."""

    def _new_worker(self, sample_csv, dry_run_only=True, credentials_are_new=False):
        return ContributorsUpdateWorker(
            username='TIB.GFZ',
            password='secret',
            csv_path=sample_csv,
            use_test_api=True,
            dry_run_only=dry_run_only,
            credentials_are_new=credentials_are_new
        )

    def _metadata_with_contributors(self, contributors):
        return {
            'data': {
                'attributes': {
                    'contributors': contributors
                }
            }
        }

    def test_run_dry_run_success_with_changes(self, sample_csv):
        """Dry run validates contributors and stops before updates."""
        worker = self._new_worker(sample_csv, dry_run_only=True)

        mock_client = Mock()
        mock_client.get_doi_metadata.return_value = self._metadata_with_contributors([])
        mock_client.validate_contributors_match.return_value = (True, "OK")

        dry_run_results = []
        finished_results = []
        worker.dry_run_complete.connect(lambda v, i, r: dry_run_results.append((v, i, r)))
        worker.finished.connect(lambda s, e, sk, el, sd: finished_results.append((s, e, sk, el, sd)))

        with patch('src.workers.contributors_update_worker.QSettings') as mock_qsettings, \
             patch('src.workers.contributors_update_worker.DataCiteClient', return_value=mock_client):
            mock_qsettings.return_value.value.return_value = False
            worker.run()

        assert dry_run_results[0][0:2] == (1, 0)
        assert dry_run_results[0][2][0]['changed'] is True
        assert finished_results == [(1, 0, 0, [], [])]
        mock_client.update_doi_contributors.assert_not_called()

    def test_run_handles_csv_parse_error(self, tmp_path):
        """Invalid contributor CSV emits an error and a zero-result finish."""
        csv_path = tmp_path / "invalid_contributors.csv"
        csv_path.write_text("Wrong,Header\n10.5880/GFZ.1,Name\n", encoding="utf-8")

        worker = ContributorsUpdateWorker(
            username='TIB.GFZ',
            password='secret',
            csv_path=str(csv_path),
            use_test_api=True,
            dry_run_only=True
        )

        errors = []
        finished_results = []
        worker.error_occurred.connect(errors.append)
        worker.finished.connect(lambda s, e, sk, el, sd: finished_results.append((s, e, sk, el, sd)))

        with patch('src.workers.contributors_update_worker.DataCiteClient') as mock_client_class:
            worker.run()

        assert len(errors) == 1
        assert "CSV" in errors[0]
        assert finished_results == [(0, 0, 0, [], [])]
        mock_client_class.assert_not_called()

    def test_run_handles_datacite_client_initialization_error(self, sample_csv):
        """A DataCite client initialization failure aborts cleanly."""
        worker = self._new_worker(sample_csv, dry_run_only=True)

        errors = []
        finished_results = []
        worker.error_occurred.connect(errors.append)
        worker.finished.connect(lambda s, e, sk, el, sd: finished_results.append((s, e, sk, el, sd)))

        with patch('src.workers.contributors_update_worker.QSettings') as mock_qsettings, \
             patch('src.workers.contributors_update_worker.DataCiteClient', side_effect=RuntimeError("boom")):
            mock_qsettings.return_value.value.return_value = False
            worker.run()

        assert len(errors) == 1
        assert "DataCite" in errors[0]
        assert finished_results == [(0, 0, 0, [], [])]

    def test_run_marks_missing_metadata_invalid(self, sample_csv):
        """A DOI without metadata is reported as invalid during dry run."""
        worker = self._new_worker(sample_csv, dry_run_only=True)

        mock_client = Mock()
        mock_client.get_doi_metadata.return_value = None

        dry_run_results = []
        finished_results = []
        worker.dry_run_complete.connect(lambda v, i, r: dry_run_results.append((v, i, r)))
        worker.finished.connect(lambda s, e, sk, el, sd: finished_results.append((s, e, sk, el, sd)))

        with patch('src.workers.contributors_update_worker.QSettings') as mock_qsettings, \
             patch('src.workers.contributors_update_worker.DataCiteClient', return_value=mock_client):
            mock_qsettings.return_value.value.return_value = False
            worker.run()

        assert dry_run_results[0][0:2] == (0, 1)
        assert dry_run_results[0][2][0]['valid'] is False
        assert finished_results == [(0, 1, 0, [], [])]
        mock_client.validate_contributors_match.assert_not_called()

    def test_run_actual_update_datacite_only_requests_credential_save(self, sample_csv):
        """Successful DataCite-only update offers to save new credentials once."""
        worker = self._new_worker(sample_csv, dry_run_only=False, credentials_are_new=True)

        mock_client = Mock()
        mock_client.get_doi_metadata.return_value = self._metadata_with_contributors([])
        mock_client.validate_contributors_match.return_value = (True, "OK")
        mock_client.update_doi_contributors.return_value = (True, "updated")

        save_requests = []
        finished_results = []
        worker.request_save_credentials.connect(lambda u, p, a: save_requests.append((u, p, a)))
        worker.finished.connect(lambda s, e, sk, el, sd: finished_results.append((s, e, sk, el, sd)))

        with patch('src.workers.contributors_update_worker.QSettings') as mock_qsettings, \
             patch('src.workers.contributors_update_worker.DataCiteClient', return_value=mock_client):
            mock_qsettings.return_value.value.return_value = False
            worker.run()

        assert finished_results == [(1, 0, 0, [], [])]
        assert save_requests == [('TIB.GFZ', 'secret', 'test')]
        mock_client.update_doi_contributors.assert_called_once()

    def test_run_actual_update_aborts_on_network_error(self, sample_csv):
        """A network error during update emits an error and final partial counts."""
        worker = self._new_worker(sample_csv, dry_run_only=False)

        mock_client = Mock()
        mock_client.get_doi_metadata.return_value = self._metadata_with_contributors([])
        mock_client.validate_contributors_match.return_value = (True, "OK")
        mock_client.update_doi_contributors.side_effect = NetworkError("offline")

        errors = []
        finished_results = []
        worker.error_occurred.connect(errors.append)
        worker.finished.connect(lambda s, e, sk, el, sd: finished_results.append((s, e, sk, el, sd)))

        with patch('src.workers.contributors_update_worker.QSettings') as mock_qsettings, \
             patch('src.workers.contributors_update_worker.DataCiteClient', return_value=mock_client):
            mock_qsettings.return_value.value.return_value = False
            worker.run()

        assert len(errors) == 1
        assert "Netzwerkfehler" in errors[0]
        assert finished_results == [(0, 0, 0, [], [])]

    def test_run_with_database_updates_success(self, sample_csv):
        """Database-enabled updates use the mocked DB before DataCite."""
        worker = self._new_worker(sample_csv, dry_run_only=False)

        mock_client = Mock()
        mock_client.get_doi_metadata.return_value = self._metadata_with_contributors([])
        mock_client.validate_contributors_match.return_value = (True, "OK")
        mock_client.update_doi_contributors.return_value = (True, "updated")

        mock_db_client = Mock()
        mock_db_client.test_connection.return_value = (True, "db ok")
        mock_db_client.get_resource_id_for_doi.return_value = 42
        mock_db_client.update_contributors_transactional.return_value = (True, "db updated", [])

        finished_results = []
        db_messages = []
        worker.finished.connect(lambda s, e, sk, el, sd: finished_results.append((s, e, sk, el, sd)))
        worker.database_update.connect(db_messages.append)

        with patch('src.workers.contributors_update_worker.QSettings') as mock_qsettings, \
             patch('src.workers.contributors_update_worker.load_db_credentials') as mock_load_db_credentials, \
             patch('src.workers.contributors_update_worker.SumarioPMDClient', return_value=mock_db_client), \
             patch('src.workers.contributors_update_worker.DataCiteClient', return_value=mock_client):
            mock_qsettings.return_value.value.return_value = True
            mock_load_db_credentials.return_value = {
                'host': 'sumario',
                'database': 'sumario-pmd',
                'username': 'db-user',
                'password': 'db-pass'
            }
            worker.run()

        assert finished_results == [(1, 0, 0, [], [])]
        assert any("Datenbank" in message for message in db_messages)
        mock_db_client.update_contributors_transactional.assert_called_once()
        mock_client.update_doi_contributors.assert_called_once()

    def test_run_aborts_when_enabled_database_unavailable(self, sample_csv):
        """Enabled but unavailable DB aborts before DOI validation."""
        worker = self._new_worker(sample_csv, dry_run_only=False)

        mock_client = Mock()
        mock_db_client = Mock()
        mock_db_client.test_connection.return_value = (False, "no vpn")

        errors = []
        finished_results = []
        worker.error_occurred.connect(errors.append)
        worker.finished.connect(lambda s, e, sk, el, sd: finished_results.append((s, e, sk, el, sd)))

        with patch('src.workers.contributors_update_worker.QSettings') as mock_qsettings, \
             patch('src.workers.contributors_update_worker.load_db_credentials') as mock_load_db_credentials, \
             patch('src.workers.contributors_update_worker.SumarioPMDClient', return_value=mock_db_client), \
             patch('src.workers.contributors_update_worker.DataCiteClient', return_value=mock_client):
            mock_qsettings.return_value.value.return_value = True
            mock_load_db_credentials.return_value = {
                'host': 'sumario',
                'database': 'sumario-pmd',
                'username': 'db-user',
                'password': 'db-pass'
            }
            worker.run()

        assert len(errors) == 1
        assert "Datenbank-Updates sind aktiviert" in errors[0]
        assert finished_results == [(0, 0, 0, [], [])]
        mock_client.get_doi_metadata.assert_not_called()

    def test_run_skips_datacite_when_database_update_fails(self, sample_csv):
        """A DB transaction failure prevents a DataCite update."""
        worker = self._new_worker(sample_csv, dry_run_only=False)

        mock_client = Mock()
        mock_client.get_doi_metadata.return_value = self._metadata_with_contributors([])
        mock_client.validate_contributors_match.return_value = (True, "OK")

        mock_db_client = Mock()
        mock_db_client.test_connection.return_value = (True, "db ok")
        mock_db_client.get_resource_id_for_doi.return_value = 42
        mock_db_client.update_contributors_transactional.return_value = (False, "rollback", ["rollback"])

        finished_results = []
        worker.finished.connect(lambda s, e, sk, el, sd: finished_results.append((s, e, sk, el, sd)))

        with patch('src.workers.contributors_update_worker.QSettings') as mock_qsettings, \
             patch('src.workers.contributors_update_worker.load_db_credentials') as mock_load_db_credentials, \
             patch('src.workers.contributors_update_worker.SumarioPMDClient', return_value=mock_db_client), \
             patch('src.workers.contributors_update_worker.DataCiteClient', return_value=mock_client):
            mock_qsettings.return_value.value.return_value = True
            mock_load_db_credentials.return_value = {
                'host': 'sumario',
                'database': 'sumario-pmd',
                'username': 'db-user',
                'password': 'db-pass'
            }
            worker.run()

        success_count, error_count, skipped_count, error_list, skipped_details = finished_results[0]
        assert (success_count, error_count, skipped_count) == (0, 1, 0)
        assert "Datenbank-Update fehlgeschlagen" in error_list[0]
        assert skipped_details == []
        mock_client.update_doi_contributors.assert_not_called()

    def test_run_continues_datacite_when_doi_missing_in_database(self, sample_csv):
        """A DOI missing in SUMARIOPMD still gets a DataCite update."""
        worker = self._new_worker(sample_csv, dry_run_only=False)

        mock_client = Mock()
        mock_client.get_doi_metadata.return_value = self._metadata_with_contributors([])
        mock_client.validate_contributors_match.return_value = (True, "OK")
        mock_client.update_doi_contributors.return_value = (True, "updated")

        mock_db_client = Mock()
        mock_db_client.test_connection.return_value = (True, "db ok")
        mock_db_client.get_resource_id_for_doi.return_value = None

        finished_results = []
        worker.finished.connect(lambda s, e, sk, el, sd: finished_results.append((s, e, sk, el, sd)))

        with patch('src.workers.contributors_update_worker.QSettings') as mock_qsettings, \
             patch('src.workers.contributors_update_worker.load_db_credentials') as mock_load_db_credentials, \
             patch('src.workers.contributors_update_worker.SumarioPMDClient', return_value=mock_db_client), \
             patch('src.workers.contributors_update_worker.DataCiteClient', return_value=mock_client):
            mock_qsettings.return_value.value.return_value = True
            mock_load_db_credentials.return_value = {
                'host': 'sumario',
                'database': 'sumario-pmd',
                'username': 'db-user',
                'password': 'db-pass'
            }
            worker.run()

        assert finished_results == [(1, 0, 0, [], [])]
        mock_db_client.update_contributors_transactional.assert_not_called()
        mock_client.update_doi_contributors.assert_called_once()

    def test_run_handles_database_exception_during_update(self, sample_csv):
        """DatabaseError during DB update is reported without calling DataCite."""
        worker = self._new_worker(sample_csv, dry_run_only=False)

        mock_client = Mock()
        mock_client.get_doi_metadata.return_value = self._metadata_with_contributors([])
        mock_client.validate_contributors_match.return_value = (True, "OK")

        mock_db_client = Mock()
        mock_db_client.test_connection.return_value = (True, "db ok")
        mock_db_client.get_resource_id_for_doi.side_effect = DatabaseError("query failed")

        finished_results = []
        worker.finished.connect(lambda s, e, sk, el, sd: finished_results.append((s, e, sk, el, sd)))

        with patch('src.workers.contributors_update_worker.QSettings') as mock_qsettings, \
             patch('src.workers.contributors_update_worker.load_db_credentials') as mock_load_db_credentials, \
             patch('src.workers.contributors_update_worker.SumarioPMDClient', return_value=mock_db_client), \
             patch('src.workers.contributors_update_worker.DataCiteClient', return_value=mock_client):
            mock_qsettings.return_value.value.return_value = True
            mock_load_db_credentials.return_value = {
                'host': 'sumario',
                'database': 'sumario-pmd',
                'username': 'db-user',
                'password': 'db-pass'
            }
            worker.run()

        success_count, error_count, skipped_count, error_list, _ = finished_results[0]
        assert (success_count, error_count, skipped_count) == (0, 1, 0)
        assert "Datenbank-Fehler" in error_list[0]
        mock_client.update_doi_contributors.assert_not_called()


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
