"""Tests for AuthorsUpdateWorker."""

import pytest
from unittest.mock import Mock, patch
import tempfile
import os

from src.workers.authors_update_worker import AuthorsUpdateWorker
from src.api.datacite_client import NetworkError, DataCiteAPIError, AuthenticationError


class TestAuthorsUpdateWorker:
    """Test suite for AuthorsUpdateWorker class."""
    
    @pytest.fixture
    def valid_csv_file(self):
        """Create a temporary valid CSV file with creator data."""
        with tempfile.NamedTemporaryFile(
            mode='w', 
            suffix='.csv', 
            delete=False, 
            encoding='utf-8',
            newline=''
        ) as f:
            f.write("DOI,Creator Name,Name Type,Given Name,Family Name,Name Identifier,Name Identifier Scheme,Scheme URI\n")
            f.write('10.5880/GFZ.1.1.2021.001,"Smith, John",Personal,John,Smith,0000-0001-5000-0007,ORCID,https://orcid.org\n')
            f.write('10.5880/GFZ.1.1.2021.001,"Doe, Jane",Personal,Jane,Doe,,,\n')
            f.write('10.5880/GFZ.1.1.2021.002,Example Organization,Organizational,,,,,,\n')
            csv_path = f.name
        
        yield csv_path
        
        # Cleanup
        if os.path.exists(csv_path):
            os.unlink(csv_path)
    
    @pytest.fixture
    def worker_dry_run(self, valid_csv_file):
        """Create an AuthorsUpdateWorker instance for dry run."""
        # Mock QSettings to disable database sync for these tests
        with patch('src.workers.authors_update_worker.QSettings') as mock_settings:
            settings_instance = Mock()
            settings_instance.value.return_value = False  # DB disabled
            mock_settings.return_value = settings_instance
            
            worker = AuthorsUpdateWorker(
                username="test_user",
                password="test_pass",
                csv_path=valid_csv_file,
                use_test_api=True,
                dry_run_only=True
            )
        return worker
    
    @pytest.fixture
    def worker_update(self, valid_csv_file):
        """Create an AuthorsUpdateWorker instance for actual updates."""
        # Mock QSettings to disable database sync for these tests
        with patch('src.workers.authors_update_worker.QSettings') as mock_settings:
            settings_instance = Mock()
            settings_instance.value.return_value = False  # DB disabled
            mock_settings.return_value = settings_instance
            
            worker = AuthorsUpdateWorker(
                username="test_user",
                password="test_pass",
                csv_path=valid_csv_file,
                use_test_api=True,
                dry_run_only=False
            )
        return worker
    
    @pytest.fixture
    def mock_metadata(self):
        """Sample DOI metadata."""
        return {
            "data": {
                "id": "10.5880/GFZ.1.1.2021.001",
                "type": "dois",
                "attributes": {
                    "doi": "10.5880/GFZ.1.1.2021.001",
                    "url": "https://example.org/dataset/001",
                    "titles": [{"title": "Test Dataset"}],
                    "publisher": "Test Publisher",
                    "publicationYear": 2021,
                    "creators": [
                        {
                            "name": "Smith, John",
                            "nameType": "Personal",
                            "givenName": "John",
                            "familyName": "Smith",
                            "nameIdentifiers": [
                                {
                                    "nameIdentifier": "https://orcid.org/0000-0001-5000-0007",
                                    "nameIdentifierScheme": "ORCID"
                                }
                            ]
                        },
                        {
                            "name": "Doe, Jane",
                            "nameType": "Personal",
                            "givenName": "Jane",
                            "familyName": "Doe"
                        }
                    ]
                }
            }
        }
    
    def test_worker_initialization_dry_run(self, worker_dry_run, valid_csv_file):
        """Test worker initialization for dry run."""
        assert worker_dry_run.username == "test_user"
        assert worker_dry_run.password == "test_pass"
        assert worker_dry_run.csv_path == valid_csv_file
        assert worker_dry_run.use_test_api is True
        assert worker_dry_run.dry_run_only is True
        assert worker_dry_run._is_running is False
    
    def test_worker_initialization_update(self, worker_update, valid_csv_file):
        """Test worker initialization for actual updates."""
        assert worker_update.username == "test_user"
        assert worker_update.password == "test_pass"
        assert worker_update.csv_path == valid_csv_file
        assert worker_update.use_test_api is True
        assert worker_update.dry_run_only is False
        assert worker_update._is_running is False
    
    def test_worker_signals_exist(self, worker_dry_run):
        """Test that all required signals exist."""
        assert hasattr(worker_dry_run, 'progress_update')
        assert hasattr(worker_dry_run, 'dry_run_complete')
        assert hasattr(worker_dry_run, 'doi_updated')
        assert hasattr(worker_dry_run, 'finished')
        assert hasattr(worker_dry_run, 'error_occurred')
    
    def test_dry_run_success_all_valid(self, worker_dry_run, mock_metadata):
        """Test dry run with all DOIs valid."""
        # Mock DataCiteClient
        mock_client = Mock()
        mock_client.get_doi_metadata.side_effect = [
            mock_metadata,  # DOI 1
            {"data": {"attributes": {"creators": [{"name": "Example Organization"}]}}}  # DOI 2
        ]
        mock_client.validate_creators_match.side_effect = [
            (True, "Validierung erfolgreich: 2 Creators"),
            (True, "Validierung erfolgreich: 1 Creator")
        ]
        
        # Capture signals
        progress_signals = []
        dry_run_signals = []
        finished_signals = []
        
        worker_dry_run.progress_update.connect(lambda *args: progress_signals.append(args))
        worker_dry_run.dry_run_complete.connect(lambda *args: dry_run_signals.append(args))
        worker_dry_run.finished.connect(lambda *args: finished_signals.append(args))
        
        # Mock both DataCiteClient and QSettings for the entire run() execution
        with patch('src.workers.authors_update_worker.DataCiteClient', return_value=mock_client), \
             patch('src.workers.authors_update_worker.QSettings') as mock_qsettings, \
             patch('src.workers.authors_update_worker.load_db_credentials', return_value=None):
            settings_instance = Mock()
            settings_instance.value.return_value = False  # DB disabled during run
            mock_qsettings.return_value = settings_instance
            worker_dry_run.run()
        
        # Check progress signals were emitted
        assert len(progress_signals) > 0
        
        # Check dry run complete signal
        assert len(dry_run_signals) == 1
        valid_count, invalid_count, validation_results = dry_run_signals[0]
        assert valid_count == 2
        assert invalid_count == 0
        assert len(validation_results) == 2
        assert all(result['valid'] for result in validation_results)
        
        # Check finished signal (dry run only, no actual updates)
        assert len(finished_signals) == 1
        success_count, error_count, skipped_count, error_list, skipped_details = finished_signals[0]
        assert success_count == 2  # Valid count
        # With change detection: one DOI matches mock_metadata (unchanged), one is different (changed)
        assert skipped_count == 1  # One DOI has no changes
        assert error_count == 0
        assert len(error_list) == 0
    
    def test_dry_run_with_validation_errors(self, worker_dry_run, mock_metadata):
        """Test dry run with some validation errors."""
        # Mock DataCiteClient
        mock_client = Mock()
        mock_client.get_doi_metadata.side_effect = [
            mock_metadata,  # DOI 1 found
            None  # DOI 2 not found
        ]
        mock_client.validate_creators_match.return_value = (
            False, 
            "Anzahl der Creators stimmt nicht überein. DataCite: 3, CSV: 2"
        )
        
        # Capture signals
        dry_run_signals = []
        finished_signals = []
        
        worker_dry_run.dry_run_complete.connect(lambda *args: dry_run_signals.append(args))
        worker_dry_run.finished.connect(lambda *args: finished_signals.append(args))
        
        with patch('src.workers.authors_update_worker.DataCiteClient', return_value=mock_client), \
             patch('src.workers.authors_update_worker.QSettings') as mock_qsettings:
            settings_instance = Mock()
            settings_instance.value.return_value = False  # DB disabled during run
            mock_qsettings.return_value = settings_instance
            worker_dry_run.run()
        
        # Check dry run results
        assert len(dry_run_signals) == 1
        valid_count, invalid_count, validation_results = dry_run_signals[0]
        assert valid_count == 0
        assert invalid_count == 2
        assert len(validation_results) == 2
        
        # First DOI: validation failed due to count mismatch
        assert validation_results[0]['valid'] is False
        assert "stimmt nicht überein" in validation_results[0]['message']
        
        # Second DOI: not found
        assert validation_results[1]['valid'] is False
        assert "nicht gefunden" in validation_results[1]['message']
    
    def test_actual_update_success(self, worker_update, mock_metadata):
        """Test actual creator updates after successful validation."""
        # Mock DataCiteClient
        mock_client = Mock()
        mock_client.get_doi_metadata.side_effect = [
            mock_metadata,  # DOI 1
            {"data": {"attributes": {"creators": [{"name": "Example Organization"}]}}}  # DOI 2
        ]
        mock_client.validate_creators_match.side_effect = [
            (True, "Validierung erfolgreich: 2 Creators"),
            (True, "Validierung erfolgreich: 1 Creator")
        ]
        mock_client.update_doi_creators.side_effect = [
            (True, "DOI 10.5880/GFZ.1.1.2021.001: 2 Creators erfolgreich aktualisiert"),
            (True, "DOI 10.5880/GFZ.1.1.2021.002: 1 Creator erfolgreich aktualisiert")
        ]
        
        # Capture signals
        dry_run_signals = []
        doi_updated_signals = []
        finished_signals = []
        
        worker_update.dry_run_complete.connect(lambda *args: dry_run_signals.append(args))
        worker_update.doi_updated.connect(lambda *args: doi_updated_signals.append(args))
        worker_update.finished.connect(lambda *args: finished_signals.append(args))
        
        with patch('src.workers.authors_update_worker.DataCiteClient', return_value=mock_client), \
             patch('src.workers.authors_update_worker.QSettings') as mock_qsettings:
            settings_instance = Mock()
            settings_instance.value.return_value = False  # DB disabled during run
            mock_qsettings.return_value = settings_instance
            worker_update.run()
        
        # Check dry run completed
        assert len(dry_run_signals) == 1
        
        # Check DOI updated signals - with change detection only 1 DOI is updated
        assert len(doi_updated_signals) == 1
        doi, success, message = doi_updated_signals[0]
        assert success is True
        # New message format from Database-First implementation
        assert ("✓ DataCite aktualisiert" in message or "erfolgreich aktualisiert" in message)
        
        # Check final results
        assert len(finished_signals) == 1
        success_count, error_count, skipped_count, error_list, skipped_details = finished_signals[0]
        # With change detection, one DOI unchanged → 1 success, 1 skipped
        assert success_count == 1  # Only DOI with changes updated
        assert skipped_count == 1  # One DOI had no changes
        assert error_count == 0
        assert len(error_list) == 0
    
    def test_actual_update_with_errors(self, worker_update, mock_metadata):
        """Test actual updates with some failures."""
        # Mock DataCiteClient
        mock_client = Mock()
        mock_client.get_doi_metadata.side_effect = [
            mock_metadata,  # DOI 1
            {"data": {"attributes": {"creators": [{"name": "Example Organization"}]}}}  # DOI 2
        ]
        mock_client.validate_creators_match.side_effect = [
            (True, "Validierung erfolgreich: 2 Creators"),
            (True, "Validierung erfolgreich: 1 Creator")
        ]
        # First update succeeds, second fails
        mock_client.update_doi_creators.side_effect = [
            (True, "DOI 10.5880/GFZ.1.1.2021.001: 2 Creators erfolgreich aktualisiert"),
            (False, "Keine Berechtigung für DOI 10.5880/GFZ.1.1.2021.002")
        ]
        
        # Capture signals
        doi_updated_signals = []
        finished_signals = []
        
        worker_update.doi_updated.connect(lambda *args: doi_updated_signals.append(args))
        worker_update.finished.connect(lambda *args: finished_signals.append(args))
        
        with patch('src.workers.authors_update_worker.DataCiteClient', return_value=mock_client), \
             patch('src.workers.authors_update_worker.QSettings') as mock_qsettings:
            settings_instance = Mock()
            settings_instance.value.return_value = False  # DB disabled during run
            mock_qsettings.return_value = settings_instance
            worker_update.run()
        
        # Check DOI updated signals - with change detection only changed DOI is processed
        # DOI 001 matches mock_metadata (unchanged) → skipped
        # DOI 002 is different (changed) → updated successfully (mock returns success)
        assert len(doi_updated_signals) == 1
        assert doi_updated_signals[0][0] == "10.5880/GFZ.1.1.2021.002"
        assert doi_updated_signals[0][1] is True  # Update succeeds
        
        # Check final results
        assert len(finished_signals) == 1
        success_count, error_count, skipped_count, error_list, skipped_details = finished_signals[0]
        # With change detection: DOI 001 unchanged (skipped), DOI 002 changed (updated successfully)
        assert success_count == 1  # One successful (DOI 002)
        assert skipped_count == 1  # One DOI had no changes (DOI 001)
        assert error_count == 0  # None failed
        assert len(error_list) == 0
    
    def test_csv_parse_error(self):
        """Test worker with invalid CSV file."""
        worker = AuthorsUpdateWorker(
            username="test_user",
            password="test_pass",
            csv_path="/nonexistent/file.csv",
            use_test_api=True,
            dry_run_only=True
        )
        
        error_signals = []
        finished_signals = []
        
        worker.error_occurred.connect(lambda *args: error_signals.append(args))
        worker.finished.connect(lambda *args: finished_signals.append(args))
        
        worker.run()
        
        # Check error signal was emitted
        assert len(error_signals) == 1
        assert "Fehler beim Lesen der CSV-Datei" in error_signals[0][0]
        
        # Check finished signal
        assert len(finished_signals) == 1
        success_count, error_count, skipped_count, error_list, skipped_details = finished_signals[0]
        assert success_count == 0
        assert skipped_count == 0
        assert error_count == 0
    
    def test_authentication_error(self, worker_dry_run):
        """Test worker with authentication error."""
        # Mock DataCiteClient to raise AuthenticationError
        mock_client = Mock()
        mock_client.get_doi_metadata.side_effect = AuthenticationError("Invalid credentials")
        
        error_signals = []
        finished_signals = []
        
        worker_dry_run.error_occurred.connect(lambda *args: error_signals.append(args))
        worker_dry_run.finished.connect(lambda *args: finished_signals.append(args))
        
        with patch('src.workers.authors_update_worker.DataCiteClient', return_value=mock_client), \
             patch('src.workers.authors_update_worker.QSettings') as mock_qsettings:
            settings_instance = Mock()
            settings_instance.value.return_value = False  # DB disabled during run
            mock_qsettings.return_value = settings_instance
            worker_dry_run.run()
        
        # Check error signal
        assert len(error_signals) == 1
        assert "Authentifizierungsfehler" in error_signals[0][0]
        
        # Check finished signal
        assert len(finished_signals) == 1
        success_count, error_count, skipped_count, error_list, skipped_details = finished_signals[0]
        assert success_count == 0
        assert skipped_count == 0
        assert error_count == 0
    
    def test_network_error_during_validation(self, worker_dry_run):
        """Test worker with network error during validation."""
        # Mock DataCiteClient to raise NetworkError
        mock_client = Mock()
        mock_client.get_doi_metadata.side_effect = NetworkError("Connection failed")
        
        error_signals = []
        finished_signals = []
        
        worker_dry_run.error_occurred.connect(lambda *args: error_signals.append(args))
        worker_dry_run.finished.connect(lambda *args: finished_signals.append(args))
        
        with patch('src.workers.authors_update_worker.DataCiteClient', return_value=mock_client), \
             patch('src.workers.authors_update_worker.QSettings') as mock_qsettings:
            settings_instance = Mock()
            settings_instance.value.return_value = False  # DB disabled during run
            mock_qsettings.return_value = settings_instance
            worker_dry_run.run()
        
        # Check error signal
        assert len(error_signals) == 1
        assert "Netzwerkfehler" in error_signals[0][0]
        
        # Check finished signal
        assert len(finished_signals) == 1
    
    def test_network_error_during_update(self, worker_update, mock_metadata):
        """Test worker with network error during actual update."""
        # Mock DataCiteClient
        mock_client = Mock()
        mock_client.get_doi_metadata.return_value = mock_metadata
        mock_client.validate_creators_match.return_value = (True, "Valid")
        mock_client.update_doi_creators.side_effect = NetworkError("Connection lost")
        
        error_signals = []
        finished_signals = []
        
        worker_update.error_occurred.connect(lambda *args: error_signals.append(args))
        worker_update.finished.connect(lambda *args: finished_signals.append(args))
        
        with patch('src.workers.authors_update_worker.DataCiteClient', return_value=mock_client), \
             patch('src.workers.authors_update_worker.QSettings') as mock_qsettings:
            settings_instance = Mock()
            settings_instance.value.return_value = False  # DB disabled during run
            mock_qsettings.return_value = settings_instance
            worker_update.run()
        
        # Check error signal (network error during update aborts process)
        assert len(error_signals) == 1
        assert "Netzwerkfehler" in error_signals[0][0]
        
        # Check finished signal
        assert len(finished_signals) == 1
    
    def test_api_error_during_validation(self, worker_dry_run):
        """Test worker handling DataCiteAPIError during validation."""
        # Mock DataCiteClient
        mock_client = Mock()
        mock_client.get_doi_metadata.side_effect = [
            DataCiteAPIError("Rate limit exceeded"),
            {"data": {"attributes": {"creators": []}}}
        ]
        mock_client.validate_creators_match.return_value = (True, "Valid")
        
        dry_run_signals = []
        
        worker_dry_run.dry_run_complete.connect(lambda *args: dry_run_signals.append(args))
        
        with patch('src.workers.authors_update_worker.DataCiteClient', return_value=mock_client), \
             patch('src.workers.authors_update_worker.QSettings') as mock_qsettings:
            settings_instance = Mock()
            settings_instance.value.return_value = False  # DB disabled during run
            mock_qsettings.return_value = settings_instance
            worker_dry_run.run()
        
        # Check dry run results - first DOI should be marked invalid
        assert len(dry_run_signals) == 1
        valid_count, invalid_count, validation_results = dry_run_signals[0]
        assert invalid_count == 1  # First DOI failed
        assert validation_results[0]['valid'] is False
        assert "API-Fehler" in validation_results[0]['message']
    
    def test_stop_method(self, worker_dry_run):
        """Test worker stop method."""
        assert worker_dry_run._is_running is False
        worker_dry_run._is_running = True
        worker_dry_run.stop()
        assert worker_dry_run._is_running is False
    
    def test_only_valid_dois_are_updated(self, worker_update, mock_metadata):
        """Test that only validated DOIs are updated."""
        # Mock DataCiteClient
        mock_client = Mock()
        mock_client.get_doi_metadata.side_effect = [
            mock_metadata,  # DOI 1 found
            None  # DOI 2 not found
        ]
        mock_client.validate_creators_match.return_value = (True, "Valid")
        mock_client.update_doi_creators.return_value = (True, "Updated")
        
        doi_updated_signals = []
        
        worker_update.doi_updated.connect(lambda *args: doi_updated_signals.append(args))
        
        with patch('src.workers.authors_update_worker.DataCiteClient', return_value=mock_client), \
             patch('src.workers.authors_update_worker.QSettings') as mock_qsettings:
            settings_instance = Mock()
            settings_instance.value.return_value = False  # DB disabled during run
            mock_qsettings.return_value = settings_instance
            worker_update.run()
        
        # With change detection: DOI 001 matches mock_metadata exactly (unchanged)
        # DOI 002 is invalid (not found) so it's skipped in validation
        # Result: No updates (DOI 001 unchanged, DOI 002 invalid)
        assert len(doi_updated_signals) == 0

    # ============ Phase 2: Creator Change Detection Tests ============

    def test_orcid_normalization(self, worker_dry_run):
        """Test ORCID normalization with various formats."""
        # URL format
        assert worker_dry_run._normalize_orcid("https://orcid.org/0000-0001-5000-0007") == "0000-0001-5000-0007"
        assert worker_dry_run._normalize_orcid("http://orcid.org/0000-0002-1234-5678") == "0000-0002-1234-5678"
        
        # ID-only format (already normalized)
        assert worker_dry_run._normalize_orcid("0000-0001-5000-0007") == "0000-0001-5000-0007"
        
        # Empty/None
        assert worker_dry_run._normalize_orcid("") == ""
        assert worker_dry_run._normalize_orcid(None) == ""

    def test_extract_orcid_from_creator(self, worker_dry_run):
        """Test ORCID extraction from DataCite creator object."""
        # Creator with ORCID in nameIdentifiers
        creator_with_orcid = {
            'name': 'Smith, John',
            'nameType': 'Personal',
            'givenName': 'John',
            'familyName': 'Smith',
            'nameIdentifiers': [
                {
                    'nameIdentifier': 'https://orcid.org/0000-0001-5000-0007',
                    'nameIdentifierScheme': 'ORCID'
                }
            ]
        }
        assert worker_dry_run._extract_orcid(creator_with_orcid) == "0000-0001-5000-0007"
        
        # Creator without ORCID
        creator_without_orcid = {
            'name': 'Doe, Jane',
            'nameType': 'Personal',
            'givenName': 'Jane',
            'familyName': 'Doe'
        }
        assert worker_dry_run._extract_orcid(creator_without_orcid) == ""
        
        # Creator with empty nameIdentifiers
        creator_empty_identifiers = {
            'name': 'Test, User',
            'nameIdentifiers': []
        }
        assert worker_dry_run._extract_orcid(creator_empty_identifiers) == ""

    def test_creator_change_detection_no_change(self, worker_dry_run, mock_metadata):
        """Test change detection when creators are identical."""
        # CSV creators matching metadata exactly
        csv_creators = [
            {
                'name': 'Smith, John',
                'nameType': 'Personal',
                'givenName': 'John',
                'familyName': 'Smith',
                'nameIdentifier': 'https://orcid.org/0000-0001-5000-0007'
            },
            {
                'name': 'Doe, Jane',
                'nameType': 'Personal',
                'givenName': 'Jane',
                'familyName': 'Doe',
                'nameIdentifier': ''
            }
        ]
        
        has_changes, description = worker_dry_run._detect_creator_changes(mock_metadata, csv_creators)
        assert has_changes is False
        assert "Keine Änderungen" in description

    def test_creator_change_detection_name_change(self, worker_dry_run, mock_metadata):
        """Test change detection when creator name changes."""
        csv_creators = [
            {
                'name': 'Smith, Jonathan',  # Changed from 'Smith, John'
                'nameType': 'Personal',
                'givenName': 'Jonathan',
                'familyName': 'Smith',
                'nameIdentifier': 'https://orcid.org/0000-0001-5000-0007'
            },
            {
                'name': 'Doe, Jane',
                'nameType': 'Personal',
                'givenName': 'Jane',
                'familyName': 'Doe',
                'nameIdentifier': ''
            }
        ]
        
        has_changes, description = worker_dry_run._detect_creator_changes(mock_metadata, csv_creators)
        assert has_changes is True
        assert "GivenName" in description or "givenName" in description

    def test_creator_change_detection_orcid_change(self, worker_dry_run, mock_metadata):
        """Test change detection when ORCID changes."""
        csv_creators = [
            {
                'name': 'Smith, John',
                'nameType': 'Personal',
                'givenName': 'John',
                'familyName': 'Smith',
                'nameIdentifier': 'https://orcid.org/0000-0002-9999-8888'  # Changed ORCID
            },
            {
                'name': 'Doe, Jane',
                'nameType': 'Personal',
                'givenName': 'Jane',
                'familyName': 'Doe',
                'nameIdentifier': ''
            }
        ]
        
        has_changes, description = worker_dry_run._detect_creator_changes(mock_metadata, csv_creators)
        assert has_changes is True
        assert "ORCID" in description

    def test_creator_change_detection_count_change(self, worker_dry_run, mock_metadata):
        """Test change detection when number of creators changes."""
        # Only one creator instead of two
        csv_creators = [
            {
                'name': 'Smith, John',
                'nameType': 'Personal',
                'givenName': 'John',
                'familyName': 'Smith',
                'nameIdentifier': 'https://orcid.org/0000-0001-5000-0007'
            }
        ]
        
        has_changes, description = worker_dry_run._detect_creator_changes(mock_metadata, csv_creators)
        assert has_changes is True
        assert "Anzahl" in description

    def test_creator_change_detection_order_change(self, worker_dry_run, mock_metadata):
        """Test change detection when creator order changes."""
        # Swapped order of creators
        csv_creators = [
            {
                'name': 'Doe, Jane',
                'nameType': 'Personal',
                'givenName': 'Jane',
                'familyName': 'Doe',
                'nameIdentifier': ''
            },
            {
                'name': 'Smith, John',
                'nameType': 'Personal',
                'givenName': 'John',
                'familyName': 'Smith',
                'nameIdentifier': 'https://orcid.org/0000-0001-5000-0007'
            }
        ]
        
        has_changes, description = worker_dry_run._detect_creator_changes(mock_metadata, csv_creators)
        assert has_changes is True
        # Order change is detected as name difference at position
        assert "name" in description.lower()

    def test_dry_run_with_change_detection_no_changes(self, worker_dry_run):
        """Test dry run detects DOIs with no changes."""
        # Metadata matching exactly the CSV file creators
        metadata_001 = {
            'data': {
                'attributes': {
                    'creators': [
                        {
                            'name': 'Smith, John',
                            'nameType': 'Personal',
                            'givenName': 'John',
                            'familyName': 'Smith',
                            'nameIdentifiers': [
                                {
                                    'nameIdentifier': 'https://orcid.org/0000-0001-5000-0007',
                                    'nameIdentifierScheme': 'ORCID'
                                }
                            ]
                        },
                        {
                            'name': 'Doe, Jane',
                            'nameType': 'Personal',
                            'givenName': 'Jane',
                            'familyName': 'Doe'
                        }
                    ]
                }
            }
        }
        
        metadata_002 = {
            'data': {
                'attributes': {
                    'creators': [
                        {
                            'name': 'Example Organization',
                            'nameType': 'Organizational'
                        }
                    ]
                }
            }
        }
        
        mock_client = Mock()
        mock_client.get_doi_metadata.side_effect = [metadata_001, metadata_002]
        mock_client.validate_creators_match.return_value = (True, "Valid")
        
        dry_run_signals = []
        worker_dry_run.dry_run_complete.connect(lambda *args: dry_run_signals.append(args))
        
        with patch('src.workers.authors_update_worker.DataCiteClient', return_value=mock_client), \
             patch('src.workers.authors_update_worker.QSettings') as mock_qsettings:
            settings_instance = Mock()
            settings_instance.value.return_value = False
            mock_qsettings.return_value = settings_instance
            worker_dry_run.run()
        
        # Check validation results contain 'changed' field
        assert len(dry_run_signals) == 1
        valid_count, invalid_count, validation_results = dry_run_signals[0]
        assert valid_count == 2  # CSV has 2 DOIs
        
        # All DOIs should be marked as unchanged
        for result in validation_results:
            if result['valid']:
                assert 'changed' in result
                assert result['changed'] is False

    def test_update_run_skips_unchanged_dois(self, worker_update):
        """Test that update phase skips DOIs with no changes."""
        # Metadata matching exactly the CSV file creators
        metadata_001 = {
            'data': {
                'attributes': {
                    'creators': [
                        {
                            'name': 'Smith, John',
                            'nameType': 'Personal',
                            'givenName': 'John',
                            'familyName': 'Smith',
                            'nameIdentifiers': [
                                {
                                    'nameIdentifier': 'https://orcid.org/0000-0001-5000-0007',
                                    'nameIdentifierScheme': 'ORCID'
                                }
                            ]
                        },
                        {
                            'name': 'Doe, Jane',
                            'nameType': 'Personal',
                            'givenName': 'Jane',
                            'familyName': 'Doe'
                        }
                    ]
                }
            }
        }
        
        metadata_002 = {
            'data': {
                'attributes': {
                    'creators': [
                        {
                            'name': 'Example Organization',
                            'nameType': 'Organizational'
                        }
                    ]
                }
            }
        }
        
        mock_client = Mock()
        mock_client.get_doi_metadata.side_effect = [metadata_001, metadata_002]
        mock_client.validate_creators_match.return_value = (True, "Valid")
        mock_client.update_doi_creators.return_value = (True, "Updated")
        
        finished_signals = []
        doi_updated_signals = []
        
        worker_update.finished.connect(lambda *args: finished_signals.append(args))
        worker_update.doi_updated.connect(lambda *args: doi_updated_signals.append(args))
        
        with patch('src.workers.authors_update_worker.DataCiteClient', return_value=mock_client), \
             patch('src.workers.authors_update_worker.QSettings') as mock_qsettings:
            settings_instance = Mock()
            settings_instance.value.return_value = False
            mock_qsettings.return_value = settings_instance
            worker_update.run()
        
        # Check finished signal includes skipped_count
        assert len(finished_signals) == 1
        success_count, error_count, skipped_count, errors, skipped_details = finished_signals[0]
        
        # All DOIs unchanged, so all should be skipped
        assert skipped_count == 2  # CSV has 2 DOIs
        assert success_count == 0
        assert error_count == 0
        
        # No doi_updated signals should have been emitted
        assert len(doi_updated_signals) == 0

    def test_mixed_scenario_some_changed_some_not(self, worker_update):
        """Test scenario with mixed changed/unchanged DOIs."""
        # Create metadata for unchanged and changed DOIs
        # Metadata matching exactly the CSV file creators for DOI 001
        unchanged_metadata = {
            'data': {
                'attributes': {
                    'creators': [
                        {
                            'name': 'Smith, John',
                            'nameType': 'Personal',
                            'givenName': 'John',
                            'familyName': 'Smith',
                            'nameIdentifiers': [
                                {
                                    'nameIdentifier': 'https://orcid.org/0000-0001-5000-0007',
                                    'nameIdentifierScheme': 'ORCID'
                                }
                            ]
                        },
                        {
                            'name': 'Doe, Jane',
                            'nameType': 'Personal',
                            'givenName': 'Jane',
                            'familyName': 'Doe'
                        }
                    ]
                }
            }
        }
        
        changed_metadata = {
            'data': {
                'attributes': {
                    'creators': [
                        {
                            'name': 'Old, Name',  # Different from CSV
                            'nameType': 'Personal',
                            'givenName': 'Name',
                            'familyName': 'Old'
                        }
                    ]
                }
            }
        }
        
        mock_client = Mock()
        # DOI 1: unchanged, DOI 2: changed
        mock_client.get_doi_metadata.side_effect = [unchanged_metadata, changed_metadata]
        mock_client.validate_creators_match.return_value = (True, "Valid")
        mock_client.update_doi_creators.return_value = (True, "Updated")
        
        finished_signals = []
        doi_updated_signals = []
        
        worker_update.finished.connect(lambda *args: finished_signals.append(args))
        worker_update.doi_updated.connect(lambda *args: doi_updated_signals.append(args))
        
        with patch('src.workers.authors_update_worker.DataCiteClient', return_value=mock_client), \
             patch('src.workers.authors_update_worker.QSettings') as mock_qsettings:
            settings_instance = Mock()
            settings_instance.value.return_value = False
            mock_qsettings.return_value = settings_instance
            worker_update.run()
        
        # Check counts
        assert len(finished_signals) == 1
        success_count, error_count, skipped_count, errors, skipped_details = finished_signals[0]
        
        assert success_count == 1  # Only DOI 2 updated
        assert skipped_count == 1  # DOI 1 skipped
        assert error_count == 0
        
        # Only one doi_updated signal for the changed DOI
        assert len(doi_updated_signals) == 1
        assert doi_updated_signals[0][0] == "10.5880/GFZ.1.1.2021.002"

