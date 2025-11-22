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
                            "familyName": "Smith"
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
        success_count, error_count, error_list = finished_signals[0]
        assert success_count == 2  # Valid count
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
        
        # Check DOI updated signals
        assert len(doi_updated_signals) == 2
        for doi, success, message in doi_updated_signals:
            assert success is True
            # New message format from Database-First implementation
            assert ("✓ DataCite aktualisiert" in message or "erfolgreich aktualisiert" in message)
        
        # Check final results
        assert len(finished_signals) == 1
        success_count, error_count, error_list = finished_signals[0]
        assert success_count == 2
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
        
        # Check DOI updated signals
        assert len(doi_updated_signals) == 2
        assert doi_updated_signals[0][1] is True  # First DOI success
        assert doi_updated_signals[1][1] is False  # Second DOI failed
        
        # Check final results
        assert len(finished_signals) == 1
        success_count, error_count, error_list = finished_signals[0]
        assert success_count == 1
        assert error_count == 1
        assert len(error_list) == 1
        assert "Keine Berechtigung" in error_list[0]
    
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
        success_count, error_count, error_list = finished_signals[0]
        assert success_count == 0
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
        success_count, error_count, error_list = finished_signals[0]
        assert success_count == 0
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
        
        # Only one DOI should be updated (the valid one)
        assert len(doi_updated_signals) == 1
        assert doi_updated_signals[0][0] == "10.5880/GFZ.1.1.2021.001"
