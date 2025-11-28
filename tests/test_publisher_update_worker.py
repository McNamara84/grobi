"""Tests for PublisherUpdateWorker."""

import pytest
from unittest.mock import Mock, patch
import tempfile
import os

from src.workers.publisher_update_worker import PublisherUpdateWorker
from src.api.datacite_client import NetworkError, DataCiteAPIError, AuthenticationError


class TestPublisherUpdateWorker:
    """Test suite for PublisherUpdateWorker class."""
    
    @pytest.fixture
    def valid_csv_file(self):
        """Create a temporary valid CSV file with publisher data."""
        with tempfile.NamedTemporaryFile(
            mode='w', 
            suffix='.csv', 
            delete=False, 
            encoding='utf-8',
            newline=''
        ) as f:
            f.write("DOI,Publisher Name,Publisher Identifier,Publisher Identifier Scheme,Scheme URI,Language\n")
            f.write('10.5880/GFZ.1.1.2021.001,GFZ German Research Centre for Geosciences,https://ror.org/04z8jg394,ROR,https://ror.org,en\n')
            f.write('10.5880/GFZ.1.1.2021.002,Helmholtz Centre Potsdam,,,,\n')
            csv_path = f.name
        
        yield csv_path
        
        # Cleanup
        if os.path.exists(csv_path):
            os.unlink(csv_path)
    
    @pytest.fixture
    def worker_dry_run(self, valid_csv_file):
        """Create a PublisherUpdateWorker instance for dry run."""
        with patch('src.workers.publisher_update_worker.QSettings') as mock_settings:
            settings_instance = Mock()
            settings_instance.value.return_value = False  # DB disabled
            mock_settings.return_value = settings_instance
            
            worker = PublisherUpdateWorker(
                username="test_user",
                password="test_pass",
                csv_path=valid_csv_file,
                use_test_api=True,
                dry_run_only=True
            )
        return worker
    
    @pytest.fixture
    def worker_update(self, valid_csv_file):
        """Create a PublisherUpdateWorker instance for actual updates."""
        with patch('src.workers.publisher_update_worker.QSettings') as mock_settings:
            settings_instance = Mock()
            settings_instance.value.return_value = False  # DB disabled
            mock_settings.return_value = settings_instance
            
            worker = PublisherUpdateWorker(
                username="test_user",
                password="test_pass",
                csv_path=valid_csv_file,
                use_test_api=True,
                dry_run_only=False
            )
        return worker
    
    @pytest.fixture
    def mock_metadata(self):
        """Sample DOI metadata with publisher."""
        return {
            "data": {
                "id": "10.5880/GFZ.1.1.2021.001",
                "type": "dois",
                "attributes": {
                    "doi": "10.5880/GFZ.1.1.2021.001",
                    "url": "https://example.org/dataset/001",
                    "titles": [{"title": "Test Dataset"}],
                    "publisher": {
                        "name": "GFZ German Research Centre for Geosciences",
                        "publisherIdentifier": "https://ror.org/04z8jg394",
                        "publisherIdentifierScheme": "ROR",
                        "schemeUri": "https://ror.org",
                        "lang": "en"
                    },
                    "publicationYear": 2021,
                    "creators": []
                }
            }
        }
    
    @pytest.fixture
    def mock_metadata_string_publisher(self):
        """Sample DOI metadata with publisher as string (legacy format)."""
        return {
            "data": {
                "id": "10.5880/GFZ.1.1.2021.002",
                "type": "dois",
                "attributes": {
                    "doi": "10.5880/GFZ.1.1.2021.002",
                    "url": "https://example.org/dataset/002",
                    "titles": [{"title": "Test Dataset 2"}],
                    "publisher": "Helmholtz Centre Potsdam",  # String format
                    "publicationYear": 2021,
                    "creators": []
                }
            }
        }
    
    # ===================== INITIALIZATION TESTS =====================
    
    def test_worker_initialization_dry_run(self, worker_dry_run):
        """Test worker initialization for dry run."""
        assert worker_dry_run.username == "test_user"
        assert worker_dry_run.password == "test_pass"
        assert worker_dry_run.use_test_api is True
        assert worker_dry_run.dry_run_only is True
        assert worker_dry_run._is_running is False
    
    def test_worker_initialization_update(self, worker_update):
        """Test worker initialization for actual updates."""
        assert worker_update.dry_run_only is False
    
    def test_worker_signals_exist(self, worker_dry_run):
        """Test that all required signals exist."""
        assert hasattr(worker_dry_run, 'progress_update')
        assert hasattr(worker_dry_run, 'dry_run_complete')
        assert hasattr(worker_dry_run, 'doi_updated')
        assert hasattr(worker_dry_run, 'finished')
        assert hasattr(worker_dry_run, 'error_occurred')
        assert hasattr(worker_dry_run, 'validation_update')
        assert hasattr(worker_dry_run, 'datacite_update')
        assert hasattr(worker_dry_run, 'database_update')
        assert hasattr(worker_dry_run, 'request_save_credentials')
    
    # ===================== DRY RUN TESTS =====================
    
    def test_dry_run_success_all_valid(self, valid_csv_file):
        """Test dry run with all valid DOIs."""
        with patch('src.workers.publisher_update_worker.QSettings') as mock_settings:
            settings_instance = Mock()
            settings_instance.value.return_value = False
            mock_settings.return_value = settings_instance
            
            worker = PublisherUpdateWorker(
                username="test_user",
                password="test_pass",
                csv_path=valid_csv_file,
                use_test_api=True,
                dry_run_only=True
            )
        
        # Create mock client
        mock_client = Mock()
        mock_client.get_doi_metadata.return_value = {
            "data": {
                "attributes": {
                    "publisher": "Old Publisher"
                }
            }
        }
        
        # Collect signal emissions
        dry_run_results = []
        worker.dry_run_complete.connect(
            lambda v, i, r: dry_run_results.append((v, i, r))
        )
        
        with patch('src.workers.publisher_update_worker.DataCiteClient', return_value=mock_client):
            with patch('src.workers.publisher_update_worker.load_db_credentials', return_value=None):
                worker.run()
        
        assert len(dry_run_results) == 1
        valid_count, invalid_count, results = dry_run_results[0]
        assert valid_count == 2
        assert invalid_count == 0
    
    def test_dry_run_with_validation_errors(self, valid_csv_file):
        """Test dry run with some invalid DOIs."""
        with patch('src.workers.publisher_update_worker.QSettings') as mock_settings:
            settings_instance = Mock()
            settings_instance.value.return_value = False
            mock_settings.return_value = settings_instance
            
            worker = PublisherUpdateWorker(
                username="test_user",
                password="test_pass",
                csv_path=valid_csv_file,
                use_test_api=True,
                dry_run_only=True
            )
        
        # Create mock client that raises error for one DOI
        mock_client = Mock()
        def side_effect(doi):
            if doi == "10.5880/GFZ.1.1.2021.001":
                return {"data": {"attributes": {"publisher": "Old Publisher"}}}
            else:
                raise DataCiteAPIError("DOI not found")
        
        mock_client.get_doi_metadata.side_effect = side_effect
        
        dry_run_results = []
        worker.dry_run_complete.connect(
            lambda v, i, r: dry_run_results.append((v, i, r))
        )
        
        with patch('src.workers.publisher_update_worker.DataCiteClient', return_value=mock_client):
            with patch('src.workers.publisher_update_worker.load_db_credentials', return_value=None):
                worker.run()
        
        assert len(dry_run_results) == 1
        valid_count, invalid_count, results = dry_run_results[0]
        assert valid_count == 1
        assert invalid_count == 1
    
    # ===================== ACTUAL UPDATE TESTS =====================
    
    def test_actual_update_success(self, valid_csv_file):
        """Test actual update with successful API calls."""
        with patch('src.workers.publisher_update_worker.QSettings') as mock_settings:
            settings_instance = Mock()
            settings_instance.value.return_value = False
            mock_settings.return_value = settings_instance
            
            worker = PublisherUpdateWorker(
                username="test_user",
                password="test_pass",
                csv_path=valid_csv_file,
                use_test_api=True,
                dry_run_only=False
            )
        
        # Create mock client
        mock_client = Mock()
        mock_client.get_doi_metadata.return_value = {
            "data": {
                "attributes": {
                    "publisher": "Old Publisher"  # Different from CSV
                }
            }
        }
        # Return tuple (success, message) as the real method does
        mock_client.update_doi_publisher.return_value = (True, "Successfully updated")
        
        finished_results = []
        worker.finished.connect(
            lambda s, e, sk, el, sd: finished_results.append((s, e, sk, el, sd))
        )
        
        with patch('src.workers.publisher_update_worker.DataCiteClient', return_value=mock_client):
            with patch('src.workers.publisher_update_worker.load_db_credentials', return_value=None):
                worker.run()
        
        assert len(finished_results) == 1
        success_count, error_count, skipped_count, error_list, skipped_details = finished_results[0]
        assert success_count == 2
        assert error_count == 0
        assert skipped_count == 0
    
    def test_actual_update_with_errors(self, valid_csv_file):
        """Test actual update with some failed API calls."""
        with patch('src.workers.publisher_update_worker.QSettings') as mock_settings:
            settings_instance = Mock()
            settings_instance.value.return_value = False
            mock_settings.return_value = settings_instance
            
            worker = PublisherUpdateWorker(
                username="test_user",
                password="test_pass",
                csv_path=valid_csv_file,
                use_test_api=True,
                dry_run_only=False
            )
        
        mock_client = Mock()
        mock_client.get_doi_metadata.return_value = {
            "data": {"attributes": {"publisher": "Old Publisher"}}
        }
        
        # First call succeeds, second fails - return tuples
        mock_client.update_doi_publisher.side_effect = [
            (True, "Successfully updated"),
            (False, "Update failed: API error")
        ]
        
        finished_results = []
        worker.finished.connect(
            lambda s, e, sk, el, sd: finished_results.append((s, e, sk, el, sd))
        )
        
        with patch('src.workers.publisher_update_worker.DataCiteClient', return_value=mock_client):
            with patch('src.workers.publisher_update_worker.load_db_credentials', return_value=None):
                worker.run()
        
        assert len(finished_results) == 1
        success_count, error_count, skipped_count, error_list, skipped_details = finished_results[0]
        assert success_count == 1
        assert error_count == 1
        assert len(error_list) == 1
    
    # ===================== CSV PARSE ERROR TESTS =====================
    
    def test_csv_parse_error(self):
        """Test handling of CSV parse errors."""
        with tempfile.NamedTemporaryFile(
            mode='w', suffix='.csv', delete=False, encoding='utf-8'
        ) as f:
            f.write("Invalid,Headers\n")
            f.write("data,values\n")
            csv_path = f.name
        
        try:
            with patch('src.workers.publisher_update_worker.QSettings') as mock_settings:
                settings_instance = Mock()
                settings_instance.value.return_value = False
                mock_settings.return_value = settings_instance
                
                worker = PublisherUpdateWorker(
                    username="test_user",
                    password="test_pass",
                    csv_path=csv_path,
                    use_test_api=True,
                    dry_run_only=True
                )
            
            error_messages = []
            worker.error_occurred.connect(lambda msg: error_messages.append(msg))
            
            with patch('src.workers.publisher_update_worker.DataCiteClient'):
                with patch('src.workers.publisher_update_worker.load_db_credentials', return_value=None):
                    worker.run()
            
            assert len(error_messages) == 1
            assert "Header" in error_messages[0] or "CSV" in error_messages[0]
        finally:
            os.unlink(csv_path)
    
    # ===================== AUTHENTICATION ERROR TESTS =====================
    
    def test_authentication_error(self, valid_csv_file):
        """Test handling of authentication errors."""
        with patch('src.workers.publisher_update_worker.QSettings') as mock_settings:
            settings_instance = Mock()
            settings_instance.value.return_value = False
            mock_settings.return_value = settings_instance
            
            worker = PublisherUpdateWorker(
                username="test_user",
                password="wrong_pass",
                csv_path=valid_csv_file,
                use_test_api=True,
                dry_run_only=True
            )
        
        error_messages = []
        worker.error_occurred.connect(lambda msg: error_messages.append(msg))
        
        with patch('src.workers.publisher_update_worker.DataCiteClient') as mock_client_class:
            mock_client_class.side_effect = AuthenticationError("Invalid credentials")
            with patch('src.workers.publisher_update_worker.load_db_credentials', return_value=None):
                worker.run()
        
        assert len(error_messages) == 1
        # Check for either German or English error message
        error_lower = error_messages[0].lower()
        assert "authentifizierung" in error_lower or "invalid" in error_lower
    
    # ===================== NETWORK ERROR TESTS =====================
    
    def test_network_error_during_validation(self, valid_csv_file):
        """Test handling of network errors during API availability check."""
        with patch('src.workers.publisher_update_worker.QSettings') as mock_settings:
            settings_instance = Mock()
            settings_instance.value.return_value = False
            mock_settings.return_value = settings_instance
            
            worker = PublisherUpdateWorker(
                username="test_user",
                password="test_pass",
                csv_path=valid_csv_file,
                use_test_api=True,
                dry_run_only=True
            )
        
        mock_client = Mock()
        mock_client.get_doi_metadata.side_effect = NetworkError("Connection failed")
        
        error_results = []
        worker.error_occurred.connect(lambda msg: error_results.append(msg))
        
        finished_results = []
        worker.finished.connect(
            lambda s, e, sk, el, sd: finished_results.append((s, e, sk, el, sd))
        )
        
        with patch('src.workers.publisher_update_worker.DataCiteClient', return_value=mock_client):
            with patch('src.workers.publisher_update_worker.load_db_credentials', return_value=None):
                worker.run()
        
        # Network error during API check should abort with error
        assert len(error_results) == 1
        # Check for either German or English error message
        error_lower = error_results[0].lower()
        assert "nicht erreichbar" in error_lower or "connection" in error_lower
        
        # Finished signal should indicate no updates
        assert len(finished_results) == 1
        success_count, error_count, skipped_count, error_list, skipped_details = finished_results[0]
        assert success_count == 0
        assert error_count == 0
    
    # ===================== STOP METHOD TESTS =====================
    
    def test_stop_method(self, worker_update):
        """Test that stop method sets _is_running to False."""
        worker_update._is_running = True
        worker_update.stop()
        assert worker_update._is_running is False
    
    # ===================== CHANGE DETECTION TESTS =====================
    
    def test_publisher_change_detection_no_change(self, worker_dry_run, mock_metadata):
        """Test that identical publisher data is detected as no change."""
        csv_publisher = {
            "name": "GFZ German Research Centre for Geosciences",
            "publisherIdentifier": "https://ror.org/04z8jg394",
            "publisherIdentifierScheme": "ROR",
            "schemeUri": "https://ror.org",
            "lang": "en"
        }
        
        has_changes, description = worker_dry_run._detect_publisher_changes(mock_metadata, csv_publisher)
        
        assert has_changes is False
        # Match the exact message from implementation ("Keine Änderungen")
        assert "keine änderungen" in description.lower()
    
    def test_publisher_change_detection_name_change(self, worker_dry_run, mock_metadata):
        """Test that name change is detected."""
        csv_publisher = {
            "name": "Different Publisher Name",  # Changed
            "publisherIdentifier": "https://ror.org/04z8jg394",
            "publisherIdentifierScheme": "ROR",
            "schemeUri": "https://ror.org",
            "lang": "en"
        }
        
        has_changes, description = worker_dry_run._detect_publisher_changes(mock_metadata, csv_publisher)
        
        assert has_changes is True
        assert "name" in description.lower()
    
    def test_publisher_change_detection_identifier_change(self, worker_dry_run, mock_metadata):
        """Test that identifier change is detected."""
        csv_publisher = {
            "name": "GFZ German Research Centre for Geosciences",
            "publisherIdentifier": "https://ror.org/different",  # Changed
            "publisherIdentifierScheme": "ROR",
            "schemeUri": "https://ror.org",
            "lang": "en"
        }
        
        has_changes, description = worker_dry_run._detect_publisher_changes(mock_metadata, csv_publisher)
        
        assert has_changes is True
    
    def test_publisher_change_detection_string_to_extended(self, worker_dry_run, mock_metadata_string_publisher):
        """Test upgrade from string publisher to extended format is detected as change."""
        csv_publisher = {
            "name": "Helmholtz Centre Potsdam",
            "publisherIdentifier": "https://ror.org/new-id",  # Adding identifier
            "publisherIdentifierScheme": "ROR",
            "schemeUri": "https://ror.org",
            "lang": "en"
        }
        
        has_changes, description = worker_dry_run._detect_publisher_changes(
            mock_metadata_string_publisher, csv_publisher
        )
        
        assert has_changes is True
    
    def test_update_run_skips_unchanged_dois(self, valid_csv_file):
        """Test that DOIs with no publisher changes are skipped."""
        with patch('src.workers.publisher_update_worker.QSettings') as mock_settings:
            settings_instance = Mock()
            settings_instance.value.return_value = False
            mock_settings.return_value = settings_instance
            
            worker = PublisherUpdateWorker(
                username="test_user",
                password="test_pass",
                csv_path=valid_csv_file,
                use_test_api=True,
                dry_run_only=False
            )
        
        # Mock metadata that exactly matches CSV data
        mock_client = Mock()
        def get_metadata_side_effect(doi):
            if doi == "10.5880/GFZ.1.1.2021.001":
                return {
                    "data": {
                        "attributes": {
                            "publisher": {
                                "name": "GFZ German Research Centre for Geosciences",
                                "publisherIdentifier": "https://ror.org/04z8jg394",
                                "publisherIdentifierScheme": "ROR",
                                "schemeUri": "https://ror.org",
                                "lang": "en"
                            }
                        }
                    }
                }
            else:
                return {
                    "data": {
                        "attributes": {
                            "publisher": "Helmholtz Centre Potsdam"  # Matches CSV
                        }
                    }
                }
        
        mock_client.get_doi_metadata.side_effect = get_metadata_side_effect
        mock_client.update_doi_publisher.return_value = True
        
        finished_results = []
        worker.finished.connect(
            lambda s, e, sk, el, sd: finished_results.append((s, e, sk, el, sd))
        )
        
        with patch('src.workers.publisher_update_worker.DataCiteClient', return_value=mock_client):
            with patch('src.workers.publisher_update_worker.load_db_credentials', return_value=None):
                worker.run()
        
        assert len(finished_results) == 1
        success_count, error_count, skipped_count, error_list, skipped_details = finished_results[0]
        # Both DOIs should be skipped as they match
        assert skipped_count == 2
        assert success_count == 0
        # update_doi_publisher should not have been called
        mock_client.update_doi_publisher.assert_not_called()


class TestPublisherUpdateWorkerSkippedDetails:
    """Tests for skipped_details tracking in PublisherUpdateWorker."""
    
    @pytest.fixture
    def csv_with_two_dois(self):
        """Create CSV with two DOIs."""
        with tempfile.NamedTemporaryFile(
            mode='w', suffix='.csv', delete=False, encoding='utf-8', newline=''
        ) as f:
            f.write("DOI,Publisher Name,Publisher Identifier,Publisher Identifier Scheme,Scheme URI,Language\n")
            f.write('10.5880/GFZ.1,Old Publisher,,,,\n')
            f.write('10.5880/GFZ.2,New Publisher,https://ror.org/test,ROR,https://ror.org,en\n')
            csv_path = f.name
        yield csv_path
        if os.path.exists(csv_path):
            os.unlink(csv_path)
    
    def test_skipped_details_mixed_scenario(self, csv_with_two_dois):
        """Test that skipped_details contains reason for each skipped DOI."""
        with patch('src.workers.publisher_update_worker.QSettings') as mock_settings:
            settings_instance = Mock()
            settings_instance.value.return_value = False
            mock_settings.return_value = settings_instance
            
            worker = PublisherUpdateWorker(
                username="test_user",
                password="test_pass",
                csv_path=csv_with_two_dois,
                use_test_api=True,
                dry_run_only=False
            )
        
        # First DOI unchanged, second changed
        mock_client = Mock()
        def get_metadata_side_effect(doi):
            if doi == "10.5880/GFZ.1":
                return {"data": {"attributes": {"publisher": "Old Publisher"}}}  # No change
            else:
                return {"data": {"attributes": {"publisher": "Different Publisher"}}}  # Changed
        
        mock_client.get_doi_metadata.side_effect = get_metadata_side_effect
        # Return tuple (success, message) as the real method does
        mock_client.update_doi_publisher.return_value = (True, "Successfully updated")
        
        finished_results = []
        worker.finished.connect(
            lambda s, e, sk, el, sd: finished_results.append((s, e, sk, el, sd))
        )
        
        with patch('src.workers.publisher_update_worker.DataCiteClient', return_value=mock_client):
            with patch('src.workers.publisher_update_worker.load_db_credentials', return_value=None):
                worker.run()
        
        assert len(finished_results) == 1
        success_count, error_count, skipped_count, error_list, skipped_details = finished_results[0]
        
        assert skipped_count == 1
        assert success_count == 1
        assert len(skipped_details) == 1
        
        # Check that skipped details contains DOI and reason
        skipped_doi, reason = skipped_details[0]
        assert skipped_doi == "10.5880/GFZ.1"
        assert "keine änderungen" in reason.lower()
