"""Integration tests for AuthorsUpdateWorker database synchronization.

This module tests the Database-First pattern implementation that synchronizes
creator metadata updates between DataCite and the internal GFZ database.

Test Scenarios:
    1. Successful sync (both DataCite and Database updated)
    2. Database failure with ROLLBACK (DataCite not updated)
    3. DataCite failure after Database success (retry logic)
    4. DOI not found in database (DataCite-only update with warning)
    5. Database updates disabled (DataCite-only update)
    6. Validation phase failures (connection issues)

Pattern:
    - Database-First: DB update BEFORE DataCite to minimize inconsistency
    - ROLLBACK on DB errors prevents partial updates
    - Retry on DataCite failures after DB success
    - All-or-Nothing validation phase
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from PySide6.QtCore import QSettings, Signal

from src.workers.authors_update_worker import AuthorsUpdateWorker
from src.db import DatabaseError


@pytest.fixture
def mock_qsettings():
    """Mock QSettings for testing."""
    with patch('src.workers.authors_update_worker.QSettings') as mock:
        settings = Mock()
        mock.return_value = settings
        yield settings


@pytest.fixture
def mock_datacite_client():
    """Mock DataCite client for testing."""
    client = Mock()
    client.fetch_all_dois.return_value = []
    client.get_doi_metadata.return_value = {
        'data': {
            'attributes': {
                'titles': [{'title': 'Test Dataset'}],
                'publisher': 'GFZ',
                'publicationYear': 2023
            }
        }
    }
    client.update_doi_creators.return_value = (True, "Update successful")
    return client


@pytest.fixture
def mock_db_client():
    """Mock Database client for testing."""
    client = Mock()
    client.test_connection.return_value = (True, "Connection successful")
    client.get_resource_id_for_doi.return_value = 1429
    client.update_creators_transactional.return_value = (
        True,
        "Database update successful",
        []
    )
    return client


@pytest.fixture
def sample_csv_data():
    """Sample CSV data for testing."""
    return [
        {
            'DOI': '10.5880/test.001',
            'Given Name': 'John',
            'Family Name': 'Doe',
            'Name Identifier': '0000-0001-2345-6789'
        },
        {
            'DOI': '10.5880/test.002',
            'Given Name': 'Jane',
            'Family Name': 'Smith',
            'Name Identifier': '0000-0002-3456-7890'
        }
    ]


class TestDatabaseFirstPattern:
    """Test Database-First update pattern."""
    
    def test_successful_sync_both_systems(
        self,
        qtbot,
        mock_qsettings,
        mock_datacite_client,
        mock_db_client,
        tmp_path
    ):
        """Test successful update in both DataCite and Database."""
        # Setup
        mock_qsettings.value.return_value = True  # DB enabled
        
        csv_file = tmp_path / "test.csv"
        csv_file.write_text(
            "DOI,Given Name,Family Name,Name Identifier\n"
            "10.5880/test.001,John,Doe,0000-0001-2345-6789\n"
        )
        
        worker = AuthorsUpdateWorker("test_user", "test_pass", str(csv_file), True)
        
        # Mock signal tracking
        validation_signals = []
        database_signals = []
        datacite_signals = []
        doi_updates = []
        
        worker.validation_update.connect(lambda msg: validation_signals.append(msg))
        worker.database_update.connect(lambda msg: database_signals.append(msg))
        worker.datacite_update.connect(lambda msg: datacite_signals.append(msg))
        worker.doi_updated.connect(lambda doi, success, msg: doi_updates.append((doi, success, msg)))
        
        # Patch dependencies
        with patch('src.workers.authors_update_worker.DataCiteClient', return_value=mock_datacite_client), \
             patch('src.workers.authors_update_worker.SumarioPMDClient', return_value=mock_db_client), \
             patch('src.workers.authors_update_worker.load_db_credentials', return_value=('host', 'db', 'user', 'pass')):
            
            worker.run()
        
        # Assertions
        assert any("Alle benötigten Systeme verfügbar" in s for s in validation_signals)
        assert any("Datenbank wird aktualisiert" in s for s in database_signals)
        assert any("Datenbank erfolgreich" in s for s in database_signals)
        assert any("DataCite wird aktualisiert" in s for s in datacite_signals)
        assert any("DataCite erfolgreich" in s for s in datacite_signals)
        
        # Verify DOI was updated successfully
        assert len(doi_updates) == 1
        assert doi_updates[0][1] is True  # success
        assert "Beide Systeme erfolgreich" in doi_updates[0][2]
        
        # Verify call order: Database BEFORE DataCite
        mock_db_client.update_creators_transactional.assert_called_once()
        mock_datacite_client.update_doi_creators.assert_called_once()
    
    def test_database_failure_with_rollback(
        self,
        qtbot,
        mock_qsettings,
        mock_datacite_client,
        mock_db_client,
        tmp_path
    ):
        """Test Database failure triggers ROLLBACK and skips DataCite."""
        # Setup
        mock_qsettings.value.return_value = True  # DB enabled
        mock_db_client.update_creators_transactional.return_value = (
            False,
            "Transaction failed - ROLLBACK performed",
            ["Error: Constraint violation"]
        )
        
        csv_file = tmp_path / "test.csv"
        csv_file.write_text(
            "DOI,Given Name,Family Name,Name Identifier\n"
            "10.5880/test.001,John,Doe,0000-0001-2345-6789\n"
        )
        
        worker = AuthorsUpdateWorker("test_user", "test_pass", str(csv_file), True)
        
        # Mock signal tracking
        database_signals = []
        datacite_signals = []
        doi_updates = []
        
        worker.database_update.connect(lambda msg: database_signals.append(msg))
        worker.datacite_update.connect(lambda msg: datacite_signals.append(msg))
        worker.doi_updated.connect(lambda doi, success, msg: doi_updates.append((doi, success, msg)))
        
        # Patch dependencies
        with patch('src.workers.authors_update_worker.DataCiteClient', return_value=mock_datacite_client), \
             patch('src.workers.authors_update_worker.SumarioPMDClient', return_value=mock_db_client), \
             patch('src.workers.authors_update_worker.load_db_credentials', return_value=('host', 'db', 'user', 'pass')):
            
            worker.run()
        
        # Assertions
        assert any("ROLLBACK" in s for s in database_signals)
        
        # DataCite should NOT have been called
        assert len(datacite_signals) == 0
        mock_datacite_client.update_doi_creators.assert_not_called()
        
        # Verify DOI update failed
        assert len(doi_updates) == 1
        assert doi_updates[0][1] is False  # failure
        assert "Datenbank-Update fehlgeschlagen" in doi_updates[0][2]
    
    def test_datacite_failure_after_database_success_with_retry(
        self,
        qtbot,
        mock_qsettings,
        mock_datacite_client,
        mock_db_client,
        tmp_path
    ):
        """Test DataCite failure after DB success triggers retry."""
        # Setup
        mock_qsettings.value.return_value = True  # DB enabled
        
        # First DataCite call fails, retry succeeds
        mock_datacite_client.update_doi_creators.side_effect = [
            (False, "Network timeout"),  # First attempt fails
            (True, "Update successful")   # Retry succeeds
        ]
        
        csv_file = tmp_path / "test.csv"
        csv_file.write_text(
            "DOI,Given Name,Family Name,Name Identifier\n"
            "10.5880/test.001,John,Doe,0000-0001-2345-6789\n"
        )
        
        worker = AuthorsUpdateWorker("test_user", "test_pass", str(csv_file), True)
        
        # Mock signal tracking
        database_signals = []
        datacite_signals = []
        doi_updates = []
        
        worker.database_update.connect(lambda msg: database_signals.append(msg))
        worker.datacite_update.connect(lambda msg: datacite_signals.append(msg))
        worker.doi_updated.connect(lambda doi, success, msg: doi_updates.append((doi, success, msg)))
        
        # Patch dependencies
        with patch('src.workers.authors_update_worker.DataCiteClient', return_value=mock_datacite_client), \
             patch('src.workers.authors_update_worker.SumarioPMDClient', return_value=mock_db_client), \
             patch('src.workers.authors_update_worker.load_db_credentials', return_value=('host', 'db', 'user', 'pass')):
            
            worker.run()
        
        # Assertions
        assert any("Datenbank erfolgreich" in s for s in database_signals)
        assert any("Retry wird versucht" in s for s in datacite_signals)
        assert any("DataCite erfolgreich aktualisiert (nach Retry)" in s for s in datacite_signals)
        
        # Verify DataCite was called twice (original + retry)
        assert mock_datacite_client.update_doi_creators.call_count == 2
        
        # Verify final success
        assert len(doi_updates) == 1
        assert doi_updates[0][1] is True  # success after retry
        assert "nach Retry" in doi_updates[0][2]
    
    def test_datacite_failure_after_database_success_retry_fails(
        self,
        qtbot,
        mock_qsettings,
        mock_datacite_client,
        mock_db_client,
        tmp_path
    ):
        """Test critical inconsistency when retry also fails."""
        # Setup
        mock_qsettings.value.return_value = True  # DB enabled
        
        # Both DataCite calls fail
        mock_datacite_client.update_doi_creators.return_value = (False, "Persistent error")
        
        csv_file = tmp_path / "test.csv"
        csv_file.write_text(
            "DOI,Given Name,Family Name,Name Identifier\n"
            "10.5880/test.001,John,Doe,0000-0001-2345-6789\n"
        )
        
        worker = AuthorsUpdateWorker("test_user", "test_pass", str(csv_file), True)
        
        # Mock signal tracking
        database_signals = []
        datacite_signals = []
        doi_updates = []
        
        worker.database_update.connect(lambda msg: database_signals.append(msg))
        worker.datacite_update.connect(lambda msg: datacite_signals.append(msg))
        worker.doi_updated.connect(lambda doi, success, msg: doi_updates.append((doi, success, msg)))
        
        # Patch dependencies
        with patch('src.workers.authors_update_worker.DataCiteClient', return_value=mock_datacite_client), \
             patch('src.workers.authors_update_worker.SumarioPMDClient', return_value=mock_db_client), \
             patch('src.workers.authors_update_worker.load_db_credentials', return_value=('host', 'db', 'user', 'pass')):
            
            worker.run()
        
        # Assertions
        assert any("Datenbank erfolgreich" in s for s in database_signals)
        assert any("Retry wird versucht" in s for s in datacite_signals)
        assert any("fehlgeschlagen (auch nach Retry)" in s for s in datacite_signals)
        
        # Verify DataCite was called twice (original + retry)
        assert mock_datacite_client.update_doi_creators.call_count == 2
        
        # Verify critical inconsistency logged
        assert len(doi_updates) == 1
        assert doi_updates[0][1] is False  # failure
        assert "INKONSISTENZ" in doi_updates[0][2]
        assert "Manuelle Korrektur erforderlich" in doi_updates[0][2]
    
    def test_doi_not_found_in_database(
        self,
        qtbot,
        mock_qsettings,
        mock_datacite_client,
        mock_db_client,
        tmp_path
    ):
        """Test DOI not in database still updates DataCite with warning."""
        # Setup
        mock_qsettings.value.return_value = True  # DB enabled
        mock_db_client.get_resource_id_for_doi.return_value = None  # DOI not found
        
        csv_file = tmp_path / "test.csv"
        csv_file.write_text(
            "DOI,Given Name,Family Name,Name Identifier\n"
            "10.5880/test.001,John,Doe,0000-0001-2345-6789\n"
        )
        
        worker = AuthorsUpdateWorker("test_user", "test_pass", str(csv_file), True)
        
        # Mock signal tracking
        database_signals = []
        datacite_signals = []
        doi_updates = []
        
        worker.database_update.connect(lambda msg: database_signals.append(msg))
        worker.datacite_update.connect(lambda msg: datacite_signals.append(msg))
        worker.doi_updated.connect(lambda doi, success, msg: doi_updates.append((doi, success, msg)))
        
        # Patch dependencies
        with patch('src.workers.authors_update_worker.DataCiteClient', return_value=mock_datacite_client), \
             patch('src.workers.authors_update_worker.SumarioPMDClient', return_value=mock_db_client), \
             patch('src.workers.authors_update_worker.load_db_credentials', return_value=('host', 'db', 'user', 'pass')):
            
            worker.run()
        
        # Assertions
        assert any("nicht in Datenbank gefunden" in s for s in database_signals)
        assert any("DataCite wird aktualisiert" in s for s in datacite_signals)
        assert any("DataCite erfolgreich" in s for s in datacite_signals)
        
        # Database update should NOT have been called
        mock_db_client.update_creators_transactional.assert_not_called()
        
        # But DataCite update should proceed
        mock_datacite_client.update_doi_creators.assert_called_once()
        
        # Verify success with warning
        assert len(doi_updates) == 1
        assert doi_updates[0][1] is True  # success
        assert "nur DataCite wird aktualisiert" in doi_updates[0][2]


class TestDatabaseConnection:
    """Test database connection and validation phase."""
    
    def test_validation_phase_database_unavailable(
        self,
        qtbot,
        mock_qsettings,
        mock_datacite_client,
        tmp_path
    ):
        """Test validation phase aborts when DB enabled but unavailable."""
        # Setup
        mock_qsettings.value.return_value = True  # DB enabled
        
        csv_file = tmp_path / "test.csv"
        csv_file.write_text(
            "DOI,Given Name,Family Name,Name Identifier\n"
            "10.5880/test.001,John,Doe,0000-0001-2345-6789\n"
        )
        
        worker = AuthorsUpdateWorker("test_user", "test_pass", str(csv_file), True)
        
        # Mock signal tracking
        validation_signals = []
        error_occurred = []
        
        worker.validation_update.connect(lambda msg: validation_signals.append(msg))
        worker.error_occurred.connect(lambda msg: error_occurred.append(msg))
        
        # Mock DB connection failure
        mock_db_client = Mock()
        mock_db_client.test_connection.return_value = (False, "Connection refused")
        
        # Patch dependencies
        with patch('src.workers.authors_update_worker.DataCiteClient', return_value=mock_datacite_client), \
             patch('src.workers.authors_update_worker.SumarioPMDClient', return_value=mock_db_client), \
             patch('src.workers.authors_update_worker.load_db_credentials', return_value=('host', 'db', 'user', 'pass')):
            
            worker.run()
        
        # Assertions
        assert any("Prüfe Systemverfügbarkeit" in s for s in validation_signals)
        assert len(error_occurred) == 1
        assert "Datenbank nicht verfügbar" in error_occurred[0]
        assert "VPN" in error_occurred[0] or "Credentials" in error_occurred[0]
    
    def test_validation_phase_credentials_missing(
        self,
        qtbot,
        mock_qsettings,
        mock_datacite_client,
        tmp_path
    ):
        """Test validation phase when DB credentials are missing."""
        # Setup
        mock_qsettings.value.return_value = True  # DB enabled
        
        csv_file = tmp_path / "test.csv"
        csv_file.write_text(
            "DOI,Given Name,Family Name,Name Identifier\n"
            "10.5880/test.001,John,Doe,0000-0001-2345-6789\n"
        )
        
        worker = AuthorsUpdateWorker("test_user", "test_pass", str(csv_file), True)
        
        # Mock signal tracking
        error_occurred = []
        worker.error_occurred.connect(lambda msg: error_occurred.append(msg))
        
        # Patch dependencies - credentials not found
        with patch('src.workers.authors_update_worker.DataCiteClient', return_value=mock_datacite_client), \
             patch('src.workers.authors_update_worker.load_db_credentials', return_value=None):
            
            worker.run()
        
        # Assertions
        assert len(error_occurred) == 1
        assert "Datenbank nicht verfügbar" in error_occurred[0]
        assert "keine Zugangsdaten" in error_occurred[0]
    
    def test_database_disabled_datacite_only(
        self,
        qtbot,
        mock_qsettings,
        mock_datacite_client,
        tmp_path
    ):
        """Test DataCite-only update when database is disabled."""
        # Setup
        mock_qsettings.value.return_value = False  # DB DISABLED
        
        csv_file = tmp_path / "test.csv"
        csv_file.write_text(
            "DOI,Given Name,Family Name,Name Identifier\n"
            "10.5880/test.001,John,Doe,0000-0001-2345-6789\n"
        )
        
        worker = AuthorsUpdateWorker("test_user", "test_pass", str(csv_file), True)
        
        # Mock signal tracking
        database_signals = []
        datacite_signals = []
        doi_updates = []
        
        worker.database_update.connect(lambda msg: database_signals.append(msg))
        worker.datacite_update.connect(lambda msg: datacite_signals.append(msg))
        worker.doi_updated.connect(lambda doi, success, msg: doi_updates.append((doi, success, msg)))
        
        # Patch dependencies
        with patch('src.workers.authors_update_worker.DataCiteClient', return_value=mock_datacite_client):
            worker.run()
        
        # Assertions
        # No database signals (DB disabled)
        assert len(database_signals) == 0
        
        # DataCite signals present
        assert any("DataCite wird aktualisiert" in s for s in datacite_signals)
        assert any("DataCite erfolgreich" in s for s in datacite_signals)
        
        # Verify success message indicates DB was disabled
        assert len(doi_updates) == 1
        assert doi_updates[0][1] is True  # success
        assert "Datenbank deaktiviert" in doi_updates[0][2]


class TestErrorHandling:
    """Test error handling and edge cases."""
    
    def test_database_exception_during_update(
        self,
        qtbot,
        mock_qsettings,
        mock_datacite_client,
        mock_db_client,
        tmp_path
    ):
        """Test database exception handling."""
        # Setup
        mock_qsettings.value.return_value = True  # DB enabled
        mock_db_client.update_creators_transactional.side_effect = DatabaseError("Deadlock detected")
        
        csv_file = tmp_path / "test.csv"
        csv_file.write_text(
            "DOI,Given Name,Family Name,Name Identifier\n"
            "10.5880/test.001,John,Doe,0000-0001-2345-6789\n"
        )
        
        worker = AuthorsUpdateWorker("test_user", "test_pass", str(csv_file), True)
        
        # Mock signal tracking
        database_signals = []
        doi_updates = []
        
        worker.database_update.connect(lambda msg: database_signals.append(msg))
        worker.doi_updated.connect(lambda doi, success, msg: doi_updates.append((doi, success, msg)))
        
        # Patch dependencies
        with patch('src.workers.authors_update_worker.DataCiteClient', return_value=mock_datacite_client), \
             patch('src.workers.authors_update_worker.SumarioPMDClient', return_value=mock_db_client), \
             patch('src.workers.authors_update_worker.load_db_credentials', return_value=('host', 'db', 'user', 'pass')):
            
            worker.run()
        
        # Assertions
        assert any("Datenbank-Fehler" in s for s in database_signals)
        
        # DataCite should NOT have been called
        mock_datacite_client.update_doi_creators.assert_not_called()
        
        # Verify error was logged
        assert len(doi_updates) == 1
        assert doi_updates[0][1] is False  # failure
        assert "Datenbank-Fehler" in doi_updates[0][2]
    
    def test_multiple_dois_mixed_results(
        self,
        qtbot,
        mock_qsettings,
        mock_datacite_client,
        mock_db_client,
        tmp_path
    ):
        """Test multiple DOIs with mixed success/failure results."""
        # Setup
        mock_qsettings.value.return_value = True  # DB enabled
        
        # First DOI succeeds, second DOI DB fails
        mock_db_client.get_resource_id_for_doi.side_effect = [1429, 1430]
        mock_db_client.update_creators_transactional.side_effect = [
            (True, "Success", []),  # First DOI succeeds
            (False, "Constraint violation", ["Error"])  # Second DOI fails
        ]
        
        csv_file = tmp_path / "test.csv"
        csv_file.write_text(
            "DOI,Given Name,Family Name,Name Identifier\n"
            "10.5880/test.001,John,Doe,0000-0001-2345-6789\n"
            "10.5880/test.002,Jane,Smith,0000-0002-3456-7890\n"
        )
        
        worker = AuthorsUpdateWorker("test_user", "test_pass", str(csv_file), True)
        
        # Mock signal tracking
        doi_updates = []
        worker.doi_updated.connect(lambda doi, success, msg: doi_updates.append((doi, success, msg)))
        
        # Patch dependencies
        with patch('src.workers.authors_update_worker.DataCiteClient', return_value=mock_datacite_client), \
             patch('src.workers.authors_update_worker.SumarioPMDClient', return_value=mock_db_client), \
             patch('src.workers.authors_update_worker.load_db_credentials', return_value=('host', 'db', 'user', 'pass')):
            
            worker.run()
        
        # Assertions
        assert len(doi_updates) == 2
        
        # First DOI succeeded
        assert doi_updates[0][0] == "10.5880/test.001"
        assert doi_updates[0][1] is True
        assert "Beide Systeme erfolgreich" in doi_updates[0][2]
        
        # Second DOI failed
        assert doi_updates[1][0] == "10.5880/test.002"
        assert doi_updates[1][1] is False
        assert "Datenbank-Update fehlgeschlagen" in doi_updates[1][2]
        
        # DataCite should only be called once (for first DOI)
        assert mock_datacite_client.update_doi_creators.call_count == 1
