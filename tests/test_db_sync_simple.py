"""Simplified integration tests for Database-First pattern.

Tests the critical scenarios for database synchronization with proper CSV format.
"""

import pytest
from unittest.mock import Mock, patch
from pathlib import Path

from src.workers.authors_update_worker import AuthorsUpdateWorker
from src.db import DatabaseError


# Valid CSV header with all required fields
CSV_HEADER = "DOI,Creator Name,Name Type,Given Name,Family Name,Name Identifier,Name Identifier Scheme,Scheme URI\n"


@pytest.fixture
def valid_csv_file(tmp_path):
    """Create a valid CSV file with all required headers."""
    csv_file = tmp_path / "test.csv"
    csv_file.write_text(
        CSV_HEADER +
        "10.5880/test.001,John Doe,Personal,John,Doe,0000-0001-2345-6789,ORCID,https://orcid.org/\n"
    )
    return str(csv_file)


@pytest.fixture
def mock_qsettings():
    """Mock QSettings."""
    with patch('src.workers.authors_update_worker.QSettings') as mock:
        settings = Mock()
        settings.value.return_value = True  # DB enabled by default
        mock.return_value = settings
        yield settings


@pytest.fixture
def mock_datacite_client():
    """Mock DataCite client."""
    client = Mock()
    client.fetch_all_dois.return_value = []
    client.get_doi_metadata.return_value = {
        'data': {
            'attributes': {
                'titles': [{'title': 'Test'}],
                'publisher': 'GFZ',
                'publicationYear': 2023
            }
        }
    }
    client.validate_creators_match.return_value = (True, "Valid")
    client.update_doi_creators.return_value = (True, "Success")
    return client


@pytest.fixture
def mock_db_client():
    """Mock Database client."""
    client = Mock()
    client.test_connection.return_value = (True, "Connected")
    client.get_resource_id_for_doi.return_value = 1429
    client.update_creators_transactional.return_value = (True, "DB updated", [])
    return client


class TestDatabaseFirstCore:
    """Core Database-First pattern tests."""
    
    def test_both_systems_success(
        self,
        qtbot,
        valid_csv_file,
        mock_qsettings,
        mock_datacite_client,
        mock_db_client
    ):
        """Test successful update in both systems."""
        worker = AuthorsUpdateWorker("user", "pass", valid_csv_file, True, False)
        
        # Track signals
        db_signals = []
        dc_signals = []
        doi_updates = []
        
        worker.database_update.connect(lambda msg: db_signals.append(msg))
        worker.datacite_update.connect(lambda msg: dc_signals.append(msg))
        worker.doi_updated.connect(lambda doi, success, msg: doi_updates.append((doi, success, msg)))
        
        with patch('src.workers.authors_update_worker.DataCiteClient', return_value=mock_datacite_client), \
             patch('src.workers.authors_update_worker.SumarioPMDClient', return_value=mock_db_client), \
             patch('src.workers.authors_update_worker.load_db_credentials', return_value={'host': 'h', 'database': 'd', 'username': 'u', 'password': 'p'}):
            worker.run()
        
        # Verify Database was called BEFORE DataCite
        mock_db_client.update_creators_transactional.assert_called_once()
        mock_datacite_client.update_doi_creators.assert_called_once()
        
        # Verify signals
        assert any("Datenbank wird aktualisiert" in s for s in db_signals)
        assert any("Datenbank erfolgreich" in s for s in db_signals)
        assert any("DataCite wird aktualisiert" in s for s in dc_signals)
        assert any("DataCite erfolgreich" in s for s in dc_signals)
        
        # Verify success
        assert len(doi_updates) == 1
        assert doi_updates[0][1] is True
        assert "Beide Systeme erfolgreich" in doi_updates[0][2]
    
    def test_database_failure_skips_datacite(
        self,
        qtbot,
        valid_csv_file,
        mock_qsettings,
        mock_datacite_client,
        mock_db_client
    ):
        """Test Database failure with ROLLBACK skips DataCite."""
        # DB update fails
        mock_db_client.update_creators_transactional.return_value = (
            False,
            "ROLLBACK",
            ["Error"]
        )
        
        worker = AuthorsUpdateWorker("user", "pass", valid_csv_file, True, False)
        
        db_signals = []
        doi_updates = []
        
        worker.database_update.connect(lambda msg: db_signals.append(msg))
        worker.doi_updated.connect(lambda doi, success, msg: doi_updates.append((doi, success, msg)))
        
        with patch('src.workers.authors_update_worker.DataCiteClient', return_value=mock_datacite_client), \
             patch('src.workers.authors_update_worker.SumarioPMDClient', return_value=mock_db_client), \
             patch('src.workers.authors_update_worker.load_db_credentials', return_value={'host': 'h', 'database': 'd', 'username': 'u', 'password': 'p'}):
            worker.run()
        
        # DB was called, but DataCite was NOT
        mock_db_client.update_creators_transactional.assert_called_once()
        mock_datacite_client.update_doi_creators.assert_not_called()
        
        # Verify ROLLBACK signal
        assert any("ROLLBACK" in s for s in db_signals)
        
        # Verify failure
        assert len(doi_updates) == 1
        assert doi_updates[0][1] is False
        assert "Datenbank-Update fehlgeschlagen" in doi_updates[0][2]
    
    def test_datacite_failure_with_successful_retry(
        self,
        qtbot,
        valid_csv_file,
        mock_qsettings,
        mock_datacite_client,
        mock_db_client
    ):
        """Test DataCite failure after DB success triggers retry."""
        # First DataCite call fails, retry succeeds
        mock_datacite_client.update_doi_creators.side_effect = [
            (False, "Timeout"),  # First attempt
            (True, "Success")    # Retry
        ]
        
        worker = AuthorsUpdateWorker("user", "pass", valid_csv_file, True, False)
        
        dc_signals = []
        doi_updates = []
        
        worker.datacite_update.connect(lambda msg: dc_signals.append(msg))
        worker.doi_updated.connect(lambda doi, success, msg: doi_updates.append((doi, success, msg)))
        
        with patch('src.workers.authors_update_worker.DataCiteClient', return_value=mock_datacite_client), \
             patch('src.workers.authors_update_worker.SumarioPMDClient', return_value=mock_db_client), \
             patch('src.workers.authors_update_worker.load_db_credentials', return_value={'host': 'h', 'database': 'd', 'username': 'u', 'password': 'p'}):
            worker.run()
        
        # DataCite was called twice
        assert mock_datacite_client.update_doi_creators.call_count == 2
        
        # Verify retry signal
        assert any("Retry wird versucht" in s for s in dc_signals)
        assert any("nach Retry" in s for s in dc_signals)
        
        # Final success
        assert len(doi_updates) == 1
        assert doi_updates[0][1] is True
        assert "nach Retry" in doi_updates[0][2]
    
    def test_datacite_failure_retry_fails_inconsistency(
        self,
        qtbot,
        valid_csv_file,
        mock_qsettings,
        mock_datacite_client,
        mock_db_client
    ):
        """Test critical inconsistency when both DataCite attempts fail."""
        # Both DataCite calls fail
        mock_datacite_client.update_doi_creators.return_value = (False, "Error")
        
        worker = AuthorsUpdateWorker("user", "pass", valid_csv_file, True, False)
        
        dc_signals = []
        doi_updates = []
        
        worker.datacite_update.connect(lambda msg: dc_signals.append(msg))
        worker.doi_updated.connect(lambda doi, success, msg: doi_updates.append((doi, success, msg)))
        
        with patch('src.workers.authors_update_worker.DataCiteClient', return_value=mock_datacite_client), \
             patch('src.workers.authors_update_worker.SumarioPMDClient', return_value=mock_db_client), \
             patch('src.workers.authors_update_worker.load_db_credentials', return_value={'host': 'h', 'database': 'd', 'username': 'u', 'password': 'p'}):
            worker.run()
        
        # Both attempts made
        assert mock_datacite_client.update_doi_creators.call_count == 2
        
        # Verify critical error
        assert any("fehlgeschlagen (auch nach Retry)" in s for s in dc_signals)
        
        # Inconsistency logged
        assert len(doi_updates) == 1
        assert doi_updates[0][1] is False
        assert "INKONSISTENZ" in doi_updates[0][2]
        assert "Manuelle Korrektur erforderlich" in doi_updates[0][2]
    
    def test_doi_not_in_database_warning(
        self,
        qtbot,
        valid_csv_file,
        mock_qsettings,
        mock_datacite_client,
        mock_db_client
    ):
        """Test DOI not found in DB gives warning but updates DataCite."""
        # DOI not in DB
        mock_db_client.get_resource_id_for_doi.return_value = None
        
        worker = AuthorsUpdateWorker("user", "pass", valid_csv_file, True, False)
        
        db_signals = []
        doi_updates = []
        
        worker.database_update.connect(lambda msg: db_signals.append(msg))
        worker.doi_updated.connect(lambda doi, success, msg: doi_updates.append((doi, success, msg)))
        
        with patch('src.workers.authors_update_worker.DataCiteClient', return_value=mock_datacite_client), \
             patch('src.workers.authors_update_worker.SumarioPMDClient', return_value=mock_db_client), \
             patch('src.workers.authors_update_worker.load_db_credentials', return_value={'host': 'h', 'database': 'd', 'username': 'u', 'password': 'p'}):
            worker.run()
        
        # DB update not called
        mock_db_client.update_creators_transactional.assert_not_called()
        
        # DataCite still updated
        mock_datacite_client.update_doi_creators.assert_called_once()
        
        # Warning signal
        assert any("nicht in Datenbank gefunden" in s for s in db_signals)
        
        # Success with warning
        assert len(doi_updates) == 1
        assert doi_updates[0][1] is True
        assert "nur DataCite wird aktualisiert" in doi_updates[0][2]


class TestValidationPhase:
    """Test validation phase before updates."""
    
    def test_database_unavailable_aborts(
        self,
        qtbot,
        valid_csv_file,
        mock_qsettings,
        mock_datacite_client
    ):
        """Test validation aborts when DB enabled but unavailable."""
        worker = AuthorsUpdateWorker("user", "pass", valid_csv_file, True, False)
        
        error_signals = []
        worker.error_occurred.connect(lambda msg: error_signals.append(msg))
        
        # DB connection fails
        mock_db = Mock()
        mock_db.test_connection.return_value = (False, "Connection refused")
        
        with patch('src.workers.authors_update_worker.DataCiteClient', return_value=mock_datacite_client), \
             patch('src.workers.authors_update_worker.SumarioPMDClient', return_value=mock_db), \
             patch('src.workers.authors_update_worker.load_db_credentials', return_value={'host': 'h', 'database': 'd', 'username': 'u', 'password': 'p'}):
            worker.run()
        
        # Error occurred
        assert len(error_signals) == 1
        assert "Datenbank" in error_signals[0] and "nicht erreichbar" in error_signals[0]
    
    def test_database_disabled_datacite_only(
        self,
        qtbot,
        valid_csv_file,
        mock_qsettings,
        mock_datacite_client
    ):
        """Test DataCite-only when DB disabled."""
        mock_qsettings.value.return_value = False  # DB disabled
        
        worker = AuthorsUpdateWorker("user", "pass", valid_csv_file, True, False)
        
        db_signals = []
        dc_signals = []
        doi_updates = []
        
        worker.database_update.connect(lambda msg: db_signals.append(msg))
        worker.datacite_update.connect(lambda msg: dc_signals.append(msg))
        worker.doi_updated.connect(lambda doi, success, msg: doi_updates.append((doi, success, msg)))
        
        with patch('src.workers.authors_update_worker.DataCiteClient', return_value=mock_datacite_client):
            worker.run()
        
        # No DB signals
        assert len(db_signals) == 0
        
        # DataCite signals
        assert any("DataCite wird aktualisiert" in s for s in dc_signals)
        
        # Success
        assert len(doi_updates) == 1
        assert doi_updates[0][1] is True
        assert "Datenbank deaktiviert" in doi_updates[0][2]
