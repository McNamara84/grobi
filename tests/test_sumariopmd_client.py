"""
Unit tests for SumarioPMDClient.
"""

import pytest
from unittest.mock import Mock, patch
import pymysql

from src.db.sumariopmd_client import (
    SumarioPMDClient,
    ConnectionError
)


@pytest.fixture
def mock_pymysql_connect():
    """Mock PyMySQL connect function."""
    with patch('src.db.sumariopmd_client.pymysql.connect') as mock:
        yield mock


@pytest.fixture
def mock_connection():
    """Mock MySQL connection."""
    connection = Mock()
    connection.is_connected.return_value = True
    cursor = Mock()
    connection.cursor.return_value = cursor
    return connection, cursor


class TestClientInitialization:
    """Tests for SumarioPMDClient initialization."""
    
    def test_client_creation_success(self, mock_pymysql_connect):
        """Test successful client creation."""
        client = SumarioPMDClient(
            host="rz-mysql3.gfz-potsdam.de",
            database="sumario-pmd",
            username="test_user",
            password="test_password"
        )
        
        assert client.host == "rz-mysql3.gfz-potsdam.de"
        assert client.database == "sumario-pmd"
        assert client.username == "test_user"
    
    def test_host_suffix_added_automatically(self, mock_pymysql_connect):
        """Test that .gfz-potsdam.de suffix is added if missing."""
        client = SumarioPMDClient(
            host="rz-mysql3",
            database="sumario-pmd",
            username="test_user",
            password="test_password"
        )
        
        assert client.host == "rz-mysql3.gfz-potsdam.de"
    
    def test_localhost_not_modified(self, mock_pymysql_connect):
        """Test that localhost hostname is not modified."""
        client = SumarioPMDClient(
            host="localhost",
            database="test_db",
            username="test_user",
            password="test_password"
        )
        
        assert client.host == "localhost"
    
    def test_connection_failure_raises_error(self, mock_pymysql_connect):
        """Test that connection failure raises ConnectionError when actually connecting."""
        # PyMySQL connects on-demand, so client creation always succeeds
        # Connection errors happen when get_connection() is called
        mock_pymysql_connect.side_effect = pymysql.Error("Connection failed")
        
        # Client creation should succeed (no connection test in __init__)
        client = SumarioPMDClient(
            host="invalid-host",
            database="db",
            username="user",
            password="pass"
        )
        
        # But get_connection() should raise ConnectionError
        with pytest.raises(ConnectionError, match="Database connection failed"):
            with client.get_connection():
                pass


class TestConnectionManagement:
    """Tests for database connection management."""
    
    def test_get_connection_success(self, mock_pymysql_connect):
        """Test successful connection retrieval."""
        mock_connection = Mock()
        mock_pymysql_connect.return_value = mock_connection
        
        client = SumarioPMDClient("host", "db", "user", "pass")
        
        with client.get_connection() as conn:
            assert conn == mock_connection
        
        # Connection should be closed after context exit
        mock_connection.close.assert_called_once()
    
    def test_get_connection_failure(self, mock_pymysql_connect):
        """Test connection failure raises ConnectionError."""
        # Client creation succeeds (connection not tested in __init__)
        client = SumarioPMDClient("host", "db", "user", "pass")
        
        # Set side_effect AFTER client creation
        mock_pymysql_connect.side_effect = pymysql.Error("Connection refused")
        
        with pytest.raises(ConnectionError, match="Database connection failed"):
            with client.get_connection():
                pass
    
    def test_test_connection_success(self, mock_pymysql_connect):
        """Test successful connection test."""
        mock_connection = Mock()
        mock_cursor = Mock()
        # DictCursor returns dict, not tuple!
        mock_cursor.fetchone.return_value = {"VERSION()": "8.0.33"}
        mock_connection.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_connection.cursor.return_value.__exit__ = Mock(return_value=False)
        mock_pymysql_connect.return_value = mock_connection
        
        client = SumarioPMDClient("host", "db", "user", "pass")
        success, message = client.test_connection()
        
        assert success is True
        assert "8.0.33" in message
        assert "✓" in message
    
    def test_test_connection_failure(self, mock_pymysql_connect):
        """Test failed connection test."""
        # Client creation succeeds (connection not tested in __init__)
        client = SumarioPMDClient("host", "db", "user", "pass")
        
        # Set side_effect AFTER client creation
        mock_pymysql_connect.side_effect = pymysql.Error("Connection timeout")
        
        success, message = client.test_connection()
        
        assert success is False
        assert "✗" in message
        assert "failed" in message.lower()


class TestResourceLookup:
    """Tests for DOI to resource_id lookup."""
    
    def test_get_resource_id_found(self, mock_pymysql_connect):
        """Test successful resource_id lookup."""
        mock_connection = Mock()
        mock_cursor = Mock()
        # DictCursor returns dict, not tuple!
        mock_cursor.fetchone.return_value = {"id": 1429}
        mock_connection.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_connection.cursor.return_value.__exit__ = Mock(return_value=False)
        mock_pymysql_connect.return_value = mock_connection
        
        client = SumarioPMDClient("host", "db", "user", "pass")
        resource_id = client.get_resource_id_for_doi("10.5880/test.doi")
        
        assert resource_id == 1429
        mock_cursor.execute.assert_called_once()
        assert "SELECT id" in mock_cursor.execute.call_args[0][0]
    
    def test_get_resource_id_not_found(self, mock_pymysql_connect):
        """Test resource_id lookup returns None when not found."""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = None
        mock_connection.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_connection.cursor.return_value.__exit__ = Mock(return_value=False)
        mock_pymysql_connect.return_value = mock_connection
        
        client = SumarioPMDClient("host", "db", "user", "pass")
        resource_id = client.get_resource_id_for_doi("10.5880/nonexistent")
        
        assert resource_id is None
    
    def test_get_resource_id_database_error(self, mock_pymysql_connect):
        """Test database error raises DatabaseError for query failures."""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_cursor.execute.side_effect = pymysql.Error("Query failed")
        mock_connection.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_connection.cursor.return_value.__exit__ = Mock(return_value=False)
        mock_pymysql_connect.return_value = mock_connection
        
        client = SumarioPMDClient("host", "db", "user", "pass")
        
        # Import DatabaseError from the module
        from src.db.sumariopmd_client import DatabaseError
        
        with pytest.raises(DatabaseError, match="Failed to fetch resource_id"):
            client.get_resource_id_for_doi("10.5880/test.doi")


class TestFetchCreators:
    """Tests for fetching creators."""
    
    def test_fetch_creators_success(self, mock_pymysql_connect):
        """Test successful creator fetch."""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchall.return_value = [
            {
                'order': 1,
                'firstname': 'John',
                'lastname': 'Doe',
                'name': 'Doe, John',
                'orcid': '0000-0001-2345-6789',
                'identifiertype': 'ORCID',
                'nametype': 'Personal'
            },
            {
                'order': 2,
                'firstname': 'Jane',
                'lastname': 'Smith',
                'name': 'Smith, Jane',
                'orcid': '0000-0002-3456-7890',
                'identifiertype': 'ORCID',
                'nametype': 'Personal'
            }
        ]
        mock_connection.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_connection.cursor.return_value.__exit__ = Mock(return_value=False)
        mock_pymysql_connect.return_value = mock_connection
        
        client = SumarioPMDClient("host", "db", "user", "pass")
        creators = client.fetch_creators_for_resource(1429)
        
        assert len(creators) == 2
        assert creators[0]['firstname'] == 'John'
        assert creators[1]['lastname'] == 'Smith'
        
        # Verify query includes role='Creator' filter
        call_args = mock_cursor.execute.call_args[0][0]
        assert "role = 'Creator'" in call_args or 'r.role = \'Creator\'' in call_args
    
    def test_fetch_creators_empty_result(self, mock_pymysql_connect):
        """Test fetching creators returns empty list when none found."""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchall.return_value = []
        mock_connection.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_connection.cursor.return_value.__exit__ = Mock(return_value=False)
        mock_pymysql_connect.return_value = mock_connection
        
        client = SumarioPMDClient("host", "db", "user", "pass")
        creators = client.fetch_creators_for_resource(9999)
        
        assert creators == []
    
    def test_fetch_creators_database_error(self, mock_pymysql_connect):
        """Test database error raises DatabaseError for query failures."""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_cursor.execute.side_effect = pymysql.Error("Query failed")
        mock_connection.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_connection.cursor.return_value.__exit__ = Mock(return_value=False)
        mock_pymysql_connect.return_value = mock_connection
        
        client = SumarioPMDClient("host", "db", "user", "pass")
        
        # Import DatabaseError from the module
        from src.db.sumariopmd_client import DatabaseError
        
        with pytest.raises(DatabaseError, match="Failed to fetch creators"):
            client.fetch_creators_for_resource(1429)


class TestUpdateCreators:
    """Tests for transactional creator updates."""
    
    def test_update_creators_success(self, mock_pymysql_connect):
        """Test successful creator update with transaction."""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_cursor.rowcount = 2  # 2 rows deleted
        # DictCursor returns list of dicts, not tuples!
        mock_cursor.fetchall.return_value = [{"order": 1}, {"order": 2}]
        mock_connection.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_connection.cursor.return_value.__exit__ = Mock(return_value=False)
        mock_pymysql_connect.return_value = mock_connection
        
        client = SumarioPMDClient("host", "db", "user", "pass")
        
        creators = [
            {'firstname': 'John', 'lastname': 'Doe', 'orcid': '0000-0001-2345-6789'},
            {'firstname': 'Jane', 'lastname': 'Smith', 'orcid': '0000-0002-3456-7890'}
        ]
        
        success, message, errors = client.update_creators_transactional(1429, creators)
        
        assert success is True
        assert "Successfully updated" in message
        assert len(errors) == 0
        
        # Verify transaction was used (PyMySQL uses begin() instead of start_transaction())
        mock_connection.begin.assert_called_once()
        mock_connection.commit.assert_called_once()
        mock_connection.rollback.assert_not_called()
    
    def test_update_creators_orcid_url_normalization(self, mock_pymysql_connect):
        """Test that ORCID URLs are normalized to IDs."""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_cursor.rowcount = 1
        # DictCursor returns list of dicts!
        mock_cursor.fetchall.return_value = [{"order": 1}]
        mock_connection.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_connection.cursor.return_value.__exit__ = Mock(return_value=False)
        mock_pymysql_connect.return_value = mock_connection
        
        client = SumarioPMDClient("host", "db", "user", "pass")
        
        creators = [
            {
                'firstname': 'John',
                'lastname': 'Doe',
                'orcid': 'https://orcid.org/0000-0001-2345-6789'  # Full URL
            }
        ]
        
        success, message, errors = client.update_creators_transactional(1429, creators)
        
        assert success is True
        
        # Verify INSERT was called with normalized ORCID (ID only)
        insert_calls = [call for call in mock_cursor.execute.call_args_list 
                       if 'INSERT INTO resourceagent' in str(call)]
        assert len(insert_calls) > 0
        
        # Check that the ORCID value passed was the ID only (without URL)
        insert_values = insert_calls[0][0][1]
        orcid_value = insert_values[5]  # 6th parameter is ORCID
        assert orcid_value == '0000-0001-2345-6789'
        assert 'https://' not in orcid_value
    
    def test_update_creators_rollback_on_error(self, mock_pymysql_connect):
        """Test that transaction is rolled back on error."""
        # Client creation succeeds
        client = SumarioPMDClient("host", "db", "user", "pass")
        
        # Now setup the failing connection for update operation
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_cursor.execute.side_effect = pymysql.Error("Insert failed")
        mock_connection.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_connection.cursor.return_value.__exit__ = Mock(return_value=False)
        mock_pymysql_connect.return_value = mock_connection
        
        creators = [
            {'firstname': 'John', 'lastname': 'Doe', 'orcid': '0000-0001-2345-6789'}
        ]
        
        success, message, errors = client.update_creators_transactional(1429, creators)
        
        assert success is False
        assert "failed" in message.lower()
        assert len(errors) > 0
        
        # Verify rollback was called (PyMySQL uses begin() instead of start_transaction())
        mock_connection.begin.assert_called_once()
        mock_connection.rollback.assert_called_once()
        mock_connection.commit.assert_not_called()
    
    def test_update_creators_only_affects_creators_role(self, mock_pymysql_connect):
        """Test that only role='Creator' entries are deleted/updated."""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_cursor.rowcount = 1
        # DictCursor returns list of dicts!
        mock_cursor.fetchall.return_value = [{"order": 1}]
        mock_connection.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_connection.cursor.return_value.__exit__ = Mock(return_value=False)
        mock_pymysql_connect.return_value = mock_connection
        
        client = SumarioPMDClient("host", "db", "user", "pass")
        
        creators = [
            {'firstname': 'John', 'lastname': 'Doe', 'orcid': '0000-0001-2345-6789'}
        ]
        
        client.update_creators_transactional(1429, creators)
        
        # Verify DELETE queries include role='Creator' filter
        delete_calls = [call for call in mock_cursor.execute.call_args_list 
                       if 'DELETE FROM role' in str(call)]
        assert len(delete_calls) > 0
        
        # Check that the DELETE query filters by role='Creator'
        delete_query = delete_calls[0][0][0]
        assert "role = 'Creator'" in delete_query
    
    def test_update_creators_missing_lastname(self, mock_pymysql_connect):
        """Test that creators without lastname are skipped."""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_cursor.rowcount = 0
        mock_cursor.fetchall.return_value = []
        mock_connection.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_connection.cursor.return_value.__exit__ = Mock(return_value=False)
        mock_pymysql_connect.return_value = mock_connection
        
        client = SumarioPMDClient("host", "db", "user", "pass")
        
        creators = [
            {'firstname': 'John', 'lastname': '', 'orcid': '0000-0001-2345-6789'}  # Empty lastname
        ]
        
        success, message, errors = client.update_creators_transactional(1429, creators)
        
        # Should succeed but with validation errors
        assert success is True
        assert len(errors) > 0
        assert "Missing lastname" in errors[0]


class TestEdgeCases:
    """Tests for edge cases and special scenarios."""
    
    def test_creators_without_orcid(self, mock_pymysql_connect):
        """Test handling creators without ORCID."""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_cursor.rowcount = 0
        mock_cursor.fetchall.return_value = []
        mock_connection.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_connection.cursor.return_value.__exit__ = Mock(return_value=False)
        mock_pymysql_connect.return_value = mock_connection
        
        client = SumarioPMDClient("host", "db", "user", "pass")
        
        creators = [
            {'firstname': 'John', 'lastname': 'Doe', 'orcid': ''}  # Empty ORCID
        ]
        
        success, message, errors = client.update_creators_transactional(1429, creators)
        
        assert success is True
        
        # Verify NULL was inserted for ORCID
        insert_calls = [call for call in mock_cursor.execute.call_args_list 
                       if 'INSERT INTO resourceagent' in str(call)]
        insert_values = insert_calls[0][0][1]
        orcid_value = insert_values[5]  # ORCID parameter
        assert orcid_value is None
    
    def test_creators_without_firstname(self, mock_pymysql_connect):
        """Test handling creators without firstname (lastname only)."""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_cursor.rowcount = 0
        mock_cursor.fetchall.return_value = []
        mock_connection.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_connection.cursor.return_value.__exit__ = Mock(return_value=False)
        mock_pymysql_connect.return_value = mock_connection
        
        client = SumarioPMDClient("host", "db", "user", "pass")
        
        creators = [
            {'firstname': '', 'lastname': 'Institution', 'orcid': ''}
        ]
        
        success, message, errors = client.update_creators_transactional(1429, creators)
        
        assert success is True
        
        # Verify name field contains only lastname (no comma)
        insert_calls = [call for call in mock_cursor.execute.call_args_list 
                       if 'INSERT INTO resourceagent' in str(call)]
        insert_values = insert_calls[0][0][1]
        name_value = insert_values[2]  # name parameter
        assert name_value == 'Institution'
        assert ',' not in name_value
