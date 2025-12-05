"""Tests for SumarioPMD database client contributor methods."""

import pytest
from unittest.mock import MagicMock, patch

import pymysql

from src.db.sumariopmd_client import (
    SumarioPMDClient,
    DatabaseError,
)


@pytest.fixture
def mock_connection():
    """Create a mock database connection."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    return mock_conn, mock_cursor


@pytest.fixture
def client():
    """Create a SumarioPMDClient instance without connecting."""
    with patch.object(SumarioPMDClient, 'get_connection'):
        return SumarioPMDClient(
            host="localhost",
            database="test_db",
            username="test_user",
            password="test_pass"
        )


class TestFetchContributorsForResource:
    """Tests for fetch_contributors_for_resource method."""
    
    def test_fetch_contributors_success(self, client, mock_connection):
        """Test successful fetch of contributors with multiple roles."""
        mock_conn, mock_cursor = mock_connection
        
        # Mock the query result
        mock_cursor.fetchall.return_value = [
            {
                'order': 1,
                'firstname': 'Hans',
                'lastname': 'Müller',
                'name': 'Müller, Hans',
                'orcid': '0000-0001-2345-6789',
                'identifiertype': 'ORCID',
                'nametype': 'Personal',
                'roles': 'ContactPerson, DataManager',
                'email': 'hans.mueller@gfz.de',
                'website': 'https://www.gfz.de',
                'position': 'Senior Scientist'
            },
            {
                'order': 2,
                'firstname': None,
                'lastname': None,
                'name': 'GFZ Data Services',
                'orcid': 'https://ror.org/04z8jg394',
                'identifiertype': 'ROR',
                'nametype': 'Organizational',
                'roles': 'HostingInstitution',
                'email': None,
                'website': None,
                'position': None
            }
        ]
        
        with patch.object(client, 'get_connection') as mock_get_conn:
            mock_get_conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
            mock_get_conn.return_value.__exit__ = MagicMock(return_value=False)
            
            result = client.fetch_contributors_for_resource(123)
        
        assert len(result) == 2
        
        # First contributor (Personal with ContactInfo)
        assert result[0]['name'] == 'Müller, Hans'
        assert result[0]['roles'] == 'ContactPerson, DataManager'
        assert result[0]['email'] == 'hans.mueller@gfz.de'
        assert result[0]['website'] == 'https://www.gfz.de'
        
        # Second contributor (Organizational without ContactInfo)
        assert result[1]['name'] == 'GFZ Data Services'
        assert result[1]['nametype'] == 'Organizational'
        assert result[1]['email'] is None
    
    def test_fetch_contributors_empty_result(self, client, mock_connection):
        """Test fetch when no contributors exist."""
        mock_conn, mock_cursor = mock_connection
        mock_cursor.fetchall.return_value = []
        
        with patch.object(client, 'get_connection') as mock_get_conn:
            mock_get_conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
            mock_get_conn.return_value.__exit__ = MagicMock(return_value=False)
            
            result = client.fetch_contributors_for_resource(123)
        
        assert result == []
    
    def test_fetch_contributors_database_error(self, client, mock_connection):
        """Test handling of database errors."""
        mock_conn, mock_cursor = mock_connection
        mock_cursor.execute.side_effect = pymysql.Error("Connection lost")
        
        with patch.object(client, 'get_connection') as mock_get_conn:
            mock_get_conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
            mock_get_conn.return_value.__exit__ = MagicMock(return_value=False)
            
            with pytest.raises(DatabaseError) as exc_info:
                client.fetch_contributors_for_resource(123)
            
            assert "Failed to fetch contributors" in str(exc_info.value)


class TestFetchContactinfoForContributor:
    """Tests for fetch_contactinfo_for_contributor method."""
    
    def test_fetch_contactinfo_found(self, client, mock_connection):
        """Test successful fetch of contactinfo."""
        mock_conn, mock_cursor = mock_connection
        mock_cursor.fetchone.return_value = {
            'email': 'test@example.com',
            'website': 'https://example.com',
            'position': 'Researcher'
        }
        
        with patch.object(client, 'get_connection') as mock_get_conn:
            mock_get_conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
            mock_get_conn.return_value.__exit__ = MagicMock(return_value=False)
            
            result = client.fetch_contactinfo_for_contributor(123, 1)
        
        assert result['email'] == 'test@example.com'
        assert result['website'] == 'https://example.com'
        assert result['position'] == 'Researcher'
    
    def test_fetch_contactinfo_not_found(self, client, mock_connection):
        """Test when no contactinfo exists."""
        mock_conn, mock_cursor = mock_connection
        mock_cursor.fetchone.return_value = None
        
        with patch.object(client, 'get_connection') as mock_get_conn:
            mock_get_conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
            mock_get_conn.return_value.__exit__ = MagicMock(return_value=False)
            
            result = client.fetch_contactinfo_for_contributor(123, 1)
        
        assert result is None


class TestUpdateContributorsTransactional:
    """Tests for update_contributors_transactional method."""
    
    def test_update_contributors_success(self, client, mock_connection):
        """Test successful contributor update."""
        mock_conn, mock_cursor = mock_connection
        
        # Mock for getting existing contributor orders
        mock_cursor.fetchall.side_effect = [
            [{'order': 5}, {'order': 6}],  # Existing contributor orders
        ]
        
        # Mock for getting max order
        mock_cursor.fetchone.side_effect = [
            {'cnt': 0},  # No remaining roles for order 5
            {'cnt': 0},  # No remaining roles for order 6
            {'max_order': 4},  # Max order after deletions
        ]
        
        with patch.object(client, 'get_connection') as mock_get_conn:
            mock_get_conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
            mock_get_conn.return_value.__exit__ = MagicMock(return_value=False)
            
            contributors = [
                {
                    'firstname': 'Hans',
                    'lastname': 'Müller',
                    'orcid': 'https://orcid.org/0000-0001-2345-6789',
                    'nametype': 'Personal',
                    'contributorTypes': 'ContactPerson, DataManager',
                    'email': 'hans@gfz.de',
                    'website': 'https://gfz.de',
                    'position': 'Scientist'
                }
            ]
            
            success, message, errors = client.update_contributors_transactional(123, contributors)
        
        assert success is True
        assert "Successfully updated" in message
        assert errors == []
        
        # Verify commit was called
        mock_conn.commit.assert_called_once()
    
    def test_update_contributors_orcid_normalization(self, client, mock_connection):
        """Test that ORCID URLs are normalized to ID only."""
        mock_conn, mock_cursor = mock_connection
        
        mock_cursor.fetchall.return_value = []  # No existing contributors
        mock_cursor.fetchone.return_value = {'max_order': 0}
        
        # Capture the insert query arguments
        insert_calls = []
        original_execute = mock_cursor.execute
        def capture_execute(query, args=None):
            if 'INSERT INTO resourceagent' in query:
                insert_calls.append(args)
            return original_execute(query, args)
        mock_cursor.execute = capture_execute
        
        with patch.object(client, 'get_connection') as mock_get_conn:
            mock_get_conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
            mock_get_conn.return_value.__exit__ = MagicMock(return_value=False)
            
            contributors = [
                {
                    'lastname': 'Test',
                    'orcid': 'https://orcid.org/0000-0001-2345-6789',  # Full URL
                    'contributorTypes': 'Researcher'
                }
            ]
            
            success, _, _ = client.update_contributors_transactional(123, contributors)
        
        assert success is True
        # Verify ORCID was normalized (check the 6th element of the tuple)
        if insert_calls:
            assert insert_calls[0][5] == '0000-0001-2345-6789'
    
    def test_update_contributors_multiple_roles(self, client, mock_connection):
        """Test that multiple roles create multiple role entries."""
        mock_conn, mock_cursor = mock_connection
        
        mock_cursor.fetchall.return_value = []
        mock_cursor.fetchone.return_value = {'max_order': 0}
        
        role_insert_count = 0
        original_execute = mock_cursor.execute
        def count_role_inserts(query, args=None):
            nonlocal role_insert_count
            if 'INSERT INTO role' in query:
                role_insert_count += 1
            return original_execute(query, args)
        mock_cursor.execute = count_role_inserts
        
        with patch.object(client, 'get_connection') as mock_get_conn:
            mock_get_conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
            mock_get_conn.return_value.__exit__ = MagicMock(return_value=False)
            
            contributors = [
                {
                    'lastname': 'Multi Role',
                    'contributorTypes': 'ContactPerson, DataManager, Researcher'
                }
            ]
            
            client.update_contributors_transactional(123, contributors)
        
        # Should have inserted 3 role entries
        assert role_insert_count == 3
    
    def test_update_contributors_contactinfo_only_for_contactperson(self, client, mock_connection):
        """Test that contactinfo is only inserted for ContactPerson role."""
        mock_conn, mock_cursor = mock_connection
        
        mock_cursor.fetchall.return_value = []
        mock_cursor.fetchone.return_value = {'max_order': 0}
        
        contactinfo_inserts = []
        original_execute = mock_cursor.execute
        def capture_ci_inserts(query, args=None):
            if 'INSERT INTO contactinfo' in query:
                contactinfo_inserts.append(args)
            return original_execute(query, args)
        mock_cursor.execute = capture_ci_inserts
        
        with patch.object(client, 'get_connection') as mock_get_conn:
            mock_get_conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
            mock_get_conn.return_value.__exit__ = MagicMock(return_value=False)
            
            contributors = [
                {
                    'lastname': 'Contact Person',
                    'contributorTypes': 'ContactPerson',
                    'email': 'contact@gfz.de'
                },
                {
                    'lastname': 'Researcher Only',
                    'contributorTypes': 'Researcher',
                    'email': 'researcher@gfz.de'  # Has email but not ContactPerson
                }
            ]
            
            client.update_contributors_transactional(123, contributors)
        
        # Should only have 1 contactinfo insert (for ContactPerson)
        assert len(contactinfo_inserts) == 1
        assert contactinfo_inserts[0][2] == 'contact@gfz.de'
    
    def test_update_contributors_rollback_on_error(self, client, mock_connection):
        """Test that transaction is rolled back on error."""
        mock_conn, mock_cursor = mock_connection
        
        mock_cursor.fetchall.return_value = []
        mock_cursor.fetchone.return_value = {'max_order': 0}
        
        # Simulate error during insert
        def error_on_insert(query, args=None):
            if 'INSERT INTO resourceagent' in query:
                raise pymysql.Error("Insert failed")
        mock_cursor.execute = error_on_insert
        
        with patch.object(client, 'get_connection') as mock_get_conn:
            mock_get_conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
            mock_get_conn.return_value.__exit__ = MagicMock(return_value=False)
            
            contributors = [{'lastname': 'Test', 'contributorTypes': 'Researcher'}]
            
            success, message, errors = client.update_contributors_transactional(123, contributors)
        
        assert success is False
        assert "transaction failed" in message.lower()
        mock_conn.rollback.assert_called_once()
    
    def test_update_contributors_missing_lastname_skipped(self, client, mock_connection):
        """Test that contributors without lastname are skipped with error."""
        mock_conn, mock_cursor = mock_connection
        
        mock_cursor.fetchall.return_value = []
        mock_cursor.fetchone.return_value = {'max_order': 0}
        
        with patch.object(client, 'get_connection') as mock_get_conn:
            mock_get_conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
            mock_get_conn.return_value.__exit__ = MagicMock(return_value=False)
            
            contributors = [
                {'firstname': 'OnlyFirst', 'contributorTypes': 'Researcher'},  # No lastname
                {'lastname': 'Valid', 'contributorTypes': 'Researcher'}  # Valid
            ]
            
            success, message, errors = client.update_contributors_transactional(123, contributors)
        
        assert success is True
        assert len(errors) == 1
        assert "Missing lastname" in errors[0]


class TestUpsertContactinfo:
    """Tests for upsert_contactinfo method."""
    
    def test_insert_new_contactinfo(self, client, mock_connection):
        """Test inserting new contactinfo."""
        mock_conn, mock_cursor = mock_connection
        mock_cursor.fetchone.return_value = None  # Entry doesn't exist
        
        with patch.object(client, 'get_connection') as mock_get_conn:
            mock_get_conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
            mock_get_conn.return_value.__exit__ = MagicMock(return_value=False)
            
            success, message = client.upsert_contactinfo(
                123, 1, 'test@example.com', 'https://example.com', 'Scientist'
            )
        
        assert success is True
        assert "inserted" in message.lower()
        mock_conn.commit.assert_called_once()
    
    def test_update_existing_contactinfo(self, client, mock_connection):
        """Test updating existing contactinfo."""
        mock_conn, mock_cursor = mock_connection
        mock_cursor.fetchone.return_value = {'exists': 1}  # Entry exists
        
        with patch.object(client, 'get_connection') as mock_get_conn:
            mock_get_conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
            mock_get_conn.return_value.__exit__ = MagicMock(return_value=False)
            
            success, message = client.upsert_contactinfo(
                123, 1, 'new@example.com', None, None
            )
        
        assert success is True
        assert "updated" in message.lower()


class TestGetContributorRolesForResource:
    """Tests for get_contributor_roles_for_resource method."""
    
    def test_get_roles_grouped_by_order(self, client, mock_connection):
        """Test that roles are correctly grouped by order."""
        mock_conn, mock_cursor = mock_connection
        mock_cursor.fetchall.return_value = [
            {'resourceagent_order': 1, 'role': 'ContactPerson'},
            {'resourceagent_order': 1, 'role': 'DataManager'},
            {'resourceagent_order': 2, 'role': 'Researcher'},
            {'resourceagent_order': 3, 'role': 'HostingInstitution'}
        ]
        
        with patch.object(client, 'get_connection') as mock_get_conn:
            mock_get_conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
            mock_get_conn.return_value.__exit__ = MagicMock(return_value=False)
            
            result = client.get_contributor_roles_for_resource(123)
        
        assert 1 in result
        assert result[1] == ['ContactPerson', 'DataManager']
        assert result[2] == ['Researcher']
        assert result[3] == ['HostingInstitution']


class TestValidContributorTypes:
    """Tests for VALID_CONTRIBUTOR_TYPES constant."""
    
    def test_includes_gfz_internal_type(self):
        """Test that GFZ-internal pointOfContact type is included."""
        assert "pointOfContact" in SumarioPMDClient.VALID_CONTRIBUTOR_TYPES
    
    def test_includes_all_datacite_types(self):
        """Test that all standard DataCite types are included."""
        expected = [
            "ContactPerson", "DataCollector", "DataCurator", "DataManager",
            "HostingInstitution", "Researcher", "Other"
        ]
        for contrib_type in expected:
            assert contrib_type in SumarioPMDClient.VALID_CONTRIBUTOR_TYPES
