"""Tests for SumarioPMDClient file/download-URL methods."""

import pytest
from unittest.mock import MagicMock, patch
import pymysql

from src.db.sumariopmd_client import SumarioPMDClient, DatabaseError


class TestFetchDownloadURLs:
    """Tests for fetching download URLs from file table."""
    
    def test_fetch_download_urls_for_resource(self):
        """Test fetching download URLs for a specific resource."""
        client = SumarioPMDClient(
            host="test.host",
            database="test_db",
            username="test_user",
            password="test_pass"
        )
        
        mock_files = [
            {
                'filename': 'data.zip',
                'location': 'https://download.gfz.de/data.zip',
                'format': 'ZIP',
                'size': 1048576
            },
            {
                'filename': 'metadata.xml',
                'location': 'https://download.gfz.de/metadata.xml',
                'format': 'XML',
                'size': 2048
            }
        ]
        
        with patch.object(client, 'get_connection') as mock_conn:
            mock_cursor = MagicMock()
            mock_cursor.fetchall.return_value = mock_files
            mock_conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = mock_cursor
            
            files = client.fetch_download_urls_for_resource(123)
            
            assert len(files) == 2
            assert files[0]['filename'] == 'data.zip'
            assert files[0]['location'] == 'https://download.gfz.de/data.zip'
            assert files[1]['filename'] == 'metadata.xml'
    
    def test_fetch_download_urls_empty_result(self):
        """Test fetching download URLs when resource has no files."""
        client = SumarioPMDClient(
            host="test.host",
            database="test_db",
            username="test_user",
            password="test_pass"
        )
        
        with patch.object(client, 'get_connection') as mock_conn:
            mock_cursor = MagicMock()
            mock_cursor.fetchall.return_value = []
            mock_conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = mock_cursor
            
            files = client.fetch_download_urls_for_resource(999)
            
            assert len(files) == 0
    
    def test_fetch_download_urls_database_error(self):
        """Test handling of database errors."""
        client = SumarioPMDClient(
            host="test.host",
            database="test_db",
            username="test_user",
            password="test_pass"
        )
        
        with patch.object(client, 'get_connection') as mock_conn:
            mock_cursor = MagicMock()
            mock_cursor.execute.side_effect = pymysql.Error("Connection lost")
            mock_conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = mock_cursor
            
            with pytest.raises(DatabaseError, match="Failed to fetch files"):
                client.fetch_download_urls_for_resource(123)
    
    def test_fetch_all_dois_with_downloads(self):
        """Test fetching all DOIs with download URLs."""
        client = SumarioPMDClient(
            host="test.host",
            database="test_db",
            username="test_user",
            password="test_pass"
        )
        
        mock_results = [
            {
                'doi': '10.5880/GFZ.1',
                'filename': 'data.zip',
                'download_url': 'https://download.gfz.de/data1.zip',
                'description': 'Download data and description',
                'format': 'ZIP',
                'size': 1048576
            },
            {
                'doi': '10.5880/GFZ.1',
                'filename': 'metadata.xml',
                'download_url': 'https://download.gfz.de/meta1.xml',
                'description': 'Metadata file',
                'format': 'XML',
                'size': 2048
            },
            {
                'doi': '10.5880/GFZ.2',
                'filename': 'dataset.nc',
                'download_url': 'https://download.gfz.de/data2.nc',
                'description': 'NetCDF dataset',
                'format': 'NetCDF',
                'size': 5242880
            }
        ]
        
        with patch.object(client, 'get_connection') as mock_conn:
            mock_cursor = MagicMock()
            mock_cursor.fetchall.return_value = mock_results
            mock_conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = mock_cursor
            
            dois_files = client.fetch_all_dois_with_downloads()
            
            assert len(dois_files) == 3
            # First file for DOI 1
            assert dois_files[0] == (
                '10.5880/GFZ.1',
                'data.zip',
                'https://download.gfz.de/data1.zip',
                'Download data and description',
                'ZIP',
                1048576
            )
            # Second file for DOI 1
            assert dois_files[1] == (
                '10.5880/GFZ.1',
                'metadata.xml',
                'https://download.gfz.de/meta1.xml',
                'Metadata file',
                'XML',
                2048
            )
            # File for DOI 2
            assert dois_files[2] == (
                '10.5880/GFZ.2',
                'dataset.nc',
                'https://download.gfz.de/data2.nc',
                'NetCDF dataset',
                'NetCDF',
                5242880
            )
    
    def test_fetch_all_dois_with_downloads_empty(self):
        """Test fetching when no files exist."""
        client = SumarioPMDClient(
            host="test.host",
            database="test_db",
            username="test_user",
            password="test_pass"
        )
        
        with patch.object(client, 'get_connection') as mock_conn:
            mock_cursor = MagicMock()
            mock_cursor.fetchall.return_value = []
            mock_conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = mock_cursor
            
            dois_files = client.fetch_all_dois_with_downloads()
            
            assert len(dois_files) == 0
    
    def test_fetch_all_dois_with_downloads_database_error(self):
        """Test handling of database errors in fetch_all."""
        client = SumarioPMDClient(
            host="test.host",
            database="test_db",
            username="test_user",
            password="test_pass"
        )
        
        with patch.object(client, 'get_connection') as mock_conn:
            mock_cursor = MagicMock()
            mock_cursor.execute.side_effect = pymysql.Error("Query timeout")
            mock_conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = mock_cursor
            
            with pytest.raises(DatabaseError, match="Failed to fetch DOIs with downloads"):
                client.fetch_all_dois_with_downloads()


class TestGetFileByDoiAndFilename:
    """Tests for get_file_by_doi_and_filename method."""
    
    def test_get_file_by_doi_and_filename_found(self):
        """Test successful retrieval of a file entry."""
        client = SumarioPMDClient(
            host="test.host",
            database="test_db",
            username="test_user",
            password="test_pass"
        )
        
        mock_result = {
            'resource_id': 123,
            'name': 'data.csv',
            'url': 'https://download.gfz.de/data.csv',
            'description': 'Download data',
            'filemimetype': 'text/csv',
            'size': 62207
        }
        
        with patch.object(client, 'get_connection') as mock_conn:
            mock_cursor = MagicMock()
            mock_cursor.fetchone.return_value = mock_result
            mock_conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = mock_cursor
            
            result = client.get_file_by_doi_and_filename('10.1594/GFZ.SDDB.1004', 'data.csv')
            
            assert result is not None
            assert result['resource_id'] == 123
            assert result['name'] == 'data.csv'
            assert result['url'] == 'https://download.gfz.de/data.csv'
            assert result['description'] == 'Download data'
            assert result['filemimetype'] == 'text/csv'
            assert result['size'] == 62207
    
    def test_get_file_by_doi_and_filename_not_found(self):
        """Test retrieval when entry does not exist."""
        client = SumarioPMDClient(
            host="test.host",
            database="test_db",
            username="test_user",
            password="test_pass"
        )
        
        with patch.object(client, 'get_connection') as mock_conn:
            mock_cursor = MagicMock()
            mock_cursor.fetchone.return_value = None
            mock_conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = mock_cursor
            
            result = client.get_file_by_doi_and_filename('10.1594/GFZ.NONEXISTENT', 'missing.csv')
            
            assert result is None
    
    def test_get_file_by_doi_and_filename_database_error(self):
        """Test handling of database errors."""
        client = SumarioPMDClient(
            host="test.host",
            database="test_db",
            username="test_user",
            password="test_pass"
        )
        
        with patch.object(client, 'get_connection') as mock_conn:
            mock_cursor = MagicMock()
            mock_cursor.execute.side_effect = pymysql.Error("Connection lost")
            mock_conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = mock_cursor
            
            with pytest.raises(DatabaseError, match="Failed to fetch file entry"):
                client.get_file_by_doi_and_filename('10.1594/GFZ.SDDB.1004', 'data.csv')


class TestUpdateFileEntry:
    """Tests for update_file_entry method."""
    
    def test_update_file_entry_all_fields(self):
        """Test updating all fields of a file entry."""
        client = SumarioPMDClient(
            host="test.host",
            database="test_db",
            username="test_user",
            password="test_pass"
        )
        
        with patch.object(client, 'get_connection') as mock_conn:
            mock_cursor = MagicMock()
            mock_cursor.rowcount = 1
            mock_conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = mock_cursor
            
            result = client.update_file_entry(
                resource_id=123,
                filename='data.csv',
                url='https://new-url.gfz.de/data.csv',
                description='New description',
                filemimetype='text/plain',
                size=99999
            )
            
            assert result is True
            mock_cursor.execute.assert_called_once()
            mock_conn.return_value.__enter__.return_value.commit.assert_called_once()
    
    def test_update_file_entry_partial_update(self):
        """Test updating only some fields."""
        client = SumarioPMDClient(
            host="test.host",
            database="test_db",
            username="test_user",
            password="test_pass"
        )
        
        with patch.object(client, 'get_connection') as mock_conn:
            mock_cursor = MagicMock()
            mock_cursor.rowcount = 1
            mock_conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = mock_cursor
            
            # Only update URL
            result = client.update_file_entry(
                resource_id=123,
                filename='data.csv',
                url='https://new-url.gfz.de/data.csv'
            )
            
            assert result is True
            # Check that only url is in the query
            call_args = mock_cursor.execute.call_args
            query = call_args[0][0]
            
            assert 'url = %s' in query
            assert 'description = %s' not in query
            assert 'filemimetype = %s' not in query
            assert 'size = %s' not in query
    
    def test_update_file_entry_no_fields_provided(self):
        """Test that update returns False when no fields are provided."""
        client = SumarioPMDClient(
            host="test.host",
            database="test_db",
            username="test_user",
            password="test_pass"
        )
        
        # No need to mock - should return early
        result = client.update_file_entry(
            resource_id=123,
            filename='data.csv'
            # No optional parameters provided
        )
        
        assert result is False
    
    def test_update_file_entry_not_found(self):
        """Test update when entry does not exist."""
        client = SumarioPMDClient(
            host="test.host",
            database="test_db",
            username="test_user",
            password="test_pass"
        )
        
        with patch.object(client, 'get_connection') as mock_conn:
            mock_cursor = MagicMock()
            mock_cursor.rowcount = 0  # No rows affected
            mock_conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = mock_cursor
            
            result = client.update_file_entry(
                resource_id=999,
                filename='nonexistent.csv',
                url='https://example.org'
            )
            
            assert result is False
    
    def test_update_file_entry_database_error(self):
        """Test handling of database errors during update."""
        client = SumarioPMDClient(
            host="test.host",
            database="test_db",
            username="test_user",
            password="test_pass"
        )
        
        with patch.object(client, 'get_connection') as mock_conn:
            mock_cursor = MagicMock()
            mock_cursor.execute.side_effect = pymysql.Error("Update failed")
            mock_conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = mock_cursor
            
            with pytest.raises(DatabaseError, match="Failed to update file entry"):
                client.update_file_entry(
                    resource_id=123,
                    filename='data.csv',
                    url='https://example.org'
                )
    
    def test_update_file_entry_multiple_fields_combination(self):
        """Test updating different field combinations."""
        client = SumarioPMDClient(
            host="test.host",
            database="test_db",
            username="test_user",
            password="test_pass"
        )
        
        with patch.object(client, 'get_connection') as mock_conn:
            mock_cursor = MagicMock()
            mock_cursor.rowcount = 1
            mock_conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = mock_cursor
            
            # Update description and size only
            result = client.update_file_entry(
                resource_id=123,
                filename='data.csv',
                description='Updated description',
                size=12345
            )
            
            assert result is True
            
            call_args = mock_cursor.execute.call_args
            query = call_args[0][0]
            params = call_args[0][1]
            
            assert 'description = %s' in query
            assert 'size = %s' in query
            assert 'url = %s' not in query
            assert 'filemimetype = %s' not in query
            # Check params: description, size, resource_id, filename
            assert list(params) == ['Updated description', 12345, 123, 'data.csv']
