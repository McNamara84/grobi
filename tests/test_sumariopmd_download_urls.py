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
