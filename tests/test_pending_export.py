"""Tests for pending DOIs export functionality."""

import csv
import os
import pytest
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

from src.db.sumariopmd_client import SumarioPMDClient, DatabaseError
from src.utils.csv_exporter import export_pending_dois, CSVExportError


class TestFetchPendingDois:
    """Tests for SumarioPMDClient.fetch_pending_dois()."""
    
    @patch('src.db.sumariopmd_client.pymysql')
    def test_fetch_pending_dois_returns_correct_format(self, mock_pymysql):
        """Test that fetch_pending_dois returns list of tuples with correct format."""
        # Setup mock
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_pymysql.connect.return_value = mock_connection
        mock_connection.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_connection.cursor.return_value.__exit__ = Mock(return_value=False)
        mock_connection.__enter__ = Mock(return_value=mock_connection)
        mock_connection.__exit__ = Mock(return_value=False)
        
        # Mock fetchall to return dict-like results (DictCursor format)
        mock_cursor.fetchall.return_value = [
            {'doi': '10.5880/test.001', 'title': 'Test Title', 'first_author': 'Doe, John'},
            {'doi': '10.5880/test.002', 'title': 'Another Title', 'first_author': 'Smith, Jane'},
        ]
        
        # Create client and fetch
        client = SumarioPMDClient(
            host='localhost',
            database='test-db',
            username='test-user',
            password='test-pass'
        )
        result = client.fetch_pending_dois()
        
        # Verify
        assert len(result) == 2
        assert result[0] == ('10.5880/test.001', 'Test Title', 'Doe, John')
        assert result[1] == ('10.5880/test.002', 'Another Title', 'Smith, Jane')
    
    @patch('src.db.sumariopmd_client.pymysql')
    def test_fetch_pending_dois_handles_empty_doi(self, mock_pymysql):
        """Test that empty DOIs are handled correctly."""
        # Setup mock
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_pymysql.connect.return_value = mock_connection
        mock_connection.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_connection.cursor.return_value.__exit__ = Mock(return_value=False)
        mock_connection.__enter__ = Mock(return_value=mock_connection)
        mock_connection.__exit__ = Mock(return_value=False)
        
        # Mock fetchall with empty DOI
        mock_cursor.fetchall.return_value = [
            {'doi': '', 'title': 'Test Title Without DOI', 'first_author': 'Doe, John'},
        ]
        
        # Create client and fetch
        client = SumarioPMDClient(
            host='localhost',
            database='test-db',
            username='test-user',
            password='test-pass'
        )
        result = client.fetch_pending_dois()
        
        # Verify empty DOI is preserved
        assert len(result) == 1
        assert result[0][0] == ''  # DOI should be empty string
        assert result[0][1] == 'Test Title Without DOI'
    
    @patch('src.db.sumariopmd_client.pymysql')
    def test_fetch_pending_dois_handles_empty_result(self, mock_pymysql):
        """Test that empty result set is handled correctly."""
        # Setup mock
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_pymysql.connect.return_value = mock_connection
        mock_connection.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_connection.cursor.return_value.__exit__ = Mock(return_value=False)
        mock_connection.__enter__ = Mock(return_value=mock_connection)
        mock_connection.__exit__ = Mock(return_value=False)
        
        # Mock empty result
        mock_cursor.fetchall.return_value = []
        
        # Create client and fetch
        client = SumarioPMDClient(
            host='localhost',
            database='test-db',
            username='test-user',
            password='test-pass'
        )
        result = client.fetch_pending_dois()
        
        # Verify
        assert result == []
    
    @patch('src.db.sumariopmd_client.pymysql')
    def test_fetch_pending_dois_raises_database_error(self, mock_pymysql):
        """Test that database errors are properly raised."""
        # Setup mock connection that works for init but fails on query
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_pymysql.connect.return_value = mock_connection
        mock_connection.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_connection.cursor.return_value.__exit__ = Mock(return_value=False)
        mock_connection.__enter__ = Mock(return_value=mock_connection)
        mock_connection.__exit__ = Mock(return_value=False)
        
        # Make the cursor.execute raise an error
        mock_pymysql.Error = Exception
        mock_cursor.execute.side_effect = mock_pymysql.Error("Query failed")
        
        # Create client
        client = SumarioPMDClient(
            host='localhost',
            database='test-db',
            username='test-user',
            password='test-pass'
        )
        
        # Verify error is raised
        with pytest.raises(DatabaseError):
            client.fetch_pending_dois()


class TestExportPendingDois:
    """Tests for export_pending_dois() function."""
    
    def test_export_pending_dois_creates_valid_csv(self, tmp_path):
        """Test that export creates a valid CSV file with correct format."""
        # Prepare test data
        data = [
            ('10.5880/test.001', 'Test Title', 'Doe, John'),
            ('10.5880/test.002', 'Another Title', 'Smith, Jane'),
        ]
        output_path = str(tmp_path / "test_pending.csv")
        
        # Export
        export_pending_dois(data, output_path)
        
        # Verify file exists
        assert os.path.exists(output_path)
        
        # Read and verify content (with UTF-8 BOM)
        with open(output_path, 'r', encoding='utf-8-sig') as f:
            reader = csv.reader(f)
            rows = list(reader)
        
        # Verify header
        assert rows[0] == ['DOI', 'Title', 'First Author']
        
        # Verify data rows
        assert rows[1] == ['10.5880/test.001', 'Test Title', 'Doe, John']
        assert rows[2] == ['10.5880/test.002', 'Another Title', 'Smith, Jane']
    
    def test_export_pending_dois_uses_utf8_bom(self, tmp_path):
        """Test that export uses UTF-8 with BOM encoding."""
        data = [('10.5880/test.001', 'Tëst Títle with Ümlauts', 'Müller, Hans')]
        output_path = str(tmp_path / "test_pending_bom.csv")
        
        # Export
        export_pending_dois(data, output_path)
        
        # Read raw bytes and check for BOM
        with open(output_path, 'rb') as f:
            raw_content = f.read()
        
        # UTF-8 BOM is: EF BB BF
        assert raw_content.startswith(b'\xef\xbb\xbf')
    
    def test_export_pending_dois_handles_empty_doi(self, tmp_path):
        """Test that empty DOIs are correctly exported."""
        data = [
            ('', 'Title Without DOI', 'Author, Test'),
            ('10.5880/test.001', 'Title With DOI', 'Author, Other'),
        ]
        output_path = str(tmp_path / "test_empty_doi.csv")
        
        # Export
        export_pending_dois(data, output_path)
        
        # Read and verify
        with open(output_path, 'r', encoding='utf-8-sig') as f:
            reader = csv.reader(f)
            rows = list(reader)
        
        # First data row should have empty DOI
        assert rows[1][0] == ''
        assert rows[1][1] == 'Title Without DOI'
    
    def test_export_pending_dois_handles_special_characters(self, tmp_path):
        """Test that special characters (quotes, commas) are properly escaped."""
        data = [
            ('10.5880/test.001', 'Title with "quotes" and, commas', 'O\'Brien, John'),
        ]
        output_path = str(tmp_path / "test_special_chars.csv")
        
        # Export
        export_pending_dois(data, output_path)
        
        # Read and verify
        with open(output_path, 'r', encoding='utf-8-sig') as f:
            reader = csv.reader(f)
            rows = list(reader)
        
        # Verify special characters are preserved
        assert 'quotes' in rows[1][1]
        assert 'commas' in rows[1][1]
        assert "O'Brien" in rows[1][2]
    
    def test_export_pending_dois_creates_directory(self, tmp_path):
        """Test that missing directories are created."""
        data = [('10.5880/test.001', 'Test Title', 'Doe, John')]
        output_path = str(tmp_path / "subdir" / "nested" / "test_pending.csv")
        
        # Export (should create directories)
        export_pending_dois(data, output_path)
        
        # Verify file exists
        assert os.path.exists(output_path)
    
    def test_export_pending_dois_permission_error(self, tmp_path):
        """Test that permission errors raise CSVExportError."""
        data = [('10.5880/test.001', 'Test Title', 'Doe, John')]
        
        # Try to write to a read-only location (this may not work on all systems)
        # Instead, mock the open function to raise PermissionError
        with patch('builtins.open', side_effect=PermissionError("Access denied")):
            with pytest.raises(CSVExportError) as exc_info:
                export_pending_dois(data, str(tmp_path / "test.csv"))
            
            assert "Berechtigung" in str(exc_info.value) or "Permission" in str(exc_info.value).lower()


class TestPendingExportWorker:
    """Tests for PendingExportWorker class."""
    
    def test_worker_initialization(self):
        """Test that worker initializes with correct parameters."""
        from src.workers.pending_export_worker import PendingExportWorker
        
        worker = PendingExportWorker(
            db_host='test-host',
            db_name='test-db',
            db_user='test-user',
            db_password='test-pass',
            output_path='/tmp/test.csv'
        )
        
        assert worker.db_host == 'test-host'
        assert worker.db_name == 'test-db'
        assert worker.db_user == 'test-user'
        assert worker.db_password == 'test-pass'
        assert worker.output_path == '/tmp/test.csv'
    
    @patch('src.workers.pending_export_worker.SumarioPMDClient')
    @patch('src.workers.pending_export_worker.export_pending_dois')
    def test_worker_emits_progress_signals(self, mock_export, mock_client_class, qtbot):
        """Test that worker emits progress signals correctly."""
        from src.workers.pending_export_worker import PendingExportWorker
        
        # Setup mock
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.fetch_pending_dois.return_value = [
            ('10.5880/test.001', 'Test Title', 'Doe, John')
        ]
        
        worker = PendingExportWorker(
            db_host='test-host',
            db_name='test-db',
            db_user='test-user',
            db_password='test-pass',
            output_path='/tmp/test.csv'
        )
        
        # Track signals
        progress_messages = []
        progress_counts = []
        
        worker.progress.connect(progress_messages.append)
        worker.progress_count.connect(lambda c, t: progress_counts.append((c, t)))
        
        # Run worker
        worker.run()
        
        # Verify progress signals were emitted
        assert len(progress_messages) > 0
        assert any("Verbindung" in msg for msg in progress_messages)
        assert len(progress_counts) > 0
    
    @patch('src.workers.pending_export_worker.SumarioPMDClient')
    @patch('src.workers.pending_export_worker.export_pending_dois')
    def test_worker_emits_finished_signal_on_success(self, mock_export, mock_client_class, qtbot):
        """Test that worker emits finished signal with correct data on success."""
        from src.workers.pending_export_worker import PendingExportWorker
        
        # Setup mock
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.fetch_pending_dois.return_value = [
            ('10.5880/test.001', 'Test Title', 'Doe, John'),
            ('10.5880/test.002', 'Test Title 2', 'Smith, Jane'),
        ]
        
        worker = PendingExportWorker(
            db_host='test-host',
            db_name='test-db',
            db_user='test-user',
            db_password='test-pass',
            output_path='/tmp/test.csv'
        )
        
        # Track finished signal
        finished_data = []
        worker.finished.connect(lambda path, count: finished_data.append((path, count)))
        
        # Run worker
        worker.run()
        
        # Verify finished signal
        assert len(finished_data) == 1
        assert finished_data[0][0] == '/tmp/test.csv'
        assert finished_data[0][1] == 2
    
    @patch('src.workers.pending_export_worker.SumarioPMDClient')
    def test_worker_emits_error_signal_on_db_error(self, mock_client_class, qtbot):
        """Test that worker emits error signal on database error."""
        from src.workers.pending_export_worker import PendingExportWorker
        from src.db.sumariopmd_client import ConnectionError
        
        # Setup mock to raise connection error
        mock_client_class.side_effect = ConnectionError("Connection refused")
        
        worker = PendingExportWorker(
            db_host='invalid-host',
            db_name='test-db',
            db_user='test-user',
            db_password='test-pass',
            output_path='/tmp/test.csv'
        )
        
        # Track error signal
        errors = []
        worker.error.connect(errors.append)
        
        # Run worker
        worker.run()
        
        # Verify error signal
        assert len(errors) == 1
        assert "Datenbankverbindung" in errors[0] or "Connection" in errors[0]
