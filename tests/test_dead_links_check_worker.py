"""Tests for DeadLinksCheckWorker."""

from unittest.mock import MagicMock, patch

import pytest
from PySide6.QtWidgets import QApplication

from src.workers.dead_links_check_worker import DeadLinksCheckWorker
from src.db.sumariopmd_client import DatabaseError, ConnectionError as DBConnectionError


class DummyResponse:
    """Simple response stub for requests."""

    def __init__(self, status_code: int):
        self.status_code = status_code

    def close(self):
        pass


class DummySession:
    """Session stub with controllable status codes per URL."""

    def __init__(self, head_map=None, get_map=None, raise_on_head=None):
        self.headers = {}
        self._head_map = head_map or {}
        self._get_map = get_map or {}
        self._raise_on_head = set(raise_on_head or [])

    def head(self, url, allow_redirects=True, timeout=10):
        if url in self._raise_on_head:
            raise RuntimeError("head failed")
        return DummyResponse(self._head_map.get(url, 200))

    def get(self, url, allow_redirects=True, stream=True, timeout=10):
        return DummyResponse(self._get_map.get(url, 200))

    def close(self):
        pass


@pytest.fixture(scope="module")
def qapp():
    """Create QApplication instance for tests."""
    app = QApplication.instance() or QApplication([])
    yield app


class TestDeadLinksCheckWorker:
    """Tests for DeadLinksCheckWorker class."""

    def test_worker_initialization(self, qapp):
        """Test worker initialization with credentials."""
        worker = DeadLinksCheckWorker(
            db_host="test.host",
            db_name="test_db",
            db_user="test_user",
            db_password="test_pass",
            timeout=5
        )

        assert worker.db_host == "test.host"
        assert worker.db_name == "test_db"
        assert worker.db_user == "test_user"
        assert worker.db_password == "test_pass"
        assert worker.timeout == 5

    def test_worker_successful_check(self, qapp, qtbot):
        """Test successful dead link check with 404 results."""
        worker = DeadLinksCheckWorker(
            db_host="test.host",
            db_name="test_db",
            db_user="test_user",
            db_password="test_pass"
        )

        mock_data = [
            ("10.5880/GFZ.1", "file1", "https://example.org/a", "", "", 10),
            ("10.5880/GFZ.1", "file2", "https://example.org/a", "", "", 10),
            ("10.5880/GFZ.2", "file3", "https://example.org/b", "", "", 10),
            ("10.5880/GFZ.3", "file4", "", "", "", 10),
            ("10.5880/GFZ.4", "file5", "ftp://example.org/c", "", "", 10),
        ]

        dummy_session = DummySession(
            head_map={
                "https://example.org/a": 404,
                "https://example.org/b": 405,
            },
            get_map={
                "https://example.org/b": 200,
            }
        )

        with patch('src.workers.dead_links_check_worker.SumarioPMDClient') as mock_client_class:
            mock_client = MagicMock()
            mock_client.test_connection.return_value = (True, "Connected")
            mock_client.fetch_all_dois_with_downloads.return_value = mock_data
            mock_client_class.return_value = mock_client

            with patch('src.workers.dead_links_check_worker.requests.Session', return_value=dummy_session):
                with qtbot.waitSignal(worker.finished, timeout=2000) as blocker:
                    worker.run()

        dead_links, checked_count, skipped_count, error_count = blocker.args
        dead_links = [tuple(item) for item in dead_links]
        assert checked_count == 2
        assert skipped_count == 2
        assert error_count == 0
        assert dead_links == [("10.5880/GFZ.1", "https://example.org/a")]

    def test_worker_check_with_errors(self, qapp, qtbot):
        """Test check handling when a URL check fails."""
        worker = DeadLinksCheckWorker(
            db_host="test.host",
            db_name="test_db",
            db_user="test_user",
            db_password="test_pass"
        )

        mock_data = [
            ("10.5880/GFZ.1", "file1", "https://example.org/a", "", "", 10),
            ("10.5880/GFZ.2", "file2", "https://example.org/b", "", "", 10),
        ]

        dummy_session = DummySession(
            head_map={"https://example.org/a": 404},
            raise_on_head={"https://example.org/b"}
        )

        with patch('src.workers.dead_links_check_worker.SumarioPMDClient') as mock_client_class:
            mock_client = MagicMock()
            mock_client.test_connection.return_value = (True, "Connected")
            mock_client.fetch_all_dois_with_downloads.return_value = mock_data
            mock_client_class.return_value = mock_client

            with patch('src.workers.dead_links_check_worker.requests.Session', return_value=dummy_session):
                with qtbot.waitSignal(worker.finished, timeout=2000) as blocker:
                    worker.run()

        dead_links, checked_count, skipped_count, error_count = blocker.args
        dead_links = [tuple(item) for item in dead_links]
        assert checked_count == 1
        assert skipped_count == 0
        assert error_count == 1
        assert dead_links == [("10.5880/GFZ.1", "https://example.org/a")]

    def test_worker_connection_failure(self, qapp, qtbot):
        """Test handling of connection failure."""
        worker = DeadLinksCheckWorker(
            db_host="test.host",
            db_name="test_db",
            db_user="test_user",
            db_password="test_pass"
        )

        with patch('src.workers.dead_links_check_worker.SumarioPMDClient') as mock_client_class:
            mock_client = MagicMock()
            mock_client.test_connection.return_value = (False, "Connection refused")
            mock_client_class.return_value = mock_client

            with qtbot.waitSignal(worker.error_occurred, timeout=2000) as blocker:
                worker.run()

        assert "Datenbankverbindung fehlgeschlagen" in blocker.args[0]

    def test_worker_no_data(self, qapp, qtbot):
        """Test handling when no download URLs are found."""
        worker = DeadLinksCheckWorker(
            db_host="test.host",
            db_name="test_db",
            db_user="test_user",
            db_password="test_pass"
        )

        with patch('src.workers.dead_links_check_worker.SumarioPMDClient') as mock_client_class:
            mock_client = MagicMock()
            mock_client.test_connection.return_value = (True, "Connected")
            mock_client.fetch_all_dois_with_downloads.return_value = []
            mock_client_class.return_value = mock_client

            with qtbot.waitSignal(worker.error_occurred, timeout=2000) as blocker:
                worker.run()

        assert "Keine Download-URLs" in blocker.args[0]

    def test_worker_database_error(self, qapp, qtbot):
        """Test handling of database errors during fetch."""
        worker = DeadLinksCheckWorker(
            db_host="test.host",
            db_name="test_db",
            db_user="test_user",
            db_password="test_pass"
        )

        with patch('src.workers.dead_links_check_worker.SumarioPMDClient') as mock_client_class:
            mock_client = MagicMock()
            mock_client.test_connection.return_value = (True, "Connected")
            mock_client.fetch_all_dois_with_downloads.side_effect = DatabaseError("Query failed")
            mock_client_class.return_value = mock_client

            with qtbot.waitSignal(worker.error_occurred, timeout=2000) as blocker:
                worker.run()

        assert "Datenbankfehler" in blocker.args[0]

    def test_worker_connection_error(self, qapp, qtbot):
        """Test handling of connection errors."""
        worker = DeadLinksCheckWorker(
            db_host="test.host",
            db_name="test_db",
            db_user="test_user",
            db_password="test_pass"
        )

        with patch('src.workers.dead_links_check_worker.SumarioPMDClient') as mock_client_class:
            mock_client_class.side_effect = DBConnectionError("Cannot connect")

            with qtbot.waitSignal(worker.error_occurred, timeout=2000) as blocker:
                worker.run()

        assert "Datenbankverbindung fehlgeschlagen" in blocker.args[0]

    def test_worker_unexpected_error(self, qapp, qtbot):
        """Test handling of unexpected errors."""
        worker = DeadLinksCheckWorker(
            db_host="test.host",
            db_name="test_db",
            db_user="test_user",
            db_password="test_pass"
        )

        with patch('src.workers.dead_links_check_worker.SumarioPMDClient') as mock_client_class:
            mock_client_class.side_effect = RuntimeError("Unexpected error")

            with qtbot.waitSignal(worker.error_occurred, timeout=2000) as blocker:
                worker.run()

        assert "Unerwarteter Fehler" in blocker.args[0]
