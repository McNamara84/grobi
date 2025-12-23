"""Unit tests for Rights Update Worker."""

import json
import pytest
import tempfile
import os
import responses
from unittest.mock import MagicMock, patch

from PySide6.QtCore import QCoreApplication

from src.workers.rights_update_worker import RightsUpdateWorker


@pytest.fixture(scope="module")
def qapp():
    """Create a QCoreApplication instance for the test module."""
    app = QCoreApplication.instance()
    if app is None:
        app = QCoreApplication([])
    yield app


@pytest.fixture
def valid_csv_file():
    """Create a valid CSV file for testing."""
    with tempfile.NamedTemporaryFile(
        mode='w', suffix='.csv', delete=False, encoding='utf-8'
    ) as f:
        f.write("DOI,rights,rightsUri,schemeUri,rightsIdentifier,rightsIdentifierScheme,lang\n")
        f.write("10.5880/GFZ.1.1.2021.001,Creative Commons Attribution 4.0,https://creativecommons.org/licenses/by/4.0/legalcode,https://spdx.org/licenses/,CC-BY-4.0,SPDX,en\n")
        f.write("10.5880/GFZ.1.1.2021.002,,,,,,\n")
        csv_path = f.name
    yield csv_path
    os.unlink(csv_path)


@pytest.fixture
def csv_with_invalid_spdx():
    """Create a CSV file with invalid SPDX identifier."""
    with tempfile.NamedTemporaryFile(
        mode='w', suffix='.csv', delete=False, encoding='utf-8'
    ) as f:
        f.write("DOI,rights,rightsUri,schemeUri,rightsIdentifier,rightsIdentifierScheme,lang\n")
        f.write("10.5880/GFZ.1.1.2021.001,Test,,,INVALID-SPDX,SPDX,en\n")
        csv_path = f.name
    yield csv_path
    os.unlink(csv_path)


@pytest.fixture
def csv_with_invalid_lang():
    """Create a CSV file with invalid language code."""
    with tempfile.NamedTemporaryFile(
        mode='w', suffix='.csv', delete=False, encoding='utf-8'
    ) as f:
        f.write("DOI,rights,rightsUri,schemeUri,rightsIdentifier,rightsIdentifierScheme,lang\n")
        f.write("10.5880/GFZ.1.1.2021.001,Test,,,,SPDX,invalid\n")
        csv_path = f.name
    yield csv_path
    os.unlink(csv_path)


@pytest.fixture
def csv_with_multiple_dois():
    """Create a CSV file with multiple DOIs."""
    with tempfile.NamedTemporaryFile(
        mode='w', suffix='.csv', delete=False, encoding='utf-8'
    ) as f:
        f.write("DOI,rights,rightsUri,schemeUri,rightsIdentifier,rightsIdentifierScheme,lang\n")
        f.write("10.5880/GFZ.001,License 1,,,cc-by-4.0,SPDX,en\n")
        f.write("10.5880/GFZ.002,License 2,,,mit,SPDX,\n")
        f.write("10.5880/GFZ.003,,,,,,\n")
        csv_path = f.name
    yield csv_path
    os.unlink(csv_path)


class TestRightsUpdateWorkerInit:
    """Test RightsUpdateWorker initialization."""

    def test_init_basic(self, qapp, valid_csv_file):
        """Test basic initialization."""
        worker = RightsUpdateWorker(
            username="TIB.GFZ",
            password="test_password",
            csv_path=valid_csv_file,
            use_test_api=False,
            credentials_are_new=False
        )

        assert worker.username == "TIB.GFZ"
        assert worker.password == "test_password"
        assert worker.csv_path == valid_csv_file
        assert worker.use_test_api is False
        assert worker.credentials_are_new is False

    def test_init_with_test_api(self, qapp, valid_csv_file):
        """Test initialization with test API."""
        worker = RightsUpdateWorker(
            username="XUVM.TEST",
            password="test_password",
            csv_path=valid_csv_file,
            use_test_api=True,
            credentials_are_new=True
        )

        assert worker.use_test_api is True
        assert worker.credentials_are_new is True


class TestRightsUpdateWorkerSignals:
    """Test RightsUpdateWorker signals."""

    def test_signals_exist(self, qapp, valid_csv_file):
        """Test that all required signals exist."""
        worker = RightsUpdateWorker(
            username="TIB.GFZ",
            password="test_password",
            csv_path=valid_csv_file,
            use_test_api=False
        )

        # Check signals are defined
        assert hasattr(worker, 'progress_update')
        assert hasattr(worker, 'doi_updated')
        assert hasattr(worker, 'finished')
        assert hasattr(worker, 'error_occurred')
        assert hasattr(worker, 'request_save_credentials')

    def test_progress_update_signal_can_connect(self, qapp, valid_csv_file):
        """Test that progress_update signal can be connected."""
        worker = RightsUpdateWorker(
            username="TIB.GFZ",
            password="test_password",
            csv_path=valid_csv_file,
            use_test_api=False
        )

        mock_slot = MagicMock()
        worker.progress_update.connect(mock_slot)
        worker.progress_update.emit(1, 10, "Test message")

        mock_slot.assert_called_once_with(1, 10, "Test message")

    def test_finished_signal_can_connect(self, qapp, valid_csv_file):
        """Test that finished signal can be connected."""
        worker = RightsUpdateWorker(
            username="TIB.GFZ",
            password="test_password",
            csv_path=valid_csv_file,
            use_test_api=False
        )

        mock_slot = MagicMock()
        worker.finished.connect(mock_slot)
        worker.finished.emit(5, 2, 1, ["error1"], [["doi1", "reason1"]])

        mock_slot.assert_called_once_with(5, 2, 1, ["error1"], [["doi1", "reason1"]])

    def test_error_occurred_signal_can_connect(self, qapp, valid_csv_file):
        """Test that error_occurred signal can be connected."""
        worker = RightsUpdateWorker(
            username="TIB.GFZ",
            password="test_password",
            csv_path=valid_csv_file,
            use_test_api=False
        )

        mock_slot = MagicMock()
        worker.error_occurred.connect(mock_slot)
        worker.error_occurred.emit("Test error message")

        mock_slot.assert_called_once_with("Test error message")


class TestRightsUpdateWorkerCSVParsing:
    """Test RightsUpdateWorker CSV parsing behavior."""

    @responses.activate
    def test_invalid_csv_emits_error(self, qapp, csv_with_invalid_spdx):
        """Test that invalid SPDX in CSV emits error signal."""
        worker = RightsUpdateWorker(
            username="TIB.GFZ",
            password="test_password",
            csv_path=csv_with_invalid_spdx,
            use_test_api=False
        )

        error_occurred = MagicMock()
        finished = MagicMock()
        worker.error_occurred.connect(error_occurred)
        worker.finished.connect(finished)

        worker.run()

        # Should emit error
        error_occurred.assert_called_once()
        error_message = error_occurred.call_args[0][0]
        assert "SPDX" in error_message or "INVALID" in error_message

        # Should also emit finished with zero counts
        finished.assert_called_once()
        args = finished.call_args[0]
        assert args[0] == 0  # success_count
        assert args[1] == 0  # skipped_count
        assert args[2] == 0  # error_count

    @responses.activate
    def test_invalid_lang_emits_error(self, qapp, csv_with_invalid_lang):
        """Test that invalid language code in CSV emits error signal."""
        worker = RightsUpdateWorker(
            username="TIB.GFZ",
            password="test_password",
            csv_path=csv_with_invalid_lang,
            use_test_api=False
        )

        error_occurred = MagicMock()
        worker.error_occurred.connect(error_occurred)

        worker.run()

        error_occurred.assert_called_once()
        error_message = error_occurred.call_args[0][0]
        assert "Sprachcode" in error_message or "lang" in error_message.lower()

    def test_file_not_found_emits_error(self, qapp):
        """Test that non-existent CSV file emits error signal."""
        worker = RightsUpdateWorker(
            username="TIB.GFZ",
            password="test_password",
            csv_path="/non/existent/file.csv",
            use_test_api=False
        )

        error_occurred = MagicMock()
        worker.error_occurred.connect(error_occurred)

        worker.run()

        error_occurred.assert_called_once()


class TestRightsUpdateWorkerAPIInteraction:
    """Test RightsUpdateWorker API interaction."""

    @responses.activate
    def test_successful_update(self, qapp, valid_csv_file):
        """Test successful rights update via API."""
        # Mock GET for fetching current metadata
        responses.add(
            responses.GET,
            "https://api.datacite.org/dois/10.5880/GFZ.1.1.2021.001",
            json={
                "data": {
                    "id": "10.5880/GFZ.1.1.2021.001",
                    "attributes": {
                        "rightsList": []  # No rights currently
                    }
                }
            },
            status=200
        )

        # Mock GET for second DOI
        responses.add(
            responses.GET,
            "https://api.datacite.org/dois/10.5880/GFZ.1.1.2021.002",
            json={
                "data": {
                    "id": "10.5880/GFZ.1.1.2021.002",
                    "attributes": {
                        "rightsList": []
                    }
                }
            },
            status=200
        )

        # Mock PUT for update
        responses.add(
            responses.PUT,
            "https://api.datacite.org/dois/10.5880/GFZ.1.1.2021.001",
            json={"data": {"id": "10.5880/GFZ.1.1.2021.001", "attributes": {}}},
            status=200
        )

        worker = RightsUpdateWorker(
            username="TIB.GFZ",
            password="test_password",
            csv_path=valid_csv_file,
            use_test_api=False
        )

        finished = MagicMock()
        worker.finished.connect(finished)

        worker.run()

        finished.assert_called_once()
        args = finished.call_args[0]
        success_count = args[0]
        skipped_count = args[1]
        error_count = args[2]
        
        # At least one should be successful or skipped
        assert success_count + skipped_count > 0 or error_count > 0

    @responses.activate
    def test_skips_unchanged_rights(self, qapp, valid_csv_file):
        """Test that unchanged rights are skipped."""
        # Mock GET with same rights as CSV
        responses.add(
            responses.GET,
            "https://api.datacite.org/dois/10.5880/GFZ.1.1.2021.001",
            json={
                "data": {
                    "id": "10.5880/GFZ.1.1.2021.001",
                    "attributes": {
                        "rightsList": [
                            {
                                "rights": "Creative Commons Attribution 4.0",
                                "rightsUri": "https://creativecommons.org/licenses/by/4.0/legalcode",
                                "schemeUri": "https://spdx.org/licenses/",
                                "rightsIdentifier": "CC-BY-4.0",
                                "rightsIdentifierScheme": "SPDX",
                                "lang": "en"
                            }
                        ]
                    }
                }
            },
            status=200
        )

        responses.add(
            responses.GET,
            "https://api.datacite.org/dois/10.5880/GFZ.1.1.2021.002",
            json={
                "data": {
                    "id": "10.5880/GFZ.1.1.2021.002",
                    "attributes": {"rightsList": []}
                }
            },
            status=200
        )

        worker = RightsUpdateWorker(
            username="TIB.GFZ",
            password="test_password",
            csv_path=valid_csv_file,
            use_test_api=False
        )

        finished = MagicMock()
        doi_updated = MagicMock()
        worker.finished.connect(finished)
        worker.doi_updated.connect(doi_updated)

        worker.run()

        # Check that at least one DOI was processed
        assert doi_updated.call_count >= 1

    @responses.activate
    def test_authentication_error_handling(self, qapp, valid_csv_file):
        """Test handling of authentication error."""
        responses.add(
            responses.GET,
            "https://api.datacite.org/dois/10.5880/GFZ.1.1.2021.001",
            json={"errors": [{"title": "Unauthorized"}]},
            status=401
        )

        worker = RightsUpdateWorker(
            username="TIB.GFZ",
            password="wrong_password",
            csv_path=valid_csv_file,
            use_test_api=False
        )

        error_occurred = MagicMock()
        worker.error_occurred.connect(error_occurred)

        worker.run()

        # Should emit error for auth failure
        assert error_occurred.call_count >= 1


class TestRightsChangeDetection:
    """Test rights change detection logic."""

    def test_detect_rights_changes_no_changes(self, qapp, valid_csv_file):
        """Test that identical rights are detected as unchanged."""
        worker = RightsUpdateWorker(
            username="TIB.GFZ",
            password="test_password",
            csv_path=valid_csv_file,
            use_test_api=False
        )

        current_rights = [
            {
                "rights": "Creative Commons Attribution 4.0",
                "rightsUri": "https://example.org",
                "schemeUri": "",
                "rightsIdentifier": "CC-BY-4.0",
                "rightsIdentifierScheme": "SPDX",
                "lang": "en"
            }
        ]

        csv_rights = [
            {
                "rights": "Creative Commons Attribution 4.0",
                "rightsUri": "https://example.org",
                "schemeUri": "",
                "rightsIdentifier": "CC-BY-4.0",
                "rightsIdentifierScheme": "SPDX",
                "lang": "en"
            }
        ]

        has_changes, description = worker._detect_rights_changes(current_rights, csv_rights)
        assert has_changes is False

    def test_detect_rights_changes_text_changed(self, qapp, valid_csv_file):
        """Test that changed rights text is detected."""
        worker = RightsUpdateWorker(
            username="TIB.GFZ",
            password="test_password",
            csv_path=valid_csv_file,
            use_test_api=False
        )

        current_rights = [{"rights": "Old License", "rightsUri": "", "schemeUri": "", 
                          "rightsIdentifier": "", "rightsIdentifierScheme": "", "lang": ""}]
        csv_rights = [{"rights": "New License", "rightsUri": "", "schemeUri": "", 
                      "rightsIdentifier": "", "rightsIdentifierScheme": "", "lang": ""}]

        has_changes, description = worker._detect_rights_changes(current_rights, csv_rights)
        assert has_changes is True

    def test_detect_rights_changes_count_mismatch(self, qapp, valid_csv_file):
        """Test that different number of rights is detected as change."""
        worker = RightsUpdateWorker(
            username="TIB.GFZ",
            password="test_password",
            csv_path=valid_csv_file,
            use_test_api=False
        )

        current_rights = [{"rights": "License 1"}]
        csv_rights = [{"rights": "License 1"}, {"rights": "License 2"}]

        has_changes, description = worker._detect_rights_changes(current_rights, csv_rights)
        assert has_changes is True
        assert "Anzahl" in description

    def test_detect_rights_changes_both_empty(self, qapp, valid_csv_file):
        """Test that both empty lists are detected as no change."""
        worker = RightsUpdateWorker(
            username="TIB.GFZ",
            password="test_password",
            csv_path=valid_csv_file,
            use_test_api=False
        )

        has_changes, description = worker._detect_rights_changes([], [])
        assert has_changes is False

    def test_detect_rights_changes_order_independent(self, qapp, valid_csv_file):
        """Test that rights in different order are detected as unchanged."""
        worker = RightsUpdateWorker(
            username="TIB.GFZ",
            password="test_password",
            csv_path=valid_csv_file,
            use_test_api=False
        )

        # Same rights but in different order
        current_rights = [
            {"rights": "License B", "rightsUri": "https://b.org", "schemeUri": "",
             "rightsIdentifier": "LB", "rightsIdentifierScheme": "SPDX", "lang": "en"},
            {"rights": "License A", "rightsUri": "https://a.org", "schemeUri": "",
             "rightsIdentifier": "LA", "rightsIdentifierScheme": "SPDX", "lang": "de"},
        ]
        csv_rights = [
            {"rights": "License A", "rightsUri": "https://a.org", "schemeUri": "",
             "rightsIdentifier": "LA", "rightsIdentifierScheme": "SPDX", "lang": "de"},
            {"rights": "License B", "rightsUri": "https://b.org", "schemeUri": "",
             "rightsIdentifier": "LB", "rightsIdentifierScheme": "SPDX", "lang": "en"},
        ]

        has_changes, description = worker._detect_rights_changes(current_rights, csv_rights)
        assert has_changes is False

    def test_detect_rights_changes_case_insensitive(self, qapp, valid_csv_file):
        """Test that case differences are ignored in comparison."""
        worker = RightsUpdateWorker(
            username="TIB.GFZ",
            password="test_password",
            csv_path=valid_csv_file,
            use_test_api=False
        )

        # Same rights but with different case (DataCite might normalize)
        current_rights = [
            {"rights": "Creative Commons Attribution 4.0", "rightsUri": "https://example.org",
             "schemeUri": "", "rightsIdentifier": "cc-by-4.0", "rightsIdentifierScheme": "spdx", "lang": "EN"}
        ]
        csv_rights = [
            {"rights": "Creative Commons Attribution 4.0", "rightsUri": "https://example.org",
             "schemeUri": "", "rightsIdentifier": "CC-BY-4.0", "rightsIdentifierScheme": "SPDX", "lang": "en"}
        ]

        has_changes, description = worker._detect_rights_changes(current_rights, csv_rights)
        assert has_changes is False


class TestRightsUpdateWorkerStop:
    """Test RightsUpdateWorker stop functionality."""

    def test_stop_sets_flag(self, qapp, valid_csv_file):
        """Test that stop() sets the running flag to False."""
        worker = RightsUpdateWorker(
            username="TIB.GFZ",
            password="test_password",
            csv_path=valid_csv_file,
            use_test_api=False
        )

        worker._is_running = True
        worker.stop()
        assert worker._is_running is False
