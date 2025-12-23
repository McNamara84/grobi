"""Unit tests for DataCite API Client - Rights functionality."""

import json
import pytest
import responses
from pathlib import Path

from src.api.datacite_client import (
    DataCiteClient,
    DataCiteAPIError,
    AuthenticationError,
    NetworkError
)


@pytest.fixture
def client():
    """Create a test DataCite client."""
    return DataCiteClient("TIB.GFZ", "test_password", use_test_api=False)


@pytest.fixture
def test_client():
    """Create a test DataCite client using test API."""
    return DataCiteClient("XUVM.KDVJHQ", "test_password", use_test_api=True)


# Sample API responses for rights tests
RIGHTS_SINGLE_PAGE_RESPONSE = {
    "data": [
        {
            "id": "10.5880/GFZ.1.1.2021.001",
            "type": "dois",
            "attributes": {
                "doi": "10.5880/GFZ.1.1.2021.001",
                "rightsList": [
                    {
                        "rights": "Creative Commons Attribution 4.0 International",
                        "rightsUri": "https://creativecommons.org/licenses/by/4.0/legalcode",
                        "schemeUri": "https://spdx.org/licenses/",
                        "rightsIdentifier": "cc-by-4.0",
                        "rightsIdentifierScheme": "SPDX",
                        "lang": "en"
                    }
                ]
            }
        },
        {
            "id": "10.5880/GFZ.1.1.2021.002",
            "type": "dois",
            "attributes": {
                "doi": "10.5880/GFZ.1.1.2021.002",
                "rightsList": []  # No rights
            }
        },
        {
            "id": "10.5880/GFZ.1.1.2021.003",
            "type": "dois",
            "attributes": {
                "doi": "10.5880/GFZ.1.1.2021.003",
                "rightsList": [
                    {
                        "rights": "CC BY 4.0",
                        "rightsUri": "http://creativecommons.org/licenses/by/4.0"
                    },
                    {
                        "rights": "Open Data",
                        "lang": "en"
                    }
                ]
            }
        }
    ],
    "links": {
        "self": "https://api.datacite.org/dois?page[cursor]=1"
    }
}

RIGHTS_FIRST_PAGE_RESPONSE = {
    "data": [
        {
            "id": "10.5880/GFZ.1.1.2021.001",
            "type": "dois",
            "attributes": {
                "doi": "10.5880/GFZ.1.1.2021.001",
                "rightsList": [
                    {
                        "rights": "Creative Commons Attribution 4.0 International",
                        "rightsUri": "https://creativecommons.org/licenses/by/4.0/legalcode",
                        "schemeUri": "https://spdx.org/licenses/",
                        "rightsIdentifier": "cc-by-4.0",
                        "rightsIdentifierScheme": "SPDX",
                        "lang": "en"
                    }
                ]
            }
        }
    ],
    "links": {
        "self": "https://api.datacite.org/dois?page[cursor]=1",
        "next": "https://api.datacite.org/dois?page[cursor]=abc123"
    }
}

RIGHTS_SECOND_PAGE_RESPONSE = {
    "data": [
        {
            "id": "10.5880/GFZ.1.1.2021.002",
            "type": "dois",
            "attributes": {
                "doi": "10.5880/GFZ.1.1.2021.002",
                "rightsList": []
            }
        }
    ],
    "links": {
        "self": "https://api.datacite.org/dois?page[cursor]=abc123"
    }
}


class TestFetchAllDOIsWithRights:
    """Test fetching DOIs with rights information from DataCite API."""

    @responses.activate
    def test_single_page_with_rights(self, client):
        """Test fetching rights data from a single page."""
        responses.add(
            responses.GET,
            "https://api.datacite.org/dois",
            json=RIGHTS_SINGLE_PAGE_RESPONSE,
            status=200
        )

        rights_data = client.fetch_all_dois_with_rights()

        # DOI 1: Has full rights data
        assert len(rights_data) == 4  # 1 + 1 (empty) + 2 (multiple rights)
        
        # First DOI with full rights
        assert rights_data[0][0] == "10.5880/GFZ.1.1.2021.001"
        assert rights_data[0][1] == "Creative Commons Attribution 4.0 International"
        assert rights_data[0][2] == "https://creativecommons.org/licenses/by/4.0/legalcode"
        assert rights_data[0][3] == "https://spdx.org/licenses/"
        assert rights_data[0][4] == "cc-by-4.0"
        assert rights_data[0][5] == "SPDX"
        assert rights_data[0][6] == "en"

    @responses.activate
    def test_doi_without_rights_has_empty_entry(self, client):
        """Test that DOIs without rights get an empty entry."""
        responses.add(
            responses.GET,
            "https://api.datacite.org/dois",
            json=RIGHTS_SINGLE_PAGE_RESPONSE,
            status=200
        )

        rights_data = client.fetch_all_dois_with_rights()

        # Find the entry for DOI 002 (no rights)
        doi_002_entries = [r for r in rights_data if r[0] == "10.5880/GFZ.1.1.2021.002"]
        assert len(doi_002_entries) == 1
        assert doi_002_entries[0] == ("10.5880/GFZ.1.1.2021.002", "", "", "", "", "", "")

    @responses.activate
    def test_doi_with_multiple_rights_entries(self, client):
        """Test that DOIs with multiple rights entries get multiple rows."""
        responses.add(
            responses.GET,
            "https://api.datacite.org/dois",
            json=RIGHTS_SINGLE_PAGE_RESPONSE,
            status=200
        )

        rights_data = client.fetch_all_dois_with_rights()

        # DOI 003 has two rights entries
        doi_003_entries = [r for r in rights_data if r[0] == "10.5880/GFZ.1.1.2021.003"]
        assert len(doi_003_entries) == 2
        
        # First rights entry
        assert doi_003_entries[0][1] == "CC BY 4.0"
        assert doi_003_entries[0][2] == "http://creativecommons.org/licenses/by/4.0"
        
        # Second rights entry
        assert doi_003_entries[1][1] == "Open Data"
        assert doi_003_entries[1][6] == "en"

    @responses.activate
    def test_pagination_with_rights(self, client):
        """Test fetching rights data with pagination."""
        # First page
        responses.add(
            responses.GET,
            "https://api.datacite.org/dois",
            json=RIGHTS_FIRST_PAGE_RESPONSE,
            status=200
        )
        
        # Second page
        responses.add(
            responses.GET,
            "https://api.datacite.org/dois?page[cursor]=abc123",
            json=RIGHTS_SECOND_PAGE_RESPONSE,
            status=200
        )

        rights_data = client.fetch_all_dois_with_rights()

        assert len(rights_data) == 2
        assert rights_data[0][0] == "10.5880/GFZ.1.1.2021.001"
        assert rights_data[1][0] == "10.5880/GFZ.1.1.2021.002"

    @responses.activate
    def test_authentication_error(self, client):
        """Test handling of authentication error."""
        responses.add(
            responses.GET,
            "https://api.datacite.org/dois",
            json={"errors": [{"title": "Unauthorized"}]},
            status=401
        )

        with pytest.raises(AuthenticationError):
            client.fetch_all_dois_with_rights()

    @responses.activate
    def test_rate_limit_error(self, client):
        """Test handling of rate limit error."""
        responses.add(
            responses.GET,
            "https://api.datacite.org/dois",
            json={"errors": [{"title": "Too Many Requests"}]},
            status=429
        )

        with pytest.raises(DataCiteAPIError) as exc_info:
            client.fetch_all_dois_with_rights()
        assert "Zu viele Anfragen" in str(exc_info.value)

    @responses.activate
    def test_empty_result(self, client):
        """Test handling of empty result set."""
        responses.add(
            responses.GET,
            "https://api.datacite.org/dois",
            json={"data": [], "links": {"self": "https://api.datacite.org/dois"}},
            status=200
        )

        rights_data = client.fetch_all_dois_with_rights()
        assert rights_data == []


class TestUpdateDOIRights:
    """Test updating DOI rights via DataCite API."""

    @responses.activate
    def test_update_rights_success(self, client):
        """Test successful rights update."""
        # Mock PUT request
        responses.add(
            responses.PUT,
            "https://api.datacite.org/dois/10.5880/GFZ.1.1.2021.001",
            json={
                "data": {
                    "id": "10.5880/GFZ.1.1.2021.001",
                    "type": "dois",
                    "attributes": {
                        "rightsList": [
                            {
                                "rights": "Creative Commons Attribution 4.0 International",
                                "rightsUri": "https://creativecommons.org/licenses/by/4.0/legalcode",
                                "rightsIdentifier": "CC-BY-4.0"
                            }
                        ]
                    }
                }
            },
            status=200
        )

        rights_list = [
            {
                "rights": "Creative Commons Attribution 4.0 International",
                "rightsUri": "https://creativecommons.org/licenses/by/4.0/legalcode",
                "rightsIdentifier": "CC-BY-4.0"
            }
        ]

        success, message = client.update_doi_rights("10.5880/GFZ.1.1.2021.001", rights_list)

        assert success is True
        assert "erfolgreich" in message.lower() or "aktualisiert" in message.lower()

    @responses.activate
    def test_update_rights_empty_list(self, client):
        """Test updating with empty rights list (removing all rights)."""
        responses.add(
            responses.PUT,
            "https://api.datacite.org/dois/10.5880/GFZ.1.1.2021.001",
            json={
                "data": {
                    "id": "10.5880/GFZ.1.1.2021.001",
                    "type": "dois",
                    "attributes": {"rightsList": []}
                }
            },
            status=200
        )

        success, message = client.update_doi_rights("10.5880/GFZ.1.1.2021.001", [])

        assert success is True

    @responses.activate
    def test_update_rights_authentication_error(self, client):
        """Test handling of authentication error during update."""
        responses.add(
            responses.PUT,
            "https://api.datacite.org/dois/10.5880/GFZ.1.1.2021.001",
            json={"errors": [{"title": "Unauthorized"}]},
            status=401
        )

        success, message = client.update_doi_rights(
            "10.5880/GFZ.1.1.2021.001",
            [{"rights": "Test"}]
        )

        assert success is False
        assert "Authentifizierung" in message or "401" in message

    @responses.activate
    def test_update_rights_not_found(self, client):
        """Test handling of DOI not found error."""
        responses.add(
            responses.PUT,
            "https://api.datacite.org/dois/10.5880/INVALID.DOI",
            json={"errors": [{"title": "Not Found"}]},
            status=404
        )

        success, message = client.update_doi_rights(
            "10.5880/INVALID.DOI",
            [{"rights": "Test"}]
        )

        assert success is False
        assert "404" in message or "nicht gefunden" in message.lower()

    @responses.activate
    def test_update_rights_with_all_fields(self, client):
        """Test update with all rights fields populated."""
        responses.add(
            responses.PUT,
            "https://api.datacite.org/dois/10.5880/GFZ.1.1.2021.001",
            json={"data": {"id": "10.5880/GFZ.1.1.2021.001", "type": "dois", "attributes": {}}},
            status=200
        )

        rights_list = [
            {
                "rights": "Creative Commons Attribution 4.0 International",
                "rightsUri": "https://creativecommons.org/licenses/by/4.0/legalcode",
                "schemeUri": "https://spdx.org/licenses/",
                "rightsIdentifier": "CC-BY-4.0",
                "rightsIdentifierScheme": "SPDX",
                "lang": "en"
            }
        ]

        success, message = client.update_doi_rights("10.5880/GFZ.1.1.2021.001", rights_list)

        assert success is True
        
        # Verify the request body
        request_body = json.loads(responses.calls[0].request.body)
        assert "data" in request_body
        assert "attributes" in request_body["data"]
        assert "rightsList" in request_body["data"]["attributes"]
        assert len(request_body["data"]["attributes"]["rightsList"]) == 1

    @responses.activate
    def test_update_rights_multiple_entries(self, client):
        """Test update with multiple rights entries."""
        responses.add(
            responses.PUT,
            "https://api.datacite.org/dois/10.5880/GFZ.1.1.2021.001",
            json={"data": {"id": "10.5880/GFZ.1.1.2021.001", "type": "dois", "attributes": {}}},
            status=200
        )

        rights_list = [
            {"rights": "License 1", "rightsUri": "https://example.org/license1"},
            {"rights": "License 2", "lang": "de"}
        ]

        success, message = client.update_doi_rights("10.5880/GFZ.1.1.2021.001", rights_list)

        assert success is True
        
        request_body = json.loads(responses.calls[0].request.body)
        assert len(request_body["data"]["attributes"]["rightsList"]) == 2
