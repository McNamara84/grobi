"""Unit tests for DataCite API Client."""

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


# Load fixture data
FIXTURES_PATH = Path(__file__).parent / "fixtures" / "sample_responses.json"
with open(FIXTURES_PATH, "r", encoding="utf-8") as f:
    FIXTURES = json.load(f)


@pytest.fixture
def client():
    """Create a test DataCite client."""
    return DataCiteClient("TIB.GFZ", "test_password", use_test_api=False)


@pytest.fixture
def test_client():
    """Create a test DataCite client using test API."""
    return DataCiteClient("XUVM.KDVJHQ", "test_password", use_test_api=True)


class TestDataCiteClientInit:
    """Test DataCiteClient initialization."""
    
    def test_production_endpoint(self, client):
        """Test that production endpoint is set correctly."""
        assert client.base_url == "https://api.datacite.org"
        assert client.username == "TIB.GFZ"
    
    def test_test_endpoint(self, test_client):
        """Test that test endpoint is set correctly."""
        assert test_client.base_url == "https://api.test.datacite.org"
        assert test_client.username == "XUVM.KDVJHQ"


class TestFetchAllDOIs:
    """Test fetching DOIs from DataCite API."""
    
    @responses.activate
    def test_single_page_success(self, client):
        """Test successful fetch with a single page of results."""
        responses.add(
            responses.GET,
            "https://api.datacite.org/dois",
            json=FIXTURES["single_page_success"],
            status=200
        )
        
        dois = client.fetch_all_dois()
        
        assert len(dois) == 3
        assert dois[0] == ("10.5880/GFZ.1.1.2021.001", 
                          "https://dataservices.gfz-potsdam.de/panmetaworks/showshort.php?id=1234")
        assert dois[1] == ("10.5880/GFZ.1.1.2021.002",
                          "https://dataservices.gfz-potsdam.de/panmetaworks/showshort.php?id=5678")
        assert dois[2] == ("10.5880/GFZ.1.1.2021.003",
                          "https://dataservices.gfz-potsdam.de/panmetaworks/showshort.php?id=9012")
    
    @responses.activate
    def test_multiple_pages(self, client):
        """Test successful fetch with pagination across multiple pages."""
        # Mock first page
        responses.add(
            responses.GET,
            "https://api.datacite.org/dois",
            json=FIXTURES["first_page_multi"],
            status=200
        )
        
        # Mock second page
        responses.add(
            responses.GET,
            "https://api.datacite.org/dois",
            json=FIXTURES["second_page_multi"],
            status=200
        )
        
        # Mock third page
        responses.add(
            responses.GET,
            "https://api.datacite.org/dois",
            json=FIXTURES["third_page_multi"],
            status=200
        )
        
        dois = client.fetch_all_dois()
        
        # Should have collected all DOIs from all 3 pages
        assert len(dois) == 5
        assert dois[0][0] == "10.5880/GFZ.1.1.2021.001"
        assert dois[4][0] == "10.5880/GFZ.1.1.2021.005"
    
    @responses.activate
    def test_empty_response(self, client):
        """Test handling of empty result set."""
        responses.add(
            responses.GET,
            "https://api.datacite.org/dois",
            json=FIXTURES["empty_response"],
            status=200
        )
        
        dois = client.fetch_all_dois()
        
        assert len(dois) == 0
        assert dois == []
    
    @responses.activate
    def test_incomplete_data(self, client):
        """Test handling of incomplete DOI entries."""
        responses.add(
            responses.GET,
            "https://api.datacite.org/dois",
            json=FIXTURES["incomplete_data"],
            status=200
        )
        
        dois = client.fetch_all_dois()
        
        # Should only include the first entry which has both DOI and URL
        assert len(dois) == 1
        assert dois[0] == ("10.5880/GFZ.1.1.2021.001",
                          "https://dataservices.gfz-potsdam.de/panmetaworks/showshort.php?id=1234")
    
    @responses.activate
    def test_authentication_error(self, client):
        """Test handling of authentication failure."""
        responses.add(
            responses.GET,
            "https://api.datacite.org/dois",
            json={"errors": [{"title": "Unauthorized"}]},
            status=401
        )
        
        with pytest.raises(AuthenticationError) as exc_info:
            client.fetch_all_dois()
        
        assert "Anmeldung fehlgeschlagen" in str(exc_info.value)
    
    @responses.activate
    def test_rate_limiting(self, client):
        """Test handling of rate limiting."""
        responses.add(
            responses.GET,
            "https://api.datacite.org/dois",
            json={"errors": [{"title": "Too Many Requests"}]},
            status=429
        )
        
        with pytest.raises(DataCiteAPIError) as exc_info:
            client.fetch_all_dois()
        
        assert "Zu viele Anfragen" in str(exc_info.value)
    
    @responses.activate
    def test_server_error(self, client):
        """Test handling of server errors."""
        responses.add(
            responses.GET,
            "https://api.datacite.org/dois",
            json={"errors": [{"title": "Internal Server Error"}]},
            status=500
        )
        
        with pytest.raises(DataCiteAPIError) as exc_info:
            client.fetch_all_dois()
        
        assert "DataCite API Fehler" in str(exc_info.value)
        assert "500" in str(exc_info.value)
    
    @responses.activate
    def test_invalid_json(self, client):
        """Test handling of invalid JSON response."""
        responses.add(
            responses.GET,
            "https://api.datacite.org/dois",
            body="This is not JSON",
            status=200
        )
        
        with pytest.raises(DataCiteAPIError) as exc_info:
            client.fetch_all_dois()
        
        assert "Ungültige Antwort" in str(exc_info.value)
    
    @responses.activate
    def test_timeout(self, client):
        """Test handling of timeout."""
        responses.add(
            responses.GET,
            "https://api.datacite.org/dois",
            body=responses.ConnectionError("Connection timeout")
        )
        
        with pytest.raises(NetworkError) as exc_info:
            client.fetch_all_dois()
        
        assert "Verbindung zur DataCite API fehlgeschlagen" in str(exc_info.value)
    
    @responses.activate
    def test_request_parameters(self, client):
        """Test that correct parameters are sent to API."""
        responses.add(
            responses.GET,
            "https://api.datacite.org/dois",
            json=FIXTURES["single_page_success"],
            status=200
        )
        
        client.fetch_all_dois()
        
        # Check that the request was made with correct parameters
        assert len(responses.calls) == 1
        request = responses.calls[0].request
        assert "client-id=TIB.GFZ" in request.url
        assert "page%5Bsize%5D=100" in request.url or "page[size]=100" in request.url
        assert "page%5Bnumber%5D=1" in request.url or "page[number]=1" in request.url
    
    @responses.activate
    def test_test_api_endpoint(self, test_client):
        """Test that test API endpoint is used correctly."""
        responses.add(
            responses.GET,
            "https://api.test.datacite.org/dois",
            json=FIXTURES["single_page_success"],
            status=200
        )
        
        test_client.fetch_all_dois()
        
        # Verify test endpoint was called
        assert len(responses.calls) == 1
        assert "api.test.datacite.org" in responses.calls[0].request.url


class TestErrorMessages:
    """Test that error messages are user-friendly and in German."""
    
    @responses.activate
    def test_auth_error_message_german(self, client):
        """Test authentication error message is in German."""
        responses.add(
            responses.GET,
            "https://api.datacite.org/dois",
            status=401
        )
        
        with pytest.raises(AuthenticationError) as exc_info:
            client.fetch_all_dois()
        
        error_message = str(exc_info.value)
        assert "Benutzername" in error_message or "Passwort" in error_message
    
    @responses.activate
    def test_network_error_message_german(self, client):
        """Test network error message is in German."""
        responses.add(
            responses.GET,
            "https://api.datacite.org/dois",
            body=responses.ConnectionError()
        )
        
        with pytest.raises(NetworkError) as exc_info:
            client.fetch_all_dois()
        
        error_message = str(exc_info.value)
        assert "Verbindung" in error_message or "Internetverbindung" in error_message
