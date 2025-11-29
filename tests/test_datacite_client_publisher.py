"""Tests for DataCite API client publisher fetching and updating functionality."""

import pytest
import requests
from unittest.mock import Mock, patch

from src.api.datacite_client import (
    DataCiteClient,
    DataCiteAPIError,
    AuthenticationError,
    NetworkError
)


@pytest.fixture
def client():
    """Create a DataCite client instance for testing."""
    return DataCiteClient("TEST.CLIENT", "test_password", use_test_api=True)


def create_mock_response_with_publisher(dois_data, has_next=False):
    """
    Create a mock response with publisher data.
    
    Args:
        dois_data: List of dicts with 'id' and 'publisher' keys
        has_next: Whether there's a next page
    
    Returns:
        Mock response object
    """
    data_items = []
    for doi_data in dois_data:
        item = {
            "id": doi_data["id"],
            "attributes": {
                "url": f"https://example.org/{doi_data['id']}",
                "publisher": doi_data.get("publisher", "Unknown Publisher")
            }
        }
        data_items.append(item)
    
    response_data = {
        "data": data_items,
        "links": {}
    }
    
    if has_next:
        response_data["links"]["next"] = "https://api.test.datacite.org/dois?page=2"
    
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = response_data
    return mock_response


class TestFetchAllDOIsWithPublisher:
    """Tests for fetching DOIs with publisher information."""
    
    def test_fetch_dois_with_publisher_string_format(self, client):
        """Test fetching DOIs with publisher as string."""
        dois_data = [
            {
                "id": "10.5880/test.001",
                "publisher": "GFZ German Research Centre for Geosciences"
            }
        ]
        
        mock_response = create_mock_response_with_publisher(dois_data)
        
        with patch('requests.get', return_value=mock_response):
            result = client.fetch_all_dois_with_publisher()
        
        assert len(result) == 1
        assert result[0][0] == "10.5880/test.001"  # DOI
        assert result[0][1] == "GFZ German Research Centre for Geosciences"  # Name
        assert result[0][2] == ""  # Publisher Identifier (empty for string format)
        assert result[0][3] == ""  # Publisher Identifier Scheme
        assert result[0][4] == ""  # Scheme URI
        assert result[0][5] == ""  # Language
    
    def test_fetch_dois_with_publisher_extended_format(self, client):
        """Test fetching DOIs with publisher as extended object."""
        dois_data = [
            {
                "id": "10.5880/test.001",
                "publisher": {
                    "name": "GFZ German Research Centre for Geosciences",
                    "publisherIdentifier": "https://ror.org/04z8jg394",
                    "publisherIdentifierScheme": "ROR",
                    "schemeUri": "https://ror.org",
                    "lang": "en"
                }
            }
        ]
        
        # Custom mock for extended publisher
        data_items = [{
            "id": dois_data[0]["id"],
            "attributes": {
                "url": "https://example.org/test",
                "publisher": dois_data[0]["publisher"]
            }
        }]
        
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": data_items, "links": {}}
        
        with patch('requests.get', return_value=mock_response):
            result = client.fetch_all_dois_with_publisher()
        
        assert len(result) == 1
        assert result[0][0] == "10.5880/test.001"  # DOI
        assert result[0][1] == "GFZ German Research Centre for Geosciences"  # Name
        assert result[0][2] == "https://ror.org/04z8jg394"  # Publisher Identifier
        assert result[0][3] == "ROR"  # Publisher Identifier Scheme
        assert result[0][4] == "https://ror.org"  # Scheme URI
        assert result[0][5] == "en"  # Language
    
    def test_fetch_dois_with_publisher_pagination(self, client):
        """Test pagination for publisher fetch."""
        # First page
        page1_response = Mock()
        page1_response.status_code = 200
        page1_response.json.return_value = {
            "data": [{
                "id": "10.5880/test.001",
                "attributes": {"url": "https://example.org/1", "publisher": "Publisher 1"}
            }],
            "links": {"next": "https://api.test.datacite.org/dois?page=2"}
        }
        
        # Second page
        page2_response = Mock()
        page2_response.status_code = 200
        page2_response.json.return_value = {
            "data": [{
                "id": "10.5880/test.002",
                "attributes": {"url": "https://example.org/2", "publisher": "Publisher 2"}
            }],
            "links": {}
        }
        
        with patch('requests.get', side_effect=[page1_response, page2_response]):
            result = client.fetch_all_dois_with_publisher()
        
        assert len(result) == 2
        assert result[0][0] == "10.5880/test.001"
        assert result[0][1] == "Publisher 1"
        assert result[1][0] == "10.5880/test.002"
        assert result[1][1] == "Publisher 2"
    
    def test_fetch_dois_with_publisher_missing_publisher(self, client):
        """Test DOI without publisher data is skipped."""
        # Publisher missing entirely - entry should be skipped
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "data": [{
                "id": "10.5880/test.001",
                "attributes": {"url": "https://example.org/1"}
                # No publisher field
            }],
            "links": {}
        }
        
        with patch('requests.get', return_value=mock_response):
            result = client.fetch_all_dois_with_publisher()
        
        # DOI without publisher should be skipped
        assert len(result) == 0
    
    def test_fetch_dois_with_publisher_auth_error(self, client):
        """Test authentication error during fetch."""
        mock_response = Mock()
        mock_response.status_code = 401
        mock_response.text = "Unauthorized"
        
        with patch('requests.get', return_value=mock_response):
            with pytest.raises(AuthenticationError):
                client.fetch_all_dois_with_publisher()
    
    def test_fetch_dois_with_publisher_network_error(self, client):
        """Test network error during fetch."""
        with patch('requests.get', side_effect=requests.exceptions.ConnectionError("Network error")):
            with pytest.raises(NetworkError):
                client.fetch_all_dois_with_publisher()
    
    def test_fetch_dois_with_publisher_timeout(self, client):
        """Test timeout error during fetch."""
        with patch('requests.get', side_effect=requests.exceptions.Timeout("Request timed out")):
            with pytest.raises(DataCiteAPIError):
                client.fetch_all_dois_with_publisher()


class TestUpdateDOIPublisher:
    """Tests for updating DOI publisher metadata."""
    
    def test_update_doi_publisher_simple_name(self, client):
        """Test updating publisher with simple name only."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": {"id": "10.5880/test.001"}}
        
        current_metadata = {
            "data": {
                "attributes": {
                    "publisher": "Old Publisher",
                    "titles": [{"title": "Test Title"}]
                }
            }
        }
        
        publisher_data = {"name": "GFZ German Research Centre for Geosciences"}
        
        with patch('requests.put', return_value=mock_response) as mock_put:
            success, message = client.update_doi_publisher(
                "10.5880/test.001",
                publisher_data,
                current_metadata
            )
        
        assert success is True
        
        # Verify the request
        call_args = mock_put.call_args
        request_data = call_args.kwargs.get('json') or call_args[1].get('json')
        
        # Should send simple string if no extended fields
        publisher = request_data['data']['attributes']['publisher']
        assert publisher == "GFZ German Research Centre for Geosciences"
    
    def test_update_doi_publisher_extended_format(self, client):
        """Test updating publisher with extended format (identifier, etc.)."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": {"id": "10.5880/test.001"}}
        
        current_metadata = {
            "data": {
                "attributes": {
                    "publisher": "Old Publisher",
                    "titles": [{"title": "Test Title"}]
                }
            }
        }
        
        publisher_data = {
            "name": "GFZ German Research Centre for Geosciences",
            "publisherIdentifier": "https://ror.org/04z8jg394",
            "publisherIdentifierScheme": "ROR",
            "schemeUri": "https://ror.org",
            "lang": "en"
        }
        
        with patch('requests.put', return_value=mock_response) as mock_put:
            success, message = client.update_doi_publisher(
                "10.5880/test.001",
                publisher_data,
                current_metadata
            )
        
        assert success is True
        
        # Verify the request has extended format
        call_args = mock_put.call_args
        request_data = call_args.kwargs.get('json') or call_args[1].get('json')
        
        publisher = request_data['data']['attributes']['publisher']
        assert isinstance(publisher, dict)
        assert publisher['name'] == "GFZ German Research Centre for Geosciences"
        assert publisher['publisherIdentifier'] == "https://ror.org/04z8jg394"
        assert publisher['publisherIdentifierScheme'] == "ROR"
        assert publisher['schemeUri'] == "https://ror.org"
        assert publisher['lang'] == "en"
    
    def test_update_doi_publisher_authentication_error(self, client):
        """Test authentication error during update returns failure tuple."""
        mock_response = Mock()
        mock_response.status_code = 401
        mock_response.text = "Unauthorized"
        
        current_metadata = {"data": {"attributes": {"publisher": "Old"}}}
        publisher_data = {"name": "New Publisher"}
        
        with patch('requests.put', return_value=mock_response):
            success, message = client.update_doi_publisher("10.5880/test.001", publisher_data, current_metadata)
            assert success is False
            assert "Authentifizierung" in message or "401" in message
    
    def test_update_doi_publisher_forbidden(self, client):
        """Test forbidden error (wrong client for DOI)."""
        mock_response = Mock()
        mock_response.status_code = 403
        mock_response.text = "Forbidden"
        
        current_metadata = {"data": {"attributes": {"publisher": "Old"}}}
        publisher_data = {"name": "New Publisher"}
        
        with patch('requests.put', return_value=mock_response):
            success, message = client.update_doi_publisher("10.5880/test.001", publisher_data, current_metadata)
            assert success is False
            assert "Berechtigung" in message or "403" in message
    
    def test_update_doi_publisher_not_found(self, client):
        """Test DOI not found error."""
        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.text = "Not found"
        
        current_metadata = {"data": {"attributes": {"publisher": "Old"}}}
        publisher_data = {"name": "New Publisher"}
        
        with patch('requests.put', return_value=mock_response):
            success, message = client.update_doi_publisher("10.5880/nonexistent", publisher_data, current_metadata)
            assert success is False
            assert "nicht gefunden" in message.lower() or "not found" in message.lower() or "404" in message
    
    def test_update_doi_publisher_validation_error(self, client):
        """Test validation error from API - missing publisher name."""
        current_metadata = {"data": {"attributes": {"publisher": "Old"}}}
        publisher_data = {"name": ""}  # Empty name
        
        # The method should return error tuple for empty name
        success, message = client.update_doi_publisher("10.5880/test.001", publisher_data, current_metadata)
        assert success is False
        assert "Publisher" in message or "fehlt" in message
    
    def test_update_doi_publisher_network_error(self, client):
        """Test network error during update."""
        current_metadata = {"data": {"attributes": {"publisher": "Old"}}}
        publisher_data = {"name": "New Publisher"}
        
        with patch('requests.put', side_effect=requests.exceptions.ConnectionError("Network error")):
            with pytest.raises(NetworkError):
                client.update_doi_publisher("10.5880/test.001", publisher_data, current_metadata)
    
    def test_update_doi_publisher_timeout(self, client):
        """Test timeout during update returns failure tuple."""
        current_metadata = {"data": {"attributes": {"publisher": "Old"}}}
        publisher_data = {"name": "New Publisher"}
        
        with patch('requests.put', side_effect=requests.exceptions.Timeout("Request timed out")):
            success, message = client.update_doi_publisher("10.5880/test.001", publisher_data, current_metadata)
            assert success is False
            assert "Timeout" in message or "Zeitüberschreitung" in message
    
    def test_update_doi_publisher_rate_limit(self, client):
        """Test rate limiting error."""
        mock_response = Mock()
        mock_response.status_code = 429
        mock_response.text = "Too many requests"
        
        current_metadata = {"data": {"attributes": {"publisher": "Old"}}}
        publisher_data = {"name": "New Publisher"}
        
        with patch('requests.put', return_value=mock_response):
            success, message = client.update_doi_publisher("10.5880/test.001", publisher_data, current_metadata)
            assert success is False
            assert "429" in message or "Rate" in message or "zu viele" in message.lower()
    
    def test_update_doi_publisher_partial_extended_format(self, client):
        """Test updating with only some extended fields."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": {"id": "10.5880/test.001"}}
        
        current_metadata = {
            "data": {
                "attributes": {
                    "publisher": "Old Publisher",
                    "titles": [{"title": "Test Title"}]
                }
            }
        }
        
        # Only name and identifier, no scheme or URI
        publisher_data = {
            "name": "GFZ German Research Centre for Geosciences",
            "publisherIdentifier": "https://ror.org/04z8jg394"
        }
        
        with patch('requests.put', return_value=mock_response) as mock_put:
            success, message = client.update_doi_publisher(
                "10.5880/test.001",
                publisher_data,
                current_metadata
            )
        
        assert success is True
        
        # Should still use extended format because identifier is provided
        call_args = mock_put.call_args
        request_data = call_args.kwargs.get('json') or call_args[1].get('json')
        
        publisher = request_data['data']['attributes']['publisher']
        assert isinstance(publisher, dict)
        assert publisher['name'] == "GFZ German Research Centre for Geosciences"
        assert publisher['publisherIdentifier'] == "https://ror.org/04z8jg394"


class TestGetDOIMetadataPublisher:
    """Tests for getting DOI metadata with publisher info."""
    
    def test_get_metadata_with_string_publisher(self, client):
        """Test getting metadata where publisher is a string."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "data": {
                "id": "10.5880/test.001",
                "attributes": {
                    "publisher": "Simple Publisher Name"
                }
            }
        }
        
        with patch('requests.get', return_value=mock_response):
            result = client.get_doi_metadata("10.5880/test.001")
        
        assert result["data"]["attributes"]["publisher"] == "Simple Publisher Name"
    
    def test_get_metadata_with_extended_publisher(self, client):
        """Test getting metadata where publisher is an object."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "data": {
                "id": "10.5880/test.001",
                "attributes": {
                    "publisher": {
                        "name": "Extended Publisher",
                        "publisherIdentifier": "https://ror.org/12345",
                        "publisherIdentifierScheme": "ROR"
                    }
                }
            }
        }
        
        with patch('requests.get', return_value=mock_response):
            result = client.get_doi_metadata("10.5880/test.001")
        
        publisher = result["data"]["attributes"]["publisher"]
        assert isinstance(publisher, dict)
        assert publisher["name"] == "Extended Publisher"
        assert publisher["publisherIdentifier"] == "https://ror.org/12345"
