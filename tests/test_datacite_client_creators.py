"""Tests for DataCite API client creator fetching functionality."""

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


def create_mock_response_with_creators(dois_data, has_next=False):
    """
    Create a mock response with creator data.
    
    Args:
        dois_data: List of dicts with 'id' and 'creators' keys
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
                "creators": doi_data.get("creators", [])
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


def test_fetch_dois_with_creators_success(client):
    """Test successful fetch of DOIs with creators."""
    dois_data = [
        {
            "id": "10.5880/test.001",
            "creators": [
                {
                    "name": "Miller, Elizabeth",
                    "nameType": "Personal",
                    "givenName": "Elizabeth",
                    "familyName": "Miller",
                    "nameIdentifiers": [
                        {
                            "nameIdentifier": "https://orcid.org/0000-0001-5000-0007",
                            "nameIdentifierScheme": "ORCID",
                            "schemeUri": "https://orcid.org"
                        }
                    ]
                }
            ]
        }
    ]
    
    mock_response = create_mock_response_with_creators(dois_data)
    
    with patch('requests.get', return_value=mock_response):
        result = client.fetch_all_dois_with_creators()
    
    assert len(result) == 1
    assert result[0][0] == "10.5880/test.001"
    assert result[0][1] == "Miller, Elizabeth"
    assert result[0][2] == "Personal"
    assert result[0][3] == "Elizabeth"
    assert result[0][4] == "Miller"
    assert result[0][5] == "https://orcid.org/0000-0001-5000-0007"
    assert result[0][6] == "ORCID"
    assert result[0][7] == "https://orcid.org"


def test_fetch_dois_with_creators_multiple_per_doi(client):
    """Test DOI with multiple creators returns multiple rows."""
    dois_data = [
        {
            "id": "10.5880/test.001",
            "creators": [
                {
                    "name": "Smith, John",
                    "nameType": "Personal",
                    "givenName": "John",
                    "familyName": "Smith",
                    "nameIdentifiers": []
                },
                {
                    "name": "Doe, Jane",
                    "nameType": "Personal",
                    "givenName": "Jane",
                    "familyName": "Doe",
                    "nameIdentifiers": [
                        {
                            "nameIdentifier": "https://orcid.org/0000-0002-1234-5678",
                            "nameIdentifierScheme": "ORCID",
                            "schemeUri": "https://orcid.org"
                        }
                    ]
                }
            ]
        }
    ]
    
    mock_response = create_mock_response_with_creators(dois_data)
    
    with patch('requests.get', return_value=mock_response):
        result = client.fetch_all_dois_with_creators()
    
    assert len(result) == 2
    # First creator - no ORCID
    assert result[0][0] == "10.5880/test.001"
    assert result[0][1] == "Smith, John"
    assert result[0][5] == ""  # No ORCID
    # Second creator - with ORCID
    assert result[1][0] == "10.5880/test.001"
    assert result[1][1] == "Doe, Jane"
    assert result[1][5] == "https://orcid.org/0000-0002-1234-5678"


def test_fetch_dois_with_creators_organizational(client):
    """Test organizational creators (no givenName/familyName)."""
    dois_data = [
        {
            "id": "10.5880/test.001",
            "creators": [
                {
                    "name": "GFZ Data Services",
                    "nameType": "Organizational",
                    "nameIdentifiers": []
                }
            ]
        }
    ]
    
    mock_response = create_mock_response_with_creators(dois_data)
    
    with patch('requests.get', return_value=mock_response):
        result = client.fetch_all_dois_with_creators()
    
    assert len(result) == 1
    assert result[0][1] == "GFZ Data Services"
    assert result[0][2] == "Organizational"
    assert result[0][3] == ""  # No givenName
    assert result[0][4] == ""  # No familyName
    assert result[0][5] == ""  # No ORCID


def test_fetch_dois_with_creators_no_orcid(client):
    """Test creators without ORCID identifier."""
    dois_data = [
        {
            "id": "10.5880/test.001",
            "creators": [
                {
                    "name": "Smith, John",
                    "nameType": "Personal",
                    "givenName": "John",
                    "familyName": "Smith",
                    "nameIdentifiers": []
                }
            ]
        }
    ]
    
    mock_response = create_mock_response_with_creators(dois_data)
    
    with patch('requests.get', return_value=mock_response):
        result = client.fetch_all_dois_with_creators()
    
    assert len(result) == 1
    assert result[0][5] == ""  # No name identifier
    assert result[0][6] == ""  # No scheme
    assert result[0][7] == ""  # No scheme URI


def test_fetch_dois_with_creators_mixed_identifiers(client):
    """Test that only ORCID identifiers are included, others ignored."""
    dois_data = [
        {
            "id": "10.5880/test.001",
            "creators": [
                {
                    "name": "Smith, John",
                    "nameType": "Personal",
                    "givenName": "John",
                    "familyName": "Smith",
                    "nameIdentifiers": [
                        {
                            "nameIdentifier": "https://researcherid.com/rid/A-1234-2019",
                            "nameIdentifierScheme": "ResearcherID",
                            "schemeUri": "https://researcherid.com"
                        },
                        {
                            "nameIdentifier": "https://orcid.org/0000-0001-2345-6789",
                            "nameIdentifierScheme": "ORCID",
                            "schemeUri": "https://orcid.org"
                        }
                    ]
                }
            ]
        }
    ]
    
    mock_response = create_mock_response_with_creators(dois_data)
    
    with patch('requests.get', return_value=mock_response):
        result = client.fetch_all_dois_with_creators()
    
    assert len(result) == 1
    # Should only include ORCID, not ResearcherID
    assert result[0][5] == "https://orcid.org/0000-0001-2345-6789"
    assert result[0][6] == "ORCID"


def test_fetch_dois_with_creators_no_creators(client):
    """Test DOIs without creators are skipped."""
    dois_data = [
        {
            "id": "10.5880/test.001",
            "creators": []
        },
        {
            "id": "10.5880/test.002",
            "creators": [
                {
                    "name": "Smith, John",
                    "nameType": "Personal",
                    "givenName": "John",
                    "familyName": "Smith",
                    "nameIdentifiers": []
                }
            ]
        }
    ]
    
    mock_response = create_mock_response_with_creators(dois_data)
    
    with patch('requests.get', return_value=mock_response):
        result = client.fetch_all_dois_with_creators()
    
    # Only the second DOI should be included
    assert len(result) == 1
    assert result[0][0] == "10.5880/test.002"


def test_fetch_dois_with_creators_pagination(client):
    """Test pagination works correctly."""
    # First page
    dois_data_page1 = [
        {
            "id": "10.5880/test.001",
            "creators": [
                {
                    "name": "Smith, John",
                    "nameType": "Personal",
                    "givenName": "John",
                    "familyName": "Smith",
                    "nameIdentifiers": []
                }
            ]
        }
    ]
    
    # Second page
    dois_data_page2 = [
        {
            "id": "10.5880/test.002",
            "creators": [
                {
                    "name": "Doe, Jane",
                    "nameType": "Personal",
                    "givenName": "Jane",
                    "familyName": "Doe",
                    "nameIdentifiers": []
                }
            ]
        }
    ]
    
    mock_response_page1 = create_mock_response_with_creators(dois_data_page1, has_next=True)
    mock_response_page2 = create_mock_response_with_creators(dois_data_page2, has_next=False)
    
    with patch('requests.get', side_effect=[mock_response_page1, mock_response_page2]):
        result = client.fetch_all_dois_with_creators()
    
    assert len(result) == 2
    assert result[0][0] == "10.5880/test.001"
    assert result[1][0] == "10.5880/test.002"


def test_fetch_dois_with_creators_auth_error(client):
    """Test authentication error handling."""
    mock_response = Mock()
    mock_response.status_code = 401
    
    with patch('requests.get', return_value=mock_response):
        with pytest.raises(AuthenticationError) as exc_info:
            client.fetch_all_dois_with_creators()
        
        assert "Anmeldung fehlgeschlagen" in str(exc_info.value)


def test_fetch_dois_with_creators_network_error(client):
    """Test network error handling."""
    with patch('requests.get', side_effect=requests.exceptions.ConnectionError()):
        with pytest.raises(NetworkError) as exc_info:
            client.fetch_all_dois_with_creators()
        
        assert "Verbindung zur DataCite API fehlgeschlagen" in str(exc_info.value)


def test_fetch_dois_with_creators_timeout(client):
    """Test timeout error handling."""
    with patch('requests.get', side_effect=requests.exceptions.Timeout()):
        with pytest.raises(DataCiteAPIError) as exc_info:
            client.fetch_all_dois_with_creators()
        
        assert "zu lange gedauert" in str(exc_info.value).lower()


def test_fetch_dois_with_creators_rate_limit(client):
    """Test rate limiting error handling."""
    mock_response = Mock()
    mock_response.status_code = 429
    
    with patch('requests.get', return_value=mock_response):
        with pytest.raises(DataCiteAPIError) as exc_info:
            client.fetch_all_dois_with_creators()
        
        assert "Zu viele Anfragen" in str(exc_info.value)


def test_fetch_dois_with_creators_invalid_json(client):
    """Test invalid JSON response handling."""
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.side_effect = ValueError("Invalid JSON")
    
    with patch('requests.get', return_value=mock_response):
        with pytest.raises(DataCiteAPIError) as exc_info:
            client.fetch_all_dois_with_creators()
        
        assert "Ungültige Antwort" in str(exc_info.value)
