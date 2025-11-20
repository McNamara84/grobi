"""Tests for DataCite Client author/creator update functionality."""

import pytest
from unittest.mock import Mock, patch
import requests

from src.api.datacite_client import (
    DataCiteClient,
    DataCiteAPIError,
    AuthenticationError,
    NetworkError
)


class TestDataCiteClientAuthorsUpdate:
    """Test suite for DataCite Client creator update functionality."""
    
    @pytest.fixture
    def client(self):
        """Create a DataCite client instance."""
        return DataCiteClient("test_user", "test_pass", use_test_api=True)
    
    @pytest.fixture
    def sample_metadata(self):
        """Sample DOI metadata with creators."""
        return {
            "data": {
                "id": "10.5880/GFZ.1.1.2021.001",
                "type": "dois",
                "attributes": {
                    "doi": "10.5880/GFZ.1.1.2021.001",
                    "url": "https://example.org/dataset/001",
                    "titles": [{"title": "Test Dataset"}],
                    "publisher": "Test Publisher",
                    "publicationYear": 2021,
                    "creators": [
                        {
                            "name": "Smith, John",
                            "nameType": "Personal",
                            "givenName": "John",
                            "familyName": "Smith"
                        },
                        {
                            "name": "Doe, Jane",
                            "nameType": "Personal",
                            "givenName": "Jane",
                            "familyName": "Doe"
                        }
                    ]
                }
            }
        }
    
    # Tests for get_doi_metadata()
    
    def test_get_doi_metadata_success(self, client, sample_metadata):
        """Test successful metadata retrieval."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = sample_metadata
        
        with patch('requests.get', return_value=mock_response) as mock_get:
            metadata = client.get_doi_metadata("10.5880/GFZ.1.1.2021.001")
            
            assert metadata is not None
            assert metadata == sample_metadata
            assert metadata["data"]["id"] == "10.5880/GFZ.1.1.2021.001"
            assert len(metadata["data"]["attributes"]["creators"]) == 2
            
            # Verify GET request
            mock_get.assert_called_once()
            call_args = mock_get.call_args
            assert "10.5880/GFZ.1.1.2021.001" in call_args[0][0]
    
    def test_get_doi_metadata_not_found(self, client):
        """Test metadata retrieval for non-existent DOI."""
        mock_response = Mock()
        mock_response.status_code = 404
        
        with patch('requests.get', return_value=mock_response):
            metadata = client.get_doi_metadata("10.5880/GFZ.1.1.2021.999")
            
            assert metadata is None
    
    def test_get_doi_metadata_authentication_error(self, client):
        """Test metadata retrieval with authentication error."""
        mock_response = Mock()
        mock_response.status_code = 401
        
        with patch('requests.get', return_value=mock_response):
            with pytest.raises(AuthenticationError):
                client.get_doi_metadata("10.5880/GFZ.1.1.2021.001")
    
    def test_get_doi_metadata_rate_limit(self, client):
        """Test metadata retrieval with rate limit error."""
        mock_response = Mock()
        mock_response.status_code = 429
        
        with patch('requests.get', return_value=mock_response):
            with pytest.raises(DataCiteAPIError) as exc_info:
                client.get_doi_metadata("10.5880/GFZ.1.1.2021.001")
            
            assert "Zu viele Anfragen" in str(exc_info.value)
    
    def test_get_doi_metadata_invalid_json(self, client):
        """Test metadata retrieval with invalid JSON response."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.side_effect = ValueError("Invalid JSON")
        
        with patch('requests.get', return_value=mock_response):
            with pytest.raises(DataCiteAPIError) as exc_info:
                client.get_doi_metadata("10.5880/GFZ.1.1.2021.001")
            
            assert "Ungültige JSON-Antwort" in str(exc_info.value)
    
    def test_get_doi_metadata_network_error(self, client):
        """Test metadata retrieval with network error."""
        with patch('requests.get', side_effect=requests.exceptions.ConnectionError("Network error")):
            with pytest.raises(NetworkError):
                client.get_doi_metadata("10.5880/GFZ.1.1.2021.001")
    
    # Tests for validate_creators_match()
    
    def test_validate_creators_match_success(self, client, sample_metadata):
        """Test successful creator validation with matching counts."""
        csv_creators = [
            {"name": "Smith, John", "nameType": "Personal"},
            {"name": "Doe, Jane", "nameType": "Personal"}
        ]
        
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = sample_metadata
        
        with patch('requests.get', return_value=mock_response):
            is_valid, message = client.validate_creators_match(
                "10.5880/GFZ.1.1.2021.001",
                csv_creators
            )
            
            assert is_valid is True
            assert "validiert" in message
    
    def test_validate_creators_match_count_mismatch(self, client, sample_metadata):
        """Test creator validation with mismatched counts."""
        csv_creators = [
            {"name": "Smith, John", "nameType": "Personal"}
            # Missing second creator
        ]
        
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = sample_metadata
        
        with patch('requests.get', return_value=mock_response):
            is_valid, message = client.validate_creators_match(
                "10.5880/GFZ.1.1.2021.001",
                csv_creators
            )
            
            assert is_valid is False
            assert "Anzahl" in message
            assert "DataCite: 2, CSV: 1" in message
    
    def test_validate_creators_match_no_creators(self, client):
        """Test creator validation with no creators in both sources."""
        metadata = {
            "data": {
                "id": "10.5880/GFZ.1.1.2021.001",
                "type": "dois",
                "attributes": {
                    "creators": []
                }
            }
        }
        
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = metadata
        
        with patch('requests.get', return_value=mock_response):
            is_valid, message = client.validate_creators_match(
                "10.5880/GFZ.1.1.2021.001",
                []
            )
            
            assert is_valid is True
            assert "Keine Creators" in message
    
    def test_validate_creators_match_doi_not_found(self, client):
        """Test creator validation with non-existent DOI."""
        mock_response = Mock()
        mock_response.status_code = 404
        
        with patch('requests.get', return_value=mock_response):
            is_valid, message = client.validate_creators_match(
                "10.5880/GFZ.1.1.2021.999",
                []
            )
            
            assert is_valid is False
            assert "nicht gefunden" in message
    
    # Tests for update_doi_creators()
    
    def test_update_doi_creators_success(self, client, sample_metadata):
        """Test successful creator update."""
        new_creators = [
            {
                "name": "Smith, John",
                "nameType": "Personal",
                "givenName": "John",
                "familyName": "Smith",
                "nameIdentifier": "0000-0001-5000-0007",
                "nameIdentifierScheme": "ORCID",
                "schemeUri": "https://orcid.org"
            },
            {
                "name": "Doe, Jane",
                "nameType": "Personal",
                "givenName": "Jane",
                "familyName": "Doe",
                "nameIdentifier": "",
                "nameIdentifierScheme": "",
                "schemeUri": ""
            }
        ]
        
        mock_response = Mock()
        mock_response.status_code = 200
        
        with patch('requests.put', return_value=mock_response) as mock_put:
            success, message = client.update_doi_creators(
                "10.5880/GFZ.1.1.2021.001",
                new_creators,
                sample_metadata
            )
            
            assert success is True
            assert "erfolgreich aktualisiert" in message
            
            # Verify PUT request
            mock_put.assert_called_once()
            call_args = mock_put.call_args
            
            # Check URL
            assert "10.5880/GFZ.1.1.2021.001" in call_args[0][0]
            
            # Check JSON payload
            payload = call_args[1]['json']
            assert payload['data']['type'] == 'dois'
            assert 'creators' in payload['data']['attributes']
            
            # Verify creators were updated
            updated_creators = payload['data']['attributes']['creators']
            assert len(updated_creators) == 2
            assert updated_creators[0]['name'] == "Smith, John"
            assert 'nameIdentifiers' in updated_creators[0]
            assert updated_creators[0]['nameIdentifiers'][0]['nameIdentifier'] == "0000-0001-5000-0007"
    
    def test_update_doi_creators_with_organizational(self, client, sample_metadata):
        """Test creator update with organizational creator."""
        new_creators = [
            {
                "name": "Example Organization",
                "nameType": "Organizational",
                "givenName": "",
                "familyName": "",
                "nameIdentifier": "",
                "nameIdentifierScheme": "",
                "schemeUri": ""
            }
        ]
        
        # Modify metadata to have one organizational creator
        sample_metadata["data"]["attributes"]["creators"] = [
            {"name": "Example Organization", "nameType": "Organizational"}
        ]
        
        mock_response = Mock()
        mock_response.status_code = 200
        
        with patch('requests.put', return_value=mock_response) as mock_put:
            success, message = client.update_doi_creators(
                "10.5880/GFZ.1.1.2021.001",
                new_creators,
                sample_metadata
            )
            
            assert success is True
            
            # Verify organizational creator has no givenName/familyName
            payload = mock_put.call_args[1]['json']
            updated_creator = payload['data']['attributes']['creators'][0]
            assert updated_creator['nameType'] == "Organizational"
            assert 'givenName' not in updated_creator
            assert 'familyName' not in updated_creator
    
    def test_update_doi_creators_authentication_error(self, client, sample_metadata):
        """Test creator update with authentication error."""
        mock_response = Mock()
        mock_response.status_code = 401
        
        with patch('requests.put', return_value=mock_response):
            success, message = client.update_doi_creators(
                "10.5880/GFZ.1.1.2021.001",
                [],
                sample_metadata
            )
            
            assert success is False
            assert "Authentifizierung fehlgeschlagen" in message
    
    def test_update_doi_creators_forbidden(self, client, sample_metadata):
        """Test creator update with forbidden error."""
        mock_response = Mock()
        mock_response.status_code = 403
        
        with patch('requests.put', return_value=mock_response):
            success, message = client.update_doi_creators(
                "10.5880/GFZ.1.1.2021.001",
                [],
                sample_metadata
            )
            
            assert success is False
            assert "Keine Berechtigung" in message
    
    def test_update_doi_creators_not_found(self, client, sample_metadata):
        """Test creator update with non-existent DOI."""
        mock_response = Mock()
        mock_response.status_code = 404
        
        with patch('requests.put', return_value=mock_response):
            success, message = client.update_doi_creators(
                "10.5880/GFZ.1.1.2021.999",
                [],
                sample_metadata
            )
            
            assert success is False
            assert "nicht gefunden" in message
    
    def test_update_doi_creators_validation_error(self, client, sample_metadata):
        """Test creator update with validation error."""
        mock_response = Mock()
        mock_response.status_code = 422
        mock_response.text = "Validation failed"
        
        with patch('requests.put', return_value=mock_response):
            success, message = client.update_doi_creators(
                "10.5880/GFZ.1.1.2021.001",
                [],
                sample_metadata
            )
            
            assert success is False
            assert "Validierungsfehler" in message
    
    def test_update_doi_creators_network_error(self, client, sample_metadata):
        """Test creator update with network error."""
        with patch('requests.put', side_effect=requests.exceptions.ConnectionError("Network error")):
            with pytest.raises(NetworkError):
                client.update_doi_creators(
                    "10.5880/GFZ.1.1.2021.001",
                    [],
                    sample_metadata
                )
