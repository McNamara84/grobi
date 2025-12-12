"""Tests for DataCite Client update_doi_url method."""

import json

import pytest
import responses
from unittest.mock import Mock, patch
import requests

from src.api.datacite_client import DataCiteClient, NetworkError


class TestDataCiteClientUpdate:
    """Test suite for DataCite Client URL update functionality."""
    
    @pytest.fixture
    def client(self):
        """Create a DataCite client instance."""
        return DataCiteClient("test_user", "test_pass", use_test_api=True)
    
    def test_update_doi_url_success(self, client):
        """Test successful DOI URL update."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = "OK"
        
        with patch('requests.put', return_value=mock_response) as mock_put:
            success, message = client.update_doi_url(
                "10.5880/GFZ.1.1.2021.001",
                "https://new-url.example.org"
            )
            
            assert success is True
            assert "erfolgreich aktualisiert" in message
            
            # Verify PUT request was made correctly
            mock_put.assert_called_once()
            call_args = mock_put.call_args
            
            # Check URL
            assert "10.5880/GFZ.1.1.2021.001" in call_args[0][0]
            
            # Check JSON payload
            assert call_args[1]['json']['data']['type'] == 'dois'
            assert call_args[1]['json']['data']['attributes']['url'] == "https://new-url.example.org"
            
            # Check headers
            assert call_args[1]['headers']['Content-Type'] == 'application/vnd.api+json'
    
    def test_update_doi_url_with_colon_in_query(self, client):
        """Test that URLs with colons in query parameters are properly encoded."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = "OK"
        
        with patch('requests.put', return_value=mock_response) as mock_put:
            # URL with colon that needs encoding
            original_url = "http://dataservices.gfz.de/panmetaworks/showshort.php?id=escidoc:43448"
            expected_url = "http://dataservices.gfz.de/panmetaworks/showshort.php?id=escidoc%3A43448"
            
            success, message = client.update_doi_url(
                "10.5880/gfz.2011.100",
                original_url
            )
            
            assert success is True
            
            # Verify the URL was properly encoded before sending
            call_args = mock_put.call_args
            sent_url = call_args[1]['json']['data']['attributes']['url']
            assert sent_url == expected_url, f"Expected {expected_url}, got {sent_url}"
            # Query params should not contain unencoded colons
            query_part = sent_url.split("?")[1] if "?" in sent_url else ""
            assert ":" not in query_part, "Query params should not contain unencoded colons"
    
    def test_update_doi_url_authentication_error(self, client):
        """Test update with authentication error."""
        mock_response = Mock()
        mock_response.status_code = 401
        mock_response.text = "Unauthorized"
        
        with patch('requests.put', return_value=mock_response):
            success, message = client.update_doi_url(
                "10.5880/GFZ.1.1.2021.001",
                "https://new-url.example.org"
            )
            
            assert success is False
            assert "Authentifizierung fehlgeschlagen" in message
    
    def test_update_doi_url_forbidden(self, client):
        """Test update with forbidden error (DOI belongs to another client)."""
        mock_response = Mock()
        mock_response.status_code = 403
        mock_response.text = "Forbidden"
        
        with patch('requests.put', return_value=mock_response):
            success, message = client.update_doi_url(
                "10.5880/GFZ.1.1.2021.001",
                "https://new-url.example.org"
            )
            
            assert success is False
            assert "Keine Berechtigung" in message
    
    def test_update_doi_url_not_found(self, client):
        """Test update with DOI not found error."""
        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.text = "Not Found"
        
        with patch('requests.put', return_value=mock_response):
            success, message = client.update_doi_url(
                "10.5880/GFZ.1.1.2021.999",
                "https://new-url.example.org"
            )
            
            assert success is False
            assert "nicht gefunden" in message
    
    def test_update_doi_url_validation_error(self, client):
        """Test update with validation error (invalid URL)."""
        mock_response = Mock()
        mock_response.status_code = 422
        mock_response.text = "Unprocessable Entity"
        mock_response.json.return_value = {
            "errors": [
                {
                    "title": "URL must be a valid HTTP(S) URL",
                    "source": "/data/attributes/url"
                }
            ]
        }
        
        with patch('requests.put', return_value=mock_response):
            success, message = client.update_doi_url(
                "10.5880/GFZ.1.1.2021.001",
                "invalid-url"
            )
            
            assert success is False
            assert "Validierungsfehler" in message
            assert "valid HTTP(S) URL" in message
    
    def test_update_doi_url_validation_error_no_json(self, client):
        """Test update with validation error without structured JSON response."""
        mock_response = Mock()
        mock_response.status_code = 422
        mock_response.text = "Unprocessable Entity: Invalid URL format"
        mock_response.json.side_effect = Exception("No JSON")
        
        with patch('requests.put', return_value=mock_response):
            success, message = client.update_doi_url(
                "10.5880/GFZ.1.1.2021.001",
                "invalid-url"
            )
            
            assert success is False
            assert "Validierungsfehler" in message
            assert "Invalid URL format" in message
    
    @responses.activate
    def test_update_doi_url_deprecated_schema_auto_upgrade_with_autofill(self, client):
        """Test update with deprecated schema automatically upgrades to kernel-4 and auto-fills resourceTypeGeneral."""
        doi = "10.1594/gfz.sddb.1010"
        new_url = "http://dataservices.gfz.de/SDDB/showshort.php?id=escidoc:76037"
        
        # First PUT: Returns schema deprecation error
        responses.add(
            responses.PUT,
            f"https://api.test.datacite.org/dois/{doi}",
            json={
                "errors": [{
                    "source": "xml",
                    "title": f"DOI {doi}: Schema http://datacite.org/schema/kernel-3 is no longer supported",
                    "uid": doi
                }]
            },
            status=422
        )
        
        # GET: Returns metadata without resourceTypeGeneral (will be auto-filled)
        responses.add(
            responses.GET,
            f"https://api.test.datacite.org/dois/{doi}",
            json={
                "data": {
                    "id": doi,
                    "type": "dois",
                    "attributes": {
                        "doi": doi,
                        "url": "http://old-url.example.org",
                        "titles": [{"title": "Test Dataset"}],
                        "creators": [{"name": "Test Creator"}],
                        "types": {}  # Missing resourceTypeGeneral - will be auto-filled with 'Dataset'
                    }
                }
            },
            status=200
        )
        
        # Second PUT: Successful upgrade after auto-fill
        responses.add(
            responses.PUT,
            f"https://api.test.datacite.org/dois/{doi}",
            json={
                "data": {
                    "id": doi,
                    "type": "dois",
                    "attributes": {
                        "doi": doi,
                        "url": "http://dataservices.gfz.de/SDDB/showshort.php?id=escidoc%3A76037",
                        "schemaVersion": "http://datacite.org/schema/kernel-4"
                    }
                }
            },
            status=200
        )
        
        success, message = client.update_doi_url(doi, new_url)
        
        # Should now succeed because resourceTypeGeneral is auto-filled with 'Dataset'
        assert success is True
        assert "erfolgreich aktualisiert" in message
        
        # Verify all 3 API calls: PUT (fail) → GET (metadata) → PUT (success)
        assert len(responses.calls) == 3
    
    def test_update_doi_url_rate_limit(self, client):
        """Test update with rate limit error."""
        mock_response = Mock()
        mock_response.status_code = 429
        mock_response.text = "Too Many Requests"
        
        with patch('requests.put', return_value=mock_response):
            success, message = client.update_doi_url(
                "10.5880/GFZ.1.1.2021.001",
                "https://new-url.example.org"
            )
            
            assert success is False
            assert "Rate Limit" in message
    
    def test_update_doi_url_timeout(self, client):
        """Test update with timeout error."""
        with patch('requests.put', side_effect=requests.exceptions.Timeout):
            success, message = client.update_doi_url(
                "10.5880/GFZ.1.1.2021.001",
                "https://new-url.example.org"
            )
            
            assert success is False
            assert "Zeitüberschreitung" in message
    
    def test_update_doi_url_connection_error(self, client):
        """Test update with connection error."""
        with patch('requests.put', side_effect=requests.exceptions.ConnectionError):
            with pytest.raises(NetworkError):
                client.update_doi_url(
                    "10.5880/GFZ.1.1.2021.001",
                    "https://new-url.example.org"
                )
    
    def test_update_doi_url_unexpected_status(self, client):
        """Test update with unexpected status code."""
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.text = "Internal Server Error"
        
        with patch('requests.put', return_value=mock_response):
            success, message = client.update_doi_url(
                "10.5880/GFZ.1.1.2021.001",
                "https://new-url.example.org"
            )
            
            assert success is False
            assert "API Fehler" in message
            assert "500" in message
    
    def test_update_doi_url_production_endpoint(self):
        """Test that production client uses correct endpoint."""
        client = DataCiteClient("test_user", "test_pass", use_test_api=False)
        
        mock_response = Mock()
        mock_response.status_code = 200
        
        with patch('requests.put', return_value=mock_response) as mock_put:
            client.update_doi_url("10.5880/GFZ.1.1.2021.001", "https://example.org")
            
            # Check that production endpoint was used
            call_url = mock_put.call_args[0][0]
            assert "api.datacite.org" in call_url
            assert "api.test.datacite.org" not in call_url
    
    def test_update_doi_url_test_endpoint(self):
        """Test that test client uses correct endpoint."""
        client = DataCiteClient("test_user", "test_pass", use_test_api=True)
        
        mock_response = Mock()
        mock_response.status_code = 200
        
        with patch('requests.put', return_value=mock_response) as mock_put:
            client.update_doi_url("10.5880/GFZ.1.1.2021.001", "https://example.org")
            
            # Check that test endpoint was used
            call_url = mock_put.call_args[0][0]
            assert "api.test.datacite.org" in call_url
