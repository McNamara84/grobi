"""
Tests for DataCite client auto-fill of missing mandatory fields.

Tests the behavior when DOIs have missing mandatory fields:
- Auto-fill resourceTypeGeneral with "Dataset"
- Auto-fill publisher with "GFZ Data Services"
- Require title and creators (cannot auto-fill)
"""

import json
import pytest
import responses
from src.api.datacite_client import DataCiteClient


@pytest.fixture
def client():
    """Create a DataCite client for testing."""
    return DataCiteClient("test_user", "test_password", use_test_api=True)


@responses.activate
def test_auto_fill_resource_type_and_publisher(client):
    """Test that resourceTypeGeneral and publisher are auto-filled if missing."""
    doi = "10.1594/gfz.test.001"
    new_url = "https://example.com/test"
    
    # Mock initial PUT with schema error
    responses.add(
        responses.PUT,
        f"https://api.test.datacite.org/dois/{doi}",
        json={
            "errors": [{
                "title": "DOI 10.1594/gfz.test.001: Schema http://datacite.org/schema/kernel-3 is no longer supported"
            }]
        },
        status=422
    )
    
    # Mock GET for metadata (missing resourceTypeGeneral and publisher)
    responses.add(
        responses.GET,
        f"https://api.test.datacite.org/dois/{doi}",
        json={
            "data": {
                "attributes": {
                    "titles": [{"title": "Test Dataset"}],
                    "creators": [{"name": "Test Creator"}],
                    "publicationYear": 2024,
                    "types": {
                        # resourceTypeGeneral is missing
                        "ris": "DATA",
                        "bibtex": "misc"
                    }
                    # publisher is missing
                }
            }
        },
        status=200
    )
    
    # Mock successful PUT with auto-filled fields
    responses.add(
        responses.PUT,
        f"https://api.test.datacite.org/dois/{doi}",
        json={"data": {"id": doi}},
        status=200
    )
    
    success, message = client.update_doi_url(doi, new_url)
    
    assert success is True
    assert "kernel-4" in message
    
    # Verify the retry PUT was called with auto-filled fields
    assert len(responses.calls) == 3
    retry_request = responses.calls[2].request
    payload = json.loads(retry_request.body)
    
    # Check auto-filled resourceTypeGeneral
    assert payload['data']['attributes']['types']['resourceTypeGeneral'] == 'Dataset'
    # Check auto-filled publisher
    assert payload['data']['attributes']['publisher'] == 'GFZ Data Services'
    # Check schemaVersion was set
    assert payload['data']['attributes']['schemaVersion'] == 'http://datacite.org/schema/kernel-4'


@responses.activate
def test_missing_title_cannot_auto_fill(client):
    """Test that DOI with missing title fails with helpful error."""
    doi = "10.1594/gfz.test.002"
    new_url = "https://example.com/test"
    
    # Mock initial PUT with schema error
    responses.add(
        responses.PUT,
        f"https://api.test.datacite.org/dois/{doi}",
        json={
            "errors": [{
                "title": "DOI 10.1594/gfz.test.002: Schema http://datacite.org/schema/kernel-3 is no longer supported"
            }]
        },
        status=422
    )
    
    # Mock GET for metadata (missing title)
    responses.add(
        responses.GET,
        f"https://api.test.datacite.org/dois/{doi}",
        json={
            "data": {
                "attributes": {
                    "titles": [],  # Empty titles
                    "creators": [{"name": "Test Creator"}],
                    "publicationYear": 2024,
                    "types": {"resourceTypeGeneral": "Dataset"},
                    "publisher": "GFZ Data Services"
                }
            }
        },
        status=200
    )
    
    success, message = client.update_doi_url(doi, new_url)
    
    assert success is False
    assert "title" in message.lower()
    assert "Pflichtfeld" in message
    assert "Fabrica" in message


@responses.activate
def test_missing_creators_cannot_auto_fill(client):
    """Test that DOI with missing creators fails with helpful error."""
    doi = "10.1594/gfz.test.003"
    new_url = "https://example.com/test"
    
    # Mock initial PUT with schema error
    responses.add(
        responses.PUT,
        f"https://api.test.datacite.org/dois/{doi}",
        json={
            "errors": [{
                "title": "DOI 10.1594/gfz.test.003: Schema http://datacite.org/schema/kernel-3 is no longer supported"
            }]
        },
        status=422
    )
    
    # Mock GET for metadata (missing creators)
    responses.add(
        responses.GET,
        f"https://api.test.datacite.org/dois/{doi}",
        json={
            "data": {
                "attributes": {
                    "titles": [{"title": "Test Dataset"}],
                    "creators": [],  # Empty creators
                    "publicationYear": 2024,
                    "types": {"resourceTypeGeneral": "Dataset"},
                    "publisher": "GFZ Data Services"
                }
            }
        },
        status=200
    )
    
    success, message = client.update_doi_url(doi, new_url)
    
    assert success is False
    assert "creators" in message.lower()
    assert "Pflichtfeld" in message
    assert "Fabrica" in message


@responses.activate
def test_missing_title_and_creators(client):
    """Test that DOI with both missing title and creators shows both in error."""
    doi = "10.1594/gfz.test.004"
    new_url = "https://example.com/test"
    
    # Mock initial PUT with schema error
    responses.add(
        responses.PUT,
        f"https://api.test.datacite.org/dois/{doi}",
        json={
            "errors": [{
                "title": "DOI 10.1594/gfz.test.004: Schema http://datacite.org/schema/kernel-3 is no longer supported"
            }]
        },
        status=422
    )
    
    # Mock GET for metadata (missing both title and creators)
    responses.add(
        responses.GET,
        f"https://api.test.datacite.org/dois/{doi}",
        json={
            "data": {
                "attributes": {
                    "titles": [],
                    "creators": [],
                    "publicationYear": 2024,
                    "types": {},  # Also missing resourceTypeGeneral
                    # Also missing publisher
                }
            }
        },
        status=200
    )
    
    success, message = client.update_doi_url(doi, new_url)
    
    assert success is False
    assert "title" in message.lower()
    assert "creators" in message.lower()
    assert "und" in message  # Both fields mentioned with "und"


@responses.activate
def test_doi_with_no_schema_version_gets_upgraded(client):
    """Test that DOI with missing schemaVersion gets kernel-4 added."""
    doi = "10.1594/gfz.test.005"
    new_url = "https://example.com/test"
    
    # Mock initial PUT with "No matching global declaration" error (missing schemaVersion)
    responses.add(
        responses.PUT,
        f"https://api.test.datacite.org/dois/{doi}",
        json={
            "errors": [{
                "title": "DOI 10.1594/gfz.test.005: No matching global declaration available for the validation root. at line 2, column 0"
            }]
        },
        status=422
    )
    
    # Mock GET for metadata (DOI with all required fields but missing schemaVersion)
    responses.add(
        responses.GET,
        f"https://api.test.datacite.org/dois/{doi}",
        json={
            "data": {
                "attributes": {
                    "titles": [{"title": "Test Dataset"}],
                    "creators": [{"name": "Test Creator"}],
                    "publicationYear": 2024,
                    "types": {"resourceTypeGeneral": "Dataset"},
                    "publisher": "GFZ Data Services"
                    # Note: schemaVersion is intentionally missing
                }
            }
        },
        status=200
    )
    
    # Mock successful PUT after adding schemaVersion
    responses.add(
        responses.PUT,
        f"https://api.test.datacite.org/dois/{doi}",
        json={"data": {"id": doi}},
        status=200
    )
    
    success, message = client.update_doi_url(doi, new_url)
    
    # Should succeed after adding schemaVersion
    assert success is True
    assert "kernel-4" in message
    
    # Verify schemaVersion was added
    assert len(responses.calls) == 3
    retry_request = responses.calls[2].request
    payload = json.loads(retry_request.body)
    assert payload['data']['attributes']['schemaVersion'] == 'http://datacite.org/schema/kernel-4'

