"""Tests for 'Can't be blank' error handling with detailed field identification."""

import pytest
import responses
from src.api.datacite_client import DataCiteClient


@responses.activate
def test_cant_be_blank_error_identifies_missing_title():
    """Test that 'Can't be blank' error identifies missing title field."""
    client = DataCiteClient("test.client", "password", use_test_api=True)
    doi = "10.5880/GFZ.TEST.001"
    new_url = "https://example.org/updated"
    
    # PUT fails with "Can't be blank"
    responses.add(
        responses.PUT,
        f"https://api.test.datacite.org/dois/{doi}",
        json={
            "errors": [{
                "title": "Can't be blank"
            }]
        },
        status=422
    )
    
    # GET returns metadata without title
    responses.add(
        responses.GET,
        f"https://api.test.datacite.org/dois/{doi}",
        json={
            "data": {
                "id": doi,
                "type": "dois",
                "attributes": {
                    "doi": doi,
                    "url": "https://example.org/old",
                    "creators": [{"name": "Test Creator"}],
                    "types": {"resourceTypeGeneral": "Dataset"},
                    "publisher": "Test Publisher",
                    "schemaVersion": "http://datacite.org/schema/kernel-4"
                    # Missing: titles
                }
            }
        },
        status=200
    )
    
    success, message = client.update_doi_url(doi, new_url)
    
    assert success is False
    assert "title" in message
    assert "Pflichtfelder fehlen" in message or "kann nicht aktualisiert werden" in message
    assert "Fabrica" in message


@responses.activate
def test_cant_be_blank_error_identifies_missing_creators():
    """Test that 'Can't be blank' error identifies missing creators field."""
    client = DataCiteClient("test.client", "password", use_test_api=True)
    doi = "10.5880/GFZ.TEST.002"
    new_url = "https://example.org/updated"
    
    # PUT fails with "Can't be blank"
    responses.add(
        responses.PUT,
        f"https://api.test.datacite.org/dois/{doi}",
        json={
            "errors": [{
                "title": "Can't be blank"
            }]
        },
        status=422
    )
    
    # GET returns metadata without creators
    responses.add(
        responses.GET,
        f"https://api.test.datacite.org/dois/{doi}",
        json={
            "data": {
                "id": doi,
                "type": "dois",
                "attributes": {
                    "doi": doi,
                    "url": "https://example.org/old",
                    "titles": [{"title": "Test Dataset"}],
                    "types": {"resourceTypeGeneral": "Dataset"},
                    "publisher": "Test Publisher",
                    "schemaVersion": "http://datacite.org/schema/kernel-4"
                    # Missing: creators
                }
            }
        },
        status=200
    )
    
    success, message = client.update_doi_url(doi, new_url)
    
    assert success is False
    assert "creators" in message
    assert "Pflichtfelder fehlen" in message or "kann nicht aktualisiert werden" in message
    assert "Fabrica" in message


@responses.activate
def test_cant_be_blank_error_identifies_multiple_missing_fields():
    """Test that 'Can't be blank' error identifies all missing mandatory fields."""
    client = DataCiteClient("test.client", "password", use_test_api=True)
    doi = "10.5880/GFZ.TEST.003"
    new_url = "https://example.org/updated"
    
    # PUT fails with "Can't be blank"
    responses.add(
        responses.PUT,
        f"https://api.test.datacite.org/dois/{doi}",
        json={
            "errors": [{
                "title": "Can't be blank"
            }]
        },
        status=422
    )
    
    # GET returns metadata without title, creators, resourceTypeGeneral, and publisher
    responses.add(
        responses.GET,
        f"https://api.test.datacite.org/dois/{doi}",
        json={
            "data": {
                "id": doi,
                "type": "dois",
                "attributes": {
                    "doi": doi,
                    "url": "https://example.org/old",
                    "types": {},  # Missing resourceTypeGeneral
                    "schemaVersion": "http://datacite.org/schema/kernel-4"
                    # Missing: titles, creators, publisher
                }
            }
        },
        status=200
    )
    
    success, message = client.update_doi_url(doi, new_url)
    
    assert success is False
    assert "title" in message
    assert "creators" in message
    assert "resourceTypeGeneral" in message
    assert "publisher" in message
    assert "Pflichtfelder fehlen" in message or "kann nicht aktualisiert werden" in message
    assert "Fabrica" in message


@responses.activate
def test_cant_be_blank_error_with_fetch_failure():
    """Test that 'Can't be blank' error falls back gracefully if metadata fetch fails."""
    client = DataCiteClient("test.client", "password", use_test_api=True)
    doi = "10.5880/GFZ.TEST.004"
    new_url = "https://example.org/updated"
    
    # PUT fails with "Can't be blank"
    responses.add(
        responses.PUT,
        f"https://api.test.datacite.org/dois/{doi}",
        json={
            "errors": [{
                "title": "Can't be blank"
            }]
        },
        status=422
    )
    
    # GET fails (e.g., network error)
    responses.add(
        responses.GET,
        f"https://api.test.datacite.org/dois/{doi}",
        json={"errors": [{"title": "Server error"}]},
        status=500
    )
    
    success, message = client.update_doi_url(doi, new_url)
    
    assert success is False
    assert "Can't be blank" in message
    assert "Metadaten konnten nicht abgerufen werden" in message


@responses.activate
def test_cant_be_blank_error_includes_fabrica_link():
    """Test that 'Can't be blank' error message includes DataCite Fabrica link."""
    client = DataCiteClient("test.client", "password", use_test_api=True)
    doi = "10.5880/GFZ.TEST.005"
    new_url = "https://example.org/updated"
    
    # PUT fails with "Can't be blank"
    responses.add(
        responses.PUT,
        f"https://api.test.datacite.org/dois/{doi}",
        json={
            "errors": [{
                "title": "Can't be blank"
            }]
        },
        status=422
    )
    
    # GET returns metadata without title and creators
    responses.add(
        responses.GET,
        f"https://api.test.datacite.org/dois/{doi}",
        json={
            "data": {
                "id": doi,
                "type": "dois",
                "attributes": {
                    "doi": doi,
                    "url": "https://example.org/old",
                    "types": {"resourceTypeGeneral": "Dataset"},
                    "publisher": "Test Publisher",
                    "schemaVersion": "http://datacite.org/schema/kernel-4"
                    # Missing: titles, creators
                }
            }
        },
        status=200
    )
    
    success, message = client.update_doi_url(doi, new_url)
    
    assert success is False
    assert "https://doi.datacite.org/dois/" in message
    assert doi in message
