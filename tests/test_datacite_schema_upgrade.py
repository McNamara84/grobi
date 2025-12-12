"""Tests for automatic Schema 3 to Schema 4 upgrade functionality."""

import json
import responses
from src.api.datacite_client import DataCiteClient


class TestSchemaUpgrade:
    """Test automatic upgrade from Schema 3 to Schema 4 during URL updates."""
    
    @responses.activate
    def test_update_doi_url_with_schema_3_automatic_upgrade(self):
        """Test that DOI with Schema 3 is automatically upgraded to Schema 4."""
        client = DataCiteClient("test.client", "password", use_test_api=True)
        doi = "10.5880/GFZ.TEST.001"
        new_url = "https://example.org/updated"
        
        # First attempt: Returns 422 with schema deprecation error
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
        
        # GET request to fetch current metadata for upgrade
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
                        "creators": [{"name": "Test Creator"}],
                        "types": {
                            "resourceTypeGeneral": "Dataset",
                            "resourceType": "Test Dataset"
                        },
                        "schemaVersion": "http://datacite.org/schema/kernel-3"
                    }
                }
            },
            status=200
        )
        
        # Second attempt: Successful update with Schema 4
        responses.add(
            responses.PUT,
            f"https://api.test.datacite.org/dois/{doi}",
            json={
                "data": {
                    "id": doi,
                    "type": "dois",
                    "attributes": {
                        "doi": doi,
                        "url": new_url,
                        "schemaVersion": "http://datacite.org/schema/kernel-4"
                    }
                }
            },
            status=200
        )
        
        # Execute update
        success, message = client.update_doi_url(doi, new_url)
        
        # Verify success
        assert success is True
        assert "erfolgreich aktualisiert" in message
        assert "kernel-4" in message
        
        # Verify API calls
        assert len(responses.calls) == 3
        
        # First call: Initial PUT with URL only
        assert responses.calls[0].request.method == "PUT"
        first_payload = json.loads(responses.calls[0].request.body)
        assert first_payload["data"]["attributes"]["url"] == new_url
        assert "schemaVersion" not in first_payload["data"]["attributes"]
        
        # Second call: GET to fetch metadata
        assert responses.calls[1].request.method == "GET"
        
        # Third call: Retry PUT with Schema 4
        assert responses.calls[2].request.method == "PUT"
        second_payload = json.loads(responses.calls[2].request.body)
        assert second_payload["data"]["attributes"]["url"] == new_url
        assert second_payload["data"]["attributes"]["schemaVersion"] == "http://datacite.org/schema/kernel-4"
        assert second_payload["data"]["attributes"]["types"]["resourceTypeGeneral"] == "Dataset"
    
    @responses.activate
    def test_update_doi_url_schema_3_with_funder_migration(self):
        """Test that Funder contributors are migrated to fundingReferences during upgrade."""
        client = DataCiteClient("test.client", "password", use_test_api=True)
        doi = "10.5880/GFZ.TEST.002"
        new_url = "https://example.org/updated"
        
        # First attempt: Schema deprecation error
        responses.add(
            responses.PUT,
            f"https://api.test.datacite.org/dois/{doi}",
            json={
                "errors": [{
                    "title": "Schema http://datacite.org/schema/kernel-3 is no longer supported"
                }]
            },
            status=422
        )
        
        # GET metadata with Funder contributor
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
                        "creators": [{"name": "Test Creator"}],
                        "types": {
                            "resourceTypeGeneral": "Dataset"
                        },
                        "contributors": [
                            {
                                "name": "National Science Foundation",
                                "contributorType": "Funder",
                                "nameIdentifiers": [{
                                    "nameIdentifier": "https://ror.org/021nxhr62",
                                    "nameIdentifierScheme": "ROR"
                                }]
                            },
                            {
                                "name": "Smith, John",
                                "contributorType": "ProjectLeader"
                            }
                        ]
                    }
                }
            },
            status=200
        )
        
        # Successful Schema 4 update
        responses.add(
            responses.PUT,
            f"https://api.test.datacite.org/dois/{doi}",
            json={"data": {"id": doi}},
            status=200
        )
        
        success, message = client.update_doi_url(doi, new_url)
        
        assert success is True
        
        # Verify Funder was migrated to fundingReferences
        upgrade_payload = json.loads(responses.calls[2].request.body)
        attributes = upgrade_payload["data"]["attributes"]
        
        # Check fundingReferences was created
        assert "fundingReferences" in attributes
        assert len(attributes["fundingReferences"]) == 1
        assert attributes["fundingReferences"][0]["funderName"] == "National Science Foundation"
        assert attributes["fundingReferences"][0]["funderIdentifier"] == "https://ror.org/021nxhr62"
        assert attributes["fundingReferences"][0]["funderIdentifierType"] == "ROR"
        
        # Check remaining contributors (without Funder)
        assert "contributors" in attributes
        assert len(attributes["contributors"]) == 1
        assert attributes["contributors"][0]["name"] == "Smith, John"
        assert attributes["contributors"][0]["contributorType"] == "ProjectLeader"
    
    @responses.activate
    def test_update_doi_url_schema_3_without_resource_type_general(self):
        """Test that upgrade fails gracefully if resourceTypeGeneral is missing."""
        client = DataCiteClient("test.client", "password", use_test_api=True)
        doi = "10.5880/GFZ.TEST.003"
        new_url = "https://example.org/updated"
        
        # Schema deprecation error
        responses.add(
            responses.PUT,
            f"https://api.test.datacite.org/dois/{doi}",
            json={
                "errors": [{
                    "title": "Schema http://datacite.org/schema/kernel-3 is no longer supported"
                }]
            },
            status=422
        )
        
        # GET metadata WITHOUT resourceTypeGeneral (should be auto-filled)
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
                        "creators": [{"name": "Test Creator"}],
                        "types": {}  # Missing resourceTypeGeneral - should be auto-filled with 'Dataset'
                    }
                }
            },
            status=200
        )
        
        # Second PUT: Successful update with auto-filled resourceTypeGeneral
        responses.add(
            responses.PUT,
            f"https://api.test.datacite.org/dois/{doi}",
            json={"data": {"id": doi}},
            status=200
        )
        
        success, message = client.update_doi_url(doi, new_url)
        
        # Should now succeed because resourceTypeGeneral is auto-filled
        assert success is True
        assert "erfolgreich aktualisiert" in message
        
        # Verify resourceTypeGeneral was auto-filled
        upgrade_payload = json.loads(responses.calls[2].request.body)
        assert upgrade_payload["data"]["attributes"]["types"]["resourceTypeGeneral"] == "Dataset"
        
        # Verify all 3 API calls: PUT (fail) → GET (metadata) → PUT (success)
        assert len(responses.calls) == 3
    
    @responses.activate
    def test_update_doi_url_with_schema_4_no_upgrade_needed(self):
        """Test that DOIs with Schema 4 work without upgrade."""
        client = DataCiteClient("test.client", "password", use_test_api=True)
        doi = "10.5880/GFZ.TEST.004"
        new_url = "https://example.org/updated"
        
        # Successful update on first attempt (Schema 4)
        responses.add(
            responses.PUT,
            f"https://api.test.datacite.org/dois/{doi}",
            json={
                "data": {
                    "id": doi,
                    "type": "dois",
                    "attributes": {
                        "doi": doi,
                        "url": new_url,
                        "schemaVersion": "http://datacite.org/schema/kernel-4"
                    }
                }
            },
            status=200
        )
        
        success, message = client.update_doi_url(doi, new_url)
        
        assert success is True
        assert "erfolgreich aktualisiert" in message
        # Success message should not mention schema upgrade since DOI already uses Schema 4
        assert "kernel-4" not in message
        assert "Schema" not in message.lower()
        
        # Verify only one API call (no GET for metadata)
        assert len(responses.calls) == 1
        assert responses.calls[0].request.method == "PUT"
    
    @responses.activate
    def test_update_doi_url_schema_upgrade_with_url_normalization(self):
        """Test that URL normalization works together with schema upgrade."""
        client = DataCiteClient("test.client", "password", use_test_api=True)
        doi = "10.5880/GFZ.TEST.005"
        url_with_colon = "https://example.org?id=escidoc:12345"
        normalized_url = "https://example.org?id=escidoc%3A12345"
        
        # Schema deprecation error
        responses.add(
            responses.PUT,
            f"https://api.test.datacite.org/dois/{doi}",
            json={
                "errors": [{
                    "title": "Schema http://datacite.org/schema/kernel-3 is no longer supported"
                }]
            },
            status=422
        )
        
        # GET metadata
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
                        "creators": [{"name": "Test Creator"}],
                        "types": {"resourceTypeGeneral": "Dataset"}
                    }
                }
            },
            status=200
        )
        
        # Successful upgrade
        responses.add(
            responses.PUT,
            f"https://api.test.datacite.org/dois/{doi}",
            json={"data": {"id": doi}},
            status=200
        )
        
        success, message = client.update_doi_url(doi, url_with_colon)
        
        assert success is True
        
        # Verify both calls used normalized URL
        first_payload = json.loads(responses.calls[0].request.body)
        assert first_payload["data"]["attributes"]["url"] == normalized_url
        
        upgrade_payload = json.loads(responses.calls[2].request.body)
        assert upgrade_payload["data"]["attributes"]["url"] == normalized_url
    
    @responses.activate
    def test_update_doi_url_schema_upgrade_failure_returns_error(self):
        """Test that schema upgrade failure is properly reported."""
        client = DataCiteClient("test.client", "password", use_test_api=True)
        doi = "10.5880/GFZ.TEST.006"
        new_url = "https://example.org/updated"
        
        # Schema deprecation error
        responses.add(
            responses.PUT,
            f"https://api.test.datacite.org/dois/{doi}",
            json={
                "errors": [{
                    "title": "Schema http://datacite.org/schema/kernel-3 is no longer supported"
                }]
            },
            status=422
        )
        
        # GET metadata
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
                        "creators": [{"name": "Test Creator"}],
                        "types": {"resourceTypeGeneral": "Dataset"}
                    }
                }
            },
            status=200
        )
        
        # Upgrade attempt fails with 422 again
        responses.add(
            responses.PUT,
            f"https://api.test.datacite.org/dois/{doi}",
            json={
                "errors": [{
                    "title": "Invalid metadata: Some other error"
                }]
            },
            status=422
        )
        
        success, message = client.update_doi_url(doi, new_url)
        
        assert success is False
        assert "Schema-Upgrade" in message or "fehlgeschlagen" in message
