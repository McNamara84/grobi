"""Tests for Schema 4 compatibility checking functionality."""

import pytest
import responses
from src.api.datacite_client import DataCiteClient


class TestSchemaCompatibilityCheck:
    """Test Schema 4 compatibility checking methods."""
    
    @responses.activate
    def test_fetch_dois_with_schema_version(self):
        """Test fetching DOIs with their schema versions."""
        client = DataCiteClient("test.client", "password", use_test_api=True)
        
        # Mock first page
        responses.add(
            responses.GET,
            "https://api.test.datacite.org/dois?page[size]=100&page[cursor]=1",
            json={
                "data": [
                    {
                        "id": "10.5880/GFZ.TEST.001",
                        "attributes": {
                            "schemaVersion": "http://datacite.org/schema/kernel-4"
                        }
                    },
                    {
                        "id": "10.5880/GFZ.TEST.002",
                        "attributes": {
                            "schemaVersion": "http://datacite.org/schema/kernel-3"
                        }
                    },
                    {
                        "id": "10.5880/GFZ.TEST.003",
                        "attributes": {
                            "schemaVersion": "4.5"
                        }
                    }
                ],
                "links": {}  # No next page
            },
            status=200
        )
        
        dois = client.fetch_dois_with_schema_version()
        
        assert len(dois) == 3
        assert dois[0] == ("10.5880/GFZ.TEST.001", "http://datacite.org/schema/kernel-4")
        assert dois[1] == ("10.5880/GFZ.TEST.002", "http://datacite.org/schema/kernel-3")
        assert dois[2] == ("10.5880/GFZ.TEST.003", "4.5")
    
    @responses.activate
    def test_check_schema_4_compatibility_valid(self):
        """Test compatibility check for valid Schema 4 DOI."""
        client = DataCiteClient("test.client", "password", use_test_api=True)
        doi = "10.5880/GFZ.TEST.001"
        
        # Mock GET request for metadata
        responses.add(
            responses.GET,
            f"https://api.test.datacite.org/dois/{doi}",
            json={
                "data": {
                    "id": doi,
                    "type": "dois",
                    "attributes": {
                        "doi": doi,
                        "publisher": "GFZ Data Services",
                        "publicationYear": 2024,
                        "titles": [{"title": "Test Dataset"}],
                        "creators": [
                            {
                                "name": "Mustermann, Max",
                                "nameType": "Personal"
                            }
                        ],
                        "types": {
                            "resourceTypeGeneral": "Dataset"
                        }
                    }
                }
            },
            status=200
        )
        
        is_compatible, missing_fields = client.check_schema_4_compatibility(doi)
        
        assert is_compatible is True
        assert len(missing_fields) == 0
    
    @responses.activate
    def test_check_schema_4_compatibility_missing_publisher(self):
        """Test compatibility check with missing publisher."""
        client = DataCiteClient("test.client", "password", use_test_api=True)
        doi = "10.5880/GFZ.TEST.002"
        
        responses.add(
            responses.GET,
            f"https://api.test.datacite.org/dois/{doi}",
            json={
                "data": {
                    "id": doi,
                    "type": "dois",
                    "attributes": {
                        "doi": doi,
                        # Missing publisher
                        "publicationYear": 2024,
                        "titles": [{"title": "Test Dataset"}],
                        "creators": [{"name": "Mustermann, Max", "nameType": "Personal"}],
                        "types": {"resourceTypeGeneral": "Dataset"}
                    }
                }
            },
            status=200
        )
        
        is_compatible, missing_fields = client.check_schema_4_compatibility(doi)
        
        assert is_compatible is False
        assert "publisher" in missing_fields
        assert missing_fields["publisher"] == "missing"
    
    @responses.activate
    def test_check_schema_4_compatibility_empty_publisher(self):
        """Test compatibility check with empty publisher string."""
        client = DataCiteClient("test.client", "password", use_test_api=True)
        doi = "10.5880/GFZ.TEST.003"
        
        responses.add(
            responses.GET,
            f"https://api.test.datacite.org/dois/{doi}",
            json={
                "data": {
                    "id": doi,
                    "type": "dois",
                    "attributes": {
                        "doi": doi,
                        "publisher": "",  # Empty string
                        "publicationYear": 2024,
                        "titles": [{"title": "Test Dataset"}],
                        "creators": [{"name": "Mustermann, Max", "nameType": "Personal"}],
                        "types": {"resourceTypeGeneral": "Dataset"}
                    }
                }
            },
            status=200
        )
        
        is_compatible, missing_fields = client.check_schema_4_compatibility(doi)
        
        assert is_compatible is False
        assert "publisher" in missing_fields
        assert missing_fields["publisher"] == "empty"
    
    @responses.activate
    def test_check_schema_4_compatibility_missing_multiple_fields(self):
        """Test compatibility check with multiple missing fields."""
        client = DataCiteClient("test.client", "password", use_test_api=True)
        doi = "10.5880/GFZ.TEST.004"
        
        responses.add(
            responses.GET,
            f"https://api.test.datacite.org/dois/{doi}",
            json={
                "data": {
                    "id": doi,
                    "type": "dois",
                    "attributes": {
                        "doi": doi,
                        # Missing publisher
                        # Missing publicationYear
                        "titles": [{"title": "Test Dataset"}],
                        "creators": [{"name": "Mustermann, Max", "nameType": "Personal"}],
                        "types": {"resourceTypeGeneral": "Dataset"}
                    }
                }
            },
            status=200
        )
        
        is_compatible, missing_fields = client.check_schema_4_compatibility(doi)
        
        assert is_compatible is False
        assert "publisher" in missing_fields
        assert "publicationYear" in missing_fields
        assert missing_fields["publisher"] == "missing"
        assert missing_fields["publicationYear"] == "missing"
    
    @responses.activate
    def test_check_schema_4_compatibility_invalid_name_type(self):
        """Test compatibility check with invalid nameType."""
        client = DataCiteClient("test.client", "password", use_test_api=True)
        doi = "10.5880/GFZ.TEST.005"
        
        responses.add(
            responses.GET,
            f"https://api.test.datacite.org/dois/{doi}",
            json={
                "data": {
                    "id": doi,
                    "type": "dois",
                    "attributes": {
                        "doi": doi,
                        "publisher": "GFZ Data Services",
                        "publicationYear": 2024,
                        "titles": [{"title": "Test Dataset"}],
                        "creators": [
                            {
                                "name": "Mustermann, Max",
                                "nameType": "InvalidType"  # Invalid nameType
                            }
                        ],
                        "types": {"resourceTypeGeneral": "Dataset"}
                    }
                }
            },
            status=200
        )
        
        is_compatible, missing_fields = client.check_schema_4_compatibility(doi)
        
        assert is_compatible is False
        assert "invalid_name_types" in missing_fields
        assert "InvalidType" in missing_fields["invalid_name_types"]
    
    @responses.activate
    def test_check_schema_4_compatibility_unknown_contributor_type(self):
        """Test compatibility check with unknown contributor type."""
        client = DataCiteClient("test.client", "password", use_test_api=True)
        doi = "10.5880/GFZ.TEST.006"
        
        responses.add(
            responses.GET,
            f"https://api.test.datacite.org/dois/{doi}",
            json={
                "data": {
                    "id": doi,
                    "type": "dois",
                    "attributes": {
                        "doi": doi,
                        "publisher": "GFZ Data Services",
                        "publicationYear": 2024,
                        "titles": [{"title": "Test Dataset"}],
                        "creators": [{"name": "Mustermann, Max", "nameType": "Personal"}],
                        "types": {"resourceTypeGeneral": "Dataset"},
                        "contributors": [
                            {
                                "name": "Test Contributor",
                                "contributorType": "InvalidContributorType"
                            }
                        ]
                    }
                }
            },
            status=200
        )
        
        is_compatible, missing_fields = client.check_schema_4_compatibility(doi)
        
        assert is_compatible is False
        assert "unknown_contributor_types" in missing_fields
        assert "InvalidContributorType" in missing_fields["unknown_contributor_types"]
    
    @responses.activate
    def test_check_schema_4_compatibility_empty_titles_array(self):
        """Test compatibility check with empty titles array."""
        client = DataCiteClient("test.client", "password", use_test_api=True)
        doi = "10.5880/GFZ.TEST.007"
        
        responses.add(
            responses.GET,
            f"https://api.test.datacite.org/dois/{doi}",
            json={
                "data": {
                    "id": doi,
                    "type": "dois",
                    "attributes": {
                        "doi": doi,
                        "publisher": "GFZ Data Services",
                        "publicationYear": 2024,
                        "titles": [],  # Empty array
                        "creators": [{"name": "Mustermann, Max", "nameType": "Personal"}],
                        "types": {"resourceTypeGeneral": "Dataset"}
                    }
                }
            },
            status=200
        )
        
        is_compatible, missing_fields = client.check_schema_4_compatibility(doi)
        
        assert is_compatible is False
        assert "titles" in missing_fields
        assert missing_fields["titles"] == "empty"
    
    @responses.activate
    def test_check_schema_4_compatibility_missing_resource_type(self):
        """Test compatibility check with missing resource type."""
        client = DataCiteClient("test.client", "password", use_test_api=True)
        doi = "10.5880/GFZ.TEST.008"
        
        responses.add(
            responses.GET,
            f"https://api.test.datacite.org/dois/{doi}",
            json={
                "data": {
                    "id": doi,
                    "type": "dois",
                    "attributes": {
                        "doi": doi,
                        "publisher": "GFZ Data Services",
                        "publicationYear": 2024,
                        "titles": [{"title": "Test Dataset"}],
                        "creators": [{"name": "Mustermann, Max", "nameType": "Personal"}],
                        "types": {}  # Missing resourceTypeGeneral
                    }
                }
            },
            status=200
        )
        
        is_compatible, missing_fields = client.check_schema_4_compatibility(doi)
        
        assert is_compatible is False
        assert "resourceType" in missing_fields
        assert missing_fields["resourceType"] == "missing"
    
    @responses.activate
    def test_check_schema_4_compatibility_publisher_object_format(self):
        """Test compatibility check with publisher in object format (Schema 4.6)."""
        client = DataCiteClient("test.client", "password", use_test_api=True)
        doi = "10.5880/GFZ.TEST.009"
        
        responses.add(
            responses.GET,
            f"https://api.test.datacite.org/dois/{doi}",
            json={
                "data": {
                    "id": doi,
                    "type": "dois",
                    "attributes": {
                        "doi": doi,
                        "publisher": {
                            "name": "GFZ Data Services",
                            "publisherIdentifier": "https://ror.org/04z8jg394",
                            "publisherIdentifierScheme": "ROR"
                        },
                        "publicationYear": 2024,
                        "titles": [{"title": "Test Dataset"}],
                        "creators": [{"name": "Mustermann, Max", "nameType": "Personal"}],
                        "types": {"resourceTypeGeneral": "Dataset"}
                    }
                }
            },
            status=200
        )
        
        is_compatible, missing_fields = client.check_schema_4_compatibility(doi)
        
        assert is_compatible is True
        assert len(missing_fields) == 0
    
    @responses.activate
    def test_check_schema_4_compatibility_publisher_object_empty_name(self):
        """Test compatibility check with publisher object but empty name."""
        client = DataCiteClient("test.client", "password", use_test_api=True)
        doi = "10.5880/GFZ.TEST.010"
        
        responses.add(
            responses.GET,
            f"https://api.test.datacite.org/dois/{doi}",
            json={
                "data": {
                    "id": doi,
                    "type": "dois",
                    "attributes": {
                        "doi": doi,
                        "publisher": {
                            "name": "",  # Empty name
                            "publisherIdentifier": "https://ror.org/04z8jg394"
                        },
                        "publicationYear": 2024,
                        "titles": [{"title": "Test Dataset"}],
                        "creators": [{"name": "Mustermann, Max", "nameType": "Personal"}],
                        "types": {"resourceTypeGeneral": "Dataset"}
                    }
                }
            },
            status=200
        )
        
        is_compatible, missing_fields = client.check_schema_4_compatibility(doi)
        
        assert is_compatible is False
        assert "publisher" in missing_fields
        assert missing_fields["publisher"] == "empty"
