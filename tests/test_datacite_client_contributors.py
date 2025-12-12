"""Tests for DataCite client contributor methods."""

import json
import pytest
import responses

from src.api.datacite_client import (
    DataCiteClient,
    AuthenticationError
)


class TestFetchAllDOIsWithContributors:
    """Tests for fetch_all_dois_with_contributors method."""
    
    @responses.activate
    def test_single_page_success(self):
        """Test fetching contributors from a single page."""
        responses.add(
            responses.GET,
            "https://api.datacite.org/dois",
            json={
                "data": [
                    {
                        "id": "10.5880/gfz.test.001",
                        "attributes": {
                            "contributors": [
                                {
                                    "name": "Müller, Hans",
                                    "nameType": "Personal",
                                    "givenName": "Hans",
                                    "familyName": "Müller",
                                    "contributorType": "ContactPerson",
                                    "nameIdentifiers": [
                                        {
                                            "nameIdentifier": "https://orcid.org/0000-0001-2345-6789",
                                            "nameIdentifierScheme": "ORCID",
                                            "schemeUri": "https://orcid.org"
                                        }
                                    ]
                                },
                                {
                                    "name": "GFZ Data Services",
                                    "nameType": "Organizational",
                                    "contributorType": "HostingInstitution",
                                    "nameIdentifiers": [
                                        {
                                            "nameIdentifier": "https://ror.org/04z8jg394",
                                            "nameIdentifierScheme": "ROR",
                                            "schemeUri": "https://ror.org"
                                        }
                                    ]
                                }
                            ]
                        }
                    }
                ],
                "meta": {"page": 1, "totalPages": 1}
            },
            status=200
        )
        
        client = DataCiteClient("TIB.GFZ", "password")
        result = client.fetch_all_dois_with_contributors()
        
        assert len(result) == 2
        
        # First contributor
        assert result[0][0] == "10.5880/gfz.test.001"  # DOI
        assert result[0][1] == "Müller, Hans"  # Name
        assert result[0][2] == "Personal"  # NameType
        assert result[0][3] == "Hans"  # GivenName
        assert result[0][4] == "Müller"  # FamilyName
        assert result[0][5] == "https://orcid.org/0000-0001-2345-6789"  # NameIdentifier
        assert result[0][6] == "ORCID"  # Scheme
        assert result[0][8] == "ContactPerson"  # ContributorType
        
        # Second contributor (organizational)
        assert result[1][1] == "GFZ Data Services"
        assert result[1][2] == "Organizational"
        assert result[1][8] == "HostingInstitution"
    
    @responses.activate
    def test_multiple_pages(self):
        """Test pagination handling."""
        # Page 1
        responses.add(
            responses.GET,
            "https://api.datacite.org/dois",
            json={
                "data": [
                    {
                        "id": "10.5880/gfz.test.001",
                        "attributes": {
                            "contributors": [
                                {"name": "Person 1", "contributorType": "DataCollector"}
                            ]
                        }
                    }
                ],
                "links": {"next": "https://api.datacite.org/dois?page=2"},
                "meta": {"page": 1, "totalPages": 2}
            },
            status=200
        )
        
        # Page 2
        responses.add(
            responses.GET,
            "https://api.datacite.org/dois",
            json={
                "data": [
                    {
                        "id": "10.5880/gfz.test.002",
                        "attributes": {
                            "contributors": [
                                {"name": "Person 2", "contributorType": "Researcher"}
                            ]
                        }
                    }
                ],
                "meta": {"page": 2, "totalPages": 2}
            },
            status=200
        )
        
        client = DataCiteClient("TIB.GFZ", "password")
        result = client.fetch_all_dois_with_contributors()
        
        assert len(result) == 2
        assert result[0][0] == "10.5880/gfz.test.001"
        assert result[1][0] == "10.5880/gfz.test.002"
    
    @responses.activate
    def test_dois_without_contributors_skipped(self):
        """Test that DOIs without contributors are skipped."""
        responses.add(
            responses.GET,
            "https://api.datacite.org/dois",
            json={
                "data": [
                    {
                        "id": "10.5880/gfz.test.001",
                        "attributes": {"contributors": []}  # Empty
                    },
                    {
                        "id": "10.5880/gfz.test.002",
                        "attributes": {}  # No contributors key
                    },
                    {
                        "id": "10.5880/gfz.test.003",
                        "attributes": {
                            "contributors": [
                                {"name": "Has Contributor", "contributorType": "Other"}
                            ]
                        }
                    }
                ],
                "meta": {"page": 1, "totalPages": 1}
            },
            status=200
        )
        
        client = DataCiteClient("TIB.GFZ", "password")
        result = client.fetch_all_dois_with_contributors()
        
        assert len(result) == 1
        assert result[0][0] == "10.5880/gfz.test.003"
    
    @responses.activate
    def test_authentication_error(self):
        """Test handling of authentication errors."""
        responses.add(
            responses.GET,
            "https://api.datacite.org/dois",
            status=401
        )
        
        client = DataCiteClient("TIB.GFZ", "wrong_password")
        
        with pytest.raises(AuthenticationError):
            client.fetch_all_dois_with_contributors()


class TestValidateContributorsMatch:
    """Tests for validate_contributors_match method."""
    
    @responses.activate
    def test_matching_contributor_count(self):
        """Test validation passes when contributor counts match."""
        responses.add(
            responses.GET,
            "https://api.datacite.org/dois/10.5880/gfz.test.001",
            json={
                "data": {
                    "id": "10.5880/gfz.test.001",
                    "attributes": {
                        "contributors": [
                            {"name": "Person 1", "contributorType": "ContactPerson"},
                            {"name": "Person 2", "contributorType": "DataManager"}
                        ]
                    }
                }
            },
            status=200
        )
        
        client = DataCiteClient("TIB.GFZ", "password")
        # CSV contributors must match by name (case-insensitive)
        csv_contributors = [
            {"name": "Person 1"},
            {"name": "Person 2"}
        ]
        
        is_valid, message = client.validate_contributors_match(
            "10.5880/gfz.test.001", 
            csv_contributors
        )
        
        assert is_valid is True
        assert "2 Contributors validiert" in message
    
    @responses.activate
    def test_partial_update_allowed(self):
        """Test validation passes for partial updates (fewer CSV contributors)."""
        responses.add(
            responses.GET,
            "https://api.datacite.org/dois/10.5880/gfz.test.001",
            json={
                "data": {
                    "id": "10.5880/gfz.test.001",
                    "attributes": {
                        "contributors": [
                            {"name": "Person 1"},
                            {"name": "Person 2"}
                        ]
                    }
                }
            },
            status=200
        )
        
        client = DataCiteClient("TIB.GFZ", "password")
        # Only update Person 1, leave Person 2 unchanged (partial update)
        csv_contributors = [{"name": "Person 1"}]
        
        is_valid, message = client.validate_contributors_match(
            "10.5880/gfz.test.001", 
            csv_contributors
        )
        
        assert is_valid is True
        assert "partielles Update" in message
    
    @responses.activate
    def test_unmatched_contributor_fails(self):
        """Test validation fails when CSV contributor not found in DataCite."""
        responses.add(
            responses.GET,
            "https://api.datacite.org/dois/10.5880/gfz.test.001",
            json={
                "data": {
                    "id": "10.5880/gfz.test.001",
                    "attributes": {
                        "contributors": [
                            {"name": "Person 1"},
                            {"name": "Person 2"}
                        ]
                    }
                }
            },
            status=200
        )
        
        client = DataCiteClient("TIB.GFZ", "password")
        # Unknown person not in DataCite
        csv_contributors = [{"name": "Unknown Person"}]
        
        is_valid, message = client.validate_contributors_match(
            "10.5880/gfz.test.001", 
            csv_contributors
        )
        
        assert is_valid is False
        assert "nicht in DataCite gefunden" in message
    
    @responses.activate
    def test_doi_not_found(self):
        """Test validation fails when DOI is not found."""
        responses.add(
            responses.GET,
            "https://api.datacite.org/dois/10.5880/gfz.notfound",
            status=404
        )
        
        client = DataCiteClient("TIB.GFZ", "password")
        
        is_valid, message = client.validate_contributors_match(
            "10.5880/gfz.notfound", 
            [{"name": "Person"}]
        )
        
        assert is_valid is False
        assert "nicht gefunden" in message


class TestUpdateDOIContributors:
    """Tests for update_doi_contributors method."""
    
    @responses.activate
    def test_successful_update(self):
        """Test successful contributor update."""
        responses.add(
            responses.PUT,
            "https://api.datacite.org/dois/10.5880/gfz.test.001",
            json={"data": {"id": "10.5880/gfz.test.001"}},
            status=200
        )
        
        client = DataCiteClient("TIB.GFZ", "password")
        
        current_metadata = {
            "data": {
                "id": "10.5880/gfz.test.001",
                "attributes": {
                    "contributors": [
                        {"name": "Müller, Hans", "contributorType": "ContactPerson"}
                    ]
                }
            }
        }
        
        # New contributor must match by name for partial update to work
        new_contributors = [
            {
                "name": "Müller, Hans",
                "nameType": "Personal",
                "givenName": "Hans",
                "familyName": "Müller",
                "contributorTypes": "ContactPerson, DataManager",
                "nameIdentifier": "https://orcid.org/0000-0001-2345-6789",
                "nameIdentifierScheme": "ORCID",
                "schemeUri": "https://orcid.org"
            }
        ]
        
        success, message = client.update_doi_contributors(
            "10.5880/gfz.test.001",
            new_contributors,
            current_metadata
        )
        
        assert success is True
        assert "aktualisiert" in message
    
    @responses.activate
    def test_preserves_affiliations(self):
        """Test that affiliations are preserved during update."""
        # Capture the request to verify payload
        def request_callback(request):
            payload = json.loads(request.body)
            contributors = payload["data"]["attributes"]["contributors"]
            
            # Verify affiliations are preserved
            assert contributors[0].get("affiliation") == [
                {"name": "GFZ Potsdam", "affiliationIdentifier": "https://ror.org/04z8jg394"}
            ]
            
            return (200, {}, json.dumps({"data": {"id": "10.5880/gfz.test.001"}}))
        
        responses.add_callback(
            responses.PUT,
            "https://api.datacite.org/dois/10.5880/gfz.test.001",
            callback=request_callback,
            content_type="application/json"
        )
        
        client = DataCiteClient("TIB.GFZ", "password")
        
        current_metadata = {
            "data": {
                "attributes": {
                    "contributors": [
                        {
                            "name": "Old Name",
                            "contributorType": "Researcher",
                            "affiliation": [
                                {"name": "GFZ Potsdam", "affiliationIdentifier": "https://ror.org/04z8jg394"}
                            ]
                        }
                    ]
                }
            }
        }
        
        new_contributors = [{"name": "New Name", "contributorTypes": "Researcher"}]
        
        success, _ = client.update_doi_contributors(
            "10.5880/gfz.test.001",
            new_contributors,
            current_metadata
        )
        
        assert success is True
    
    @responses.activate
    def test_uses_first_contributor_type(self):
        """Test that first contributor type is used when multiple are provided."""
        def request_callback(request):
            payload = json.loads(request.body)
            contributors = payload["data"]["attributes"]["contributors"]
            
            # Should use first type (ContactPerson)
            assert contributors[0]["contributorType"] == "ContactPerson"
            
            return (200, {}, json.dumps({"data": {"id": "10.5880/gfz.test.001"}}))
        
        responses.add_callback(
            responses.PUT,
            "https://api.datacite.org/dois/10.5880/gfz.test.001",
            callback=request_callback,
            content_type="application/json"
        )
        
        client = DataCiteClient("TIB.GFZ", "password")
        
        # Name must match for partial update
        current_metadata = {"data": {"attributes": {"contributors": [{"name": "Person"}]}}}
        new_contributors = [
            {"name": "Person", "contributorTypes": "ContactPerson, DataManager, Researcher"}
        ]
        
        success, _ = client.update_doi_contributors(
            "10.5880/gfz.test.001",
            new_contributors,
            current_metadata
        )
        
        assert success is True
    
    @responses.activate
    def test_forbidden_error(self):
        """Test handling of 403 Forbidden error."""
        responses.add(
            responses.PUT,
            "https://api.datacite.org/dois/10.5880/gfz.other",
            status=403
        )
        
        client = DataCiteClient("TIB.GFZ", "password")
        
        success, message = client.update_doi_contributors(
            "10.5880/gfz.other",
            [{"name": "Person"}],
            {"data": {"attributes": {}}}
        )
        
        assert success is False
        assert "Keine Berechtigung" in message


class TestValidContributorTypes:
    """Tests for VALID_CONTRIBUTOR_TYPES constant."""
    
    def test_all_datacite_types_included(self):
        """Verify all DataCite contributor types are included."""
        expected_types = [
            "ContactPerson", "DataCollector", "DataCurator", "DataManager",
            "Distributor", "Editor", "HostingInstitution", "Producer",
            "ProjectLeader", "ProjectManager", "ProjectMember",
            "RegistrationAgency", "RegistrationAuthority", "RelatedPerson",
            "Researcher", "ResearchGroup", "RightsHolder", "Sponsor",
            "Supervisor", "Translator", "WorkPackageLeader", "Other"
        ]
        
        for contrib_type in expected_types:
            assert contrib_type in DataCiteClient.VALID_CONTRIBUTOR_TYPES
