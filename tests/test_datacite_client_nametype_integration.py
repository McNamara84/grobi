"""Integration tests for DataCite client nameType inference with realistic data.

These tests use mocked API responses based on real-world data patterns
from the GFZ DataCite repository to verify nameType inference logic.
"""

import pytest
import responses

from src.api.datacite_client import DataCiteClient


class TestNameTypeInferenceWithORCID:
    """Tests verifying ORCID identifier forces Personal nameType."""

    @responses.activate
    def test_contributor_with_orcid_is_personal(self):
        """Contributor with ORCID should always be Personal."""
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
                                    "name": "Schmidt, Maria",
                                    "nameType": "Organizational",  # Wrong in API
                                    "givenName": "Maria",
                                    "familyName": "Schmidt",
                                    "contributorType": "ContactPerson",
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
                    }
                ],
                "meta": {"page": 1, "totalPages": 1}
            },
            status=200
        )
        
        client = DataCiteClient("TIB.GFZ", "password")
        result = client.fetch_all_dois_with_contributors()
        
        assert len(result) == 1
        assert result[0][2] == "Personal"  # NameType corrected
        assert result[0][6] == "ORCID"

    @responses.activate
    def test_organization_name_with_orcid_becomes_personal(self):
        """Organization-like name with ORCID should be Personal."""
        responses.add(
            responses.GET,
            "https://api.datacite.org/dois",
            json={
                "data": [
                    {
                        "id": "10.5880/gfz.test.002",
                        "attributes": {
                            "contributors": [
                                {
                                    "name": "University Research Team Lead",
                                    "nameType": "Organizational",
                                    "contributorType": "ProjectLeader",
                                    "nameIdentifiers": [
                                        {
                                            "nameIdentifier": "0000-0003-9999-8888",
                                            "nameIdentifierScheme": "ORCID"
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
        
        assert result[0][2] == "Personal"


class TestNameTypeInferenceWithROR:
    """Tests verifying ROR identifier forces Organizational nameType."""

    @responses.activate
    def test_contributor_with_ror_is_organizational(self):
        """Contributor with ROR should always be Organizational."""
        responses.add(
            responses.GET,
            "https://api.datacite.org/dois",
            json={
                "data": [
                    {
                        "id": "10.5880/gfz.test.003",
                        "attributes": {
                            "contributors": [
                                {
                                    "name": "GFZ German Research Centre for Geosciences",
                                    "nameType": "Personal",  # Wrong in API
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
        
        assert result[0][2] == "Organizational"
        assert result[0][6] == "ROR"

    @responses.activate
    def test_person_name_with_ror_becomes_organizational(self):
        """Person-like name with ROR should be Organizational."""
        responses.add(
            responses.GET,
            "https://api.datacite.org/dois",
            json={
                "data": [
                    {
                        "id": "10.5880/gfz.test.004",
                        "attributes": {
                            "contributors": [
                                {
                                    "name": "John Smith",
                                    "nameType": "Personal",
                                    "givenName": "John",
                                    "familyName": "Smith",
                                    "contributorType": "Sponsor",
                                    "nameIdentifiers": [
                                        {
                                            "nameIdentifier": "https://ror.org/012345678",
                                            "nameIdentifierScheme": "ROR"
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
        
        assert result[0][2] == "Organizational"


class TestNameTypeInferencePersonWithAffiliation:
    """Tests for person names with affiliation in parentheses."""

    @responses.activate
    def test_person_with_affiliation_is_personal(self):
        """Person name with affiliation like 'Bindi, Dino (GFZ)' should be Personal."""
        responses.add(
            responses.GET,
            "https://api.datacite.org/dois",
            json={
                "data": [
                    {
                        "id": "10.5880/gfz.test.005",
                        "attributes": {
                            "contributors": [
                                {
                                    "name": "Bindi, Dino (GFZ)",
                                    "contributorType": "ContactPerson"
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
        
        # The name contains "GFZ" but it's a person with affiliation
        assert result[0][2] == "Personal"

    @responses.activate
    def test_person_with_university_affiliation_is_personal(self):
        """Person with university affiliation should be Personal."""
        responses.add(
            responses.GET,
            "https://api.datacite.org/dois",
            json={
                "data": [
                    {
                        "id": "10.5880/gfz.test.006",
                        "attributes": {
                            "contributors": [
                                {
                                    "name": "Weber, Thomas (University of Potsdam)",
                                    "contributorType": "Researcher"
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
        
        assert result[0][2] == "Personal"

    @responses.activate  
    def test_person_with_email_is_personal(self):
        """Person name followed by email should be Personal."""
        responses.add(
            responses.GET,
            "https://api.datacite.org/dois",
            json={
                "data": [
                    {
                        "id": "10.5880/gfz.test.007",
                        "attributes": {
                            "contributors": [
                                {
                                    "name": "Simone Cesca, cesca@gfz-potsdam.de",
                                    "contributorType": "ContactPerson"
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
        
        assert result[0][2] == "Personal"


class TestNameTypeInferenceOrganizations:
    """Tests for organization keyword detection."""

    @responses.activate
    def test_university_is_organizational(self):
        """University should be Organizational."""
        responses.add(
            responses.GET,
            "https://api.datacite.org/dois",
            json={
                "data": [
                    {
                        "id": "10.5880/gfz.test.008",
                        "attributes": {
                            "contributors": [
                                {
                                    "name": "Technical University of Berlin",
                                    "contributorType": "HostingInstitution"
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
        
        assert result[0][2] == "Organizational"

    @responses.activate
    def test_gfz_is_organizational(self):
        """GFZ as word boundary match should be Organizational."""
        responses.add(
            responses.GET,
            "https://api.datacite.org/dois",
            json={
                "data": [
                    {
                        "id": "10.5880/gfz.test.009",
                        "attributes": {
                            "contributors": [
                                {
                                    "name": "GFZ Data Services",
                                    "contributorType": "HostingInstitution"
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
        
        assert result[0][2] == "Organizational"

    @responses.activate
    def test_caiag_is_organizational(self):
        """CAIAG should be Organizational."""
        responses.add(
            responses.GET,
            "https://api.datacite.org/dois",
            json={
                "data": [
                    {
                        "id": "10.5880/gfz.test.010",
                        "attributes": {
                            "contributors": [
                                {
                                    "name": "CAIAG",
                                    "contributorType": "Sponsor"
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
        
        assert result[0][2] == "Organizational"

    @responses.activate
    def test_sfb_project_is_organizational(self):
        """SFB project should be Organizational."""
        responses.add(
            responses.GET,
            "https://api.datacite.org/dois",
            json={
                "data": [
                    {
                        "id": "10.5880/gfz.test.011",
                        "attributes": {
                            "contributors": [
                                {
                                    "name": "SFB 1294",
                                    "contributorType": "Funder"
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
        
        assert result[0][2] == "Organizational"

    @responses.activate
    def test_dekorp_project_is_organizational(self):
        """DEKORP project should be Organizational."""
        responses.add(
            responses.GET,
            "https://api.datacite.org/dois",
            json={
                "data": [
                    {
                        "id": "10.5880/gfz.test.012",
                        "attributes": {
                            "contributors": [
                                {
                                    "name": "DEKORP",
                                    "contributorType": "DataCollector"
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
        
        assert result[0][2] == "Organizational"


class TestNameTypeInferenceRegularPersons:
    """Tests for regular person name patterns."""

    @responses.activate
    def test_comma_format_name_is_personal(self):
        """Name in 'Family, Given' format should be Personal."""
        responses.add(
            responses.GET,
            "https://api.datacite.org/dois",
            json={
                "data": [
                    {
                        "id": "10.5880/gfz.test.013",
                        "attributes": {
                            "contributors": [
                                {
                                    "name": "Müller, Hans-Peter",
                                    "contributorType": "Researcher"
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
        
        assert result[0][2] == "Personal"

    @responses.activate
    def test_name_with_given_family_fields_is_personal(self):
        """Name with givenName/familyName should be Personal."""
        responses.add(
            responses.GET,
            "https://api.datacite.org/dois",
            json={
                "data": [
                    {
                        "id": "10.5880/gfz.test.014",
                        "attributes": {
                            "contributors": [
                                {
                                    "name": "Chen Wei",
                                    "givenName": "Wei",
                                    "familyName": "Chen",
                                    "contributorType": "DataCurator"
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
        
        assert result[0][2] == "Personal"

    @responses.activate
    def test_two_word_name_is_personal(self):
        """Simple two-word name should be Personal."""
        responses.add(
            responses.GET,
            "https://api.datacite.org/dois",
            json={
                "data": [
                    {
                        "id": "10.5880/gfz.test.015",
                        "attributes": {
                            "contributors": [
                                {
                                    "name": "Maria Schmidt",
                                    "contributorType": "ContactPerson"
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
        
        assert result[0][2] == "Personal"


class TestNameTypeInferenceComplexScenarios:
    """Tests for complex real-world scenarios."""

    @responses.activate
    def test_mixed_contributors_on_single_doi(self):
        """DOI with both personal and organizational contributors."""
        responses.add(
            responses.GET,
            "https://api.datacite.org/dois",
            json={
                "data": [
                    {
                        "id": "10.5880/gfz.test.016",
                        "attributes": {
                            "contributors": [
                                {
                                    "name": "Helmholtz Centre Potsdam",
                                    "contributorType": "HostingInstitution"
                                },
                                {
                                    "name": "Schneider, Klaus",
                                    "givenName": "Klaus",
                                    "familyName": "Schneider",
                                    "contributorType": "ContactPerson",
                                    "nameIdentifiers": [
                                        {
                                            "nameIdentifier": "https://orcid.org/0000-0001-1111-2222",
                                            "nameIdentifierScheme": "ORCID"
                                        }
                                    ]
                                },
                                {
                                    "name": "DFG",
                                    "contributorType": "Funder"
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
        
        assert len(result) == 3
        assert result[0][2] == "Organizational"  # Helmholtz Centre
        assert result[1][2] == "Personal"        # Schneider with ORCID
        assert result[2][2] == "Organizational"  # DFG

    @responses.activate
    def test_multiple_dois_pagination(self):
        """Test correct nameType inference across paginated results."""
        # Page 1
        responses.add(
            responses.GET,
            "https://api.datacite.org/dois",
            json={
                "data": [
                    {
                        "id": "10.5880/gfz.test.017",
                        "attributes": {
                            "contributors": [
                                {
                                    "name": "Meyer, Anna (AWI)",
                                    "contributorType": "ContactPerson"
                                }
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
                        "id": "10.5880/gfz.test.018",
                        "attributes": {
                            "contributors": [
                                {
                                    "name": "Alfred Wegener Institute",
                                    "contributorType": "HostingInstitution",
                                    "nameIdentifiers": [
                                        {
                                            "nameIdentifier": "https://ror.org/032e6b942",
                                            "nameIdentifierScheme": "ROR"
                                        }
                                    ]
                                }
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
        assert result[0][0] == "10.5880/gfz.test.017"
        assert result[0][2] == "Personal"  # Person with affiliation
        assert result[1][0] == "10.5880/gfz.test.018"
        assert result[1][2] == "Organizational"  # Institution with ROR

    @responses.activate
    def test_contributor_with_url_in_name(self):
        """Name containing URL should be Organizational."""
        responses.add(
            responses.GET,
            "https://api.datacite.org/dois",
            json={
                "data": [
                    {
                        "id": "10.5880/gfz.test.019",
                        "attributes": {
                            "contributors": [
                                {
                                    "name": "Data Archive (https://data.gfz-potsdam.de)",
                                    "contributorType": "HostingInstitution"
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
        
        assert result[0][2] == "Organizational"

    @responses.activate
    def test_empty_contributors_skipped(self):
        """DOIs without contributors should be skipped."""
        responses.add(
            responses.GET,
            "https://api.datacite.org/dois",
            json={
                "data": [
                    {
                        "id": "10.5880/gfz.test.020",
                        "attributes": {
                            "contributors": []
                        }
                    },
                    {
                        "id": "10.5880/gfz.test.021",
                        "attributes": {
                            "contributors": [
                                {
                                    "name": "Fischer, Tobias",
                                    "contributorType": "ContactPerson"
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
        
        assert len(result) == 1
        assert result[0][0] == "10.5880/gfz.test.021"


class TestNameTypeInferenceEdgeCases:
    """Tests for edge cases in nameType inference."""

    @responses.activate
    def test_single_word_name_ambiguous(self):
        """Single word name without identifiers - defaults to Organizational."""
        responses.add(
            responses.GET,
            "https://api.datacite.org/dois",
            json={
                "data": [
                    {
                        "id": "10.5880/gfz.test.022",
                        "attributes": {
                            "contributors": [
                                {
                                    "name": "Anonymous",
                                    "contributorType": "Other"
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
        
        # Single word without org keyword or person pattern - defaults to Organizational
        # (no comma format, no given/family split detected)
        assert result[0][2] == "Organizational"

    @responses.activate
    def test_consortium_is_organizational(self):
        """Consortium should be Organizational."""
        responses.add(
            responses.GET,
            "https://api.datacite.org/dois",
            json={
                "data": [
                    {
                        "id": "10.5880/gfz.test.023",
                        "attributes": {
                            "contributors": [
                                {
                                    "name": "EPOS Consortium",
                                    "contributorType": "Distributor"
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
        
        assert result[0][2] == "Organizational"

    @responses.activate
    def test_working_group_is_organizational(self):
        """Working Group should be Organizational."""
        responses.add(
            responses.GET,
            "https://api.datacite.org/dois",
            json={
                "data": [
                    {
                        "id": "10.5880/gfz.test.024",
                        "attributes": {
                            "contributors": [
                                {
                                    "name": "IGSN Working Group",
                                    "contributorType": "Other"
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
        
        assert result[0][2] == "Organizational"

    @responses.activate
    def test_ilge_project_is_organizational(self):
        """ILGE project should be Organizational."""
        responses.add(
            responses.GET,
            "https://api.datacite.org/dois",
            json={
                "data": [
                    {
                        "id": "10.5880/gfz.test.025",
                        "attributes": {
                            "contributors": [
                                {
                                    "name": "ILGE",
                                    "contributorType": "DataCollector"
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
        
        assert result[0][2] == "Organizational"

    @responses.activate
    def test_geopribor_is_organizational(self):
        """GEOPRIBOR should be Organizational."""
        responses.add(
            responses.GET,
            "https://api.datacite.org/dois",
            json={
                "data": [
                    {
                        "id": "10.5880/gfz.test.026",
                        "attributes": {
                            "contributors": [
                                {
                                    "name": "GEOPRIBOR",
                                    "contributorType": "Producer"
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
        
        assert result[0][2] == "Organizational"

    @responses.activate
    def test_api_nametype_respected_when_no_identifiers(self):
        """API nameType Organizational is kept when name doesn't clearly look like person."""
        responses.add(
            responses.GET,
            "https://api.datacite.org/dois",
            json={
                "data": [
                    {
                        "id": "10.5880/gfz.test.027",
                        "attributes": {
                            "contributors": [
                                {
                                    "name": "Smith Jones",
                                    "nameType": "Organizational",
                                    "contributorType": "Other"
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
        
        # API says Organizational and there's no ORCID/ROR to override it
        # Two-word names without comma or given/family don't trigger person detection
        assert result[0][2] == "Organizational"


class TestContributorFieldExtraction:
    """Tests verifying all contributor fields are correctly extracted."""

    @responses.activate
    def test_all_fields_extracted(self):
        """Verify all 14 fields are correctly extracted."""
        responses.add(
            responses.GET,
            "https://api.datacite.org/dois",
            json={
                "data": [
                    {
                        "id": "10.5880/gfz.complete.001",
                        "attributes": {
                            "contributors": [
                                {
                                    "name": "Müller, Hans-Peter",
                                    "nameType": "Personal",
                                    "givenName": "Hans-Peter",
                                    "familyName": "Müller",
                                    "contributorType": "ContactPerson",
                                    "affiliation": [
                                        {
                                            "name": "GFZ German Research Centre for Geosciences",
                                            "affiliationIdentifier": "https://ror.org/04z8jg394",
                                            "affiliationIdentifierScheme": "ROR"
                                        }
                                    ],
                                    "nameIdentifiers": [
                                        {
                                            "nameIdentifier": "https://orcid.org/0000-0001-2345-6789",
                                            "nameIdentifierScheme": "ORCID",
                                            "schemeUri": "https://orcid.org"
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
        
        assert len(result) == 1
        contributor = result[0]
        
        # Check tuple structure (14 fields)
        assert len(contributor) == 14
        
        assert contributor[0] == "10.5880/gfz.complete.001"  # DOI
        assert contributor[1] == "Müller, Hans-Peter"  # Name
        assert contributor[2] == "Personal"  # NameType
        assert contributor[3] == "Hans-Peter"  # GivenName
        assert contributor[4] == "Müller"  # FamilyName
        assert contributor[5] == "https://orcid.org/0000-0001-2345-6789"  # NameIdentifier
        assert contributor[6] == "ORCID"  # NameIdentifierScheme
        assert contributor[7] == "https://orcid.org"  # SchemeUri
        assert contributor[8] == "ContactPerson"  # ContributorType
        # Note: Affiliation fields are NOT extracted for contributors in this method
        # (only nameIdentifier info is extracted to fields 9-13)
        # Fields 9-13 contain additional nameIdentifier fields or empty strings

    @responses.activate
    def test_missing_optional_fields_default_empty(self):
        """Missing optional fields should default to empty strings."""
        responses.add(
            responses.GET,
            "https://api.datacite.org/dois",
            json={
                "data": [
                    {
                        "id": "10.5880/gfz.minimal.001",
                        "attributes": {
                            "contributors": [
                                {
                                    "name": "Minimal Contributor",
                                    "contributorType": "Other"
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
        
        assert len(result) == 1
        contributor = result[0]
        assert contributor[0] == "10.5880/gfz.minimal.001"  # DOI
        assert contributor[1] == "Minimal Contributor"  # Name
        assert contributor[3] == ""  # GivenName - empty
        assert contributor[4] == ""  # FamilyName - empty
        assert contributor[5] == ""  # NameIdentifier - empty
        assert contributor[9] == ""  # Affiliation - empty
