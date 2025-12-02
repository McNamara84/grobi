"""
Tests for contributor update functionality using real-world data structures.

These tests validate the contributor update workflow without connecting to
the actual DataCite API or database. They use realistic data patterns based
on actual GFZ contributors.
"""

import pytest
from unittest.mock import MagicMock, patch

from src.workers.contributors_update_worker import ContributorsUpdateWorker
from src.api.datacite_client import DataCiteClient


# ============================================================================
# Real-world test data based on actual GFZ contributors
# ============================================================================

# Sample DataCite response for DOI 10.14470/nj617293 (seismic network)
DATACITE_CONTRIBUTORS_SEISMIC_NETWORK = [
    {
        "name": "AWI",
        "nameType": "Organizational",
        "affiliation": [],
        "contributorType": "DataCollector",
        "nameIdentifiers": []
    },
    {
        "name": "GEOFON Data Centre",
        "nameType": "Organizational",
        "affiliation": [],
        "contributorType": "DataManager",
        "nameIdentifiers": []
    },
    {
        "name": "GEOFON Data Centre",
        "nameType": "Organizational",
        "affiliation": [],
        "contributorType": "Distributor",
        "nameIdentifiers": []
    },
    {
        "name": "geofon@gfz.de",
        "nameType": "Organizational",
        "affiliation": [],
        "contributorType": "ContactPerson",
        "nameIdentifiers": []
    },
    {
        "name": "Deutsches GeoForschungsZentrum GFZ",
        "nameType": "Organizational",
        "affiliation": [],
        "contributorType": "HostingInstitution",
        "nameIdentifiers": []
    }
]

# After adding ROR identifier
DATACITE_CONTRIBUTORS_WITH_ROR = [
    {
        "name": "AWI",
        "nameType": "Organizational",
        "affiliation": [],
        "contributorType": "DataCollector",
        "nameIdentifiers": []
    },
    {
        "name": "GEOFON Data Centre",
        "nameType": "Organizational",
        "affiliation": [],
        "contributorType": "DataManager",
        "nameIdentifiers": []
    },
    {
        "name": "GEOFON Data Centre",
        "nameType": "Organizational",
        "affiliation": [],
        "contributorType": "Distributor",
        "nameIdentifiers": []
    },
    {
        "name": "geofon@gfz.de",
        "nameType": "Organizational",
        "affiliation": [],
        "contributorType": "ContactPerson",
        "nameIdentifiers": []
    },
    {
        "name": "Deutsches GeoForschungsZentrum GFZ",
        "nameType": "Organizational",
        "affiliation": [],
        "contributorType": "HostingInstitution",
        "nameIdentifiers": [
            {
                "schemeUri": "https://ror.org/",
                "nameIdentifier": "https://ror.org/04z8jg394",
                "nameIdentifierScheme": "ROR"
            }
        ]
    }
]

# Sample DataCite response for DOI with ContactPerson (10.5880/gfz.4.2.2021.003)
DATACITE_CONTRIBUTORS_WITH_CONTACT = [
    {
        "name": "Rybacki, Erik",
        "nameType": "Personal",
        "givenName": "Erik",
        "familyName": "Rybacki",
        "affiliation": [
            {"name": "GFZ German Research Centre for Geosciences"}
        ],
        "contributorType": "ContactPerson",
        "nameIdentifiers": [
            {
                "schemeUri": "https://orcid.org",
                "nameIdentifier": "https://orcid.org/0000-0002-1367-9687",
                "nameIdentifierScheme": "ORCID"
            }
        ]
    },
    {
        "name": "Helmholtz-Zentrum Potsdam Deutsches GeoForschungsZentrum GFZ",
        "nameType": "Organizational",
        "affiliation": [],
        "contributorType": "HostingInstitution",
        "nameIdentifiers": []
    }
]

# Sample DataCite response for DOI with multiple contributor types
DATACITE_CONTRIBUTORS_COMPLEX = [
    {
        "name": "Reich, Marvin",
        "nameType": "Personal",
        "givenName": "Marvin",
        "familyName": "Reich",
        "affiliation": [],
        "contributorType": "ContactPerson",
        "nameIdentifiers": [
            {
                "schemeUri": "https://orcid.org",
                "nameIdentifier": "https://orcid.org/0000-0001-7301-2094",
                "nameIdentifierScheme": "ORCID"
            }
        ]
    },
    {
        "name": "GFZ Data Services",
        "nameType": "Organizational",
        "affiliation": [],
        "contributorType": "HostingInstitution",
        "nameIdentifiers": [
            {
                "schemeUri": "https://ror.org/",
                "nameIdentifier": "https://ror.org/04z8jg394",
                "nameIdentifierScheme": "ROR"
            }
        ]
    }
]


# ============================================================================
# CSV contributor data (as parsed by csv_parser.py)
# ============================================================================

CSV_CONTRIBUTORS_ADD_ROR = [
    {
        "name": "AWI",
        "nameType": "Organizational",
        "givenName": "",
        "familyName": "",
        "nameIdentifier": "",
        "nameIdentifierScheme": "",
        "schemeUri": "",
        "contributorTypes": ["DataCollector"],
        "affiliation": "",
        "affiliationIdentifier": "",
        "email": "",
        "website": "",
        "position": ""
    },
    {
        "name": "GEOFON Data Centre",
        "nameType": "Organizational",
        "givenName": "",
        "familyName": "",
        "nameIdentifier": "",
        "nameIdentifierScheme": "",
        "schemeUri": "",
        "contributorTypes": ["DataManager"],
        "affiliation": "",
        "affiliationIdentifier": "",
        "email": "",
        "website": "",
        "position": ""
    },
    {
        "name": "GEOFON Data Centre",
        "nameType": "Organizational",
        "givenName": "",
        "familyName": "",
        "nameIdentifier": "",
        "nameIdentifierScheme": "",
        "schemeUri": "",
        "contributorTypes": ["Distributor"],
        "affiliation": "",
        "affiliationIdentifier": "",
        "email": "",
        "website": "",
        "position": ""
    },
    {
        "name": "geofon@gfz.de",
        "nameType": "Organizational",
        "givenName": "",
        "familyName": "",
        "nameIdentifier": "",
        "nameIdentifierScheme": "",
        "schemeUri": "",
        "contributorTypes": ["ContactPerson"],
        "affiliation": "",
        "affiliationIdentifier": "",
        "email": "",
        "website": "",
        "position": ""
    },
    {
        "name": "Deutsches GeoForschungsZentrum GFZ",
        "nameType": "Organizational",
        "givenName": "",
        "familyName": "",
        "nameIdentifier": "https://ror.org/04z8jg394",
        "nameIdentifierScheme": "ROR",
        "schemeUri": "https://ror.org/",
        "contributorTypes": ["HostingInstitution"],
        "affiliation": "",
        "affiliationIdentifier": "",
        "email": "",
        "website": "",
        "position": ""
    }
]

CSV_CONTRIBUTORS_EMAIL_UPDATE = [
    {
        "name": "Rybacki, Erik",
        "nameType": "Personal",
        "givenName": "Erik",
        "familyName": "Rybacki",
        "nameIdentifier": "https://orcid.org/0000-0002-1367-9687",
        "nameIdentifierScheme": "ORCID",
        "schemeUri": "https://orcid.org",
        "contributorTypes": ["ContactPerson"],
        "affiliation": "",
        "affiliationIdentifier": "",
        "email": "erik.rybacki@gfz.de",  # Changed from @gfz-potsdam.de
        "website": "",
        "position": ""
    }
]

CSV_CONTRIBUTORS_MULTIPLE_DB_UPDATES = [
    {
        "name": "Reich, Marvin",
        "nameType": "Personal",
        "givenName": "Marvin",
        "familyName": "Reich",
        "nameIdentifier": "https://orcid.org/0000-0001-7301-2094",
        "nameIdentifierScheme": "ORCID",
        "schemeUri": "https://orcid.org",
        "contributorTypes": ["ContactPerson"],
        "affiliation": "",
        "affiliationIdentifier": "",
        "email": "mreich@gfz.de",  # Changed
        "website": "www.gfz.de/staff/marvin.reich",  # Changed
        "position": "PhD"
    }
]


@pytest.fixture
def worker(tmp_path):
    """Create a ContributorsUpdateWorker for testing."""
    # Create a minimal CSV file
    csv_path = tmp_path / "test.csv"
    csv_path.write_text(
        "DOI,Contributor Name,Name Type,Given Name,Family Name,"
        "Name Identifier,Name Identifier Scheme,Scheme URI,"
        "Contributor Types,Affiliation,Affiliation Identifier,"
        "Email,Website,Position\n"
        "10.5880/test,Test,Personal,Test,User,,,,"
        "Researcher,,,,,"
    )
    
    return ContributorsUpdateWorker(
        username='TIB.GFZ',
        password='secret',
        csv_path=str(csv_path),
        use_test_api=True,
        dry_run_only=True
    )


@pytest.fixture
def datacite_client():
    """Create a DataCiteClient for testing."""
    return DataCiteClient(
        username='TIB.GFZ',
        password='secret',
        use_test_api=True
    )


class TestRealDataChangeDetection:
    """Tests for change detection using real-world data structures."""
    
    def test_detect_ror_addition(self, worker):
        """Test detecting addition of ROR identifier to organization."""
        current_metadata = {
            'data': {
                'attributes': {
                    'contributors': DATACITE_CONTRIBUTORS_SEISMIC_NETWORK
                }
            }
        }
        
        has_changes, description = worker._detect_contributor_changes(
            current_metadata, CSV_CONTRIBUTORS_ADD_ROR
        )
        
        assert has_changes is True
        # Should detect ORCID (actually ROR) change for GFZ
        assert 'ORCID geändert' in description or 'GFZ' in description or 'geändert' in description
    
    def test_detect_email_only_change(self, worker):
        """Test detecting email-only change (DB field)."""
        current_metadata = {
            'data': {
                'attributes': {
                    'contributors': DATACITE_CONTRIBUTORS_WITH_CONTACT
                }
            }
        }
        
        has_changes, description = worker._detect_contributor_changes(
            current_metadata, CSV_CONTRIBUTORS_EMAIL_UPDATE
        )
        
        # Should detect DB-only email change
        assert has_changes is True
        assert 'E-Mail' in description or 'DB' in description
    
    def test_detect_multiple_db_changes(self, worker):
        """Test detecting multiple DB field changes (email + website)."""
        current_metadata = {
            'data': {
                'attributes': {
                    'contributors': DATACITE_CONTRIBUTORS_COMPLEX
                }
            }
        }
        
        has_changes, description = worker._detect_contributor_changes(
            current_metadata, CSV_CONTRIBUTORS_MULTIPLE_DB_UPDATES
        )
        
        assert has_changes is True
        # Should mention email, website, and position
        assert 'E-Mail' in description or 'Website' in description or 'Position' in description
    
    def test_no_changes_identical_data(self, worker):
        """Test no changes when data is identical."""
        # Create CSV data that exactly matches DataCite
        csv_contributors = [
            {
                "name": "AWI",
                "nameType": "Organizational",
                "givenName": "",
                "familyName": "",
                "nameIdentifier": "",
                "contributorTypes": ["DataCollector"],
                "email": "",
                "website": "",
                "position": ""
            }
        ]
        
        current_metadata = {
            'data': {
                'attributes': {
                    'contributors': [
                        {
                            "name": "AWI",
                            "nameType": "Organizational",
                            "contributorType": "DataCollector",
                            "nameIdentifiers": []
                        }
                    ]
                }
            }
        }
        
        has_changes, description = worker._detect_contributor_changes(
            current_metadata, csv_contributors
        )
        
        assert has_changes is False
        assert 'Keine Änderungen' in description
    
    def test_partial_update_single_contributor(self, worker):
        """Test partial update with only one of multiple contributors."""
        current_metadata = {
            'data': {
                'attributes': {
                    'contributors': DATACITE_CONTRIBUTORS_WITH_CONTACT
                }
            }
        }
        
        # Only update Rybacki, not the other contributor
        has_changes, description = worker._detect_contributor_changes(
            current_metadata, CSV_CONTRIBUTORS_EMAIL_UPDATE
        )
        
        assert has_changes is True
    
    def test_matching_by_orcid(self, worker):
        """Test that contributors are matched by ORCID."""
        current_metadata = {
            'data': {
                'attributes': {
                    'contributors': DATACITE_CONTRIBUTORS_COMPLEX
                }
            }
        }
        
        # CSV with same ORCID but slightly different name format
        csv_contributors = [
            {
                "name": "Marvin Reich",  # Different format: "Given Family" vs "Family, Given"
                "nameType": "Personal",
                "givenName": "Marvin",
                "familyName": "Reich",
                "nameIdentifier": "https://orcid.org/0000-0001-7301-2094",  # Same ORCID
                "contributorTypes": ["ContactPerson"],
                "email": "test@example.com",
                "website": "",
                "position": ""
            }
        ]
        
        has_changes, description = worker._detect_contributor_changes(
            current_metadata, csv_contributors
        )
        
        # Should match by ORCID and detect email change
        assert has_changes is True
        assert 'E-Mail' in description or 'DB' in description


class TestRealDataBuildContributorObject:
    """Tests for building contributor objects using real-world data."""
    
    def test_build_organizational_with_ror(self, datacite_client):
        """Test building organizational contributor with ROR."""
        csv_contrib = {
            "name": "Deutsches GeoForschungsZentrum GFZ",
            "nameType": "Organizational",
            "givenName": "",
            "familyName": "",
            "nameIdentifier": "https://ror.org/04z8jg394",
            "nameIdentifierScheme": "ROR",
            "schemeUri": "https://ror.org/",
            "contributorTypes": ["HostingInstitution"]
        }
        
        dc_contrib = DATACITE_CONTRIBUTORS_SEISMIC_NETWORK[4]  # GFZ without ROR
        
        result = datacite_client._build_contributor_object(csv_contrib, dc_contrib)
        
        assert result["name"] == "Deutsches GeoForschungsZentrum GFZ"
        assert result["nameType"] == "Organizational"
        assert result["contributorType"] == "HostingInstitution"
        assert "nameIdentifiers" in result
        assert len(result["nameIdentifiers"]) == 1
        assert result["nameIdentifiers"][0]["nameIdentifier"] == "https://ror.org/04z8jg394"
        assert result["nameIdentifiers"][0]["nameIdentifierScheme"] == "ROR"
    
    def test_build_personal_with_orcid(self, datacite_client):
        """Test building personal contributor with ORCID."""
        csv_contrib = {
            "name": "Rybacki, Erik",
            "nameType": "Personal",
            "givenName": "Erik",
            "familyName": "Rybacki",
            "nameIdentifier": "https://orcid.org/0000-0002-1367-9687",
            "nameIdentifierScheme": "ORCID",
            "schemeUri": "https://orcid.org",
            "contributorTypes": ["ContactPerson"]
        }
        
        dc_contrib = DATACITE_CONTRIBUTORS_WITH_CONTACT[0]
        
        result = datacite_client._build_contributor_object(csv_contrib, dc_contrib)
        
        assert result["name"] == "Rybacki, Erik"
        assert result["nameType"] == "Personal"
        assert result["givenName"] == "Erik"
        assert result["familyName"] == "Rybacki"
        assert result["contributorType"] == "ContactPerson"
        assert "nameIdentifiers" in result
        assert result["nameIdentifiers"][0]["nameIdentifierScheme"] == "ORCID"
    
    def test_build_preserves_affiliations(self, datacite_client):
        """Test that building contributor preserves original affiliations."""
        csv_contrib = {
            "name": "Rybacki, Erik",
            "nameType": "Personal",
            "givenName": "Erik",
            "familyName": "Rybacki",
            "nameIdentifier": "https://orcid.org/0000-0002-1367-9687",
            "nameIdentifierScheme": "ORCID",
            "schemeUri": "https://orcid.org",
            "contributorTypes": ["ContactPerson"]
        }
        
        dc_contrib = DATACITE_CONTRIBUTORS_WITH_CONTACT[0]
        
        result = datacite_client._build_contributor_object(csv_contrib, dc_contrib)
        
        # Should preserve the affiliation from DataCite
        assert "affiliation" in result
        assert len(result["affiliation"]) == 1
        assert result["affiliation"][0]["name"] == "GFZ German Research Centre for Geosciences"
    
    def test_build_contributor_list_format(self, datacite_client):
        """Test building contributor with contributorTypes as list."""
        csv_contrib = {
            "name": "Test Organization",
            "nameType": "Organizational",
            "contributorTypes": ["HostingInstitution", "Distributor"],  # List, not string
            "nameIdentifier": ""
        }
        
        dc_contrib = {"name": "Test Organization", "nameType": "Organizational"}
        
        result = datacite_client._build_contributor_object(csv_contrib, dc_contrib)
        
        # Should use the first contributor type
        assert result["contributorType"] == "HostingInstitution"


class TestRealDataValidation:
    """Tests for validation using real-world data."""
    
    def test_validate_matching_contributors(self, datacite_client):
        """Test validation passes when all CSV contributors match DataCite."""
        current_metadata = {
            'data': {
                'attributes': {
                    'contributors': DATACITE_CONTRIBUTORS_SEISMIC_NETWORK
                }
            }
        }
        
        # Mock get_doi_metadata to return our test data
        with patch.object(datacite_client, 'get_doi_metadata', return_value=current_metadata):
            is_valid, message = datacite_client.validate_contributors_match(
                "10.14470/nj617293",
                CSV_CONTRIBUTORS_ADD_ROR
            )
        
        assert is_valid is True
    
    def test_validate_partial_update(self, datacite_client):
        """Test validation passes for partial update (fewer CSV contributors)."""
        current_metadata = {
            'data': {
                'attributes': {
                    'contributors': DATACITE_CONTRIBUTORS_WITH_CONTACT
                }
            }
        }
        
        with patch.object(datacite_client, 'get_doi_metadata', return_value=current_metadata):
            is_valid, message = datacite_client.validate_contributors_match(
                "10.5880/gfz.4.2.2021.003",
                CSV_CONTRIBUTORS_EMAIL_UPDATE
            )
        
        assert is_valid is True
    
    def test_validate_unmatched_contributor_fails(self, datacite_client):
        """Test validation fails when CSV contributor not found in DataCite."""
        current_metadata = {
            'data': {
                'attributes': {
                    'contributors': DATACITE_CONTRIBUTORS_SEISMIC_NETWORK
                }
            }
        }
        
        csv_contributors = [
            {
                "name": "Unknown Organization",
                "nameType": "Organizational",
                "contributorTypes": ["Other"],
                "nameIdentifier": ""
            }
        ]
        
        with patch.object(datacite_client, 'get_doi_metadata', return_value=current_metadata):
            is_valid, message = datacite_client.validate_contributors_match(
                "10.14470/nj617293",
                csv_contributors
            )
        
        assert is_valid is False
        assert "nicht in DataCite gefunden" in message or "Unknown Organization" in message


class TestRealDataUpdate:
    """Tests for update logic using real-world data structures."""
    
    def test_update_adds_ror_identifier(self, datacite_client):
        """Test that update correctly adds ROR identifier."""
        current_metadata = {
            'data': {
                'attributes': {
                    'doi': '10.14470/nj617293',
                    'contributors': DATACITE_CONTRIBUTORS_SEISMIC_NETWORK
                }
            }
        }
        
        # Mock the requests.put call
        with patch('src.api.datacite_client.requests.put') as mock_put:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_put.return_value = mock_response
            
            success, message = datacite_client.update_doi_contributors(
                "10.14470/nj617293",
                CSV_CONTRIBUTORS_ADD_ROR,
                current_metadata
            )
            
            assert success is True
            
            # Verify the payload contains the ROR
            call_args = mock_put.call_args
            payload = call_args.kwargs['json']
            contributors = payload['data']['attributes']['contributors']
            
            # Find GFZ contributor
            gfz_contrib = next(
                c for c in contributors 
                if c['name'] == 'Deutsches GeoForschungsZentrum GFZ'
            )
            
            assert 'nameIdentifiers' in gfz_contrib
            assert len(gfz_contrib['nameIdentifiers']) == 1
            assert gfz_contrib['nameIdentifiers'][0]['nameIdentifierScheme'] == 'ROR'
    
    def test_update_preserves_unmatched_contributors(self, datacite_client):
        """Test that update preserves contributors not in CSV."""
        current_metadata = {
            'data': {
                'attributes': {
                    'doi': '10.5880/gfz.4.2.2021.003',
                    'contributors': DATACITE_CONTRIBUTORS_WITH_CONTACT
                }
            }
        }
        
        with patch('src.api.datacite_client.requests.put') as mock_put:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_put.return_value = mock_response
            
            success, message = datacite_client.update_doi_contributors(
                "10.5880/gfz.4.2.2021.003",
                CSV_CONTRIBUTORS_EMAIL_UPDATE,  # Only Rybacki
                current_metadata
            )
            
            assert success is True
            
            # Verify both contributors are in payload
            call_args = mock_put.call_args
            payload = call_args.kwargs['json']
            contributors = payload['data']['attributes']['contributors']
            
            assert len(contributors) == 2  # Both original contributors preserved
            
            # Verify Rybacki was updated
            rybacki = next(c for c in contributors if 'Rybacki' in c['name'])
            assert rybacki is not None
            
            # Verify GFZ was preserved unchanged
            gfz = next(c for c in contributors if 'GFZ' in c['name'])
            assert gfz is not None


class TestEdgeCases:
    """Tests for edge cases in contributor handling."""
    
    def test_duplicate_contributor_names(self, worker):
        """Test handling of duplicate contributor names (GEOFON Data Centre)."""
        # GEOFON Data Centre appears twice with different ContributorTypes
        current_metadata = {
            'data': {
                'attributes': {
                    'contributors': [
                        {
                            "name": "GEOFON Data Centre",
                            "nameType": "Organizational",
                            "contributorType": "DataManager",
                            "nameIdentifiers": []
                        },
                        {
                            "name": "GEOFON Data Centre",
                            "nameType": "Organizational",
                            "contributorType": "Distributor",
                            "nameIdentifiers": []
                        }
                    ]
                }
            }
        }
        
        csv_contributors = [
            {
                "name": "GEOFON Data Centre",
                "nameType": "Organizational",
                "contributorTypes": ["DataManager"],
                "nameIdentifier": "",
                "email": "",
                "website": "",
                "position": ""
            }
        ]
        
        # Should match first occurrence
        has_changes, description = worker._detect_contributor_changes(
            current_metadata, csv_contributors
        )
        
        # No changes since the first GEOFON matches
        assert has_changes is False
    
    def test_case_insensitive_matching(self, worker):
        """Test that name matching is case-insensitive."""
        current_metadata = {
            'data': {
                'attributes': {
                    'contributors': [
                        {
                            "name": "GEOFON Data Centre",
                            "nameType": "Organizational",
                            "contributorType": "DataManager",
                            "nameIdentifiers": []
                        }
                    ]
                }
            }
        }
        
        csv_contributors = [
            {
                "name": "geofon data centre",  # Lowercase
                "nameType": "Organizational",
                "contributorTypes": ["DataManager"],
                "nameIdentifier": "",
                "email": "",
                "website": "",
                "position": ""
            }
        ]
        
        has_changes, description = worker._detect_contributor_changes(
            current_metadata, csv_contributors
        )
        
        # Should match despite case difference
        assert has_changes is False
    
    def test_empty_contributors(self, worker):
        """Test handling when DataCite has no contributors."""
        current_metadata = {
            'data': {
                'attributes': {
                    'contributors': []
                }
            }
        }
        
        csv_contributors = [
            {
                "name": "New Contributor",
                "nameType": "Organizational",
                "contributorTypes": ["Other"],
                "nameIdentifier": "",
                "email": "",
                "website": "",
                "position": ""
            }
        ]
        
        has_changes, description = worker._detect_contributor_changes(
            current_metadata, csv_contributors
        )
        
        assert has_changes is True
        assert 'CSV enthält' in description
    
    def test_email_address_as_contributor_name(self, worker):
        """Test handling contributor where name is an email address."""
        # geofon@gfz.de is used as a contributor name in real data
        current_metadata = {
            'data': {
                'attributes': {
                    'contributors': [
                        {
                            "name": "geofon@gfz.de",
                            "nameType": "Organizational",
                            "contributorType": "ContactPerson",
                            "nameIdentifiers": []
                        }
                    ]
                }
            }
        }
        
        csv_contributors = [
            {
                "name": "geofon@gfz.de",
                "nameType": "Organizational",
                "contributorTypes": ["ContactPerson"],
                "nameIdentifier": "",
                "email": "",
                "website": "",
                "position": ""
            }
        ]
        
        has_changes, description = worker._detect_contributor_changes(
            current_metadata, csv_contributors
        )
        
        assert has_changes is False


class TestDBOnlyFields:
    """Tests specifically for DB-only field handling."""
    
    def test_email_detected_as_db_change(self, worker):
        """Test that email field is detected as DB-only change."""
        db_changes = worker._detect_db_field_changes([
            {
                "name": "Test Person",
                "email": "test@example.com",
                "website": "",
                "position": ""
            }
        ])
        
        assert len(db_changes) == 1
        assert 'E-Mail (DB)' in db_changes[0]
    
    def test_website_detected_as_db_change(self, worker):
        """Test that website field is detected as DB-only change."""
        db_changes = worker._detect_db_field_changes([
            {
                "name": "Test Person",
                "email": "",
                "website": "https://example.com",
                "position": ""
            }
        ])
        
        assert len(db_changes) == 1
        assert 'Website (DB)' in db_changes[0]
    
    def test_position_detected_as_db_change(self, worker):
        """Test that position field is detected as DB-only change."""
        db_changes = worker._detect_db_field_changes([
            {
                "name": "Test Person",
                "email": "",
                "website": "",
                "position": "PhD"
            }
        ])
        
        assert len(db_changes) == 1
        assert 'Position (DB)' in db_changes[0]
    
    def test_all_db_fields_detected(self, worker):
        """Test that all DB fields are detected."""
        db_changes = worker._detect_db_field_changes([
            {
                "name": "Test Person",
                "email": "test@example.com",
                "website": "https://example.com",
                "position": "PhD"
            }
        ])
        
        assert len(db_changes) == 3
        assert any('E-Mail' in c for c in db_changes)
        assert any('Website' in c for c in db_changes)
        assert any('Position' in c for c in db_changes)
    
    def test_no_db_changes_when_empty(self, worker):
        """Test that no DB changes detected when fields are empty."""
        db_changes = worker._detect_db_field_changes([
            {
                "name": "Test Person",
                "email": "",
                "website": "",
                "position": ""
            }
        ])
        
        assert len(db_changes) == 0
