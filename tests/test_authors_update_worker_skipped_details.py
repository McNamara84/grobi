"""Tests for AuthorsUpdateWorker skipped_details functionality (Phase 4)."""

import pytest
from unittest.mock import Mock, patch
from pathlib import Path
import tempfile

from src.workers.authors_update_worker import AuthorsUpdateWorker


class TestAuthorsUpdateWorkerSkippedDetails:
    """Test skipped_details collection and reporting for author updates."""
    
    @pytest.fixture
    def temp_authors_csv(self):
        """Create a temporary authors CSV file for testing."""
        content = (
            'DOI,Creator Name,Name Type,Given Name,Family Name,Name Identifier,Name Identifier Scheme,Scheme URI\n'
            '10.5880/GFZ.1.1.2021.001,"Smith, John",Personal,John,Smith,0000-0001-5000-0007,ORCID,https://orcid.org\n'
            '10.5880/GFZ.1.1.2021.002,"Doe, Jane",Personal,Jane,Doe,,,\n'
        )
        
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv', encoding='utf-8') as f:
            f.write(content)
            temp_path = f.name
        
        yield temp_path
        
        # Cleanup
        Path(temp_path).unlink(missing_ok=True)
    
    def test_skipped_details_empty_when_all_changed(self, qtbot, temp_authors_csv):
        """Test that skipped_details is empty when all creators changed."""
        worker = AuthorsUpdateWorker(
            "test_user", "test_pass", temp_authors_csv, 
            use_test_api=True, dry_run_only=True
        )
        
        finished_signals = []
        worker.finished.connect(lambda *args: finished_signals.append(args))
        
        # Mock DataCiteClient with different metadata
        with patch('src.workers.authors_update_worker.DataCiteClient') as MockClient:
            mock_client = Mock()
            MockClient.return_value = mock_client
            
            # Return metadata that differs from CSV
            mock_client.get_doi_metadata.side_effect = [
                {
                    'data': {
                        'attributes': {
                            'creators': [
                                {
                                    'name': 'Smith, John',
                                    'nameType': 'Personal',
                                    'givenName': 'John',
                                    'familyName': 'Smith',
                                    'nameIdentifiers': []  # ORCID removed - this is a change
                                }
                            ]
                        }
                    }
                },
                {
                    'data': {
                        'attributes': {
                            'creators': [
                                {
                                    'name': 'Doe, Jane OLD',  # Name changed
                                    'nameType': 'Personal',
                                    'givenName': 'Jane',
                                    'familyName': 'Doe OLD'
                                }
                            ]
                        }
                    }
                }
            ]
            
            mock_client.validate_creators_match.side_effect = [
                (True, "Valid"),
                (True, "Valid")
            ]
            
            worker.run()
            qtbot.wait(500)
        
        # Check finished signal
        success_count, error_count, skipped_count, error_list, skipped_details = finished_signals[0]
        
        # All creators changed - no skips
        assert skipped_count == 0
        assert len(skipped_details) == 0
    
    def test_skipped_details_mixed_scenario(self, qtbot, temp_authors_csv):
        """Test skipped_details with mixed changed/unchanged creators."""
        worker = AuthorsUpdateWorker(
            "test_user", "test_pass", temp_authors_csv, 
            use_test_api=True, dry_run_only=True
        )
        
        finished_signals = []
        worker.finished.connect(lambda *args: finished_signals.append(args))
        
        # Mock DataCiteClient: first unchanged, second changed
        with patch('src.workers.authors_update_worker.DataCiteClient') as MockClient:
            mock_client = Mock()
            MockClient.return_value = mock_client
            
            mock_client.get_doi_metadata.side_effect = [
                # First DOI: unchanged
                {
                    'data': {
                        'attributes': {
                            'creators': [
                                {
                                    'name': 'Smith, John',
                                    'nameType': 'Personal',
                                    'givenName': 'John',
                                    'familyName': 'Smith',
                                    'nameIdentifiers': [
                                        {
                                            'nameIdentifier': 'https://orcid.org/0000-0001-5000-0007',
                                            'nameIdentifierScheme': 'ORCID'
                                        }
                                    ]
                                }
                            ]
                        }
                    }
                },
                # Second DOI: changed (name different)
                {
                    'data': {
                        'attributes': {
                            'creators': [
                                {
                                    'name': 'Doe, Jane OLD',
                                    'nameType': 'Personal',
                                    'givenName': 'Jane',
                                    'familyName': 'Doe OLD'
                                }
                            ]
                        }
                    }
                }
            ]
            
            mock_client.validate_creators_match.side_effect = [
                (True, "Valid"),
                (True, "Valid")
            ]
            
            worker.run()
            qtbot.wait(500)
        
        # Check finished signal
        success_count, error_count, skipped_count, error_list, skipped_details = finished_signals[0]
        
        # One skipped, one to be updated
        assert skipped_count == 1
        assert len(skipped_details) == 1
        
        # Verify skipped DOI
        doi, reason = skipped_details[0]
        assert doi == "10.5880/GFZ.1.1.2021.001"
    
    def test_skipped_details_during_actual_update(self, qtbot, temp_authors_csv):
        """Test skipped_details during actual update (not dry run)."""
        worker = AuthorsUpdateWorker(
            "test_user", "test_pass", temp_authors_csv, 
            use_test_api=True, dry_run_only=False
        )
        
        finished_signals = []
        worker.finished.connect(lambda *args: finished_signals.append(args))
        
        dry_run_results = []
        worker.dry_run_complete.connect(lambda *args: dry_run_results.append(args))
        
        # Mock DataCiteClient
        with patch('src.workers.authors_update_worker.DataCiteClient') as MockClient:
            mock_client = Mock()
            MockClient.return_value = mock_client
            
            # First DOI: unchanged, Second DOI: changed
            mock_client.get_doi_metadata.side_effect = [
                {
                    'data': {
                        'attributes': {
                            'creators': [
                                {
                                    'name': 'Smith, John',
                                    'nameType': 'Personal',
                                    'givenName': 'John',
                                    'familyName': 'Smith',
                                    'nameIdentifiers': [
                                        {
                                            'nameIdentifier': 'https://orcid.org/0000-0001-5000-0007',
                                            'nameIdentifierScheme': 'ORCID'
                                        }
                                    ]
                                }
                            ]
                        }
                    }
                },
                {
                    'data': {
                        'attributes': {
                            'creators': [
                                {
                                    'name': 'Doe, Jane OLD',
                                    'nameType': 'Personal',
                                    'givenName': 'Jane',
                                    'familyName': 'Doe OLD'
                                }
                            ]
                        }
                    }
                }
            ]
            
            mock_client.validate_creators_match.side_effect = [
                (True, "Valid"),
                (True, "Valid")
            ]
            
            mock_client.update_doi_creators.return_value = (True, "Success")
            
            worker.run()
            qtbot.wait(200)
        
        # Check finished signal
        assert len(finished_signals) == 1
        success_count, error_count, skipped_count, error_list, skipped_details = finished_signals[0]
        
        # One skipped (unchanged), one updated
        assert skipped_count == 1
        assert success_count == 1
        assert len(skipped_details) == 1
        
        doi, reason = skipped_details[0]
        assert doi == "10.5880/GFZ.1.1.2021.001"
    
    def test_skipped_details_logging(self, qtbot, temp_authors_csv, caplog):
        """Test that skipped details are logged."""
        import logging
        caplog.set_level(logging.INFO)
        
        worker = AuthorsUpdateWorker(
            "test_user", "test_pass", temp_authors_csv, 
            use_test_api=True, dry_run_only=False
        )
        
        finished_signals = []
        worker.finished.connect(lambda *args: finished_signals.append(args))
        
        # Mock DataCiteClient with unchanged metadata
        with patch('src.workers.authors_update_worker.DataCiteClient') as MockClient:
            mock_client = Mock()
            MockClient.return_value = mock_client
            
            mock_client.get_doi_metadata.side_effect = [
                {
                    'data': {
                        'attributes': {
                            'creators': [
                                {
                                    'name': 'Smith, John',
                                    'nameType': 'Personal',
                                    'givenName': 'John',
                                    'familyName': 'Smith',
                                    'nameIdentifiers': [
                                        {
                                            'nameIdentifier': 'https://orcid.org/0000-0001-5000-0007',
                                            'nameIdentifierScheme': 'ORCID'
                                        }
                                    ]
                                }
                            ]
                        }
                    }
                },
                {
                    'data': {
                        'attributes': {
                            'creators': [
                                {
                                    'name': 'Doe, Jane',
                                    'nameType': 'Personal',
                                    'givenName': 'Jane',
                                    'familyName': 'Doe',
                                    'nameIdentifiers': []
                                }
                            ]
                        }
                    }
                }
            ]
            
            mock_client.validate_creators_match.side_effect = [
                (True, "Valid"),
                (True, "Valid")
            ]
            
            worker.run()
            qtbot.wait(200)
        
        # Check that skipped DOIs were logged
        assert "Skipped DOIs (2 total):" in caplog.text
        assert "10.5880/GFZ.1.1.2021.001" in caplog.text
        assert "10.5880/GFZ.1.1.2021.002" in caplog.text
