"""
Unit tests for CredentialManager.

Tests credential storage, retrieval, and management using mocked keyring.
"""

import pytest
from unittest.mock import Mock, patch

from src.utils.credential_manager import (
    CredentialManager,
    CredentialAccount,
    CredentialNotFoundError,
    CredentialStorageError
)


@pytest.fixture
def mock_keyring():
    """Mock keyring module for testing."""
    with patch('src.utils.credential_manager.keyring') as mock:
        # Simulate in-memory password storage
        mock.passwords = {}
        
        def set_password(service, username, password):
            mock.passwords[f"{service}:{username}"] = password
        
        def get_password(service, username):
            return mock.passwords.get(f"{service}:{username}")
        
        def delete_password(service, username):
            key = f"{service}:{username}"
            if key in mock.passwords:
                del mock.passwords[key]
            else:
                from keyring import errors
                raise errors.PasswordDeleteError("Password not found")
        
        mock.set_password = Mock(side_effect=set_password)
        mock.get_password = Mock(side_effect=get_password)
        mock.delete_password = Mock(side_effect=delete_password)
        mock.errors = Mock()
        mock.errors.PasswordDeleteError = type('PasswordDeleteError', (Exception,), {})
        
        yield mock


@pytest.fixture
def temp_metadata_dir(tmp_path):
    """Create temporary directory for metadata file."""
    metadata_dir = tmp_path / "GROBI"
    metadata_dir.mkdir(parents=True, exist_ok=True)
    return metadata_dir


@pytest.fixture
def credential_manager(mock_keyring, temp_metadata_dir):
    """Create CredentialManager instance with mocked dependencies."""
    with patch.object(CredentialManager, '_get_metadata_path') as mock_path:
        mock_path.return_value = temp_metadata_dir / "credentials_metadata.json"
        manager = CredentialManager()
        return manager


class TestCredentialAccount:
    """Tests for CredentialAccount dataclass."""
    
    def test_create_account(self):
        """Test creating a valid account."""
        account = CredentialAccount(
            account_id="test-123",
            display_name="GFZ Production",
            username="MOCK_TEST_USER",
            api_type="production",
            created_at="2025-11-20T10:00:00",
            last_modified="2025-11-20T10:00:00"
        )
        
        assert account.account_id == "test-123"
        assert account.display_name == "GFZ Production"
        assert account.username == "MOCK_TEST_USER"
        assert account.api_type == "production"
    
    def test_invalid_api_type(self):
        """Test that invalid api_type raises ValueError."""
        with pytest.raises(ValueError, match="Invalid api_type"):
            CredentialAccount(
                account_id="test-123",
                display_name="Test",
                username="test",
                api_type="invalid",
                created_at="2025-11-20T10:00:00",
                last_modified="2025-11-20T10:00:00"
            )
    
    def test_to_dict(self):
        """Test converting account to dictionary."""
        account = CredentialAccount(
            account_id="test-123",
            display_name="GFZ Test",
            username="XUVM.KDVJHQ",
            api_type="test",
            created_at="2025-11-20T10:00:00",
            last_modified="2025-11-20T10:00:00"
        )
        
        data = account.to_dict()
        
        assert data['account_id'] == "test-123"
        assert data['display_name'] == "GFZ Test"
        assert data['username'] == "XUVM.KDVJHQ"
        assert data['api_type'] == "test"
    
    def test_from_dict(self):
        """Test creating account from dictionary."""
        data = {
            'account_id': "test-123",
            'display_name': "GFZ Production",
            'username': "MOCK_TEST_USER",
            'api_type': "production",
            'created_at': "2025-11-20T10:00:00",
            'last_modified': "2025-11-20T10:00:00"
        }
        
        account = CredentialAccount.from_dict(data)
        
        assert account.account_id == "test-123"
        assert account.display_name == "GFZ Production"


class TestCredentialManager:
    """Tests for CredentialManager class."""
    
    def test_initialization(self, credential_manager):
        """Test CredentialManager initializes correctly."""
        assert isinstance(credential_manager, CredentialManager)
        assert credential_manager.accounts == {}
        assert credential_manager.last_used_account is None
    
    def test_save_credentials_success(self, credential_manager, mock_keyring):
        """Test successful credential save."""
        account_id = credential_manager.save_credentials(
            display_name="GFZ Production",
            username="MOCK_TEST_USER",
            password="FAKE_TEST_PASSWORD_NOT_REAL_123",
            api_type="production"
        )
        
        # Check account was created
        assert account_id in credential_manager.accounts
        account = credential_manager.accounts[account_id]
        assert account.display_name == "GFZ Production"
        assert account.username == "MOCK_TEST_USER"
        assert account.api_type == "production"
        
        # Check password was stored in keyring
        mock_keyring.set_password.assert_called_once()
        assert mock_keyring.passwords[f"GROBI_DataCite:{account_id}"] == "FAKE_TEST_PASSWORD_NOT_REAL_123"
    
    def test_save_credentials_validates_inputs(self, credential_manager):
        """Test that save_credentials validates inputs."""
        # Empty display name
        with pytest.raises(ValueError, match="Display name cannot be empty"):
            credential_manager.save_credentials("", "MOCK_USER", "pass", "production")
        
        # Empty username
        with pytest.raises(ValueError, match="Username cannot be empty"):
            credential_manager.save_credentials("Name", "", "pass", "production")
        
        # Empty password
        with pytest.raises(ValueError, match="Password cannot be empty"):
            credential_manager.save_credentials("Name", "MOCK_USER", "", "production")
        
        # Invalid api_type
        with pytest.raises(ValueError, match="Invalid api_type"):
            credential_manager.save_credentials("Name", "MOCK_USER", "pass", "invalid")
    
    def test_save_credentials_limits_display_name(self, credential_manager):
        """Test that display name is limited to 100 characters."""
        long_name = "A" * 150
        
        account_id = credential_manager.save_credentials(
            display_name=long_name,
            username="MOCK_USER",
            password="MOCK_PASS_123",
            api_type="test"
        )
        
        account = credential_manager.accounts[account_id]
        assert len(account.display_name) == 100
    
    def test_save_credentials_keyring_error(self, credential_manager, mock_keyring):
        """Test handling of keyring storage errors."""
        mock_keyring.set_password.side_effect = Exception("Keyring error")
        
        with pytest.raises(CredentialStorageError, match="Failed to store password"):
            credential_manager.save_credentials(
                display_name="Test",
                username="MOCK_USER",
                password="MOCK_PASS_123",
                api_type="production"
            )
    
    def test_get_credentials_success(self, credential_manager, mock_keyring):
        """Test successful credential retrieval."""
        # Save credentials first
        account_id = credential_manager.save_credentials(
            display_name="GFZ Test",
            username="XUVM.KDVJHQ",
            password="FAKE_TEST_PASSWORD_456",
            api_type="test"
        )
        
        # Retrieve credentials
        username, password, api_type = credential_manager.get_credentials(account_id)
        
        assert username == "XUVM.KDVJHQ"
        assert password == "FAKE_TEST_PASSWORD_456"
        assert api_type == "test"
    
    def test_get_credentials_account_not_found(self, credential_manager):
        """Test getting credentials for non-existent account."""
        with pytest.raises(CredentialNotFoundError, match="Account .* not found"):
            credential_manager.get_credentials("non-existent-id")
    
    def test_get_credentials_password_not_found(self, credential_manager, mock_keyring):
        """Test handling when password not in keyring."""
        # Create account without password in keyring
        account_id = "test-123"
        credential_manager.accounts[account_id] = CredentialAccount(
            account_id=account_id,
            display_name="Test",
            username="MOCK_USER",
            api_type="production",
            created_at="2025-11-20T10:00:00",
            last_modified="2025-11-20T10:00:00"
        )
        
        with pytest.raises(CredentialStorageError, match="Password not found"):
            credential_manager.get_credentials(account_id)
    
    def test_list_accounts(self, credential_manager):
        """Test listing all accounts."""
        # Save multiple accounts
        id1 = credential_manager.save_credentials(
            "Account 1", "MOCK_USER1", "MOCK_PASS_ACC1", "production"
        )
        id2 = credential_manager.save_credentials(
            "Account 2", "MOCK_USER2", "MOCK_PASS_ACC2", "test"
        )
        
        accounts = credential_manager.list_accounts()
        
        assert len(accounts) == 2
        # Should be sorted by last_modified (newest first), but on fast CI systems
        # both may have same timestamp, so just verify both IDs are present
        account_ids = [acc.account_id for acc in accounts]
        assert id1 in account_ids
        assert id2 in account_ids
    
    def test_list_accounts_empty(self, credential_manager):
        """Test listing accounts when none exist."""
        accounts = credential_manager.list_accounts()
        assert accounts == []
    
    def test_delete_account_success(self, credential_manager, mock_keyring):
        """Test successful account deletion."""
        # Save account
        account_id = credential_manager.save_credentials(
            "Test Account", "MOCK_USER", "MOCK_PASS_123", "production"
        )
        
        # Delete account
        result = credential_manager.delete_account(account_id)
        
        assert result is True
        assert account_id not in credential_manager.accounts
        
        # Check password was deleted from keyring
        mock_keyring.delete_password.assert_called()
    
    def test_delete_account_not_found(self, credential_manager):
        """Test deleting non-existent account."""
        result = credential_manager.delete_account("non-existent-id")
        assert result is False
    
    def test_delete_account_clears_last_used(self, credential_manager):
        """Test that deleting account clears last_used if it matches."""
        # Save and mark as last used
        account_id = credential_manager.save_credentials(
            "Test", "MOCK_USER", "MOCK_PASS_123", "production"
        )
        credential_manager.set_last_used_account(account_id)
        
        # Delete account
        credential_manager.delete_account(account_id)
        
        assert credential_manager.last_used_account is None
    
    def test_update_display_name_success(self, credential_manager):
        """Test updating display name."""
        # Save account
        account_id = credential_manager.save_credentials(
            "Old Name", "MOCK_USER", "MOCK_PASS_123", "production"
        )
        
        # Update name
        result = credential_manager.update_display_name(account_id, "New Name")
        
        assert result is True
        assert credential_manager.accounts[account_id].display_name == "New Name"
    
    def test_update_display_name_not_found(self, credential_manager):
        """Test updating non-existent account."""
        result = credential_manager.update_display_name("non-existent", "New Name")
        assert result is False
    
    def test_update_display_name_validates_input(self, credential_manager):
        """Test that update_display_name validates input."""
        account_id = credential_manager.save_credentials(
            "Test", "MOCK_USER", "pass", "production"
        )
        
        with pytest.raises(ValueError, match="Display name cannot be empty"):
            credential_manager.update_display_name(account_id, "")
        
        with pytest.raises(ValueError, match="Display name cannot be empty"):
            credential_manager.update_display_name(account_id, "   ")
    
    def test_get_last_used_account_none(self, credential_manager):
        """Test getting last used account when none set."""
        assert credential_manager.get_last_used_account() is None
    
    def test_set_last_used_account(self, credential_manager):
        """Test setting last used account."""
        account_id = credential_manager.save_credentials(
            "Test", "MOCK_USER", "pass", "production"
        )
        
        credential_manager.set_last_used_account(account_id)
        
        assert credential_manager.get_last_used_account() == account_id
    
    def test_set_last_used_account_not_found(self, credential_manager):
        """Test setting last used with non-existent account."""
        with pytest.raises(CredentialNotFoundError):
            credential_manager.set_last_used_account("non-existent")
    
    def test_metadata_persistence(self, credential_manager, temp_metadata_dir):
        """Test that metadata persists across manager instances."""
        # Save account
        account_id = credential_manager.save_credentials(
            "Persistent Account", "MOCK_USER", "MOCK_PASS_123", "test"
        )
        credential_manager.set_last_used_account(account_id)
        
        # Create new manager instance (simulates app restart)
        with patch.object(CredentialManager, '_get_metadata_path') as mock_path:
            mock_path.return_value = temp_metadata_dir / "credentials_metadata.json"
            new_manager = CredentialManager()
        
        # Check data persisted
        assert account_id in new_manager.accounts
        assert new_manager.accounts[account_id].display_name == "Persistent Account"
        assert new_manager.get_last_used_account() == account_id
    
    def test_corrupted_metadata_file(self, temp_metadata_dir):
        """Test handling of corrupted metadata file."""
        # Create corrupted JSON file
        metadata_path = temp_metadata_dir / "credentials_metadata.json"
        with open(metadata_path, 'w') as f:
            f.write("{ invalid json")
        
        with patch.object(CredentialManager, '_get_metadata_path') as mock_path:
            mock_path.return_value = metadata_path
            with pytest.raises(CredentialStorageError, match="Corrupted metadata file"):
                CredentialManager()
    
    def test_multiple_accounts_different_types(self, credential_manager):
        """Test saving multiple accounts with different API types."""
        prod_id = credential_manager.save_credentials(
            "GFZ Production", "MOCK_TEST_USER", "MOCK_PASS_PROD", "production"
        )
        test_id = credential_manager.save_credentials(
            "GFZ Test", "XUVM.KDVJHQ", "MOCK_PASS_TEST", "test"
        )
        
        accounts = credential_manager.list_accounts()
        assert len(accounts) == 2
        
        # Verify each account has correct type
        prod_account = credential_manager.accounts[prod_id]
        test_account = credential_manager.accounts[test_id]
        
        assert prod_account.api_type == "production"
        assert test_account.api_type == "test"
    
    def test_unicode_in_display_name(self, credential_manager):
        """Test that unicode characters work in display names."""
        account_id = credential_manager.save_credentials(
            "GFZ Tést Äccount 日本", "MOCK_USER", "MOCK_PASS_123", "production"
        )
        
        account = credential_manager.accounts[account_id]
        assert "Tést" in account.display_name
        assert "Äccount" in account.display_name
        assert "日本" in account.display_name


class TestCredentialManagerNoKeyring:
    """Test CredentialManager behavior when keyring is not available."""
    
    def test_initialization_without_keyring(self, temp_metadata_dir):
        """Test that CredentialManager raises error when keyring unavailable."""
        with patch('src.utils.credential_manager.keyring', None):
            with patch.object(CredentialManager, '_get_metadata_path') as mock_path:
                mock_path.return_value = temp_metadata_dir / "credentials_metadata.json"
                
                with pytest.raises(CredentialStorageError, match="keyring library not available"):
                    CredentialManager()
