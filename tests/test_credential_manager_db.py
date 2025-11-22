"""
Tests for database credential management functions.
"""

import pytest
from unittest.mock import patch, MagicMock

from src.utils.credential_manager import (
    save_db_credentials,
    load_db_credentials,
    delete_db_credentials,
    db_credentials_exist,
    CredentialStorageError
)


@pytest.fixture
def mock_keyring():
    """Mock keyring module."""
    with patch('src.utils.credential_manager.keyring') as mock:
        yield mock


@pytest.fixture
def mock_qsettings():
    """Mock QSettings - needs to be patched where it's imported (PySide6.QtCore)."""
    with patch('PySide6.QtCore.QSettings') as mock_class:
        settings_instance = MagicMock()
        mock_class.return_value = settings_instance
        yield settings_instance


class TestSaveDBCredentials:
    """Tests for save_db_credentials function."""
    
    def test_save_valid_credentials(self, mock_keyring):
        """Test saving valid database credentials."""
        save_db_credentials(
            "rz-mysql3.gfz-potsdam.de",
            "sumario-pmd",
            "test_user",
            "test_password"
        )
        
        # Verify keyring.set_password was called
        mock_keyring.set_password.assert_called_once()
        args = mock_keyring.set_password.call_args[0]
        assert args[0] == "GROBI_SumarioPMD"
        assert "rz-mysql3.gfz-potsdam.de|sumario-pmd|test_user" in args[1]
        assert args[2] == "test_password"
    
    def test_save_empty_host_raises_error(self, mock_keyring):
        """Test that empty host raises ValueError."""
        with pytest.raises(ValueError, match="All database credentials must be provided"):
            save_db_credentials("", "database", "user", "password")
    
    def test_save_empty_password_raises_error(self, mock_keyring):
        """Test that empty password raises ValueError."""
        with pytest.raises(ValueError, match="All database credentials must be provided"):
            save_db_credentials("host", "database", "user", "")
    
    def test_save_keyring_failure_raises_error(self, mock_keyring):
        """Test that keyring failure raises CredentialStorageError."""
        mock_keyring.set_password.side_effect = Exception("Keyring error")
        
        with pytest.raises(CredentialStorageError, match="Failed to store database credentials"):
            save_db_credentials("host", "database", "user", "password")


class TestLoadDBCredentials:
    """Tests for load_db_credentials function."""
    
    def test_load_existing_credentials(self, mock_keyring, mock_qsettings):
        """Test loading existing credentials."""
        # Setup mock QSettings
        mock_qsettings.value.side_effect = lambda key, default=None, type=None: {
            "database/configured": True,
            "database/host": "rz-mysql3.gfz-potsdam.de",
            "database/name": "sumario-pmd",
            "database/username": "test_user"
        }.get(key, default)
        
        # Setup mock keyring
        mock_keyring.get_password.return_value = "test_password"
        
        # Load credentials
        creds = load_db_credentials()
        
        # Verify
        assert creds is not None
        assert creds['host'] == "rz-mysql3.gfz-potsdam.de"
        assert creds['database'] == "sumario-pmd"
        assert creds['username'] == "test_user"
        assert creds['password'] == "test_password"
    
    def test_load_not_configured_returns_none(self, mock_qsettings):
        """Test that non-configured DB returns None."""
        mock_qsettings.value.return_value = False
        
        creds = load_db_credentials()
        
        assert creds is None
    
    def test_load_missing_password_returns_none(self, mock_keyring, mock_qsettings):
        """Test that missing password in keyring returns None."""
        mock_qsettings.value.side_effect = lambda key, default=None, type=None: {
            "database/configured": True,
            "database/host": "host",
            "database/name": "db",
            "database/username": "user"
        }.get(key, default)
        
        mock_keyring.get_password.return_value = None
        
        creds = load_db_credentials()
        
        assert creds is None


class TestDeleteDBCredentials:
    """Tests for delete_db_credentials function."""
    
    def test_delete_existing_credentials(self, mock_keyring):
        """Test deleting existing credentials."""
        result = delete_db_credentials(
            "rz-mysql3.gfz-potsdam.de",
            "sumario-pmd",
            "test_user"
        )
        
        assert result is True
        mock_keyring.delete_password.assert_called_once()
    
    def test_delete_non_existing_credentials(self, mock_keyring):
        """Test deleting non-existing credentials returns False."""
        import keyring.errors
        mock_keyring.errors = keyring.errors
        mock_keyring.delete_password.side_effect = keyring.errors.PasswordDeleteError()
        
        result = delete_db_credentials("host", "db", "user")
        
        assert result is False


class TestDBCredentialsExist:
    """Tests for db_credentials_exist function."""
    
    def test_credentials_exist_returns_true(self, mock_qsettings):
        """Test that configured DB returns True."""
        mock_qsettings.value.return_value = True
        
        result = db_credentials_exist()
        
        assert result is True
    
    def test_credentials_not_exist_returns_false(self, mock_qsettings):
        """Test that non-configured DB returns False."""
        mock_qsettings.value.return_value = False
        
        result = db_credentials_exist()
        
        assert result is False
