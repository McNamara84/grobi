"""
Credential Manager for GROBI.

Manages secure storage and retrieval of DataCite credentials using Windows Credential Manager.
Account metadata (display names, usernames, API types) are stored in a JSON file,
while passwords are stored securely in the Windows Credential Manager via keyring library.
"""

import json
import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, asdict
from datetime import datetime
import uuid

try:
    import keyring
except ImportError:
    keyring = None

logger = logging.getLogger(__name__)


class CredentialManagerError(Exception):
    """Base exception for CredentialManager errors."""
    pass


class CredentialNotFoundError(CredentialManagerError):
    """Raised when a credential account is not found."""
    pass


class CredentialStorageError(CredentialManagerError):
    """Raised when there's a problem storing credentials."""
    pass


@dataclass
class CredentialAccount:
    """Represents a stored DataCite credential account."""
    
    account_id: str
    display_name: str
    username: str
    api_type: str  # "test" or "production"
    created_at: str
    last_modified: str
    
    def __post_init__(self):
        """Validate api_type after initialization."""
        if self.api_type not in ["test", "production"]:
            raise ValueError(f"Invalid api_type: {self.api_type}. Must be 'test' or 'production'.")
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON serialization."""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'CredentialAccount':
        """Create instance from dictionary."""
        return cls(**data)


class CredentialManager:
    """
    Manages DataCite credentials with secure password storage.
    
    Uses Windows Credential Manager (via keyring) for password storage
    and a JSON file for account metadata.
    """
    
    SERVICE_NAME = "GROBI_DataCite"
    DB_SERVICE_NAME = "GROBI_SumarioPMD"
    METADATA_FILE = "credentials_metadata.json"
    
    def __init__(self):
        """Initialize CredentialManager and load existing accounts."""
        if keyring is None:
            raise CredentialStorageError(
                "keyring library not available. Please install with: pip install keyring"
            )
        
        self.metadata_path = self._get_metadata_path()
        self.accounts: Dict[str, CredentialAccount] = {}
        self.last_used_account: Optional[str] = None
        
        self._load_metadata()
        logger.info(f"CredentialManager initialized with {len(self.accounts)} accounts")
    
    def save_credentials(
        self, 
        display_name: str, 
        username: str, 
        password: str, 
        api_type: str
    ) -> str:
        """
        Save new credentials to Windows Credential Manager and metadata file.
        
        Args:
            display_name: User-friendly name (e.g., "GFZ Production")
            username: DataCite username (e.g., "TIB.GFZ")
            password: DataCite password
            api_type: "test" or "production"
            
        Returns:
            account_id: Unique identifier for the saved account
            
        Raises:
            CredentialStorageError: If storage fails
            ValueError: If api_type is invalid
        """
        # Validate inputs
        if not display_name or not display_name.strip():
            raise ValueError("Display name cannot be empty")
        if not username or not username.strip():
            raise ValueError("Username cannot be empty")
        if not password:
            raise ValueError("Password cannot be empty")
        if api_type not in ["test", "production"]:
            raise ValueError(f"Invalid api_type: {api_type}")
        
        # Limit display name length
        display_name = display_name.strip()[:100]
        
        # Generate unique account ID
        account_id = str(uuid.uuid4())
        
        # Store password in Windows Credential Manager
        try:
            keyring.set_password(self.SERVICE_NAME, account_id, password)
            logger.info(f"Password stored in Windows Credential Manager for account {account_id}")
        except Exception as e:
            logger.error(f"Failed to store password in Credential Manager: {e}")
            raise CredentialStorageError(f"Failed to store password: {str(e)}")
        
        # Create account metadata
        now = datetime.now().isoformat()
        account = CredentialAccount(
            account_id=account_id,
            display_name=display_name,
            username=username,
            api_type=api_type,
            created_at=now,
            last_modified=now
        )
        
        # Store in memory and save to file
        self.accounts[account_id] = account
        self._save_metadata()
        
        logger.info(
            f"Saved credentials: {display_name} ({username}) - {api_type} API"
        )
        
        return account_id
    
    def get_credentials(self, account_id: str) -> Tuple[str, str, str]:
        """
        Load credentials for a specific account.
        
        Args:
            account_id: Unique account identifier
            
        Returns:
            Tuple of (username, password, api_type)
            
        Raises:
            CredentialNotFoundError: If account doesn't exist
            CredentialStorageError: If password retrieval fails
        """
        # Check if account exists
        if account_id not in self.accounts:
            raise CredentialNotFoundError(f"Account {account_id} not found")
        
        account = self.accounts[account_id]
        
        # Retrieve password from Windows Credential Manager
        try:
            password = keyring.get_password(self.SERVICE_NAME, account_id)
            if password is None:
                raise CredentialStorageError(
                    f"Password not found in Credential Manager for account {account_id}"
                )
        except Exception as e:
            logger.error(f"Failed to retrieve password: {e}")
            raise CredentialStorageError(f"Failed to retrieve password: {str(e)}")
        
        logger.info(f"Retrieved credentials for: {account.display_name}")
        
        return account.username, password, account.api_type
    
    def list_accounts(self) -> List[CredentialAccount]:
        """
        Get list of all stored accounts.
        
        Returns:
            List of CredentialAccount objects sorted by last_modified (newest first)
        """
        accounts = list(self.accounts.values())
        # Sort by last_modified, newest first
        accounts.sort(key=lambda x: x.last_modified, reverse=True)
        return accounts
    
    def delete_account(self, account_id: str) -> bool:
        """
        Delete an account and its stored password.
        
        Args:
            account_id: Unique account identifier
            
        Returns:
            True if account was deleted, False if it didn't exist
        """
        if account_id not in self.accounts:
            logger.warning(f"Attempted to delete non-existent account: {account_id}")
            return False
        
        account = self.accounts[account_id]
        
        # Delete password from Windows Credential Manager
        try:
            keyring.delete_password(self.SERVICE_NAME, account_id)
            logger.info(f"Password deleted from Credential Manager for: {account.display_name}")
        except keyring.errors.PasswordDeleteError:
            logger.warning(f"Password not found in Credential Manager for: {account.display_name}")
        except Exception as e:
            logger.error(f"Error deleting password: {e}")
            # Continue anyway - remove from metadata even if password deletion fails
        
        # Remove from memory and save
        del self.accounts[account_id]
        
        # Clear last_used if it was this account
        if self.last_used_account == account_id:
            self.last_used_account = None
        
        self._save_metadata()
        
        logger.info(f"Deleted account: {account.display_name} ({account_id})")
        
        return True
    
    def update_display_name(self, account_id: str, new_name: str) -> bool:
        """
        Update the display name of an account.
        
        Args:
            account_id: Unique account identifier
            new_name: New display name
            
        Returns:
            True if updated, False if account doesn't exist
            
        Raises:
            ValueError: If new_name is empty
        """
        if account_id not in self.accounts:
            logger.warning(f"Attempted to update non-existent account: {account_id}")
            return False
        
        if not new_name or not new_name.strip():
            raise ValueError("Display name cannot be empty")
        
        new_name = new_name.strip()[:100]  # Limit length
        
        account = self.accounts[account_id]
        old_name = account.display_name
        account.display_name = new_name
        account.last_modified = datetime.now().isoformat()
        
        self._save_metadata()
        
        logger.info(f"Updated display name: '{old_name}' → '{new_name}'")
        
        return True
    
    def get_last_used_account(self) -> Optional[str]:
        """
        Get the ID of the last used account.
        
        Returns:
            account_id or None if no account was used yet
        """
        return self.last_used_account
    
    def set_last_used_account(self, account_id: str):
        """
        Mark an account as last used.
        
        Args:
            account_id: Unique account identifier
            
        Raises:
            CredentialNotFoundError: If account doesn't exist
        """
        if account_id not in self.accounts:
            raise CredentialNotFoundError(f"Account {account_id} not found")
        
        self.last_used_account = account_id
        self._save_metadata()
        
        logger.info(f"Set last used account: {self.accounts[account_id].display_name}")
    
    def _get_metadata_path(self) -> Path:
        """
        Get path to metadata file in AppData/Roaming/GROBI.
        
        Returns:
            Path to credentials_metadata.json
        """
        # Windows: %APPDATA%\GROBI
        appdata = Path.home() / "AppData" / "Roaming" / "GROBI"
        appdata.mkdir(parents=True, exist_ok=True)
        
        return appdata / self.METADATA_FILE
    
    def _load_metadata(self):
        """Load account metadata from JSON file."""
        if not self.metadata_path.exists():
            logger.info("No existing metadata file found, starting fresh")
            return
        
        try:
            with open(self.metadata_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Load accounts
            accounts_data = data.get('accounts', {})
            for account_id, account_dict in accounts_data.items():
                try:
                    account = CredentialAccount.from_dict(account_dict)
                    self.accounts[account_id] = account
                except Exception as e:
                    logger.error(f"Failed to load account {account_id}: {e}")
            
            # Load last used account
            self.last_used_account = data.get('last_used_account')
            
            logger.info(f"Loaded {len(self.accounts)} accounts from metadata file")
            
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse metadata file: {e}")
            raise CredentialStorageError(f"Corrupted metadata file: {str(e)}")
        except Exception as e:
            logger.error(f"Failed to load metadata: {e}")
            raise CredentialStorageError(f"Failed to load metadata: {str(e)}")
    
    def _save_metadata(self):
        """Save account metadata to JSON file."""
        data = {
            'accounts': {
                account_id: account.to_dict()
                for account_id, account in self.accounts.items()
            },
            'last_used_account': self.last_used_account
        }
        
        try:
            # Write with pretty formatting for readability
            with open(self.metadata_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            
            logger.debug(f"Saved metadata to {self.metadata_path}")
            
        except Exception as e:
            logger.error(f"Failed to save metadata: {e}")
            raise CredentialStorageError(f"Failed to save metadata: {str(e)}")


# Database Credential Functions (separate from class for simplicity)

def save_db_credentials(host: str, database: str, username: str, password: str) -> None:
    """
    Save database credentials to Windows Credential Manager.
    
    Args:
        host: Database host (e.g., "rz-mysql3.gfz-potsdam.de")
        database: Database name (e.g., "sumario-pmd")
        username: Database username
        password: Database password
        
    Raises:
        CredentialStorageError: If storage fails
        ValueError: If any parameter is empty
    """
    if keyring is None:
        raise CredentialStorageError(
            "keyring library not available. Please install with: pip install keyring"
        )
    
    # Validate inputs
    if not all([host, database, username, password]):
        raise ValueError("All database credentials must be provided")
    
    # Store composite identifier as keyring username
    # Format: host|database|username
    identifier = f"{host}|{database}|{username}"
    
    try:
        keyring.set_password(CredentialManager.DB_SERVICE_NAME, identifier, password)
        logger.info(f"Database credentials saved: {username}@{host}/{database}")
    except Exception as e:
        logger.error(f"Failed to store database credentials: {e}")
        raise CredentialStorageError(f"Failed to store database credentials: {str(e)}")


def load_db_credentials() -> Optional[Dict[str, str]]:
    """
    Load database credentials from QSettings (metadata) and Keyring (password).
    
    Returns:
        Dictionary with keys: host, database, username, password
        or None if no credentials stored
        
    Raises:
        CredentialStorageError: If retrieval fails
    """
    if keyring is None:
        raise CredentialStorageError(
            "keyring library not available. Please install with: pip install keyring"
        )
    
    from PySide6.QtCore import QSettings
    
    settings = QSettings("GFZ", "GROBI")
    
    # Check if configured
    if not settings.value("database/configured", False, type=bool):
        return None
    
    try:
        # Load metadata from QSettings
        host = settings.value("database/host")
        database = settings.value("database/name")
        username = settings.value("database/username")
        
        if not all([host, database, username]):
            logger.warning("Incomplete database metadata in QSettings")
            return None
        
        # Construct identifier for keyring lookup
        identifier = f"{host}|{database}|{username}"
        
        # Load password from keyring
        password = keyring.get_password(CredentialManager.DB_SERVICE_NAME, identifier)
        
        if password is None:
            logger.warning(f"Password not found in keyring for: {identifier}")
            return None
        
        logger.info(f"Database credentials loaded: {username}@{host}/{database}")
        
        return {
            'host': host,
            'database': database,
            'username': username,
            'password': password
        }
        
    except Exception as e:
        logger.error(f"Failed to load database credentials: {e}")
        raise CredentialStorageError(f"Failed to load database credentials: {str(e)}")


def delete_db_credentials(host: str, database: str, username: str) -> bool:
    """
    Delete database credentials from Windows Credential Manager.
    
    Args:
        host: Database host
        database: Database name
        username: Database username
        
    Returns:
        True if deleted, False if not found
        
    Raises:
        CredentialStorageError: If deletion fails
    """
    if keyring is None:
        raise CredentialStorageError(
            "keyring library not available. Please install with: pip install keyring"
        )
    
    identifier = f"{host}|{database}|{username}"
    
    try:
        keyring.delete_password(CredentialManager.DB_SERVICE_NAME, identifier)
        logger.info(f"Database credentials deleted: {username}@{host}/{database}")
        return True
    except keyring.errors.PasswordDeleteError:
        logger.warning(f"Database credentials not found: {identifier}")
        return False
    except Exception as e:
        logger.error(f"Failed to delete database credentials: {e}")
        raise CredentialStorageError(f"Failed to delete database credentials: {str(e)}")


def db_credentials_exist() -> bool:
    """
    Check if database credentials are stored.
    
    Returns:
        True if credentials exist, False otherwise
    """
    # This is a simplified check - we'll use QSettings to track this
    from PySide6.QtCore import QSettings
    
    settings = QSettings("GFZ", "GROBI")
    db_configured = settings.value("database/configured", False, type=bool)
    
    return db_configured
