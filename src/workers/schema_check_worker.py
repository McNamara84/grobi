"""Worker for checking DOI schema versions and Schema 4 compatibility."""

import logging
from typing import List, Tuple, Dict, Any, Optional
from PySide6.QtCore import QObject, Signal

from src.api.datacite_client import (
    DataCiteClient, 
    DataCiteAPIError, 
    AuthenticationError, 
    NetworkError
)


logger = logging.getLogger(__name__)


class SchemaCheckWorker(QObject):
    """Worker for checking DOIs with old schema versions and Schema 4 compatibility."""
    
    # Signals
    progress_update = Signal(str)  # Progress message
    finished = Signal(list)  # List of incompatible DOIs with missing fields
    error_occurred = Signal(str)  # Error message
    
    def __init__(self, username: str, password: str, use_test_api: bool = False):
        """
        Initialize the schema check worker.
        
        Args:
            username: DataCite username
            password: DataCite password
            use_test_api: Whether to use test API
        """
        super().__init__()
        self.username = username
        self.password = password
        self.use_test_api = use_test_api
        self._should_stop = False
    
    def stop(self):
        """Signal the worker to stop processing."""
        self._should_stop = True
        logger.info("Schema check worker stop requested")
    
    def run(self):
        """
        Check all DOIs for old schema versions and Schema 4 compatibility.
        
        Emits:
            progress_update: Progress messages during processing
            finished: List of tuples (DOI, schema_version, missing_fields_dict, reason)
            error_occurred: Error message if something goes wrong
        """
        try:
            self.progress_update.emit("Verbindung zur DataCite API wird hergestellt...")
            
            client = DataCiteClient(
                self.username,
                self.password,
                self.use_test_api
            )
            
            self.progress_update.emit("DOIs mit Schema-Version werden abgerufen...")
            
            # Fetch all DOIs with schema versions
            dois_with_schema = client.fetch_dois_with_schema_version()
            
            if self._should_stop:
                logger.info("Schema check cancelled by user")
                return
            
            total_dois = len(dois_with_schema)
            self.progress_update.emit(f"{total_dois} DOIs gefunden. Überprüfe Schema-Versionen...")
            
            # Filter DOIs with old schema versions (2.x or 3.x)
            old_schema_dois = []
            for doi, schema_version in dois_with_schema:
                if schema_version and (schema_version.startswith("2.") or schema_version.startswith("3.") or 
                                      "kernel-2" in schema_version or "kernel-3" in schema_version):
                    old_schema_dois.append((doi, schema_version))
            
            if self._should_stop:
                logger.info("Schema check cancelled by user")
                return
            
            old_schema_count = len(old_schema_dois)
            
            if old_schema_count == 0:
                self.progress_update.emit("[OK] Alle DOIs verwenden bereits Schema 4.x")
                self.finished.emit([])
                return
            
            self.progress_update.emit(
                f"{old_schema_count} DOIs mit altem Schema gefunden (2.x oder 3.x). "
                f"Überprüfe Kompatibilität zu Schema 4..."
            )
            
            # Check each old schema DOI for Schema 4 compatibility
            incompatible_dois = []
            processed = 0
            
            for doi, schema_version in old_schema_dois:
                if self._should_stop:
                    logger.info("Schema check cancelled by user")
                    return
                
                processed += 1
                
                # Update progress every 10 DOIs
                if processed % 10 == 0 or processed == old_schema_count:
                    self.progress_update.emit(
                        f"Überprüfe DOI {processed}/{old_schema_count}: {doi}"
                    )
                
                # Check Schema 4 compatibility
                is_compatible, missing_fields = client.check_schema_4_compatibility(doi)
                
                if not is_compatible:
                    # Build reason string from missing fields
                    reason = self._build_reason_string(missing_fields)
                    incompatible_dois.append((doi, schema_version, missing_fields, reason))
                    logger.info(f"DOI {doi} (Schema {schema_version}): Not compatible - {reason}")
            
            if self._should_stop:
                logger.info("Schema check cancelled by user")
                return
            
            # Report results
            if incompatible_dois:
                self.progress_update.emit(
                    f"[WARNUNG] {len(incompatible_dois)} von {old_schema_count} DOIs "
                    f"sind NICHT kompatibel mit Schema 4"
                )
            else:
                self.progress_update.emit(
                    f"[OK] Alle {old_schema_count} DOIs mit altem Schema sind kompatibel mit Schema 4"
                )
            
            self.finished.emit(incompatible_dois)
            
        except AuthenticationError as e:
            logger.error(f"Authentication error: {e}")
            self.error_occurred.emit(str(e))
        except NetworkError as e:
            logger.error(f"Network error: {e}")
            self.error_occurred.emit(str(e))
        except DataCiteAPIError as e:
            logger.error(f"DataCite API error: {e}")
            self.error_occurred.emit(str(e))
        except Exception as e:
            logger.error(f"Unexpected error in schema check worker: {e}", exc_info=True)
            self.error_occurred.emit(f"Unerwarteter Fehler: {str(e)}")
    
    def _build_reason_string(self, missing_fields: Dict[str, Any]) -> str:
        """
        Build a human-readable reason string from missing fields dictionary.
        
        Args:
            missing_fields: Dictionary with missing/invalid fields
            
        Returns:
            Formatted reason string in German
        """
        reasons = []
        
        # Required fields
        if missing_fields.get("publisher") == "missing":
            reasons.append("Publisher fehlt")
        elif missing_fields.get("publisher") == "empty":
            reasons.append("Publisher ist leer")
        
        if missing_fields.get("publicationYear") == "missing":
            reasons.append("Erscheinungsjahr fehlt")
        elif missing_fields.get("publicationYear") == "empty":
            reasons.append("Erscheinungsjahr ist leer")
        
        if missing_fields.get("titles") == "missing":
            reasons.append("Titel fehlt")
        elif missing_fields.get("titles") == "empty":
            reasons.append("Titel ist leer")
        
        if missing_fields.get("creators") == "missing":
            reasons.append("Urheber (Creators) fehlen")
        elif missing_fields.get("creators") == "empty":
            reasons.append("Urheber (Creators) sind leer")
        
        if missing_fields.get("resourceType") == "missing":
            reasons.append("Ressourcentyp fehlt")
        elif missing_fields.get("resourceType") == "empty":
            reasons.append("Ressourcentyp ist leer")
        
        # Invalid name types (Schema 4 requirement)
        if "invalid_name_types" in missing_fields and missing_fields["invalid_name_types"]:
            invalid_types = missing_fields["invalid_name_types"]
            reasons.append(f"Ungültige Name Types: {', '.join(invalid_types)}")
        
        # Unknown contributor types
        if "unknown_contributor_types" in missing_fields and missing_fields["unknown_contributor_types"]:
            unknown_types = missing_fields["unknown_contributor_types"]
            reasons.append(f"Unbekannte Contributor Types: {', '.join(unknown_types)}")
        
        if not reasons:
            return "Unbekannter Grund"
        
        return "; ".join(reasons)
