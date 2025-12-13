"""Worker for upgrading DOI schema versions to Schema 4.6."""

import logging
from typing import List, Tuple, Dict, Any, Optional
from dataclasses import dataclass
from enum import Enum
from PySide6.QtCore import QObject, Signal

from src.api.datacite_client import (
    DataCiteClient, 
    DataCiteAPIError, 
    AuthenticationError, 
    NetworkError
)


logger = logging.getLogger(__name__)


class UpgradeStatus(Enum):
    """Status of a DOI upgrade."""
    UPGRADEABLE = "upgradeable"
    NOT_UPGRADEABLE = "not_upgradeable"
    ALREADY_CURRENT = "already_current"
    UPGRADED = "upgraded"
    FAILED = "failed"


@dataclass
class DOIUpgradeInfo:
    """Information about a DOI's upgrade status."""
    doi: str
    current_schema: str
    status: UpgradeStatus
    reason: str
    has_funder_contributors: bool = False
    funder_count: int = 0
    missing_fields: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.missing_fields is None:
            self.missing_fields = {}


class SchemaUpgradeAnalyzeWorker(QObject):
    """Worker for analyzing DOIs and determining which can be upgraded."""
    
    # Signals
    progress_update = Signal(str)  # Progress message
    analysis_complete = Signal(list, list, list)  # upgradeable, not_upgradeable, already_current
    error_occurred = Signal(str)  # Error message
    
    # Valid Schema 4.x contributor types (Funder is NOT valid)
    VALID_CONTRIBUTOR_TYPES = [
        "ContactPerson", "DataCollector", "DataCurator", "DataManager",
        "Distributor", "Editor", "HostingInstitution", "Producer",
        "ProjectLeader", "ProjectManager", "ProjectMember", "RegistrationAgency",
        "RegistrationAuthority", "RelatedPerson", "Researcher",
        "ResearchGroup", "RightsHolder", "Sponsor", "Supervisor",
        "WorkPackageLeader", "Other"
    ]
    
    def __init__(self, username: str, password: str, use_test_api: bool = False):
        """
        Initialize the schema upgrade analyze worker.
        
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
        logger.info("Schema upgrade analyze worker stop requested")
    
    def run(self):
        """
        Analyze all DOIs and categorize them by upgrade status.
        
        Emits:
            progress_update: Progress messages during processing
            analysis_complete: Three lists - upgradeable, not_upgradeable, already_current
            error_occurred: Error message if something goes wrong
        """
        try:
            logger.info("SchemaUpgradeAnalyzeWorker.run() gestartet")
            self.progress_update.emit("Verbindung zur DataCite API wird hergestellt...")
            
            client = DataCiteClient(
                self.username,
                self.password,
                self.use_test_api
            )
            
            self.progress_update.emit("DOIs werden abgerufen...")
            
            # Fetch all DOIs with full metadata for analysis
            dois_with_schema = client.fetch_dois_with_schema_version()
            
            if self._should_stop:
                return
            
            total_dois = len(dois_with_schema)
            self.progress_update.emit(f"{total_dois} DOIs gefunden. Analysiere Upgrade-Fähigkeit...")
            
            upgradeable: List[DOIUpgradeInfo] = []
            not_upgradeable: List[DOIUpgradeInfo] = []
            already_current: List[DOIUpgradeInfo] = []
            
            processed = 0
            
            for doi, schema_version in dois_with_schema:
                if self._should_stop:
                    return
                
                processed += 1
                
                # Update progress every 50 DOIs
                if processed % 50 == 0 or processed == total_dois:
                    self.progress_update.emit(
                        f"Analysiere DOI {processed}/{total_dois}..."
                    )
                
                # Analyze this DOI
                info = self._analyze_doi(client, doi, schema_version)
                
                if info.status == UpgradeStatus.UPGRADEABLE:
                    upgradeable.append(info)
                elif info.status == UpgradeStatus.NOT_UPGRADEABLE:
                    not_upgradeable.append(info)
                else:  # ALREADY_CURRENT
                    already_current.append(info)
            
            if self._should_stop:
                return
            
            # Report summary
            self.progress_update.emit(
                f"[OK] Analyse abgeschlossen: {len(upgradeable)} upgrade-fähig, "
                f"{len(not_upgradeable)} nicht upgrade-fähig, "
                f"{len(already_current)} bereits aktuell"
            )
            
            self.analysis_complete.emit(upgradeable, not_upgradeable, already_current)
            
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
            logger.error(f"Unexpected error in schema upgrade analyze worker: {e}", exc_info=True)
            self.error_occurred.emit(f"Unerwarteter Fehler: {str(e)}")
    
    def _analyze_doi(self, client: DataCiteClient, doi: str, schema_version: str) -> DOIUpgradeInfo:
        """
        Analyze a single DOI to determine if it can be upgraded.
        
        Args:
            client: DataCite client
            doi: DOI to analyze
            schema_version: Current schema version
            
        Returns:
            DOIUpgradeInfo with upgrade status
        """
        # Normalize schema version
        display_schema = schema_version if schema_version else "unbekannt"
        
        # Check if already on Schema 4.6 or later
        if schema_version and "kernel-4" in schema_version:
            # Check if it's already 4.6
            if "kernel-4.6" in schema_version:
                return DOIUpgradeInfo(
                    doi=doi,
                    current_schema=display_schema,
                    status=UpgradeStatus.ALREADY_CURRENT,
                    reason="Bereits auf Schema 4.6"
                )
            # Schema 4.x but not 4.6 - can be upgraded to 4.6 if all fields present
            # (This includes DOIs with just "kernel-4" without minor version)
        
        # Fetch detailed metadata for this DOI
        try:
            metadata = client.get_doi_metadata(doi)
        except Exception as e:
            logger.warning(f"Could not fetch metadata for {doi}: {e}")
            return DOIUpgradeInfo(
                doi=doi,
                current_schema=display_schema,
                status=UpgradeStatus.NOT_UPGRADEABLE,
                reason=f"Metadaten konnten nicht abgerufen werden: {str(e)}"
            )
        
        if not metadata:
            return DOIUpgradeInfo(
                doi=doi,
                current_schema=display_schema,
                status=UpgradeStatus.NOT_UPGRADEABLE,
                reason="Keine Metadaten vorhanden"
            )
        
        attributes = metadata.get("data", {}).get("attributes", {})
        
        # Check required fields for Schema 4
        missing_fields = {}
        missing_reasons = []
        
        # Publisher (required)
        publisher = attributes.get("publisher")
        if not publisher:
            missing_fields["publisher"] = "missing"
            missing_reasons.append("Publisher fehlt")
        elif isinstance(publisher, dict):
            if not publisher.get("name"):
                missing_fields["publisher"] = "empty"
                missing_reasons.append("Publisher-Name fehlt")
        elif isinstance(publisher, str) and not publisher.strip():
            missing_fields["publisher"] = "empty"
            missing_reasons.append("Publisher ist leer")
        
        # Publication Year (required)
        pub_year = attributes.get("publicationYear")
        if not pub_year:
            missing_fields["publicationYear"] = "missing"
            missing_reasons.append("Erscheinungsjahr fehlt")
        
        # Titles (required, at least one non-empty)
        titles = attributes.get("titles", [])
        if not titles:
            missing_fields["titles"] = "missing"
            missing_reasons.append("Titel fehlt")
        else:
            non_empty = [t for t in titles if t.get("title", "").strip()]
            if not non_empty:
                missing_fields["titles"] = "empty"
                missing_reasons.append("Alle Titel sind leer")
        
        # Creators (required, at least one)
        creators = attributes.get("creators", [])
        if not creators:
            missing_fields["creators"] = "missing"
            missing_reasons.append("Urheber (Creators) fehlen")
        elif len(creators) == 0:
            missing_fields["creators"] = "empty"
            missing_reasons.append("Keine Urheber vorhanden")
        
        # Resource Type General (required for Schema 4)
        types = attributes.get("types", {})
        resource_type_general = types.get("resourceTypeGeneral") if types else None
        if not resource_type_general:
            missing_fields["resourceTypeGeneral"] = "missing"
            missing_reasons.append("resourceTypeGeneral fehlt")
        
        # Check for Funder contributors (need migration to fundingReferences)
        contributors = attributes.get("contributors", [])
        funder_contributors = [c for c in contributors if c.get("contributorType") == "Funder"]
        has_funder = len(funder_contributors) > 0
        
        # If there are missing required fields, DOI is not upgradeable
        if missing_reasons:
            return DOIUpgradeInfo(
                doi=doi,
                current_schema=display_schema,
                status=UpgradeStatus.NOT_UPGRADEABLE,
                reason="; ".join(missing_reasons),
                has_funder_contributors=has_funder,
                funder_count=len(funder_contributors),
                missing_fields=missing_fields
            )
        
        # Check if already on current schema (4.6 check was done above)
        # DOIs with kernel-4 (any version) that have all required fields need schema version update
        if schema_version and "kernel-4.6" in schema_version:
            return DOIUpgradeInfo(
                doi=doi,
                current_schema=display_schema,
                status=UpgradeStatus.ALREADY_CURRENT,
                reason="Bereits auf Schema 4.6"
            )
        
        # DOI can be upgraded!
        upgrade_note = "Alle Pflichtfelder vorhanden"
        if has_funder:
            upgrade_note += f"; {len(funder_contributors)} Funder → fundingReferences"
        
        return DOIUpgradeInfo(
            doi=doi,
            current_schema=display_schema,
            status=UpgradeStatus.UPGRADEABLE,
            reason=upgrade_note,
            has_funder_contributors=has_funder,
            funder_count=len(funder_contributors)
        )


class SchemaUpgradeExecuteWorker(QObject):
    """Worker for executing the actual schema upgrades."""
    
    # Signals
    progress_update = Signal(str)  # Progress message
    doi_upgraded = Signal(str, bool, str)  # doi, success, message
    upgrade_complete = Signal(list, list)  # successful, failed
    error_occurred = Signal(str)  # Error message
    
    def __init__(self, username: str, password: str, use_test_api: bool, 
                 dois_to_upgrade: List[DOIUpgradeInfo]):
        """
        Initialize the schema upgrade execute worker.
        
        Args:
            username: DataCite username
            password: DataCite password
            use_test_api: Whether to use test API
            dois_to_upgrade: List of DOIUpgradeInfo objects to upgrade
        """
        super().__init__()
        self.username = username
        self.password = password
        self.use_test_api = use_test_api
        self.dois_to_upgrade = dois_to_upgrade
        self._should_stop = False
    
    def stop(self):
        """Signal the worker to stop processing."""
        self._should_stop = True
        logger.info("Schema upgrade execute worker stop requested")
    
    def run(self):
        """
        Execute schema upgrades for all DOIs in the list.
        
        Emits:
            progress_update: Progress messages during processing
            doi_upgraded: Emitted after each DOI is processed (doi, success, message)
            upgrade_complete: Two lists - successful and failed upgrades
            error_occurred: Error message if something goes wrong
        """
        try:
            logger.info(f"SchemaUpgradeExecuteWorker.run() gestartet - {len(self.dois_to_upgrade)} DOIs")
            self.progress_update.emit(f"Starte Upgrade von {len(self.dois_to_upgrade)} DOIs...")
            
            client = DataCiteClient(
                self.username,
                self.password,
                self.use_test_api
            )
            
            successful: List[Tuple[str, str]] = []  # (doi, message)
            failed: List[Tuple[str, str]] = []  # (doi, error_message)
            
            total = len(self.dois_to_upgrade)
            
            for i, doi_info in enumerate(self.dois_to_upgrade, 1):
                if self._should_stop:
                    self.progress_update.emit("[ABBRUCH] Upgrade durch Benutzer abgebrochen")
                    break
                
                doi = doi_info.doi
                self.progress_update.emit(f"Upgrade {i}/{total}: {doi}")
                
                try:
                    # Perform the upgrade
                    success, message = client.upgrade_doi_to_schema_4(
                        doi, 
                        migrate_funders=doi_info.has_funder_contributors
                    )
                    
                    if success:
                        successful.append((doi, message))
                        self.doi_upgraded.emit(doi, True, message)
                        self.progress_update.emit(f"  [OK] {doi}: {message}")
                    else:
                        failed.append((doi, message))
                        self.doi_upgraded.emit(doi, False, message)
                        self.progress_update.emit(f"  [FEHLER] {doi}: {message}")
                        
                except Exception as e:
                    error_msg = str(e)
                    failed.append((doi, error_msg))
                    self.doi_upgraded.emit(doi, False, error_msg)
                    self.progress_update.emit(f"  [FEHLER] {doi}: {error_msg}")
            
            # Summary
            self.progress_update.emit(
                f"[OK] Upgrade abgeschlossen: {len(successful)} erfolgreich, {len(failed)} fehlgeschlagen"
            )
            
            self.upgrade_complete.emit(successful, failed)
            
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
            logger.error(f"Unexpected error in schema upgrade execute worker: {e}", exc_info=True)
            self.error_occurred.emit(f"Unerwarteter Fehler: {str(e)}")
