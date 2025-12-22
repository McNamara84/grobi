"""Worker for parallel FAIR assessments using F-UJI API."""

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List

from PySide6.QtCore import QObject, Signal, QThread

from src.api.fuji_client import FujiClient, FujiResult, FujiConnectionError, FujiAuthenticationError


logger = logging.getLogger(__name__)


class FujiAssessmentWorker(QObject):
    """
    Worker for running FAIR assessments in parallel.
    
    Uses ThreadPoolExecutor to assess multiple DOIs concurrently,
    emitting signals as each assessment completes.
    """
    
    # Signals
    doi_assessed = Signal(str, float)  # DOI, score_percent (-1 for error)
    progress = Signal(str)              # Progress message
    error = Signal(str)                 # Error message
    finished = Signal()                 # All assessments complete
    
    def __init__(
        self,
        dois: List[str],
        fuji_client: FujiClient = None,
        max_workers: int = 5
    ):
        """
        Initialize the worker.
        
        Args:
            dois: List of DOIs to assess
            fuji_client: F-UJI client instance (creates default if None)
            max_workers: Maximum parallel workers
        """
        super().__init__()
        
        self.dois = dois
        self.fuji_client = fuji_client or FujiClient()
        self.max_workers = max_workers
        self._cancelled = False
    
    def cancel(self):
        """Cancel the assessment process."""
        self._cancelled = True
        logger.info("FAIR assessment cancelled by user")
    
    def run(self):
        """Run the FAIR assessments."""
        if not self.dois:
            logger.warning("No DOIs to assess")
            self.finished.emit()
            return
        
        logger.info(f"Starting FAIR assessment for {len(self.dois)} DOIs with {self.max_workers} workers")
        self.progress.emit(f"Starte Bewertung von {len(self.dois)} DOIs...")
        
        # Test connection first
        try:
            if not self.fuji_client.test_connection():
                self.error.emit(
                    f"Verbindung zum F-UJI Server fehlgeschlagen.\n"
                    f"Server: {self.fuji_client.endpoint}"
                )
                self.finished.emit()
                return
        except Exception as e:
            self.error.emit(f"Verbindungstest fehlgeschlagen: {str(e)}")
            self.finished.emit()
            return
        
        self.progress.emit("Verbindung zum F-UJI Server hergestellt")
        
        completed = 0
        errors = 0
        
        try:
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                # Submit all DOIs
                future_to_doi = {
                    executor.submit(self._assess_single_doi, doi): doi
                    for doi in self.dois
                }
                
                # Process completed assessments
                for future in as_completed(future_to_doi):
                    if self._cancelled:
                        # Cancel remaining futures
                        for f in future_to_doi:
                            f.cancel()
                        logger.info("Assessment cancelled, stopping workers")
                        break
                    
                    doi = future_to_doi[future]
                    
                    try:
                        result = future.result()
                        self.doi_assessed.emit(result.doi, result.score_percent)
                        
                        if not result.is_success:
                            errors += 1
                            logger.debug(f"Assessment error for {doi}: {result.error}")
                        
                    except FujiConnectionError as e:
                        # Connection error - likely affects all DOIs
                        self.error.emit(str(e))
                        self.doi_assessed.emit(doi, -1)
                        errors += 1
                        
                    except FujiAuthenticationError as e:
                        # Auth error - stop processing
                        self.error.emit(str(e))
                        self.finished.emit()
                        return
                        
                    except Exception as e:
                        logger.error(f"Unexpected error for DOI {doi}: {e}")
                        self.doi_assessed.emit(doi, -1)
                        errors += 1
                    
                    completed += 1
                    
                    # Log progress periodically
                    if completed % 10 == 0:
                        self.progress.emit(f"{completed} von {len(self.dois)} DOIs bewertet...")
        
        except Exception as e:
            logger.error(f"Error in assessment worker: {e}")
            self.error.emit(f"Fehler bei der Bewertung: {str(e)}")
        
        finally:
            success_count = completed - errors
            logger.info(f"FAIR assessment completed: {success_count} success, {errors} errors")
            self.progress.emit(f"Bewertung abgeschlossen: {success_count} erfolgreich, {errors} Fehler")
            self.finished.emit()
    
    def _assess_single_doi(self, doi: str) -> FujiResult:
        """
        Assess a single DOI.
        
        Args:
            doi: DOI to assess
            
        Returns:
            FujiResult with assessment
        """
        if self._cancelled:
            return FujiResult(
                doi=doi,
                score_percent=-1,
                score_earned=0,
                score_total=0,
                metrics_count=0,
                error="Cancelled"
            )
        
        return self.fuji_client.assess_doi(doi)


class FujiAssessmentThread(QThread):
    """
    Thread wrapper for FujiAssessmentWorker.
    
    Usage:
        thread = FujiAssessmentThread(dois)
        thread.worker.doi_assessed.connect(on_doi_assessed)
        thread.worker.finished.connect(on_finished)
        thread.start()
    """
    
    def __init__(
        self,
        dois: List[str],
        fuji_client: FujiClient = None,
        max_workers: int = 5,
        parent=None
    ):
        """
        Initialize the thread.
        
        Args:
            dois: List of DOIs to assess
            fuji_client: F-UJI client instance
            max_workers: Maximum parallel workers
            parent: Parent QObject
        """
        super().__init__(parent)
        
        self.worker = FujiAssessmentWorker(dois, fuji_client, max_workers)
        self.worker.moveToThread(self)
    
    def run(self):
        """Run the worker."""
        self.worker.run()
    
    def cancel(self):
        """Cancel the assessment."""
        self.worker.cancel()
