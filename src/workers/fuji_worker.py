"""Worker for parallel FAIR assessments using F-UJI API."""

import logging
import queue
import threading
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
                # Submit all DOIs and track futures
                future_to_doi = {
                    executor.submit(self._assess_single_doi, doi): doi
                    for doi in self.dois
                }
                # Keep list of futures for cancellation
                pending_futures = list(future_to_doi.keys())
                
                # Process completed assessments
                for future in as_completed(future_to_doi):
                    if self._cancelled:
                        # Cancel remaining pending futures
                        # Note: Already-running futures cannot be cancelled
                        for f in pending_futures:
                            if not f.done():
                                f.cancel()
                        # Shutdown executor without waiting
                        executor.shutdown(wait=False, cancel_futures=True)
                        logger.info("Assessment cancelled, stopping workers")
                        break
                    
                    # Remove from pending list
                    if future in pending_futures:
                        pending_futures.remove(future)
                    
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
    
    Note: The worker is created separately and moved to this thread,
    following Qt's recommended threading pattern.
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
        
        # Create worker without parent so it can be moved to this thread
        self._worker = FujiAssessmentWorker(dois, fuji_client, max_workers)
        # Worker stays in main thread until run() is called
        # This avoids the incorrect moveToThread(self) pattern
    
    @property
    def worker(self):
        """Access the worker for signal connections."""
        return self._worker
    
    def run(self):
        """Run the worker in this thread's context."""
        # Worker.run() is called from within QThread.run()
        # so it executes in this thread's context
        self._worker.run()
    
    def cancel(self):
        """Cancel the assessment."""
        self._worker.cancel()


class StreamingFujiWorker(QObject):
    """
    Worker that assesses DOIs as they stream in from DataCite.
    
    This allows parallel DOI fetching and assessment, starting
    assessments as soon as the first page of DOIs is available.
    """
    
    # Signals
    doi_discovered = Signal(str)          # New DOI discovered (for tile creation)
    doi_assessed = Signal(str, float)     # DOI assessed with score
    progress = Signal(str)                # Progress message
    error = Signal(str)                   # Error message
    fetch_complete = Signal(int)          # All DOIs fetched (total count)
    finished = Signal()                   # All work complete
    
    def __init__(
        self,
        datacite_client,
        fuji_client: FujiClient = None,
        max_workers: int = 5
    ):
        """
        Initialize the streaming worker.
        
        Args:
            datacite_client: DataCite API client for fetching DOIs
            fuji_client: F-UJI client instance (creates default if None)
            max_workers: Maximum parallel assessment workers
        """
        super().__init__()
        
        self.datacite_client = datacite_client
        self.fuji_client = fuji_client or FujiClient()
        self.max_workers = max_workers
        self._cancelled = False
        self._doi_queue = queue.Queue()
        self._fetch_complete = threading.Event()
        self._total_dois = 0
        self._assessed_count = 0
        self._error_count = 0
        self._lock = threading.Lock()
    
    def cancel(self):
        """Cancel the assessment process."""
        self._cancelled = True
        logger.info("Streaming FAIR assessment cancelled by user")
    
    def run(self):
        """Run the streaming assessment."""
        logger.info("Starting streaming FAIR assessment")
        
        # Test F-UJI connection first
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
        
        # Start fetcher and assessor threads
        fetch_thread = threading.Thread(target=self._fetch_dois, daemon=True)
        assess_thread = threading.Thread(target=self._assess_dois, daemon=True)
        
        fetch_thread.start()
        assess_thread.start()
        
        # Wait for both to complete
        fetch_thread.join()
        assess_thread.join()
        
        if not self._cancelled:
            success = self._assessed_count - self._error_count
            self.progress.emit(f"Bewertung abgeschlossen: {success} erfolgreich, {self._error_count} Fehler")
        
        self.finished.emit()
    
    def _fetch_dois(self):
        """Fetch DOIs from DataCite, pushing to queue."""
        try:
            page_num = 1
            total_fetched = 0
            next_url = None  # None for first page
            
            while not self._cancelled:
                # Fetch one page using cursor-based pagination
                try:
                    # _fetch_page returns (dois_list, next_url)
                    dois_with_urls, next_url = self.datacite_client._fetch_page(next_url)
                    
                    if not dois_with_urls:
                        break
                    
                    # Process each DOI (dois_with_urls is list of (doi, url) tuples)
                    for doi, url in dois_with_urls:
                        if self._cancelled:
                            break
                        
                        if doi:
                            # Emit discovery signal and queue for assessment
                            self.doi_discovered.emit(doi)
                            self._doi_queue.put(doi)
                            total_fetched += 1
                    
                    self.progress.emit(f"Seite {page_num}: {total_fetched} DOIs geladen, Bewertung läuft...")
                    
                    # Check if more pages (next_url is None when no more pages)
                    if next_url is None:
                        break
                    
                    page_num += 1
                    
                except Exception as e:
                    logger.error(f"Error fetching page {page_num}: {e}")
                    self.error.emit(f"Fehler beim Abrufen von Seite {page_num}: {str(e)}")
                    break
            
            self._total_dois = total_fetched
            self.fetch_complete.emit(total_fetched)
            logger.info(f"DOI fetch complete: {total_fetched} DOIs")
            
        finally:
            self._fetch_complete.set()
    
    def _assess_dois(self):
        """Assess DOIs from the queue using thread pool."""
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = []
            
            while not self._cancelled:
                # Check if we should stop
                if self._fetch_complete.is_set() and self._doi_queue.empty():
                    break
                
                # Get DOI from queue with timeout
                try:
                    doi = self._doi_queue.get(timeout=0.5)
                except queue.Empty:
                    continue
                
                # Submit assessment
                future = executor.submit(self._assess_single_doi, doi)
                futures.append(future)
                
                # Process completed futures
                done_futures = [f for f in futures if f.done()]
                for future in done_futures:
                    futures.remove(future)
                    self._process_result(future)
            
            # Wait for remaining futures and process their results
            for future in futures:
                if not self._cancelled:
                    # _process_result handles both waiting and processing
                    # to avoid duplicate future.result() calls
                    self._process_result(future)
    
    def _process_result(self, future):
        """Process a completed assessment future.
        
        Waits for the future to complete if needed, then processes the result.
        Safe to call multiple times - result() caches the value.
        """
        try:
            result = future.result()  # Waits if not complete, returns cached value if already done
            self.doi_assessed.emit(result.doi, result.score_percent)
            
            with self._lock:
                self._assessed_count += 1
                if not result.is_success:
                    self._error_count += 1
                
                if self._assessed_count % 10 == 0:
                    if self._total_dois > 0:
                        self.progress.emit(f"{self._assessed_count} von {self._total_dois} DOIs bewertet...")
                    else:
                        self.progress.emit(f"{self._assessed_count} DOIs bewertet...")
                        
        except Exception as e:
            logger.error(f"Error processing result: {e}")
            with self._lock:
                self._assessed_count += 1
                self._error_count += 1
    
    def _assess_single_doi(self, doi: str) -> FujiResult:
        """Assess a single DOI."""
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


class StreamingFujiThread(QThread):
    """
    Thread wrapper for StreamingFujiWorker.
    
    Usage:
        thread = StreamingFujiThread(datacite_client)
        thread.worker.doi_discovered.connect(on_new_doi)
        thread.worker.doi_assessed.connect(on_assessed)
        thread.worker.finished.connect(on_finished)
        thread.start()
    
    Note: The worker is created separately and its run() method is called
    from within QThread.run(), following Qt's recommended threading pattern.
    """
    
    def __init__(
        self,
        datacite_client,
        fuji_client: FujiClient = None,
        max_workers: int = 5,
        parent=None
    ):
        super().__init__(parent)
        
        # Create worker without parent so it can be properly managed
        self._worker = StreamingFujiWorker(datacite_client, fuji_client, max_workers)
        # Worker's run() is called from within QThread.run()
        # so it executes in this thread's context
    
    @property
    def worker(self):
        """Access the worker for signal connections."""
        return self._worker
    
    def run(self):
        """Run the worker in this thread's context."""
        self._worker.run()
    
    def cancel(self):
        """Cancel the assessment."""
        self._worker.cancel()
