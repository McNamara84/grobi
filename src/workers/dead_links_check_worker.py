"""Worker for checking download URLs for HTTP 404 responses."""

import logging
from typing import List, Tuple

import requests
from PySide6.QtCore import QObject, Signal

from src.db.sumariopmd_client import SumarioPMDClient, DatabaseError, ConnectionError as DBConnectionError

logger = logging.getLogger(__name__)


class DeadLinksCheckWorker(QObject):
    """Worker that checks download URLs from the database for dead links."""

    progress_update = Signal(int, int, str)  # current, total, message
    finished = Signal(list, int, int, int)  # dead_links, checked_count, skipped_count, error_count
    error_occurred = Signal(str)

    def __init__(
        self,
        db_host: str,
        db_name: str,
        db_user: str,
        db_password: str,
        timeout: int = 10
    ):
        super().__init__()
        self.db_host = db_host
        self.db_name = db_name
        self.db_user = db_user
        self.db_password = db_password
        self.timeout = timeout
        self._is_running = False

    def stop(self):
        """Request cancellation of the check process."""
        self._is_running = False
        logger.info("Dead link check cancelled by user")

    def run(self):
        """Fetch download URLs and check them for 404 responses."""
        self._is_running = True
        checked_count = 0
        skipped_count = 0
        error_count = 0
        dead_links: List[Tuple[str, str]] = []

        try:
            self.progress_update.emit(0, 0, "Verbindung zur Datenbank wird hergestellt...")

            db_client = SumarioPMDClient(
                host=self.db_host,
                database=self.db_name,
                username=self.db_user,
                password=self.db_password
            )

            success, message = db_client.test_connection()
            if not success:
                self.error_occurred.emit(f"Datenbankverbindung fehlgeschlagen: {message}")
                return

            self.progress_update.emit(0, 0, "Download-URLs werden aus der Datenbank geladen...")
            dois_files = db_client.fetch_all_dois_with_downloads()

            if not dois_files:
                self.error_occurred.emit("Keine Download-URLs in der Datenbank gefunden.")
                return

            unique_pairs = self._unique_doi_url_pairs(dois_files)
            total = len(unique_pairs)
            self.progress_update.emit(0, total, f"{total} eindeutige Download-URLs gefunden")

            session = requests.Session()
            session.headers.update({
                "User-Agent": "GROBI Dead Link Checker"
            })

            for idx, (doi, url) in enumerate(unique_pairs, start=1):
                if not self._is_running:
                    self.progress_update.emit(idx, total, "Abgebrochen durch Benutzer")
                    break

                if not url or not url.strip():
                    skipped_count += 1
                    continue

                normalized_url = url.strip()
                if not normalized_url.lower().startswith(("http://", "https://")):
                    skipped_count += 1
                    continue

                self.progress_update.emit(idx, total, f"Prüfe {doi}")

                try:
                    status = self._check_url(session, normalized_url)
                    checked_count += 1
                    if status == 404:
                        dead_links.append((doi, normalized_url))
                except Exception as e:
                    error_count += 1
                    logger.warning(f"Fehler beim Prüfen {normalized_url}: {e}")

            self.progress_update.emit(
                total,
                total,
                f"Fertig: {checked_count} geprüft, {len(dead_links)} mit 404, "
                f"{skipped_count} übersprungen, {error_count} Fehler"
            )

            self.finished.emit(dead_links, checked_count, skipped_count, error_count)

        except DBConnectionError as e:
            error_msg = f"Datenbankverbindung fehlgeschlagen: {str(e)}"
            logger.error(error_msg)
            self.error_occurred.emit(error_msg)

        except DatabaseError as e:
            error_msg = f"Datenbankfehler: {str(e)}"
            logger.error(error_msg)
            self.error_occurred.emit(error_msg)

        except Exception as e:
            error_msg = f"Unerwarteter Fehler: {str(e)}"
            logger.error(error_msg, exc_info=True)
            self.error_occurred.emit(error_msg)

    @staticmethod
    def _unique_doi_url_pairs(dois_files: list) -> List[Tuple[str, str]]:
        """Extract unique (doi, url) pairs from the database result list."""
        seen = set()
        unique_pairs = []
        for doi, _, url, _, _, _ in dois_files:
            key = (doi, url)
            if key in seen:
                continue
            seen.add(key)
            unique_pairs.append((doi, url))
        return unique_pairs

    def _check_url(self, session: requests.Session, url: str) -> int:
        """Return HTTP status code for the URL (404 indicates dead link)."""
        response = None
        try:
            response = session.head(url, allow_redirects=True, timeout=self.timeout)
            if response.status_code in (405, 501):
                response.close()
                response = session.get(url, allow_redirects=True, stream=True, timeout=self.timeout)
            return response.status_code
        finally:
            if response is not None:
                response.close()
