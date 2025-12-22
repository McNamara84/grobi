"""F-UJI API Client for FAIR assessments."""

import logging
from dataclasses import dataclass
from typing import Optional, Dict, Any

import requests
from requests.auth import HTTPBasicAuth


logger = logging.getLogger(__name__)


class FujiAPIError(Exception):
    """Base exception for F-UJI API errors."""
    pass


class FujiAuthenticationError(FujiAPIError):
    """Raised when authentication fails."""
    pass


class FujiConnectionError(FujiAPIError):
    """Raised when connection to F-UJI server fails."""
    pass


@dataclass
class FujiResult:
    """Result of a FAIR assessment."""
    doi: str
    score_percent: float
    score_earned: float
    score_total: float
    metrics_count: int
    raw_response: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    
    @property
    def is_success(self) -> bool:
        """Return True if assessment was successful."""
        return self.error is None and self.score_percent >= 0


class FujiClient:
    """
    Client for the F-UJI FAIR Assessment API.
    
    F-UJI (FAIRsFAIR Research Data Object Assessment Service) evaluates
    research data objects against FAIR principles.
    """
    
    # Default GFZ F-UJI server
    DEFAULT_ENDPOINT = "https://fuji.rz-vm182.gfz.de/fuji/api/v1"
    DEFAULT_USERNAME = "marvel"
    DEFAULT_PASSWORD = "wonderwoman"
    
    TIMEOUT = 120  # Assessment can take a while
    
    def __init__(
        self,
        endpoint: str = None,
        username: str = None,
        password: str = None
    ):
        """
        Initialize the F-UJI client.
        
        Args:
            endpoint: F-UJI API endpoint URL (default: GFZ server)
            username: API username (default: marvel)
            password: API password (default: wonderwoman)
        """
        self.endpoint = (endpoint or self.DEFAULT_ENDPOINT).rstrip('/')
        self.username = username or self.DEFAULT_USERNAME
        self.password = password or self.DEFAULT_PASSWORD
        self.auth = HTTPBasicAuth(self.username, self.password)
        
        logger.info(f"F-UJI client initialized with endpoint: {self.endpoint}")
    
    def test_connection(self) -> bool:
        """
        Test the connection to the F-UJI server.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            response = requests.get(
                f"{self.endpoint}/metrics/0.5",
                auth=self.auth,
                timeout=10
            )
            return response.status_code == 200
        except requests.RequestException as e:
            logger.error(f"Connection test failed: {e}")
            return False
    
    def assess_doi(
        self,
        doi: str,
        use_datacite: bool = True,
        metric_version: str = "metrics_v0.5"
    ) -> FujiResult:
        """
        Assess a DOI against FAIR principles.
        
        Args:
            doi: The DOI to assess (with or without https://doi.org/ prefix)
            use_datacite: Whether to use DataCite for metadata
            metric_version: Metric version to use
            
        Returns:
            FujiResult with assessment scores
            
        Raises:
            FujiAuthenticationError: If authentication fails
            FujiConnectionError: If connection fails
            FujiAPIError: For other API errors
        """
        # Ensure DOI has proper URL format
        if not doi.startswith('http'):
            doi_url = f"https://doi.org/{doi}"
        else:
            doi_url = doi
            # Extract DOI for result
            doi = doi.replace("https://doi.org/", "").replace("http://doi.org/", "")
        
        payload = {
            "object_identifier": doi_url,
            "test_debug": False,
            "use_datacite": use_datacite,
            "metric_version": metric_version
        }
        
        logger.debug(f"Assessing DOI: {doi}")
        
        try:
            response = requests.post(
                f"{self.endpoint}/evaluate",
                json=payload,
                auth=self.auth,
                timeout=self.TIMEOUT,
                headers={"Content-Type": "application/json"}
            )
            
            if response.status_code == 401:
                raise FujiAuthenticationError("Authentifizierung fehlgeschlagen. Bitte Zugangsdaten prüfen.")
            
            if response.status_code == 404:
                return FujiResult(
                    doi=doi,
                    score_percent=-1,
                    score_earned=0,
                    score_total=0,
                    metrics_count=0,
                    error=f"DOI nicht gefunden: {doi}"
                )
            
            if response.status_code != 200:
                error_msg = f"API Fehler: {response.status_code}"
                try:
                    error_detail = response.json()
                    if 'detail' in error_detail:
                        error_msg = f"{error_msg} - {error_detail['detail']}"
                except Exception:
                    pass
                
                return FujiResult(
                    doi=doi,
                    score_percent=-1,
                    score_earned=0,
                    score_total=0,
                    metrics_count=0,
                    error=error_msg
                )
            
            data = response.json()
            return self._parse_response(doi, data)
            
        except requests.exceptions.Timeout:
            logger.error(f"Timeout assessing DOI: {doi}")
            return FujiResult(
                doi=doi,
                score_percent=-1,
                score_earned=0,
                score_total=0,
                metrics_count=0,
                error="Timeout bei der Bewertung"
            )
            
        except requests.exceptions.ConnectionError as e:
            logger.error(f"Connection error: {e}")
            raise FujiConnectionError(
                f"Verbindung zum F-UJI Server fehlgeschlagen. "
                f"Bitte prüfe, ob der Server erreichbar ist: {self.endpoint}"
            )
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error assessing DOI {doi}: {e}")
            return FujiResult(
                doi=doi,
                score_percent=-1,
                score_earned=0,
                score_total=0,
                metrics_count=0,
                error=f"Netzwerkfehler: {str(e)}"
            )
    
    def _parse_response(self, doi: str, data: Dict[str, Any]) -> FujiResult:
        """
        Parse the F-UJI API response.
        
        Args:
            doi: The assessed DOI
            data: Raw API response
            
        Returns:
            FujiResult with parsed scores
        """
        try:
            summary = data.get('summary', {})
            
            # F-UJI returns dictionaries with FAIR principle breakdown
            # The overall score is in the "FAIR" key
            score_earned_data = summary.get('score_earned', {})
            score_total_data = summary.get('score_total', {})
            score_percent_data = summary.get('score_percent', {})
            
            # Extract FAIR totals - handle both dict and int formats
            if isinstance(score_earned_data, dict):
                score_earned = score_earned_data.get('FAIR', 0)
            else:
                score_earned = score_earned_data or 0
                
            if isinstance(score_total_data, dict):
                score_total = score_total_data.get('FAIR', 0)
            else:
                score_total = score_total_data or 0
            
            # Get percentage - prefer direct value if available
            if isinstance(score_percent_data, dict):
                score_percent = score_percent_data.get('FAIR', 0)
            else:
                score_percent = score_percent_data or 0
            
            # Fallback calculation if percent not available
            if score_percent == 0 and score_total > 0:
                score_percent = (score_earned / score_total) * 100
            
            metrics_count = data.get('total_metrics', len(data.get('results', [])))
            
            logger.debug(f"DOI {doi}: {score_percent:.1f}% ({score_earned}/{score_total})")
            
            return FujiResult(
                doi=doi,
                score_percent=score_percent,
                score_earned=score_earned,
                score_total=score_total,
                metrics_count=metrics_count,
                raw_response=data
            )
            
        except Exception as e:
            logger.error(f"Error parsing response for {doi}: {e}")
            return FujiResult(
                doi=doi,
                score_percent=-1,
                score_earned=0,
                score_total=0,
                metrics_count=0,
                error=f"Fehler beim Parsen der Antwort: {str(e)}",
                raw_response=data
            )
