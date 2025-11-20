"""CSV Parser for DOI Landing Page URL updates."""

import csv
import logging
import re
from pathlib import Path
from typing import List, Tuple


logger = logging.getLogger(__name__)


class CSVParseError(Exception):
    """Raised when CSV parsing fails."""
    pass


class CSVParser:
    """Parser and validator for CSV files containing DOI and Landing Page URL data."""
    
    # DOI regex pattern (basic validation)
    # Matches format: 10.X/... where X is the registrant code (1+ digits)
    # Pattern: ^10\.\d+/\S+$ allows any registrant code with 1+ digits
    # Note: Supports all valid DOI registrant codes (e.g., 10.1/..., 10.1234/..., 10.12345/...)
    DOI_PATTERN = re.compile(r'^10\.\d+/\S+$')
    
    # URL regex pattern (basic validation)
    # Matches http:// or https:// URLs
    # Note: Relaxed pattern since DataCite API will perform final validation
    URL_PATTERN = re.compile(
        r'^https?://'  # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,}\.?|'  # domain with 2+ char TLD
        r'localhost|'  # localhost
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # or IP
        r'(?::\d+)?'  # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE
    )
    
    @staticmethod
    def parse_update_csv(filepath: str) -> List[Tuple[str, str]]:
        """
        Parse CSV file containing DOI and Landing Page URL data.
        
        Expected CSV format:
        - Header row: DOI,Landing_Page_URL
        - Data rows: 10.5880/GFZ.xxx,https://example.org/xxx
        
        Args:
            filepath: Path to the CSV file
            
        Returns:
            List of tuples (doi, landing_page_url)
            
        Raises:
            CSVParseError: If file cannot be read or has invalid format
            FileNotFoundError: If file does not exist
        """
        file_path = Path(filepath)
        
        # Check if file exists
        if not file_path.exists():
            raise FileNotFoundError(f"CSV-Datei nicht gefunden: {filepath}")
        
        # Check if file is readable
        if not file_path.is_file():
            raise CSVParseError(f"Pfad ist keine Datei: {filepath}")
        
        logger.info(f"Parsing CSV file: {filepath}")
        
        doi_url_pairs = []
        
        try:
            with open(file_path, 'r', encoding='utf-8') as csvfile:
                # Use csv.DictReader to parse with headers
                reader = csv.DictReader(csvfile)
                
                # Validate headers
                if not reader.fieldnames or 'DOI' not in reader.fieldnames or 'Landing_Page_URL' not in reader.fieldnames:
                    raise CSVParseError(
                        "CSV-Datei muss Header 'DOI' und 'Landing_Page_URL' enthalten. "
                        f"Gefunden: {reader.fieldnames}"
                    )
                
                # Parse rows
                for row_num, row in enumerate(reader, start=2):  # Start at 2 (line 1 is header)
                    doi = row.get('DOI', '').strip()
                    url = row.get('Landing_Page_URL', '').strip()
                    
                    # Validate that both fields are present
                    if not doi:
                        logger.warning(f"Zeile {row_num}: DOI fehlt - überspringe Zeile")
                        continue
                    
                    if not url:
                        raise CSVParseError(
                            f"Zeile {row_num}: Landing Page URL fehlt für DOI '{doi}'. "
                            "Jede DOI muss eine Landing Page URL haben."
                        )
                    
                    # Validate DOI format
                    if not CSVParser.validate_doi_format(doi):
                        raise CSVParseError(
                            f"Zeile {row_num}: Ungültiges DOI-Format '{doi}'. "
                            "Erwartetes Format: 10.X/... (wobei X ein oder mehrere Ziffern sind)"
                        )
                    
                    # Validate URL format
                    if not CSVParser.validate_url_format(url):
                        raise CSVParseError(
                            f"Zeile {row_num}: Ungültige URL '{url}'. "
                            "URL muss mit http:// oder https:// beginnen."
                        )
                    
                    doi_url_pairs.append((doi, url))
                    logger.debug(f"Parsed: {doi} -> {url}")
        
        except csv.Error as e:
            raise CSVParseError(f"Fehler beim Lesen der CSV-Datei: {str(e)}")
        
        except UnicodeDecodeError:
            raise CSVParseError(
                "CSV-Datei konnte nicht gelesen werden. "
                "Stelle sicher, dass die Datei UTF-8 kodiert ist."
            )
        
        if not doi_url_pairs:
            raise CSVParseError(
                "Keine gültigen DOI/URL-Paare in der CSV-Datei gefunden. "
                "Stelle sicher, dass die Datei mindestens eine Datenzeile enthält."
            )
        
        logger.info(f"Successfully parsed {len(doi_url_pairs)} DOI/URL pairs from CSV")
        return doi_url_pairs
    
    @staticmethod
    def validate_doi_format(doi: str) -> bool:
        """
        Validate DOI format.
        
        A valid DOI starts with "10." followed by a registrant code (1+ digits),
        a forward slash, and a suffix.
        
        Examples:
        - Valid: 10.5880/GFZ.1.1.2021.001
        - Valid: 10.1234/example
        - Valid: 10.100/test (3-digit registrant code)
        - Valid: 10.1/test (1-digit registrant code)
        - Invalid: 11.5880/test (wrong prefix)
        - Invalid: 10.123/ (empty suffix)
        
        Args:
            doi: DOI string to validate
            
        Returns:
            True if DOI format is valid, False otherwise
        """
        if not doi:
            return False
        
        return bool(CSVParser.DOI_PATTERN.match(doi))
    
    @staticmethod
    def validate_url_format(url: str) -> bool:
        """
        Validate URL format.
        
        A valid URL must:
        - Start with http:// or https://
        - Contain a valid domain or IP address
        - Optionally contain port and path
        
        Examples:
        - Valid: https://example.org
        - Valid: https://example.org/path/to/resource
        - Valid: http://localhost:8080
        - Invalid: ftp://example.org
        - Invalid: example.org (missing protocol)
        
        Args:
            url: URL string to validate
            
        Returns:
            True if URL format is valid, False otherwise
        """
        if not url:
            return False
        
        return bool(CSVParser.URL_PATTERN.match(url))
