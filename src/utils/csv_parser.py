"""CSV Parser for DOI Landing Page URL updates and Authors updates."""

import csv
import logging
import re
from collections import OrderedDict
from pathlib import Path
from typing import List, Tuple, Dict


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
    
    @staticmethod
    def parse_authors_update_csv(filepath: str) -> Tuple[Dict[str, List[Dict]], List[str]]:
        """
        Parse CSV file containing DOI and creator/author metadata for updates.
        
        Expected CSV format:
        - Header: DOI,Creator Name,Name Type,Given Name,Family Name,Name Identifier,Name Identifier Scheme,Scheme URI
        - Multiple rows per DOI (one per creator)
        - Preserves order of creators per DOI
        
        Args:
            filepath: Path to the CSV file
            
        Returns:
            Tuple of:
            - Dict[DOI, List[CreatorData]]: Creators grouped by DOI, preserving order
            - List[str]: Warning messages (non-fatal issues)
            
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
        
        logger.info(f"Parsing authors CSV file: {filepath}")
        
        # Use OrderedDict to preserve DOI order
        creators_by_doi = OrderedDict()
        warnings = []
        
        # Expected headers
        expected_headers = [
            'DOI',
            'Creator Name',
            'Name Type',
            'Given Name',
            'Family Name',
            'Name Identifier',
            'Name Identifier Scheme',
            'Scheme URI'
        ]
        
        try:
            with open(file_path, 'r', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                
                # Validate headers
                if not reader.fieldnames:
                    raise CSVParseError("CSV-Datei hat keine Header-Zeile.")
                
                # Check if all expected headers are present
                missing_headers = [h for h in expected_headers if h not in reader.fieldnames]
                if missing_headers:
                    raise CSVParseError(
                        f"CSV-Datei fehlen folgende Header: {', '.join(missing_headers)}. "
                        f"Erwartet: {', '.join(expected_headers)}"
                    )
                
                # Parse rows
                for row_num, row in enumerate(reader, start=2):  # Start at 2 (line 1 is header)
                    # Extract and trim fields
                    doi = row.get('DOI', '').strip()
                    creator_name = row.get('Creator Name', '').strip()
                    name_type = row.get('Name Type', '').strip()
                    given_name = row.get('Given Name', '').strip()
                    family_name = row.get('Family Name', '').strip()
                    name_identifier = row.get('Name Identifier', '').strip()
                    name_identifier_scheme = row.get('Name Identifier Scheme', '').strip()
                    scheme_uri = row.get('Scheme URI', '').strip()
                    
                    # Validate DOI
                    if not doi:
                        warnings.append(f"Zeile {row_num}: DOI fehlt - überspringe Zeile")
                        logger.warning(f"Row {row_num}: Missing DOI")
                        continue
                    
                    if not CSVParser.validate_doi_format(doi):
                        raise CSVParseError(
                            f"Zeile {row_num}: Ungültiges DOI-Format '{doi}'. "
                            "Erwartetes Format: 10.X/..."
                        )
                    
                    # Validate Name Type
                    if name_type not in ['Personal', 'Organizational', '']:
                        raise CSVParseError(
                            f"Zeile {row_num}: Ungültiger Name Type '{name_type}'. "
                            "Erlaubt: 'Personal' oder 'Organizational'"
                        )
                    
                    # Validate creator has at least a name
                    if not creator_name:
                        raise CSVParseError(
                            f"Zeile {row_num}: Creator Name fehlt für DOI '{doi}'. "
                            "Jeder Creator muss mindestens einen Namen haben."
                        )
                    
                    # Validate Personal vs Organizational consistency
                    if name_type == 'Organizational':
                        if given_name or family_name:
                            raise CSVParseError(
                                f"Zeile {row_num}: Organizational Creator '{creator_name}' "
                                "darf keine Given Name oder Family Name haben."
                            )
                    elif name_type == 'Personal':
                        # Personal creators already validated to have creator_name above
                        # No additional validation needed here
                        pass
                    
                    # Validate ORCID format (if provided)
                    if name_identifier:
                        if name_identifier_scheme.upper() == 'ORCID':
                            if not CSVParser.validate_orcid_format(name_identifier):
                                warnings.append(
                                    f"Zeile {row_num}: ORCID-Format möglicherweise ungültig: {name_identifier}"
                                )
                                logger.warning(f"Row {row_num}: Invalid ORCID format: {name_identifier}")
                    
                    # Build creator data structure
                    creator_data = {
                        'name': creator_name,
                        'nameType': name_type if name_type else 'Personal',  # Default to Personal
                        'givenName': given_name,
                        'familyName': family_name,
                        'nameIdentifier': name_identifier,
                        'nameIdentifierScheme': name_identifier_scheme,
                        'schemeUri': scheme_uri
                    }
                    
                    # Add to creators list for this DOI (preserving order)
                    if doi not in creators_by_doi:
                        creators_by_doi[doi] = []
                    
                    creators_by_doi[doi].append(creator_data)
                    logger.debug(f"Parsed creator for {doi}: {creator_name}")
        
        except csv.Error as e:
            raise CSVParseError(f"Fehler beim Lesen der CSV-Datei: {str(e)}")
        
        except UnicodeDecodeError:
            raise CSVParseError(
                "CSV-Datei konnte nicht gelesen werden. "
                "Stelle sicher, dass die Datei UTF-8 kodiert ist."
            )
        
        if not creators_by_doi:
            raise CSVParseError(
                "Keine gültigen Creator-Daten in der CSV-Datei gefunden. "
                "Stelle sicher, dass die Datei mindestens eine Datenzeile enthält."
            )
        
        # Note: No need to validate empty creator lists - a DOI is only added to
        # creators_by_doi when at least one creator is appended (line 331)
        
        total_creators = sum(len(creators) for creators in creators_by_doi.values())
        logger.info(
            f"Successfully parsed {len(creators_by_doi)} DOIs with "
            f"{total_creators} creators from CSV"
        )
        
        return creators_by_doi, warnings
    
    @staticmethod
    def validate_orcid_format(orcid: str) -> bool:
        """
        Validate ORCID format.
        
        A valid ORCID is a 16-character identifier (15 digits plus a checksum digit which can be 0-9 or X) separated by hyphens in groups of 4.
        Format: XXXX-XXXX-XXXX-XXXX or full URL: https://orcid.org/XXXX-XXXX-XXXX-XXXX
        
        Examples:
        - Valid: 0000-0001-5000-0007
        - Valid: https://orcid.org/0000-0001-5000-0007
        - Invalid: 0000-0001-5000 (too short)
        
        Args:
            orcid: ORCID string to validate
            
        Returns:
            True if ORCID format is valid, False otherwise
        """
        if not orcid:
            return False
        
        # Pattern for ORCID ID: 0000-XXXX-XXXX-XXXX (must start with 0000)
        orcid_pattern = re.compile(
            r'^(?:https?://orcid\.org/)?'  # Optional URL prefix
            r'(0000-\d{4}-\d{4}-\d{3}[0-9X])$',  # ORCID format (must start with 0000)
            re.IGNORECASE
        )
        
        return bool(orcid_pattern.match(orcid))
