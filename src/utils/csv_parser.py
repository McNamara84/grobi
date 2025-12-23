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

    @staticmethod
    def parse_publisher_update_csv(filepath: str) -> Tuple[Dict[str, Dict], List[str]]:
        """
        Parse CSV file containing DOI and publisher metadata for updates.
        
        Expected CSV format:
        - Header: DOI,Publisher Name,Publisher Identifier,Publisher Identifier Scheme,Scheme URI,Language
        - One row per DOI (each DOI has exactly one publisher)
        
        Args:
            filepath: Path to the CSV file
            
        Returns:
            Tuple of:
            - Dict[DOI, PublisherData]: Publisher data by DOI
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
        
        logger.info(f"Parsing publisher CSV file: {filepath}")
        
        # Use OrderedDict to preserve DOI order
        publisher_by_doi = OrderedDict()
        warnings = []
        
        # Expected headers
        expected_headers = [
            'DOI',
            'Publisher Name',
            'Publisher Identifier',
            'Publisher Identifier Scheme',
            'Scheme URI',
            'Language'
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
                    publisher_name = row.get('Publisher Name', '').strip()
                    publisher_identifier = row.get('Publisher Identifier', '').strip()
                    publisher_identifier_scheme = row.get('Publisher Identifier Scheme', '').strip()
                    scheme_uri = row.get('Scheme URI', '').strip()
                    lang = row.get('Language', '').strip()
                    
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
                    
                    # Validate publisher name (required)
                    if not publisher_name:
                        raise CSVParseError(
                            f"Zeile {row_num}: Publisher Name fehlt für DOI '{doi}'. "
                            "Jede DOI muss einen Publisher haben."
                        )
                    
                    # Check for duplicate DOIs
                    if doi in publisher_by_doi:
                        raise CSVParseError(
                            f"Zeile {row_num}: DOI '{doi}' ist mehrfach in der CSV-Datei. "
                            "Jede DOI darf nur einmal vorkommen (genau ein Publisher pro DOI)."
                        )
                    
                    # Validate publisherIdentifierScheme if publisherIdentifier is provided
                    if publisher_identifier and not publisher_identifier_scheme:
                        warnings.append(
                            f"Zeile {row_num}: Publisher Identifier '{publisher_identifier}' "
                            "ohne Publisher Identifier Scheme angegeben"
                        )
                        logger.warning(f"Row {row_num}: publisherIdentifier without scheme for DOI {doi}")
                    
                    # Validate language code (BCP 47 can be up to 35 chars for complex tags)
                    # Common codes: 'en', 'de', 'zh-Hans', 'pt-BR', 'zh-Hant-HK'
                    if lang and (len(lang) < 2 or len(lang) > 35):
                        warnings.append(
                            f"Zeile {row_num}: Ungewöhnlicher Language-Code '{lang}' "
                            "(erwartet: BCP 47 Format, z.B. 'en', 'de', 'zh-Hans')"
                        )
                        logger.warning(f"Row {row_num}: Unusual language code: {lang}")
                    
                    # Build publisher data structure
                    publisher_data = {
                        'name': publisher_name,
                        'publisherIdentifier': publisher_identifier,
                        'publisherIdentifierScheme': publisher_identifier_scheme,
                        'schemeUri': scheme_uri,
                        'lang': lang
                    }
                    
                    publisher_by_doi[doi] = publisher_data
                    logger.debug(f"Parsed publisher for {doi}: {publisher_name}")
        
        except csv.Error as e:
            raise CSVParseError(f"Fehler beim Lesen der CSV-Datei: {str(e)}")
        
        except UnicodeDecodeError:
            raise CSVParseError(
                "CSV-Datei konnte nicht gelesen werden. "
                "Stelle sicher, dass die Datei UTF-8 kodiert ist."
            )
        
        if not publisher_by_doi:
            raise CSVParseError(
                "Keine gültigen Publisher-Daten in der CSV-Datei gefunden. "
                "Stelle sicher, dass die Datei mindestens eine Datenzeile enthält."
            )
        
        logger.info(
            f"Successfully parsed {len(publisher_by_doi)} DOIs with "
            f"publisher data from CSV"
        )
        
        return publisher_by_doi, warnings

    # Valid ContributorTypes as per DataCite schema
    VALID_CONTRIBUTOR_TYPES = {
        "ContactPerson", "DataCollector", "DataCurator", "DataManager",
        "Distributor", "Editor", "HostingInstitution", "Producer",
        "ProjectLeader", "ProjectManager", "ProjectMember", "RegistrationAgency",
        "RegistrationAuthority", "RelatedPerson", "Researcher",
        "ResearchGroup", "RightsHolder", "Sponsor", "Supervisor",
        "WorkPackageLeader", "Other",
        # GFZ-internal type (used in SUMARIOPMD database)
        "pointOfContact"
    }

    @staticmethod
    def parse_contributors_update_csv(filepath: str) -> Tuple[Dict[str, List[Dict]], List[str]]:
        """
        Parse CSV file containing DOI and contributor metadata for updates.
        
        Expected CSV format:
        - Header: DOI,Contributor Name,Name Type,Given Name,Family Name,
                  Name Identifier,Name Identifier Scheme,Scheme URI,
                  Contributor Types,Affiliation,Affiliation Identifier,
                  Email,Website,Position
        - Multiple rows per DOI (one per contributor)
        - Preserves order of contributors per DOI
        - ContributorTypes can be comma-separated for multiple types
        - Email/Website/Position only for ContactPerson (DB only, ignored in DataCite API)
        
        Args:
            filepath: Path to the CSV file
            
        Returns:
            Tuple of:
            - Dict[DOI, List[ContributorData]]: Contributors grouped by DOI, preserving order
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
        
        logger.info(f"Parsing contributors CSV file: {filepath}")
        
        # Use OrderedDict to preserve DOI order
        contributors_by_doi = OrderedDict()
        warnings = []
        
        # Expected headers (14 columns)
        expected_headers = [
            'DOI',
            'Contributor Name',
            'Name Type',
            'Given Name',
            'Family Name',
            'Name Identifier',
            'Name Identifier Scheme',
            'Scheme URI',
            'Contributor Types',
            'Affiliation',
            'Affiliation Identifier',
            'Email',
            'Website',
            'Position'
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
                    contributor_name = row.get('Contributor Name', '').strip()
                    name_type = row.get('Name Type', '').strip()
                    given_name = row.get('Given Name', '').strip()
                    family_name = row.get('Family Name', '').strip()
                    name_identifier = row.get('Name Identifier', '').strip()
                    name_identifier_scheme = row.get('Name Identifier Scheme', '').strip()
                    scheme_uri = row.get('Scheme URI', '').strip()
                    contributor_types_str = row.get('Contributor Types', '').strip()
                    affiliation = row.get('Affiliation', '').strip()
                    affiliation_identifier = row.get('Affiliation Identifier', '').strip()
                    email = row.get('Email', '').strip()
                    website = row.get('Website', '').strip()
                    position = row.get('Position', '').strip()
                    
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
                    
                    # Validate contributor has at least a name
                    if not contributor_name:
                        raise CSVParseError(
                            f"Zeile {row_num}: Contributor Name fehlt für DOI '{doi}'. "
                            "Jeder Contributor muss mindestens einen Namen haben."
                        )
                    
                    # Validate ContributorTypes (required, can be comma-separated)
                    if not contributor_types_str:
                        raise CSVParseError(
                            f"Zeile {row_num}: Contributor Types fehlt für DOI '{doi}'. "
                            "Mindestens ein Contributor Type ist erforderlich."
                        )
                    
                    # Parse and validate each contributor type
                    contributor_types = [ct.strip() for ct in contributor_types_str.split(',')]
                    invalid_types = [ct for ct in contributor_types if ct not in CSVParser.VALID_CONTRIBUTOR_TYPES]
                    if invalid_types:
                        warnings.append(
                            f"Zeile {row_num}: Unbekannte Contributor Types: {', '.join(invalid_types)}"
                        )
                        logger.warning(f"Row {row_num}: Unknown ContributorTypes: {invalid_types}")
                    
                    # Validate Personal vs Organizational consistency
                    if name_type == 'Organizational':
                        if given_name or family_name:
                            raise CSVParseError(
                                f"Zeile {row_num}: Organizational Contributor '{contributor_name}' "
                                "darf keine Given Name oder Family Name haben."
                            )
                    
                    # Validate ORCID format (if provided)
                    if name_identifier:
                        if name_identifier_scheme.upper() == 'ORCID':
                            if not CSVParser.validate_orcid_format(name_identifier):
                                warnings.append(
                                    f"Zeile {row_num}: ORCID-Format möglicherweise ungültig: {name_identifier}"
                                )
                                logger.warning(f"Row {row_num}: Invalid ORCID format: {name_identifier}")
                    
                    # Validate ContactInfo fields (Email, Website, Position)
                    # These are only relevant for ContactPerson type
                    has_contact_info = bool(email or website or position)
                    is_contact_person = 'ContactPerson' in contributor_types
                    
                    if has_contact_info and not is_contact_person:
                        warnings.append(
                            f"Zeile {row_num}: Email/Website/Position angegeben, aber "
                            "ContributorType ist nicht 'ContactPerson'. "
                            "ContactInfo wird nur für ContactPerson gespeichert."
                        )
                        logger.warning(
                            f"Row {row_num}: ContactInfo provided but not ContactPerson"
                        )
                    
                    # Validate email format if provided
                    if email and not CSVParser._validate_email_format(email):
                        warnings.append(
                            f"Zeile {row_num}: Email-Format möglicherweise ungültig: {email}"
                        )
                        logger.warning(f"Row {row_num}: Invalid email format: {email}")
                    
                    # Validate website URL format if provided
                    if website and not CSVParser.validate_url_format(website):
                        warnings.append(
                            f"Zeile {row_num}: Website-URL möglicherweise ungültig: {website}"
                        )
                        logger.warning(f"Row {row_num}: Invalid website URL: {website}")
                    
                    # Build contributor data structure
                    contributor_data = {
                        'name': contributor_name,
                        'nameType': name_type if name_type else 'Personal',  # Default to Personal
                        'givenName': given_name,
                        'familyName': family_name,
                        'nameIdentifier': name_identifier,
                        'nameIdentifierScheme': name_identifier_scheme,
                        'schemeUri': scheme_uri,
                        'contributorTypes': contributor_types,  # List of types
                        'affiliation': affiliation,
                        'affiliationIdentifier': affiliation_identifier,
                        # ContactInfo (DB only)
                        'email': email,
                        'website': website,
                        'position': position
                    }
                    
                    # Add to contributors list for this DOI (preserving order)
                    if doi not in contributors_by_doi:
                        contributors_by_doi[doi] = []
                    
                    contributors_by_doi[doi].append(contributor_data)
                    logger.debug(f"Parsed contributor for {doi}: {contributor_name}")
        
        except csv.Error as e:
            raise CSVParseError(f"Fehler beim Lesen der CSV-Datei: {str(e)}")
        
        except UnicodeDecodeError:
            raise CSVParseError(
                "CSV-Datei konnte nicht gelesen werden. "
                "Stelle sicher, dass die Datei UTF-8 kodiert ist."
            )
        
        if not contributors_by_doi:
            raise CSVParseError(
                "Keine gültigen Contributor-Daten in der CSV-Datei gefunden. "
                "Stelle sicher, dass die Datei mindestens eine Datenzeile enthält."
            )
        
        total_contributors = sum(len(contribs) for contribs in contributors_by_doi.values())
        logger.info(
            f"Successfully parsed {len(contributors_by_doi)} DOIs with "
            f"{total_contributors} contributors from CSV"
        )
        
        return contributors_by_doi, warnings

    @staticmethod
    def _validate_email_format(email: str) -> bool:
        """
        Validate email format (basic validation).
        
        Args:
            email: Email string to validate
            
        Returns:
            True if email format is valid, False otherwise
        """
        if not email:
            return False
        
        # Basic email pattern: something@something.something
        email_pattern = re.compile(
            r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        )
        
        return bool(email_pattern.match(email))

    @staticmethod
    def parse_download_urls_csv(filepath: str) -> List[Dict]:
        """
        Parse CSV file containing DOI download URL data for updates.
        
        Expected CSV format:
        - Header row: DOI,Filename,Download_URL,Description,Format,Size_Bytes
        - Data rows: 10.1594/GFZ.SDDB.1004,data.csv,https://...,Download data,text/csv,62207
        
        Args:
            filepath: Path to the CSV file
            
        Returns:
            List of dictionaries with keys:
            - doi: str
            - filename: str
            - download_url: str
            - description: str
            - format: str
            - size_bytes: int
            
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
        
        logger.info(f"Parsing download URLs CSV file: {filepath}")
        
        entries = []
        required_headers = ['DOI', 'Filename', 'Download_URL', 'Description', 'Format', 'Size_Bytes']
        
        try:
            with open(file_path, 'r', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                
                # Validate headers
                if not reader.fieldnames:
                    raise CSVParseError("CSV-Datei ist leer oder hat keine Header-Zeile.")
                
                missing_headers = [h for h in required_headers if h not in reader.fieldnames]
                if missing_headers:
                    raise CSVParseError(
                        f"In der CSV-Datei fehlen erforderliche Header: {missing_headers}. "
                        f"Gefunden: {reader.fieldnames}"
                    )
                
                # Parse rows
                for row_num, row in enumerate(reader, start=2):  # Start at 2 (line 1 is header)
                    doi = row.get('DOI', '').strip()
                    filename = row.get('Filename', '').strip()
                    download_url = row.get('Download_URL', '').strip()
                    description = row.get('Description', '').strip()
                    format_str = row.get('Format', '').strip()
                    size_str = row.get('Size_Bytes', '').strip()
                    
                    # Skip empty rows
                    if not doi and not filename:
                        continue
                    
                    # Validate DOI
                    if not doi:
                        logger.warning(f"Zeile {row_num}: DOI fehlt - überspringe Zeile")
                        continue
                    
                    if not CSVParser.DOI_PATTERN.match(doi):
                        logger.warning(f"Zeile {row_num}: Ungültiges DOI-Format '{doi}' - überspringe Zeile")
                        continue
                    
                    # Validate filename (required as identifier)
                    if not filename:
                        logger.warning(f"Zeile {row_num}: Filename fehlt für DOI '{doi}' - überspringe Zeile")
                        continue
                    
                    # Parse size_bytes (default to 0 if empty or invalid)
                    try:
                        size_bytes = int(size_str) if size_str else 0
                        if size_bytes < 0:
                            logger.warning(f"Zeile {row_num}: Negative Dateigröße korrigiert auf 0")
                            size_bytes = 0
                    except ValueError:
                        logger.warning(f"Zeile {row_num}: Ungültige Dateigröße '{size_str}' - verwende 0")
                        size_bytes = 0
                    
                    entries.append({
                        'doi': doi,
                        'filename': filename,
                        'download_url': download_url,
                        'description': description,
                        'format': format_str,
                        'size_bytes': size_bytes
                    })
                
                logger.info(f"Parsed {len(entries)} download URL entries from CSV")
                return entries
                
        except UnicodeDecodeError as e:
            raise CSVParseError(
                f"Datei ist nicht UTF-8 kodiert. Bitte als UTF-8 speichern. Fehler: {e}"
            )
        except csv.Error as e:
            raise CSVParseError(f"CSV-Parsing-Fehler: {e}")

    # Valid SPDX license identifiers (common ones)
    # Full list at https://spdx.org/licenses/
    VALID_SPDX_IDENTIFIERS = {
        # Creative Commons licenses
        "CC0-1.0", "CC-BY-1.0", "CC-BY-2.0", "CC-BY-2.5", "CC-BY-3.0", "CC-BY-4.0",
        "CC-BY-SA-1.0", "CC-BY-SA-2.0", "CC-BY-SA-2.5", "CC-BY-SA-3.0", "CC-BY-SA-4.0",
        "CC-BY-NC-1.0", "CC-BY-NC-2.0", "CC-BY-NC-2.5", "CC-BY-NC-3.0", "CC-BY-NC-4.0",
        "CC-BY-NC-SA-1.0", "CC-BY-NC-SA-2.0", "CC-BY-NC-SA-2.5", "CC-BY-NC-SA-3.0", "CC-BY-NC-SA-4.0",
        "CC-BY-ND-1.0", "CC-BY-ND-2.0", "CC-BY-ND-2.5", "CC-BY-ND-3.0", "CC-BY-ND-4.0",
        "CC-BY-NC-ND-1.0", "CC-BY-NC-ND-2.0", "CC-BY-NC-ND-2.5", "CC-BY-NC-ND-3.0", "CC-BY-NC-ND-4.0",
        # Open Data Commons
        "ODC-By-1.0", "ODbL-1.0", "PDDL-1.0",
        # Software licenses
        "Apache-1.0", "Apache-1.1", "Apache-2.0",
        "MIT", "MIT-0",
        "BSD-2-Clause", "BSD-3-Clause", "BSD-4-Clause",
        "GPL-2.0-only", "GPL-2.0-or-later", "GPL-3.0-only", "GPL-3.0-or-later",
        "LGPL-2.0-only", "LGPL-2.0-or-later", "LGPL-2.1-only", "LGPL-2.1-or-later", "LGPL-3.0-only", "LGPL-3.0-or-later",
        "AGPL-3.0-only", "AGPL-3.0-or-later",
        "MPL-1.0", "MPL-1.1", "MPL-2.0",
        "ISC", "Unlicense", "WTFPL", "Zlib",
        # Public Domain
        "CC-PDDC", "Unlicense",
    }

    # Valid ISO 639-1 language codes (2-letter codes)
    VALID_LANGUAGE_CODES = {
        "aa", "ab", "ae", "af", "ak", "am", "an", "ar", "as", "av", "ay", "az",
        "ba", "be", "bg", "bh", "bi", "bm", "bn", "bo", "br", "bs",
        "ca", "ce", "ch", "co", "cr", "cs", "cu", "cv", "cy",
        "da", "de", "dv", "dz",
        "ee", "el", "en", "eo", "es", "et", "eu",
        "fa", "ff", "fi", "fj", "fo", "fr", "fy",
        "ga", "gd", "gl", "gn", "gu", "gv",
        "ha", "he", "hi", "ho", "hr", "ht", "hu", "hy", "hz",
        "ia", "id", "ie", "ig", "ii", "ik", "io", "is", "it", "iu",
        "ja", "jv",
        "ka", "kg", "ki", "kj", "kk", "kl", "km", "kn", "ko", "kr", "ks", "ku", "kv", "kw", "ky",
        "la", "lb", "lg", "li", "ln", "lo", "lt", "lu", "lv",
        "mg", "mh", "mi", "mk", "ml", "mn", "mr", "ms", "mt", "my",
        "na", "nb", "nd", "ne", "ng", "nl", "nn", "no", "nr", "nv", "ny",
        "oc", "oj", "om", "or", "os",
        "pa", "pi", "pl", "ps", "pt",
        "qu",
        "rm", "rn", "ro", "ru", "rw",
        "sa", "sc", "sd", "se", "sg", "si", "sk", "sl", "sm", "sn", "so", "sq", "sr", "ss", "st", "su", "sv", "sw",
        "ta", "te", "tg", "th", "ti", "tk", "tl", "tn", "to", "tr", "ts", "tt", "tw", "ty",
        "ug", "uk", "ur", "uz",
        "ve", "vi", "vo",
        "wa", "wo",
        "xh",
        "yi", "yo",
        "za", "zh", "zu"
    }

    @staticmethod
    def validate_spdx_identifier(identifier: str) -> bool:
        """
        Validate SPDX license identifier (case-insensitive).
        
        DataCite normalizes SPDX identifiers to lowercase, so we accept
        both 'CC-BY-4.0' and 'cc-by-4.0' as valid.
        
        Args:
            identifier: SPDX license identifier to validate
            
        Returns:
            True if identifier is valid, False otherwise
        """
        if not identifier:
            return True  # Empty is allowed (optional field)
        # Case-insensitive comparison: convert both to uppercase
        return identifier.upper() in {s.upper() for s in CSVParser.VALID_SPDX_IDENTIFIERS}

    @staticmethod
    def validate_language_code(lang: str) -> bool:
        """
        Validate ISO 639-1 language code.
        
        Args:
            lang: Language code to validate
            
        Returns:
            True if code is valid, False otherwise
        """
        if not lang:
            return True  # Empty is allowed (optional field)
        return lang.lower() in CSVParser.VALID_LANGUAGE_CODES

    @staticmethod
    def parse_rights_update_csv(filepath: str) -> Tuple[Dict[str, List[Dict]], List[str]]:
        """
        Parse CSV file containing DOI and rights metadata for updates.
        
        Expected CSV format:
        - Header: DOI,rights,rightsUri,schemeUri,rightsIdentifier,rightsIdentifierScheme,lang
        - Multiple rows per DOI (one per rights entry)
        - Preserves order of rights entries per DOI
        
        Args:
            filepath: Path to the CSV file
            
        Returns:
            Tuple of:
            - Dict[DOI, List[RightsData]]: Rights grouped by DOI, preserving order
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
        
        logger.info(f"Parsing rights CSV file: {filepath}")
        
        # Use OrderedDict to preserve DOI order
        rights_by_doi = OrderedDict()
        warnings = []
        
        # Expected headers
        expected_headers = [
            'DOI',
            'rights',
            'rightsUri',
            'schemeUri',
            'rightsIdentifier',
            'rightsIdentifierScheme',
            'lang'
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
                    rights_text = row.get('rights', '').strip()
                    rights_uri = row.get('rightsUri', '').strip()
                    scheme_uri = row.get('schemeUri', '').strip()
                    rights_identifier = row.get('rightsIdentifier', '').strip()
                    rights_identifier_scheme = row.get('rightsIdentifierScheme', '').strip()
                    lang = row.get('lang', '').strip()
                    
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
                    
                    # Validate SPDX identifier (if provided)
                    if rights_identifier and rights_identifier_scheme.upper() == 'SPDX':
                        if not CSVParser.validate_spdx_identifier(rights_identifier):
                            raise CSVParseError(
                                f"Zeile {row_num}: Ungültiger SPDX-Identifier '{rights_identifier}' für DOI '{doi}'. "
                                f"Gültige SPDX-Identifier findest du unter https://spdx.org/licenses/"
                            )
                    
                    # Validate language code (if provided)
                    if lang:
                        if not CSVParser.validate_language_code(lang):
                            raise CSVParseError(
                                f"Zeile {row_num}: Ungültiger Sprachcode '{lang}' für DOI '{doi}'. "
                                f"Erlaubt sind ISO 639-1 Codes (z.B. 'en', 'de', 'fr')."
                            )
                    
                    # Validate URI formats (if provided)
                    if rights_uri and not CSVParser.validate_url_format(rights_uri):
                        raise CSVParseError(
                            f"Zeile {row_num}: Ungültige rightsUri '{rights_uri}' für DOI '{doi}'. "
                            "URL muss mit http:// oder https:// beginnen."
                        )
                    
                    if scheme_uri and not CSVParser.validate_url_format(scheme_uri):
                        raise CSVParseError(
                            f"Zeile {row_num}: Ungültige schemeUri '{scheme_uri}' für DOI '{doi}'. "
                            "URL muss mit http:// oder https:// beginnen."
                        )
                    
                    # Check if row is completely empty (except DOI) - this marks "remove all rights"
                    all_rights_fields_empty = not any([
                        rights_text, rights_uri, scheme_uri, 
                        rights_identifier, rights_identifier_scheme, lang
                    ])
                    
                    # Initialize DOI entry if needed
                    if doi not in rights_by_doi:
                        rights_by_doi[doi] = []
                    
                    # Only add if at least one rights field is set (skip empty rows)
                    if not all_rights_fields_empty:
                        rights_data = {
                            'rights': rights_text,
                            'rightsUri': rights_uri,
                            'schemeUri': scheme_uri,
                            'rightsIdentifier': rights_identifier,
                            'rightsIdentifierScheme': rights_identifier_scheme,
                            'lang': lang
                        }
                        rights_by_doi[doi].append(rights_data)
                        logger.debug(f"Parsed rights for {doi}: {rights_identifier or rights_text[:30] + '...' if rights_text else 'empty'}")
        
        except csv.Error as e:
            raise CSVParseError(f"Fehler beim Lesen der CSV-Datei: {str(e)}")
        
        except UnicodeDecodeError:
            raise CSVParseError(
                "CSV-Datei konnte nicht gelesen werden. "
                "Stelle sicher, dass die Datei UTF-8 kodiert ist."
            )
        
        if not rights_by_doi:
            raise CSVParseError(
                "Keine gültigen Rights-Daten in der CSV-Datei gefunden. "
                "Stelle sicher, dass die Datei mindestens eine Datenzeile enthält."
            )
        
        # Count stats
        total_rights = sum(len(rights) for rights in rights_by_doi.values())
        dois_without_rights = sum(1 for rights in rights_by_doi.values() if not rights)
        
        logger.info(
            f"Successfully parsed {len(rights_by_doi)} DOIs with "
            f"{total_rights} rights entries from CSV "
            f"({dois_without_rights} DOIs have empty rights)"
        )
        
        return rights_by_doi, warnings
