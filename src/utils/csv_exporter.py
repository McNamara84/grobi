"""CSV Export functionality for DOI data."""

import csv
import logging
import os
from pathlib import Path
from typing import List, Tuple


logger = logging.getLogger(__name__)


class CSVExportError(Exception):
    """Base exception for CSV export errors."""
    pass


def export_dois_to_csv(
    dois_list: List[Tuple[str, str]], 
    username: str, 
    output_dir: str = None
) -> str:
    """
    Export DOIs and their landing page URLs to a CSV file.
    
    Args:
        dois_list: List of tuples containing (DOI, Landing Page URL)
        username: DataCite username (used for filename)
        output_dir: Directory where CSV should be saved. 
                   If None, uses current working directory.
    
    Returns:
        Path to the created CSV file
        
    Raises:
        CSVExportError: If export fails due to permissions, disk space, etc.
    """
    if output_dir is None:
        output_dir = os.getcwd()
    
    # Sanitize username for filename (remove problematic characters)
    safe_username = "".join(c if c.isalnum() or c in ".-_" else "_" for c in username)
    filename = f"{safe_username}_urls.csv"
    filepath = Path(output_dir) / filename
    
    logger.info(f"Exporting {len(dois_list)} DOIs to {filepath}")
    
    # Check if directory exists and is writable
    try:
        output_path = Path(output_dir)
        if not output_path.exists():
            output_path.mkdir(parents=True, exist_ok=True)
            logger.debug(f"Created output directory: {output_dir}")
        
        # Test write permissions
        if not os.access(output_dir, os.W_OK):
            error_msg = f"Keine Schreibrechte für Verzeichnis: {output_dir}"
            logger.error(error_msg)
            raise CSVExportError(error_msg)
            
    except PermissionError as e:
        error_msg = f"Keine Berechtigung zum Erstellen des Verzeichnisses: {output_dir}"
        logger.error(f"Permission error: {e}")
        raise CSVExportError(error_msg)
    except OSError as e:
        error_msg = f"Fehler beim Erstellen des Verzeichnisses: {str(e)}"
        logger.error(f"OS error: {e}")
        raise CSVExportError(error_msg)
    
    # Write CSV file
    try:
        with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            
            # Write header
            writer.writerow(['DOI', 'Landing_Page_URL'])
            
            # Write data rows
            for doi, url in dois_list:
                writer.writerow([doi, url])
        
        logger.info(f"Successfully exported {len(dois_list)} DOIs to {filepath}")
        return str(filepath)
        
    except PermissionError as e:
        error_msg = f"Keine Berechtigung zum Schreiben der Datei: {filepath}"
        logger.error(f"Permission error writing file: {e}")
        raise CSVExportError(error_msg)
    
    except OSError as e:
        # This could be disk full, invalid path, etc.
        error_msg = f"Die CSV-Datei konnte nicht gespeichert werden: {str(e)}"
        logger.error(f"OS error writing file: {e}")
        raise CSVExportError(error_msg)
    
    except Exception as e:
        error_msg = f"Unerwarteter Fehler beim Speichern der CSV-Datei: {str(e)}"
        logger.error(f"Unexpected error: {e}")
        raise CSVExportError(error_msg)


def export_dois_with_creators_to_csv(
    data: List[Tuple[str, str, str, str, str, str, str, str]],
    username: str,
    output_dir: str = None
) -> str:
    """
    Export DOIs with creator information to a CSV file.
    
    One row per creator, so DOIs with multiple creators will appear multiple times.
    
    Args:
        data: List of tuples containing:
              (DOI, Creator Name, Name Type, Given Name, Family Name,
               Name Identifier, Name Identifier Scheme, Scheme URI)
        username: DataCite username (used for filename)
        output_dir: Directory where CSV should be saved.
                   If None, uses current working directory.
    
    Returns:
        Path to the created CSV file
        
    Raises:
        CSVExportError: If export fails due to permissions, disk space, etc.
    """
    if output_dir is None:
        output_dir = os.getcwd()
    
    # Sanitize username for filename (remove problematic characters)
    safe_username = "".join(c if c.isalnum() or c in ".-_" else "_" for c in username)
    filename = f"{safe_username}_authors.csv"
    filepath = Path(output_dir) / filename
    
    logger.info(f"Exporting {len(data)} creator entries to {filepath}")
    
    # Check if directory exists and is writable
    try:
        output_path = Path(output_dir)
        if not output_path.exists():
            output_path.mkdir(parents=True, exist_ok=True)
            logger.debug(f"Created output directory: {output_dir}")
        
        # Test write permissions
        if not os.access(output_dir, os.W_OK):
            error_msg = f"Keine Schreibrechte für Verzeichnis: {output_dir}"
            logger.error(error_msg)
            raise CSVExportError(error_msg)
            
    except PermissionError as e:
        error_msg = f"Keine Berechtigung zum Erstellen des Verzeichnisses: {output_dir}"
        logger.error(f"Permission error: {e}")
        raise CSVExportError(error_msg)
    except OSError as e:
        error_msg = f"Fehler beim Erstellen des Verzeichnisses: {str(e)}"
        logger.error(f"OS error: {e}")
        raise CSVExportError(error_msg)
    
    # Write CSV file
    try:
        with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            
            # Write header
            writer.writerow([
                'DOI',
                'Creator Name',
                'Name Type',
                'Given Name',
                'Family Name',
                'Name Identifier',
                'Name Identifier Scheme',
                'Scheme URI'
            ])
            
            # Write data rows
            for row in data:
                writer.writerow(row)
        
        logger.info(f"Successfully exported {len(data)} creator entries to {filepath}")
        return str(filepath)
        
    except PermissionError as e:
        error_msg = f"Keine Berechtigung zum Schreiben der Datei: {filepath}"
        logger.error(f"Permission error writing file: {e}")
        raise CSVExportError(error_msg)
    
    except OSError as e:
        # This could be disk full, invalid path, etc.
        error_msg = f"Die CSV-Datei konnte nicht gespeichert werden: {str(e)}"
        logger.error(f"OS error writing file: {e}")
        raise CSVExportError(error_msg)
    
    except Exception as e:
        error_msg = f"Unerwarteter Fehler beim Speichern der CSV-Datei: {str(e)}"
        logger.error(f"Unexpected error: {e}")
        raise CSVExportError(error_msg)


def export_dois_with_publisher_to_csv(
    data: List[Tuple[str, str, str, str, str, str]],
    username: str,
    output_dir: str = None
) -> Tuple[str, int]:
    """
    Export DOIs with publisher information to a CSV file.
    
    One row per DOI (each DOI has exactly one publisher).
    
    Args:
        data: List of tuples containing:
              (DOI, Publisher Name, Publisher Identifier,
               Publisher Identifier Scheme, Scheme URI, Language)
        username: DataCite username (used for filename)
        output_dir: Directory where CSV should be saved.
                   If None, uses current working directory.
    
    Returns:
        Tuple of (filepath, warnings_count) where warnings_count is the number
        of DOIs without publisherIdentifier
        
    Raises:
        CSVExportError: If export fails due to permissions, disk space, etc.
    """
    if output_dir is None:
        output_dir = os.getcwd()
    
    # Sanitize username for filename (remove problematic characters)
    safe_username = "".join(c if c.isalnum() or c in ".-_" else "_" for c in username)
    filename = f"{safe_username}_publishers.csv"
    filepath = Path(output_dir) / filename
    
    logger.info(f"Exporting {len(data)} publisher entries to {filepath}")
    
    # Check if directory exists and is writable
    try:
        output_path = Path(output_dir)
        if not output_path.exists():
            output_path.mkdir(parents=True, exist_ok=True)
            logger.debug(f"Created output directory: {output_dir}")
        
        # Test write permissions
        if not os.access(output_dir, os.W_OK):
            error_msg = f"Keine Schreibrechte für Verzeichnis: {output_dir}"
            logger.error(error_msg)
            raise CSVExportError(error_msg)
            
    except PermissionError as e:
        error_msg = f"Keine Berechtigung zum Erstellen des Verzeichnisses: {output_dir}"
        logger.error(f"Permission error: {e}")
        raise CSVExportError(error_msg)
    except OSError as e:
        error_msg = f"Fehler beim Erstellen des Verzeichnisses: {str(e)}"
        logger.error(f"OS error: {e}")
        raise CSVExportError(error_msg)
    
    # Count DOIs without publisherIdentifier for warning
    warnings_count = 0
    
    # Write CSV file
    try:
        with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            
            # Write header
            writer.writerow([
                'DOI',
                'Publisher Name',
                'Publisher Identifier',
                'Publisher Identifier Scheme',
                'Scheme URI',
                'Language'
            ])
            
            # Write data rows
            for row in data:
                writer.writerow(row)
                # Check if publisherIdentifier is empty (column index 2)
                # Handle rows with fewer elements (trailing empty values may be omitted)
                # and use strip() to properly detect empty strings with whitespace
                if len(row) >= 3 and (not row[2] or not str(row[2]).strip()):
                    warnings_count += 1
                    logger.warning(f"DOI {row[0]} has no publisherIdentifier")
        
        logger.info(f"Successfully exported {len(data)} publisher entries to {filepath}")
        if warnings_count > 0:
            logger.warning(f"{warnings_count} DOIs have no publisherIdentifier")
        
        return str(filepath), warnings_count
        
    except PermissionError as e:
        error_msg = f"Keine Berechtigung zum Schreiben der Datei: {filepath}"
        logger.error(f"Permission error writing file: {e}")
        raise CSVExportError(error_msg)
    
    except OSError as e:
        # This could be disk full, invalid path, etc.
        error_msg = f"Die CSV-Datei konnte nicht gespeichert werden: {str(e)}"
        logger.error(f"OS error writing file: {e}")
        raise CSVExportError(error_msg)
    
    except Exception as e:
        error_msg = f"Unerwarteter Fehler beim Speichern der CSV-Datei: {str(e)}"
        logger.error(f"Unexpected error: {e}")
        raise CSVExportError(error_msg)


def export_dois_with_contributors_to_csv(
    data: List[Tuple[str, str, str, str, str, str, str, str, str, str, str, str, str, str]],
    username: str,
    output_dir: str = None
) -> str:
    """
    Export DOIs with contributor information to a CSV file.
    
    One row per contributor, so DOIs with multiple contributors will appear multiple times.
    Contributors can have multiple ContributorTypes (comma-separated).
    ContactInfo (Email, Website, Position) is only populated for ContactPerson type (DB only).
    
    Args:
        data: List of tuples containing:
              (DOI, Contributor Name, Name Type, Given Name, Family Name,
               Name Identifier, Name Identifier Scheme, Scheme URI,
               Contributor Types, Affiliation, Affiliation Identifier,
               Email, Website, Position)
        username: DataCite username (used for filename)
        output_dir: Directory where CSV should be saved.
                   If None, uses current working directory.
    
    Returns:
        Path to the created CSV file
        
    Raises:
        CSVExportError: If export fails due to permissions, disk space, etc.
    """
    if output_dir is None:
        output_dir = os.getcwd()
    
    # Sanitize username for filename (remove problematic characters)
    safe_username = "".join(c if c.isalnum() or c in ".-_" else "_" for c in username)
    filename = f"{safe_username}_contributors.csv"
    filepath = Path(output_dir) / filename
    
    logger.info(f"Exporting {len(data)} contributor entries to {filepath}")
    
    # Check if directory exists and is writable
    try:
        output_path = Path(output_dir)
        if not output_path.exists():
            output_path.mkdir(parents=True, exist_ok=True)
            logger.debug(f"Created output directory: {output_dir}")
        
        # Test write permissions
        if not os.access(output_dir, os.W_OK):
            error_msg = f"Keine Schreibrechte für Verzeichnis: {output_dir}"
            logger.error(error_msg)
            raise CSVExportError(error_msg)
            
    except PermissionError as e:
        error_msg = f"Keine Berechtigung zum Erstellen des Verzeichnisses: {output_dir}"
        logger.error(f"Permission error: {e}")
        raise CSVExportError(error_msg)
    except OSError as e:
        error_msg = f"Fehler beim Erstellen des Verzeichnisses: {str(e)}"
        logger.error(f"OS error: {e}")
        raise CSVExportError(error_msg)
    
    # Write CSV file
    try:
        with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            
            # Write header (14 columns)
            writer.writerow([
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
            ])
            
            # Write data rows
            for row in data:
                writer.writerow(row)
        
        logger.info(f"Successfully exported {len(data)} contributor entries to {filepath}")
        return str(filepath)
        
    except PermissionError as e:
        error_msg = f"Keine Berechtigung zum Schreiben der Datei: {filepath}"
        logger.error(f"Permission error writing file: {e}")
        raise CSVExportError(error_msg)
    
    except OSError as e:
        # This could be disk full, invalid path, etc.
        error_msg = f"Die CSV-Datei konnte nicht gespeichert werden: {str(e)}"
        logger.error(f"OS error writing file: {e}")
        raise CSVExportError(error_msg)
    
    except Exception as e:
        error_msg = f"Unerwarteter Fehler beim Speichern der CSV-Datei: {str(e)}"
        logger.error(f"Unexpected error: {e}")
        raise CSVExportError(error_msg)


def export_dois_download_urls(
    dois_files: List[Tuple[str, str, str, str, str, int]], 
    filepath: str
) -> None:
    """
    Export DOIs with download URLs to CSV.
    
    Args:
        dois_files: List of (DOI, Filename, Download_URL, Description, Format, Size_Bytes) tuples
        filepath: Output CSV file path
        
    Raises:
        CSVExportError: If file cannot be written
    """
    logger.info(f"Exporting {len(dois_files)} file entries to {filepath}")
    
    try:
        with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            
            # Header
            writer.writerow(['DOI', 'Filename', 'Download_URL', 'Description', 'Format', 'Size_Bytes'])
            
            # Data rows
            for doi, filename, url, description, format_str, size in dois_files:
                writer.writerow([doi, filename, url, description, format_str, size])
        
        logger.info(f"Successfully exported {len(dois_files)} file entries to {filepath}")
        
    except PermissionError as e:
        error_msg = f"Keine Berechtigung zum Schreiben der Datei: {filepath}"
        logger.error(f"Permission error writing file: {e}")
        raise CSVExportError(error_msg)
    
    except OSError as e:
        error_msg = f"Die CSV-Datei konnte nicht gespeichert werden: {str(e)}"
        logger.error(f"OS error writing file: {e}")
        raise CSVExportError(error_msg)
    
    except Exception as e:
        error_msg = f"Unerwarteter Fehler beim Speichern der CSV-Datei: {str(e)}"
        logger.error(f"Unexpected error: {e}")
        raise CSVExportError(error_msg)


def validate_csv_format(filepath: str) -> bool:
    """
    Validate that a CSV file has the correct format.
    
    Args:
        filepath: Path to the CSV file to validate
        
    Returns:
        True if valid, False otherwise
    """
    try:
        with open(filepath, 'r', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            
            # Check header
            header = next(reader, None)
            if header != ['DOI', 'Landing_Page_URL']:
                logger.warning(f"Invalid header in {filepath}: {header}")
                return False
            
            # Check that all rows have exactly 2 columns
            # Empty rows are acceptable (empty list means end of file)
            for row_num, row in enumerate(reader, start=2):
                if len(row) == 0:
                    # Empty row at end of file is acceptable
                    continue
                if len(row) != 2:
                    logger.warning(f"Invalid row {row_num} in {filepath}: {row}")
                    return False
            
            return True
            
    except Exception as e:
        logger.error(f"Error validating CSV {filepath}: {e}")
        return False


def export_schema_check_results_to_csv(
    incompatible_dois: List[Tuple[str, str, dict, str]],
    username: str,
    output_dir: str = None
) -> str:
    """
    Export Schema 4 compatibility check results to a CSV file.
    
    Args:
        incompatible_dois: List of tuples containing:
            - DOI (str)
            - schema_version (str)
            - missing_fields (dict)
            - reason (str): Human-readable reason string
        username: DataCite username (used for filename)
        output_dir: Directory where CSV should be saved.
                   If None, uses current working directory.
    
    Returns:
        Path to the created CSV file
        
    Raises:
        CSVExportError: If export fails due to permissions, disk space, etc.
    """
    if output_dir is None:
        output_dir = os.getcwd()
    
    # Sanitize username for filename (remove problematic characters)
    safe_username = "".join(c if c.isalnum() or c in ".-_" else "_" for c in username)
    
    # Generate filename with timestamp
    from datetime import datetime
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{safe_username}_schema_check_{timestamp}.csv"
    filepath = Path(output_dir) / filename
    
    logger.info(f"Exporting {len(incompatible_dois)} incompatible DOIs to {filepath}")
    
    # Check if directory exists and is writable
    try:
        output_path = Path(output_dir)
        if not output_path.exists():
            error_msg = f"Das Ausgabeverzeichnis existiert nicht: {output_dir}"
            logger.error(f"Output directory does not exist: {output_dir}")
            raise CSVExportError(error_msg)
        
        if not os.access(output_dir, os.W_OK):
            error_msg = f"Keine Schreibrechte für Verzeichnis: {output_dir}"
            logger.error(f"No write permission for directory: {output_dir}")
            raise CSVExportError(error_msg)
    
    except CSVExportError:
        raise
    except Exception as e:
        error_msg = f"Fehler beim Überprüfen des Ausgabeverzeichnisses: {str(e)}"
        logger.error(f"Error checking output directory: {e}")
        raise CSVExportError(error_msg)
    
    # Write CSV file
    try:
        with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            
            # Write header
            writer.writerow([
                'DOI',
                'Current_Schema_Version',
                'Missing_Publisher',
                'Missing_PublicationYear',
                'Missing_Titles',
                'Missing_Creators',
                'Missing_ResourceType',
                'Invalid_NameTypes',
                'Unknown_ContributorTypes',
                'Reason'
            ])
            
            # Write data rows
            for doi, schema_version, missing_fields, reason in incompatible_dois:
                # Extract individual field flags
                missing_publisher = "Ja" if "publisher" in missing_fields else "Nein"
                missing_pub_year = "Ja" if "publicationYear" in missing_fields else "Nein"
                missing_titles = "Ja" if "titles" in missing_fields else "Nein"
                missing_creators = "Ja" if "creators" in missing_fields else "Nein"
                missing_resource_type = "Ja" if "resourceType" in missing_fields else "Nein"
                
                # Format invalid name types
                invalid_name_types = missing_fields.get("invalid_name_types", [])
                invalid_name_types_str = ", ".join(invalid_name_types) if invalid_name_types else ""
                
                # Format unknown contributor types
                unknown_contrib_types = missing_fields.get("unknown_contributor_types", [])
                unknown_contrib_types_str = ", ".join(unknown_contrib_types) if unknown_contrib_types else ""
                
                writer.writerow([
                    doi,
                    schema_version,
                    missing_publisher,
                    missing_pub_year,
                    missing_titles,
                    missing_creators,
                    missing_resource_type,
                    invalid_name_types_str,
                    unknown_contrib_types_str,
                    reason
                ])
        
        logger.info(f"Successfully exported {len(incompatible_dois)} rows to {filepath}")
        return str(filepath)
    
    except PermissionError as e:
        error_msg = f"Keine Berechtigung zum Schreiben der Datei {filepath}: {str(e)}"
        logger.error(f"Permission error: {e}")
        raise CSVExportError(error_msg)
    
    except OSError as e:
        error_msg = f"Fehler beim Schreiben der Datei {filepath}: {str(e)}"
        logger.error(f"OS error writing file: {e}")
        raise CSVExportError(error_msg)
    
    except Exception as e:
        error_msg = f"Unerwarteter Fehler beim Speichern der CSV-Datei: {str(e)}"
        logger.error(f"Unexpected error: {e}")
        raise CSVExportError(error_msg)

