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
    filename = f"{safe_username}.csv"
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
