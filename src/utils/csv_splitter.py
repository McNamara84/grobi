"""CSV splitter utility for splitting CSV files by DOI prefix."""

import csv
import logging
from pathlib import Path
from typing import Dict, List, Tuple
from collections import defaultdict

logger = logging.getLogger(__name__)


class CSVSplitError(Exception):
    """Raised when CSV splitting fails."""
    pass


def extract_doi_prefix(doi: str, level: int = 2) -> str:
    """
    Extract DOI prefix up to specified level.
    
    Examples:
        10.5880/gfz.2011.100 -> 10.5880/gfz.2011 (level=2)
        10.1594/gfz.geofon.gfz2008ewsv -> 10.1594/gfz.geofon (level=2)
        10.5880/gfz.2011.100 -> 10.5880 (level=1)
    
    Args:
        doi: DOI string
        level: Number of dot-separated parts to include after the slash
               level=1 includes only registrant (e.g., 10.5880)
               level=2 includes registrant + 2 parts after slash (e.g., 10.5880/gfz.2011)
    
    Returns:
        DOI prefix string
    
    Raises:
        CSVSplitError: If DOI format is invalid
    """
    if not doi or '/' not in doi:
        raise CSVSplitError(f"Ungültiges DOI-Format: {doi}")
    
    parts = doi.split('/')
    if len(parts) < 2:
        raise CSVSplitError(f"Ungültiges DOI-Format: {doi}")
    
    # First part is always the registrant (e.g., "10.5880")
    registrant = parts[0]
    
    # For level=1, return just registrant
    if level == 1:
        return registrant
    
    # Get the suffix parts (everything after the slash)
    suffix_parts = parts[1].split('.')
    
    # For level=2+, include registrant + level parts from suffix
    # level=2 means registrant + first 2 dot-separated parts (e.g., "gfz.2011")
    num_suffix_parts = level
    
    if len(suffix_parts) >= num_suffix_parts:
        # Join the first num_suffix_parts with dots
        suffix_prefix = '.'.join(suffix_parts[:num_suffix_parts])
    else:
        # If we don't have enough parts, use what we have
        suffix_prefix = '.'.join(suffix_parts)
    
    return f"{registrant}/{suffix_prefix}"


def split_csv_by_doi_prefix(
    input_file: Path,
    output_dir: Path,
    prefix_level: int = 2,
    progress_callback=None
) -> Tuple[int, Dict[str, int]]:
    """
    Split CSV file by DOI prefix into multiple files.
    
    Args:
        input_file: Path to input CSV file
        output_dir: Directory to write output files
        prefix_level: Level of DOI prefix to use for splitting (default: 2)
        progress_callback: Optional callback function(message: str) for progress updates
    
    Returns:
        Tuple of (total_rows, dict mapping prefix to row count)
    
    Raises:
        CSVSplitError: If file reading/writing fails
    """
    if not input_file.exists():
        raise CSVSplitError(f"Eingabedatei nicht gefunden: {input_file}")
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Group rows by prefix
    prefix_groups: Dict[str, List[List[str]]] = defaultdict(list)
    header = None
    total_rows = 0
    skipped_rows = 0
    
    if progress_callback:
        progress_callback(f"Lese CSV-Datei: {input_file.name}")
    
    try:
        with open(input_file, 'r', encoding='utf-8-sig', newline='') as f:
            reader = csv.reader(f)
            
            # Read header
            header = next(reader)
            
            # Check if first column is DOI
            if not header or header[0].upper() != 'DOI':
                raise CSVSplitError("CSV-Datei muss 'DOI' als erste Spalte haben")
            
            # Group rows by prefix
            for row in reader:
                if not row or not row[0]:
                    skipped_rows += 1
                    continue
                
                doi = row[0].strip()
                
                try:
                    prefix = extract_doi_prefix(doi, level=prefix_level)
                    prefix_groups[prefix].append(row)
                    total_rows += 1
                except CSVSplitError as e:
                    logger.warning(f"Überspringe ungültigen DOI: {doi} - {e}")
                    skipped_rows += 1
                    continue
    
    except Exception as e:
        raise CSVSplitError(f"Fehler beim Lesen der CSV-Datei: {str(e)}")
    
    if progress_callback:
        progress_callback(f"Gefunden: {total_rows} DOIs in {len(prefix_groups)} Gruppen")
    
    # Write separate files for each prefix
    prefix_counts = {}
    base_filename = input_file.stem
    
    for i, (prefix, rows) in enumerate(sorted(prefix_groups.items()), 1):
        # Sanitize prefix for filename (replace / with _)
        safe_prefix = prefix.replace('/', '_').replace('\\', '_')
        output_file = output_dir / f"{base_filename}_{safe_prefix}.csv"
        
        try:
            with open(output_file, 'w', encoding='utf-8-sig', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(header)
                writer.writerows(rows)
            
            prefix_counts[prefix] = len(rows)
            
            if progress_callback:
                progress_callback(
                    f"[{i}/{len(prefix_groups)}] {prefix}: {len(rows)} DOIs → {output_file.name}"
                )
        
        except Exception as e:
            raise CSVSplitError(f"Fehler beim Schreiben von {output_file}: {str(e)}")
    
    if skipped_rows > 0:
        logger.warning(f"{skipped_rows} Zeilen übersprungen (ungültige DOIs)")
    
    return total_rows, prefix_counts
