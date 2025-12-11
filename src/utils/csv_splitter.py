"""CSV splitter utility for splitting CSV files by DOI prefix."""

import csv
import logging
from pathlib import Path
from typing import Dict, Tuple, Optional, Callable
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
               level=1 includes only DOI prefix/registrant code (e.g., 10.5880)
               level=2 includes DOI prefix + 2 parts after slash (e.g., 10.5880/gfz.2011)
    
    Returns:
        DOI prefix string
    
    Raises:
        CSVSplitError: If DOI format is invalid or prefix_level is out of range
    """
    if not 1 <= level <= 4:
        raise CSVSplitError(f"Ungültiger prefix_level: {level}. Muss zwischen 1 und 4 liegen.")
    
    if not doi or '/' not in doi:
        raise CSVSplitError(f"Ungültiges DOI-Format: {doi}")
    
    parts = doi.split('/')
    if len(parts) < 2:
        raise CSVSplitError(f"Ungültiges DOI-Format: {doi}")
    
    # First part is the DOI prefix/registrant code (e.g., "10.5880")
    doi_prefix = parts[0]
    
    # For level=1, return just the DOI prefix
    if level == 1:
        return doi_prefix
    
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
    
    return f"{doi_prefix}/{suffix_prefix}"


def split_csv_by_doi_prefix(
    input_file: Path,
    output_dir: Path,
    prefix_level: int = 2,
    progress_callback: Optional[Callable[[str], None]] = None
) -> Tuple[int, Dict[str, int]]:
    """
    Split CSV file by DOI prefix into multiple files.
    
    Args:
        input_file: Path to input CSV file
        output_dir: Directory to write output files
        prefix_level: Level of DOI prefix to use for splitting (1-4, default: 2)
        progress_callback: Optional callback function(message: str) for progress updates
    
    Returns:
        Tuple of (total_rows, dict mapping prefix to row count)
    
    Raises:
        CSVSplitError: If file reading/writing fails or prefix_level is invalid
    """
    if not 1 <= prefix_level <= 4:
        raise CSVSplitError(f"Ungültiger prefix_level: {prefix_level}. Muss zwischen 1 und 4 liegen.")
    
    if not input_file.exists():
        raise CSVSplitError(f"Eingabedatei nicht gefunden: {input_file}")
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Track open file handles and writers for streaming approach
    file_handles: Dict[str, tuple] = {}  # prefix -> (file_handle, csv_writer)
    header = None
    total_rows = 0
    skipped_rows = 0
    prefix_counts: Dict[str, int] = defaultdict(int)
    base_filename = input_file.stem
    
    if progress_callback:
        progress_callback(f"Lese CSV-Datei: {input_file.name}")
    
    try:
        with open(input_file, 'r', encoding='utf-8-sig', newline='') as f:
            reader = csv.reader(f)
            
            # Read header
            try:
                header = next(reader)
            except StopIteration:
                raise CSVSplitError("CSV-Datei ist leer (keine Zeilen vorhanden)")
            
            # Check if first column is DOI
            if not header or header[0].upper() != 'DOI':
                raise CSVSplitError("CSV-Datei muss 'DOI' als erste Spalte haben")
            
            # Process rows and write directly to output files (streaming approach)
            for row in reader:
                if not row or not row[0]:
                    skipped_rows += 1
                    continue
                
                doi = row[0].strip()
                
                try:
                    prefix = extract_doi_prefix(doi, level=prefix_level)
                    
                    # Open new file if this is the first row for this prefix
                    if prefix not in file_handles:
                        safe_prefix = prefix.replace('/', '_').replace('\\', '_')
                        output_file = output_dir / f"{base_filename}_{safe_prefix}.csv"
                        fh = open(output_file, 'w', encoding='utf-8-sig', newline='')
                        # Add to file_handles immediately to ensure cleanup even if error occurs
                        file_handles[prefix] = (fh, None)
                        try:
                            writer = csv.writer(fh)
                            writer.writerow(header)
                            file_handles[prefix] = (fh, writer)
                        except Exception:
                            # If header writing fails, ensure file is closed
                            fh.close()
                            del file_handles[prefix]
                            raise
                    
                    # Write row to appropriate file
                    _, writer = file_handles[prefix]
                    writer.writerow(row)
                    prefix_counts[prefix] += 1
                    total_rows += 1
                    
                except CSVSplitError as e:
                    logger.warning(f"Überspringe ungültigen DOI: {doi} - {e}")
                    skipped_rows += 1
                    continue
    
    except Exception as e:
        raise CSVSplitError(f"Fehler beim Lesen der CSV-Datei: {str(e)}")
    finally:
        # Close all open file handles, catching cleanup exceptions separately
        for prefix, (fh, _) in file_handles.items():
            try:
                fh.close()
            except Exception as cleanup_error:
                logger.error(f"Fehler beim Schließen der Datei für Prefix {prefix}: {cleanup_error}")
    
    if progress_callback:
        progress_callback(f"Geschrieben: {total_rows} DOIs in {len(prefix_counts)} Dateien")
    
    # Log progress for each prefix
    for i, (prefix, count) in enumerate(sorted(prefix_counts.items()), 1):
        if progress_callback:
            safe_prefix = prefix.replace('/', '_').replace('\\', '_')
            output_file = output_dir / f"{base_filename}_{safe_prefix}.csv"
            progress_callback(
                f"[{i}/{len(prefix_counts)}] {prefix}: {count} DOIs → {output_file.name}"
            )
    
    # Always log skipped rows count for transparency
    if skipped_rows > 0:
        logger.warning(f"{skipped_rows} Zeilen übersprungen (ungültige DOIs)")
    else:
        logger.info("Alle Zeilen erfolgreich verarbeitet (0 Zeilen übersprungen)")
    
    return total_rows, prefix_counts
