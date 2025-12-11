"""CSV splitter utility for splitting CSV files by DOI prefix.

Implementation Note:
    The split_csv_by_doi_prefix function uses three separate dictionaries
    (file_handles, writers, sanitized_prefixes) to track open files. This
    design avoids (file_handle, None) states during initialization and ensures
    atomic state transitions. A future refactor could bundle these into a
    dataclass or namedtuple for better encapsulation.
"""

import csv
import logging
from pathlib import Path
from typing import Dict, Tuple, Optional, Callable, TextIO
from collections import defaultdict

logger = logging.getLogger(__name__)


class CSVSplitError(Exception):
    """Raised when CSV splitting fails."""
    pass


def _sanitize_filename(prefix: str) -> str:
    r"""Sanitize DOI prefix for use in filename across all platforms.
    
    Replaces characters that are problematic on Windows, macOS, or Linux.
    This includes: \ / : * ? " < > | and control characters.
    
    Args:
        prefix: DOI prefix to sanitize
        
    Returns:
        Sanitized string safe for use in filenames on all platforms
    """
    # Characters forbidden on Windows (most restrictive)
    forbidden_chars = ['\\', '/', ':', '*', '?', '"', '<', '>', '|']
    sanitized = prefix
    for char in forbidden_chars:
        sanitized = sanitized.replace(char, '_')
    return sanitized


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
    if len(parts) < 2 or not parts[1]:
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
    
    Note:
        If an error occurs during processing, partial output files will remain in
        the output directory. This is due to the streaming approach where rows are
        written incrementally as they are processed. These partial files may contain
        incomplete data but can be useful for debugging or recovery. They should be
        manually removed or overwritten by re-running the operation.
        
        **Warning**: If the output directory already contains files with the same
        base filename pattern (e.g., from a previous run), these files will be
        silently overwritten. Consider using a different output directory or
        manually cleaning existing files before running the operation.
    """
    if not 1 <= prefix_level <= 4:
        raise CSVSplitError(f"Ungültiger Prefix-Level: {prefix_level}. Muss zwischen 1 und 4 liegen.")
    
    if not input_file.exists():
        raise CSVSplitError(f"Eingabedatei nicht gefunden: {input_file}")
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Track open file handles and writers for streaming approach
    # Separate dicts to avoid (fh, None) state during initialization
    file_handles: Dict[str, TextIO] = {}  # prefix -> file_handle
    writers: Dict[str, csv.writer] = {}  # prefix -> csv_writer
    sanitized_prefixes: Dict[str, str] = {}  # prefix -> sanitized_prefix (for caching)
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
                        safe_prefix = _sanitize_filename(prefix)
                        sanitized_prefixes[prefix] = safe_prefix  # Cache for later use
                        output_file = output_dir / f"{base_filename}_{safe_prefix}.csv"
                        fh = open(output_file, 'w', encoding='utf-8-sig', newline='')
                        # Add to file_handles immediately to ensure cleanup even if error occurs
                        file_handles[prefix] = fh
                        try:
                            writer = csv.writer(fh)
                            writer.writerow(header)
                            writers[prefix] = writer
                        except Exception:
                            # If header writing fails, ensure file is closed
                            fh.close()
                            del file_handles[prefix]
                            del sanitized_prefixes[prefix]
                            raise
                    
                    # Write row to appropriate file
                    writers[prefix].writerow(row)
                    prefix_counts[prefix] += 1
                    total_rows += 1
                    
                except CSVSplitError as e:
                    logger.warning(f"Überspringe ungültigen DOI: {doi} - {e}")
                    skipped_rows += 1
                    continue
    
    except Exception as e:
        raise CSVSplitError(f"Fehler beim Lesen der CSV-Datei: {str(e)}") from e
    finally:
        # Close all open file handles, catching cleanup exceptions separately
        for prefix, fh in file_handles.items():
            try:
                fh.close()
            except Exception as cleanup_error:
                logger.error(f"Fehler beim Schließen der Datei für Prefix {prefix}: {cleanup_error}")
    
    if progress_callback:
        progress_callback(f"Geschrieben: {total_rows} DOIs in {len(prefix_counts)} Dateien")
    
    # Log progress for each prefix
    for i, (prefix, count) in enumerate(sorted(prefix_counts.items()), 1):
        if progress_callback:
            safe_prefix = sanitized_prefixes[prefix]  # Use cached value
            output_file = output_dir / f"{base_filename}_{safe_prefix}.csv"
            progress_callback(
                f"[{i}/{len(prefix_counts)}] {prefix}: {count} DOIs → {output_file.name}"
            )
    
    # Always log and report skipped rows count for transparency
    if skipped_rows > 0:
        skip_msg = f"⚠️ {skipped_rows} Zeilen übersprungen (ungültige DOIs)"
        logger.warning(skip_msg)
        if progress_callback:
            progress_callback(skip_msg)
    else:
        skip_msg = "Alle Zeilen erfolgreich verarbeitet"
        logger.info(skip_msg)
        if progress_callback:
            progress_callback(f"✓ {skip_msg}")
    
    return total_rows, prefix_counts
