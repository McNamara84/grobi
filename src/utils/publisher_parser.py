"""Utility functions for parsing publisher metadata from DataCite responses."""

import logging
from typing import Any, Dict, Tuple

logger = logging.getLogger(__name__)


def parse_publisher_from_metadata(publisher_raw: Any) -> Dict[str, str]:
    """
    Parse publisher data from DataCite API response.
    
    Publisher can be a string (legacy format) or dict (DataCite Schema 4.6 extended format).
    This function normalizes both formats into a consistent dictionary structure.
    
    Args:
        publisher_raw: Publisher data from DataCite API, can be:
            - str: Simple publisher name (legacy format)
            - dict: Extended publisher format with name, identifier, scheme, etc.
            - None or other: Will be converted to empty string
            
    Returns:
        Dictionary with normalized publisher fields:
            - name: Publisher name
            - publisherIdentifier: Publisher identifier (e.g., ROR URL)
            - publisherIdentifierScheme: Scheme name (e.g., "ROR")
            - schemeUri: Scheme URI (e.g., "https://ror.org")
            - lang: Language code (e.g., "en")
    """
    if isinstance(publisher_raw, dict):
        # Extended publisher format (DataCite Schema 4.6)
        return {
            "name": publisher_raw.get("name", ""),
            "publisherIdentifier": publisher_raw.get("publisherIdentifier", ""),
            "publisherIdentifierScheme": publisher_raw.get("publisherIdentifierScheme", ""),
            "schemeUri": publisher_raw.get("schemeUri", ""),
            "lang": publisher_raw.get("lang", "")
        }
    elif isinstance(publisher_raw, str):
        # Simple string format (legacy)
        return {
            "name": publisher_raw,
            "publisherIdentifier": "",
            "publisherIdentifierScheme": "",
            "schemeUri": "",
            "lang": ""
        }
    else:
        # Fallback for unexpected types (None, int, etc.)
        if publisher_raw:
            logger.warning(f"Unexpected publisher type: {type(publisher_raw)}")
        return {
            "name": str(publisher_raw) if publisher_raw else "",
            "publisherIdentifier": "",
            "publisherIdentifierScheme": "",
            "schemeUri": "",
            "lang": ""
        }


def parse_publisher_to_tuple(publisher_raw: Any, doi: str = "unknown") -> Tuple[str, str, str, str, str]:
    """
    Parse publisher data into a tuple format for CSV export.
    
    Args:
        publisher_raw: Publisher data from DataCite API
        doi: DOI for logging purposes
        
    Returns:
        Tuple of (name, identifier, scheme, scheme_uri, lang)
    """
    parsed = parse_publisher_from_metadata(publisher_raw)
    return (
        parsed["name"],
        parsed["publisherIdentifier"],
        parsed["publisherIdentifierScheme"],
        parsed["schemeUri"],
        parsed["lang"]
    )
