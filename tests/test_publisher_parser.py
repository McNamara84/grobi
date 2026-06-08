"""Tests for publisher metadata parsing helpers."""

import logging

from src.utils.publisher_parser import (
    parse_publisher_from_metadata,
    parse_publisher_to_tuple,
)


def test_parse_extended_publisher_dict():
    """Extended DataCite publisher metadata is preserved."""
    result = parse_publisher_from_metadata({
        "name": "GFZ German Research Centre for Geosciences",
        "publisherIdentifier": "https://ror.org/04z8jg394",
        "publisherIdentifierScheme": "ROR",
        "schemeUri": "https://ror.org",
        "lang": "en",
    })

    assert result == {
        "name": "GFZ German Research Centre for Geosciences",
        "publisherIdentifier": "https://ror.org/04z8jg394",
        "publisherIdentifierScheme": "ROR",
        "schemeUri": "https://ror.org",
        "lang": "en",
    }


def test_parse_legacy_publisher_string():
    """Legacy string publisher metadata is normalized."""
    result = parse_publisher_from_metadata("Helmholtz Centre Potsdam")

    assert result["name"] == "Helmholtz Centre Potsdam"
    assert result["publisherIdentifier"] == ""
    assert result["publisherIdentifierScheme"] == ""
    assert result["schemeUri"] == ""
    assert result["lang"] == ""


def test_parse_none_publisher_returns_empty_fields(caplog):
    """Missing publisher metadata returns empty strings without warning."""
    with caplog.at_level(logging.WARNING):
        result = parse_publisher_from_metadata(None)

    assert result == {
        "name": "",
        "publisherIdentifier": "",
        "publisherIdentifierScheme": "",
        "schemeUri": "",
        "lang": "",
    }
    assert caplog.records == []


def test_parse_unexpected_truthy_publisher_logs_warning(caplog):
    """Unexpected truthy metadata is stringified and logged."""
    with caplog.at_level(logging.WARNING):
        result = parse_publisher_from_metadata(123)

    assert result["name"] == "123"
    assert any("Unexpected publisher type" in record.message for record in caplog.records)


def test_parse_publisher_to_tuple():
    """Tuple helper returns the CSV export field order."""
    result = parse_publisher_to_tuple({
        "name": "Publisher",
        "publisherIdentifier": "https://ror.org/example",
        "publisherIdentifierScheme": "ROR",
        "schemeUri": "https://ror.org",
        "lang": "de",
    })

    assert result == (
        "Publisher",
        "https://ror.org/example",
        "ROR",
        "https://ror.org",
        "de",
    )
