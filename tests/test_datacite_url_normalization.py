"""Tests for DataCite URL normalization."""

import json

import responses
from src.api.datacite_client import DataCiteClient


class TestURLNormalization:
    """Test suite for URL normalization functionality."""
    
    def test_normalize_url_with_colon_in_query(self):
        """Test normalization of URLs with colons in query parameters."""
        # URLs with colons in query parameters should be percent-encoded
        test_cases = [
            (
                "http://dataservices.gfz.de/panmetaworks/showshort.php?id=escidoc:43448",
                "http://dataservices.gfz.de/panmetaworks/showshort.php?id=escidoc%3A43448"
            ),
            (
                "http://example.com/path?id=test:123",
                "http://example.com/path?id=test%3A123"
            ),
            (
                "https://example.org/page?param=value:123",
                "https://example.org/page?param=value%3A123"
            ),
            (
                "http://example.com/path?param1=a:b&param2=c:d",
                "http://example.com/path?param1=a%3Ab&param2=c%3Ad"
            ),
        ]
        
        for input_url, expected_url in test_cases:
            normalized = DataCiteClient.normalize_url(input_url)
            assert normalized == expected_url, f"Failed for URL: {input_url}"
    
    def test_normalize_url_already_encoded(self):
        """Test that already encoded URLs are properly normalized (decoded then re-encoded)."""
        # URLs that are already encoded are decoded and re-encoded for normalization
        url = "http://example.com/path?id=test%3A123"
        normalized = DataCiteClient.normalize_url(url)
        # URL should be properly encoded after normalization (colon stays as %3A)
        assert "%3A" in normalized
        assert ":" not in normalized.split("?")[1]  # No unencoded colons in query
    
    def test_normalize_url_with_spaces(self):
        """Test normalization of URLs with spaces."""
        url = "http://example.com/path with spaces?param=value with spaces"
        normalized = DataCiteClient.normalize_url(url)
        # Spaces should be encoded as %20
        assert " " not in normalized
        assert "%20" in normalized
    
    def test_normalize_url_with_special_chars(self):
        """Test normalization of URLs with various special characters."""
        test_cases = [
            # Brackets should be encoded
            ("http://example.com/path?param=[test]", "http://example.com/path?param=%5Btest%5D"),
            # Hash/fragment should be preserved
            ("http://example.com/path#section", "http://example.com/path#section"),
        ]
        
        for input_url, expected_pattern in test_cases:
            normalized = DataCiteClient.normalize_url(input_url)
            assert normalized == expected_pattern, f"Expected {expected_pattern}, got {normalized}"
    
    def test_normalize_url_preserves_structure(self):
        """Test that URL normalization preserves the overall structure."""
        url = "https://example.org:8080/path/to/resource?query=value#fragment"
        normalized = DataCiteClient.normalize_url(url)
        
        # Should preserve scheme, netloc, path structure
        assert normalized.startswith("https://")
        assert "example.org:8080" in normalized
        assert "/path/to/resource" in normalized
    
    def test_normalize_url_simple_urls(self):
        """Test that simple URLs without special characters remain unchanged."""
        simple_urls = [
            "https://example.org",
            "https://example.org/path",
            "https://example.org/path/to/resource",
            "https://example.org/path?query=value",
            "https://example.org:8080/path",
        ]
        
        for url in simple_urls:
            normalized = DataCiteClient.normalize_url(url)
            # Simple URLs should remain mostly unchanged
            assert normalized.startswith(url.split("?")[0])
    
    def test_normalize_url_with_multiple_query_params(self):
        """Test normalization with multiple query parameters."""
        url = "http://example.com/path?id=test:123&name=value:456&other=normal"
        normalized = DataCiteClient.normalize_url(url)
        
        # Both colons should be encoded
        assert "test%3A123" in normalized
        assert "value%3A456" in normalized
        # Ampersands should be preserved (not encoded)
        assert "&" in normalized
        assert "%26" not in normalized
    
    def test_normalize_url_invalid_url(self):
        """Test that invalid URLs are handled gracefully."""
        # URLs without proper scheme/netloc return either normalized or original
        # The key is that they don't raise exceptions
        invalid_urls = [
            "not a url",
            "",
            "://missing-scheme",
            "http://",  # Missing host
        ]
        
        for url in invalid_urls:
            # Should not raise exception, returns either normalized or original
            result = DataCiteClient.normalize_url(url)
            assert isinstance(result, str)
            # For "not a url", normalize_url encodes spaces -> "not%20a%20url"
            # This is acceptable behavior (doesn't break anything)
    
    def test_normalize_url_with_port(self):
        """Test that port numbers (with colon) are preserved."""
        url = "http://example.com:8080/path?id=test:123"
        normalized = DataCiteClient.normalize_url(url)
        
        # Port colon should NOT be encoded
        assert ":8080" in normalized
        # But query parameter colon SHOULD be encoded
        assert "test%3A123" in normalized
    
    @responses.activate
    def test_normalize_url_integration_with_api_request(self):
        """Integration test: Verify normalized URL is sent in API request payload."""
        client = DataCiteClient("test.client", "password", use_test_api=True)
        doi = "10.5880/GFZ.TEST.URL"
        url_with_special_chars = "http://example.com/path?id=test:123&name=foo+bar"
        
        # Mock successful PUT response
        def request_callback(request):
            payload = json.loads(request.body)
            sent_url = payload["data"]["attributes"]["url"]
            
            # Verify colon is encoded
            assert "test%3A123" in sent_url
            # Verify plus sign handling: '+' is in safe set, so it's preserved during normalization
            # ('+' represents space in query strings per application/x-www-form-urlencoded)
            assert "foo+bar" in sent_url
            # Verify ampersand is not encoded
            assert "&" in sent_url
            assert "%26" not in sent_url
            
            return (200, {}, json.dumps({"data": {"id": doi}}))
        
        responses.add_callback(
            responses.PUT,
            f"https://api.test.datacite.org/dois/{doi}",
            callback=request_callback,
            content_type="application/json"
        )
        
        success, message = client.update_doi_url(doi, url_with_special_chars)
        
        assert success is True
        assert "erfolgreich aktualisiert" in message
        assert "erfolgreich aktualisiert" in message
