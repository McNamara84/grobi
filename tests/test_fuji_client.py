"""Unit tests for F-UJI API Client."""

import pytest
import responses
from requests.exceptions import ConnectionError, Timeout

from src.api.fuji_client import (
    FujiClient,
    FujiResult,
    FujiAPIError,
    FujiAuthenticationError,
    FujiConnectionError
)


# Sample F-UJI API responses
FUJI_SUCCESS_RESPONSE = {
    "id": "https://doi.org/10.5880/GFZ.1.1.2021.001",
    "request": {
        "object_identifier": "https://doi.org/10.5880/GFZ.1.1.2021.001",
        "test_debug": False,
        "use_datacite": True,
        "metric_version": "metrics_v0.5"
    },
    "summary": {
        "score_earned": {"FAIR": 13, "F": 7, "A": 2, "I": 2, "R": 2},
        "score_total": {"FAIR": 24, "F": 10, "A": 4, "I": 5, "R": 5},
        "score_percent": {"FAIR": 54.17, "F": 70.0, "A": 50.0, "I": 40.0, "R": 40.0}
    },
    "total_metrics": 17,
    "results": [
        {"metric_id": "FsF-F1-01D", "score": 1, "max_score": 1},
        {"metric_id": "FsF-F2-01M", "score": 2, "max_score": 2}
    ]
}

FUJI_METRICS_RESPONSE = {
    "metric_version": "0.5",
    "metrics": [
        {"metric_id": "FsF-F1-01D", "metric_name": "Unique Identifier"}
    ]
}


@pytest.fixture
def client():
    """Create a test F-UJI client."""
    return FujiClient(
        endpoint="https://fuji.test.example.com/fuji/api/v1",
        username="testuser",
        password="testpass"
    )


@pytest.fixture
def default_client():
    """Create a F-UJI client with default settings."""
    return FujiClient()


class TestFujiClientInit:
    """Test FujiClient initialization."""
    
    def test_custom_endpoint(self, client):
        """Test that custom endpoint is set correctly."""
        assert client.endpoint == "https://fuji.test.example.com/fuji/api/v1"
        assert client.username == "testuser"
        assert client.password == "testpass"
    
    def test_default_endpoint(self, default_client):
        """Test that default endpoint is set correctly."""
        assert default_client.endpoint == FujiClient.DEFAULT_ENDPOINT
        assert default_client.username == FujiClient.DEFAULT_USERNAME
        assert default_client.password == FujiClient.DEFAULT_PASSWORD
    
    def test_endpoint_trailing_slash_removed(self):
        """Test that trailing slash is removed from endpoint."""
        client = FujiClient(endpoint="https://example.com/api/")
        assert client.endpoint == "https://example.com/api"


class TestFujiClientConnection:
    """Test F-UJI connection testing."""
    
    @responses.activate
    def test_connection_success(self, client):
        """Test successful connection."""
        responses.add(
            responses.GET,
            "https://fuji.test.example.com/fuji/api/v1/metrics/0.5",
            json=FUJI_METRICS_RESPONSE,
            status=200
        )
        
        assert client.test_connection() is True
    
    @responses.activate
    def test_connection_failure_404(self, client):
        """Test connection failure with 404."""
        responses.add(
            responses.GET,
            "https://fuji.test.example.com/fuji/api/v1/metrics/0.5",
            status=404
        )
        
        assert client.test_connection() is False
    
    @responses.activate
    def test_connection_failure_network_error(self, client):
        """Test connection failure with network error."""
        responses.add(
            responses.GET,
            "https://fuji.test.example.com/fuji/api/v1/metrics/0.5",
            body=ConnectionError("Network unreachable")
        )
        
        assert client.test_connection() is False


class TestFujiClientAssessment:
    """Test DOI assessment."""
    
    @responses.activate
    def test_assess_doi_success(self, client):
        """Test successful DOI assessment."""
        responses.add(
            responses.POST,
            "https://fuji.test.example.com/fuji/api/v1/evaluate",
            json=FUJI_SUCCESS_RESPONSE,
            status=200
        )
        
        result = client.assess_doi("10.5880/GFZ.1.1.2021.001")
        
        assert result.is_success
        assert result.doi == "10.5880/GFZ.1.1.2021.001"
        assert result.score_percent == 54.17
        assert result.score_earned == 13
        assert result.score_total == 24
        assert result.metrics_count == 17
        assert result.error is None
    
    @responses.activate
    def test_assess_doi_with_url_prefix(self, client):
        """Test assessment with DOI that has URL prefix."""
        responses.add(
            responses.POST,
            "https://fuji.test.example.com/fuji/api/v1/evaluate",
            json=FUJI_SUCCESS_RESPONSE,
            status=200
        )
        
        result = client.assess_doi("https://doi.org/10.5880/GFZ.1.1.2021.001")
        
        assert result.is_success
        assert result.doi == "10.5880/GFZ.1.1.2021.001"
    
    @responses.activate
    def test_assess_doi_authentication_failure(self, client):
        """Test authentication failure."""
        responses.add(
            responses.POST,
            "https://fuji.test.example.com/fuji/api/v1/evaluate",
            status=401
        )
        
        with pytest.raises(FujiAuthenticationError) as excinfo:
            client.assess_doi("10.5880/GFZ.1.1.2021.001")
        
        assert "Authentifizierung" in str(excinfo.value)
    
    @responses.activate
    def test_assess_doi_not_found(self, client):
        """Test DOI not found."""
        responses.add(
            responses.POST,
            "https://fuji.test.example.com/fuji/api/v1/evaluate",
            status=404
        )
        
        result = client.assess_doi("10.5880/nonexistent")
        
        assert not result.is_success
        assert result.score_percent == -1
        assert "nicht gefunden" in result.error
    
    @responses.activate
    def test_assess_doi_server_error(self, client):
        """Test server error handling."""
        responses.add(
            responses.POST,
            "https://fuji.test.example.com/fuji/api/v1/evaluate",
            json={"detail": "Internal server error"},
            status=500
        )
        
        result = client.assess_doi("10.5880/GFZ.1.1.2021.001")
        
        assert not result.is_success
        assert result.score_percent == -1
        assert "500" in result.error
    
    @responses.activate
    def test_assess_doi_timeout(self, client):
        """Test timeout handling."""
        responses.add(
            responses.POST,
            "https://fuji.test.example.com/fuji/api/v1/evaluate",
            body=Timeout("Read timed out")
        )
        
        result = client.assess_doi("10.5880/GFZ.1.1.2021.001")
        
        assert not result.is_success
        assert result.score_percent == -1
        assert "Timeout" in result.error
    
    @responses.activate
    def test_assess_doi_connection_error(self, client):
        """Test connection error raises exception."""
        responses.add(
            responses.POST,
            "https://fuji.test.example.com/fuji/api/v1/evaluate",
            body=ConnectionError("Connection refused")
        )
        
        with pytest.raises(FujiConnectionError) as excinfo:
            client.assess_doi("10.5880/GFZ.1.1.2021.001")
        
        assert "Verbindung" in str(excinfo.value)


class TestFujiResponseParsing:
    """Test response parsing edge cases."""
    
    @responses.activate
    def test_parse_response_with_int_scores(self, client):
        """Test parsing when scores are integers instead of dicts."""
        response_with_int_scores = {
            "summary": {
                "score_earned": 13,
                "score_total": 24,
                "score_percent": 54.17
            },
            "total_metrics": 17
        }
        
        responses.add(
            responses.POST,
            "https://fuji.test.example.com/fuji/api/v1/evaluate",
            json=response_with_int_scores,
            status=200
        )
        
        result = client.assess_doi("10.5880/GFZ.1.1.2021.001")
        
        assert result.is_success
        assert result.score_percent == 54.17
        assert result.score_earned == 13
        assert result.score_total == 24
    
    @responses.activate
    def test_parse_response_with_missing_summary(self, client):
        """Test parsing when summary is missing."""
        response_no_summary = {
            "results": []
        }
        
        responses.add(
            responses.POST,
            "https://fuji.test.example.com/fuji/api/v1/evaluate",
            json=response_no_summary,
            status=200
        )
        
        result = client.assess_doi("10.5880/GFZ.1.1.2021.001")
        
        # Should handle gracefully
        assert result.score_percent == 0 or result.score_percent == -1
    
    @responses.activate
    def test_parse_response_calculates_percent_if_missing(self, client):
        """Test that percentage is calculated if not provided."""
        response_no_percent = {
            "summary": {
                "score_earned": {"FAIR": 12},
                "score_total": {"FAIR": 24},
                "score_percent": {}  # Empty dict
            },
            "total_metrics": 17
        }
        
        responses.add(
            responses.POST,
            "https://fuji.test.example.com/fuji/api/v1/evaluate",
            json=response_no_percent,
            status=200
        )
        
        result = client.assess_doi("10.5880/GFZ.1.1.2021.001")
        
        assert result.is_success
        assert result.score_percent == 50.0  # 12/24 * 100


class TestFujiResult:
    """Test FujiResult dataclass."""
    
    def test_is_success_true(self):
        """Test is_success returns True for valid result."""
        result = FujiResult(
            doi="10.5880/test",
            score_percent=50.0,
            score_earned=12,
            score_total=24,
            metrics_count=17
        )
        assert result.is_success is True
    
    def test_is_success_false_with_error(self):
        """Test is_success returns False when error is set."""
        result = FujiResult(
            doi="10.5880/test",
            score_percent=50.0,
            score_earned=12,
            score_total=24,
            metrics_count=17,
            error="Some error"
        )
        assert result.is_success is False
    
    def test_is_success_false_with_negative_score(self):
        """Test is_success returns False for negative score."""
        result = FujiResult(
            doi="10.5880/test",
            score_percent=-1,
            score_earned=0,
            score_total=0,
            metrics_count=0
        )
        assert result.is_success is False
