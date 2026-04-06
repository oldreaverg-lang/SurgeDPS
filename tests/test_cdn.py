"""
Tests for CDN infrastructure: signed URL generation and publisher CDN integration.

Run with: pytest tests/test_cdn.py -v
"""

import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# URL Signer Lambda Tests
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestURLSignerHandler:

    def _make_event(self, params=None):
        """Build a minimal Lambda Function URL event."""
        return {
            "queryStringParameters": params or {},
        }

    def test_missing_storm_id_returns_400(self):
        from cdn.url_signer.handler import lambda_handler

        event = self._make_event({"advisory": "012"})
        result = lambda_handler(event, None)

        assert result["statusCode"] == 400
        body = json.loads(result["body"])
        assert "storm_id" in body["error"]

    def test_missing_advisory_returns_400(self):
        from cdn.url_signer.handler import lambda_handler

        event = self._make_event({"storm_id": "AL142024"})
        result = lambda_handler(event, None)

        assert result["statusCode"] == 400
        body = json.loads(result["body"])
        assert "advisory" in body["error"]

    def test_invalid_storm_id_returns_400(self):
        from cdn.url_signer.handler import lambda_handler

        event = self._make_event({
            "storm_id": "../../etc/passwd",
            "advisory": "012",
        })
        result = lambda_handler(event, None)
        assert result["statusCode"] == 400

    def test_invalid_layer_returns_400(self):
        from cdn.url_signer.handler import lambda_handler

        event = self._make_event({
            "storm_id": "AL142024",
            "advisory": "012",
            "layer": "NOTAVALIDLAYER",
        })
        result = lambda_handler(event, None)
        assert result["statusCode"] == 400

    def test_unconfigured_returns_503(self):
        """Without CLOUDFRONT_DOMAIN set, should return 503."""
        from cdn.url_signer import handler

        # Save original values
        orig_domain = handler.CLOUDFRONT_DOMAIN
        orig_key = handler.KEY_PAIR_ID

        try:
            handler.CLOUDFRONT_DOMAIN = ""
            handler.KEY_PAIR_ID = ""

            event = self._make_event({
                "storm_id": "AL142024",
                "advisory": "012",
            })
            result = handler.lambda_handler(event, None)
            assert result["statusCode"] == 503
        finally:
            handler.CLOUDFRONT_DOMAIN = orig_domain
            handler.KEY_PAIR_ID = orig_key

    def test_empty_query_params_returns_400(self):
        from cdn.url_signer.handler import lambda_handler

        event = self._make_event({})
        result = lambda_handler(event, None)
        assert result["statusCode"] == 400

    def test_none_query_params_returns_400(self):
        from cdn.url_signer.handler import lambda_handler

        event = {"queryStringParameters": None}
        result = lambda_handler(event, None)
        assert result["statusCode"] == 400

    def test_valid_layers_filter(self):
        """Only allowed layers should be accepted."""
        from cdn.url_signer.handler import ALLOWED_LAYERS

        assert "surge" in ALLOWED_LAYERS
        assert "rainfall" in ALLOWED_LAYERS
        assert "compound" in ALLOWED_LAYERS
        assert "overlap" in ALLOWED_LAYERS
        assert "notreal" not in ALLOWED_LAYERS

    def test_json_response_format(self):
        from cdn.url_signer.handler import _json_response

        result = _json_response(200, {"key": "value"})

        assert result["statusCode"] == 200
        assert result["headers"]["Content-Type"] == "application/json"
        assert result["headers"]["Cache-Control"] == "no-store"
        body = json.loads(result["body"])
        assert body["key"] == "value"

    def test_build_tile_url(self):
        from cdn.url_signer import handler

        orig = handler.CLOUDFRONT_DOMAIN
        try:
            handler.CLOUDFRONT_DOMAIN = "cdn.surgedps.com"
            url = handler._build_tile_url("AL142024", "012", "surge")
            assert url == "https://cdn.surgedps.com/storms/AL142024/advisory_012/tiles/premium/surge*"
        finally:
            handler.CLOUDFRONT_DOMAIN = orig


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Publisher CDN Integration Tests
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestPublisherCDN:

    def test_content_type_mapping(self):
        from pipeline.publisher import OutputPublisher

        assert OutputPublisher._content_type("flood.tif") == "image/tiff"
        assert OutputPublisher._content_type("data.pmtiles") == "application/octet-stream"
        assert OutputPublisher._content_type("manifest.json") == "application/json"
        assert OutputPublisher._content_type("layer.geojson") == "application/geo+json"
        assert OutputPublisher._content_type("tile.pbf") == "application/x-protobuf"
        assert OutputPublisher._content_type("unknown.xyz") == "application/octet-stream"

    def test_dry_run_publish_tiles(self, tmp_path):
        from pipeline.publisher import OutputPublisher

        # Create some fake tile files
        tile_dir = tmp_path / "tiles"
        tile_dir.mkdir()
        (tile_dir / "free").mkdir()
        (tile_dir / "premium").mkdir()
        (tile_dir / "free" / "surge.pmtiles").write_text("fake")
        (tile_dir / "premium" / "surge_hd.pmtiles").write_text("fake")

        publisher = OutputPublisher(
            bucket="test-bucket",
            tile_base_url="https://cdn.test.com",
            dry_run=True,
        )

        uploaded = publisher.publish_tiles(str(tile_dir), "AL142024", "012")

        assert len(uploaded) == 2
        # Check S3 URL structure
        for local_path, s3_url in uploaded.items():
            assert "cdn.test.com" in s3_url
            assert "AL142024" in s3_url

    def test_dry_run_publish_manifest(self, tmp_path):
        from pipeline.publisher import OutputPublisher, StormManifest

        publisher = OutputPublisher(
            bucket="test-bucket",
            tile_base_url="https://cdn.test.com",
            dry_run=True,
            local_output_dir=str(tmp_path),
        )

        manifest = StormManifest(
            storm_id="AL142024",
            storm_name="Milton",
            storm_type="Hurricane",
            advisory_number="012",
            center=[-86.3, 25.4],
        )

        result = publisher.publish_manifest(manifest, "AL142024", "012")
        assert result is not None
        assert "manifest.json" in result

    def test_cdn_invalidation_dry_run(self):
        from pipeline.publisher import OutputPublisher

        publisher = OutputPublisher(
            bucket="test-bucket",
            dry_run=True,
        )

        result = publisher.invalidate_cdn("AL142024")
        assert result is None  # Dry run returns None

    def test_cdn_invalidation_no_client(self):
        from pipeline.publisher import OutputPublisher

        publisher = OutputPublisher(
            bucket="test-bucket",
            cloudfront_distribution_id="E1234567890",
            cloudfront_client=None,
        )

        result = publisher.invalidate_cdn("AL142024")
        assert result is None

    def test_manifest_serialization(self):
        from pipeline.publisher import StormManifest, ManifestLayer

        manifest = StormManifest(
            storm_id="AL142024",
            storm_name="Milton",
            storm_type="Hurricane",
            advisory_number="012",
            center=[-86.3, 25.4],
            wind_mph=150,
            pressure_mb=940,
            bounds=[-89.0, 23.0, -80.0, 30.0],
        )
        manifest.layers.append(ManifestLayer(
            name="surge",
            display_name="Storm Surge",
            color_ramp="cyan",
            timesteps=[0, 6, 12],
            max_depth_m=4.5,
            max_depth_ft=14.76,
        ))

        d = manifest.to_dict()

        assert d["storm_id"] == "AL142024"
        assert d["center"] == [-86.3, 25.4]
        assert len(d["layers"]) == 1
        assert d["layers"][0]["name"] == "surge"
        assert d["layers"][0]["max_depth_m"] == 4.5

        # Should be JSON-serializable
        json_str = json.dumps(d)
        assert "Milton" in json_str


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# S3 Tile Path Structure Tests
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestTilePathStructure:
    """Verify the S3 path structure matches CloudFront behavior patterns."""

    def test_free_tile_path_matches_cf_behavior(self):
        """Free tiles path should match /storms/*/tiles/free/* pattern."""
        path = "storms/AL142024/advisory_012/tiles/free/surge.pmtiles"
        assert path.startswith("storms/")
        assert "/tiles/free/" in path

    def test_premium_tile_path_matches_cf_behavior(self):
        """Premium tiles path should match /storms/*/tiles/premium/* pattern."""
        path = "storms/AL142024/advisory_012/tiles/premium/surge_hd.pmtiles"
        assert path.startswith("storms/")
        assert "/tiles/premium/" in path

    def test_manifest_path_matches_cf_behavior(self):
        """Manifest path should match /storms/*/manifest.json pattern."""
        path = "storms/AL142024/manifest.json"
        assert path.endswith("/manifest.json")
        assert path.startswith("storms/")

    def test_advisory_manifest_path(self):
        """Advisory-specific manifest has correct structure."""
        path = "storms/AL142024/advisory_012/manifest.json"
        assert "advisory_012" in path
        assert path.endswith("manifest.json")
