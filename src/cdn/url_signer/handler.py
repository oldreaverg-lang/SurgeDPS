"""
CloudFront Signed URL Generator

Generates time-limited signed URLs for premium tile access.
Called by the frontend when a user purchases premium access.

Request:
    GET /?storm_id=AL142024&layer=compound&advisory=012

Response:
    {
        "signed_urls": {
            "compound": "https://cdn.surgedps.com/storms/.../tiles/premium/compound.pmtiles?Expires=...&Signature=...&Key-Pair-Id=..."
        },
        "expires_at": "2024-10-09T15:00:00Z"
    }

The private key for signing is fetched from SSM Parameter Store
and cached for the Lambda lifetime (warm starts reuse it).
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional
from urllib.parse import quote

logger = logging.getLogger(__name__)
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

# ── Configuration ─────────────────────────────────────────────────

CLOUDFRONT_DOMAIN = os.getenv("CLOUDFRONT_DOMAIN", "")
KEY_PAIR_ID = os.getenv("CLOUDFRONT_KEY_PAIR_ID", "")
PRIVATE_KEY_SSM_PARAM = os.getenv("PRIVATE_KEY_SSM_PARAM", "")
EXPIRY_SECONDS = int(os.getenv("SIGNED_URL_EXPIRY_SECONDS", "3600"))

# Layers that can be served as premium tiles
ALLOWED_LAYERS = {"surge", "rainfall", "compound", "overlap"}

# ── Cached private key (warm start reuse) ─────────────────────────

_cached_private_key: Optional[str] = None


def _get_private_key() -> str:
    """Fetch the CloudFront signing private key from SSM."""
    global _cached_private_key

    if _cached_private_key is not None:
        return _cached_private_key

    if not PRIVATE_KEY_SSM_PARAM:
        raise ValueError("PRIVATE_KEY_SSM_PARAM not configured")

    import boto3
    ssm = boto3.client("ssm")
    response = ssm.get_parameter(
        Name=PRIVATE_KEY_SSM_PARAM,
        WithDecryption=True,
    )
    _cached_private_key = response["Parameter"]["Value"]
    return _cached_private_key


def _sign_url(url: str, expires_at: datetime) -> str:
    """
    Generate a CloudFront signed URL using canned policy.

    Uses rsa_sign from the cryptography library (available in
    Lambda Python 3.12 runtime).
    """
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import padding
    import base64

    private_key_pem = _get_private_key()

    # Load private key
    private_key = serialization.load_pem_private_key(
        private_key_pem.encode("utf-8"),
        password=None,
    )

    # Unix timestamp for expiry
    expires_epoch = int(expires_at.timestamp())

    # CloudFront canned policy
    policy = json.dumps({
        "Statement": [{
            "Resource": url,
            "Condition": {
                "DateLessThan": {
                    "AWS:EpochTime": expires_epoch
                }
            }
        }]
    }, separators=(",", ":"))

    # Sign the policy
    signature = private_key.sign(
        policy.encode("utf-8"),
        padding.PKCS1v15(),
        hashes.SHA1(),  # CloudFront requires SHA1 for canned policies
    )

    # Base64 encode and make URL-safe
    encoded_sig = (
        base64.b64encode(signature)
        .decode("utf-8")
        .replace("+", "-")
        .replace("=", "_")
        .replace("/", "~")
    )

    # Append CloudFront query params
    signed = (
        f"{url}"
        f"{'&' if '?' in url else '?'}"
        f"Expires={expires_epoch}"
        f"&Signature={encoded_sig}"
        f"&Key-Pair-Id={KEY_PAIR_ID}"
    )

    return signed


def _build_tile_url(storm_id: str, advisory_num: str, layer: str) -> str:
    """Build the base URL for a premium tile layer."""
    return (
        f"https://{CLOUDFRONT_DOMAIN}"
        f"/storms/{storm_id}/advisory_{advisory_num}"
        f"/tiles/premium/{layer}*"
    )


def _json_response(status: int, body: dict) -> dict:
    """Build a Lambda Function URL response."""
    return {
        "statusCode": status,
        "headers": {
            "Content-Type": "application/json",
            "Cache-Control": "no-store",
        },
        "body": json.dumps(body),
    }


# ── Lambda Handler ────────────────────────────────────────────────

def lambda_handler(event: dict, context: Any) -> dict:
    """
    Generate signed URLs for premium tile access.

    Query parameters:
        storm_id    — ATCF storm identifier (e.g., AL142024)
        advisory    — Advisory number (e.g., 012)
        layer       — Comma-separated layer names (e.g., "surge,compound")
                      Defaults to all layers.

    Returns signed URLs with wildcard paths so the frontend can
    access all tiles under the premium directory for each layer.
    """
    # Parse query parameters
    params = event.get("queryStringParameters") or {}
    storm_id = params.get("storm_id", "")
    advisory_num = params.get("advisory", "")
    requested_layers = params.get("layer", "")

    # Validate
    if not storm_id or not advisory_num:
        return _json_response(400, {
            "error": "Missing required parameters: storm_id, advisory"
        })

    # Sanitize storm_id (alphanumeric only)
    if not storm_id.replace("-", "").replace("_", "").isalnum():
        return _json_response(400, {"error": "Invalid storm_id"})

    # Parse requested layers
    if requested_layers:
        layers = set(requested_layers.split(",")) & ALLOWED_LAYERS
    else:
        layers = ALLOWED_LAYERS

    if not layers:
        return _json_response(400, {
            "error": f"Invalid layer. Allowed: {', '.join(sorted(ALLOWED_LAYERS))}"
        })

    # Check configuration
    if not CLOUDFRONT_DOMAIN or not KEY_PAIR_ID:
        return _json_response(503, {
            "error": "Signed URL generation not configured"
        })

    # Generate signed URLs
    expires_at = datetime.now(timezone.utc) + timedelta(seconds=EXPIRY_SECONDS)

    try:
        signed_urls = {}
        for layer in sorted(layers):
            base_url = _build_tile_url(storm_id, advisory_num, layer)
            signed_urls[layer] = _sign_url(base_url, expires_at)

        return _json_response(200, {
            "signed_urls": signed_urls,
            "expires_at": expires_at.isoformat(),
            "expires_in_seconds": EXPIRY_SECONDS,
            "storm_id": storm_id,
            "advisory": advisory_num,
        })

    except Exception as e:
        logger.error(f"Failed to generate signed URLs: {e}")
        return _json_response(500, {
            "error": "Failed to generate signed URLs"
        })
