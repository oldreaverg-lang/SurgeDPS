"""
Cloudflare R2 Storage Client

Thin boto3 wrapper configured for Cloudflare R2.  Provides the same
interface regardless of whether R2 credentials are available — when they
are not, every method is a no-op and callers fall back to local filesystem.

Required environment variables (set in Railway dashboard):
    R2_ACCOUNT_ID        — Cloudflare account ID (found in R2 dashboard URL)
    R2_ACCESS_KEY_ID     — R2 API token "Access Key ID"
    R2_SECRET_ACCESS_KEY — R2 API token "Secret Access Key"
    R2_BUCKET_NAME       — Name of the R2 bucket (e.g. "surgedps-cells")

These are never required — the module degrades gracefully when absent.

Usage
-----
    from storage.r2_client import r2

    if r2.available:
        r2.upload_bytes("surgedps/cells/ian/2_3_damage.geojson", data)
        data = r2.download_bytes("surgedps/cells/ian/2_3_damage.geojson")
        exists = r2.exists("surgedps/cells/ian/2_3_damage.geojson")
"""

from __future__ import annotations

import logging
import os
from typing import Optional

logger = logging.getLogger(__name__)

# Lazy import — boto3 is only required when R2 is actually configured
_boto3 = None


def _get_boto3():
    global _boto3
    if _boto3 is None:
        try:
            import boto3
            _boto3 = boto3
        except ImportError:
            logger.warning("[R2] boto3 not installed — R2 storage unavailable")
    return _boto3


class R2Client:
    """
    Cloudflare R2 client with graceful fallback when unconfigured.

    All public methods return None / False silently when R2 is unavailable
    so callers can use a simple `if r2.available:` guard without try/except.
    """

    def __init__(self) -> None:
        self._client = None
        self._bucket: Optional[str] = None
        self._init_attempted = False

    def _init(self) -> bool:
        """Lazy init — only attempts connection once."""
        if self._init_attempted:
            return self._client is not None
        self._init_attempted = True

        account_id  = os.environ.get("R2_ACCOUNT_ID")
        access_key  = os.environ.get("R2_ACCESS_KEY_ID")
        secret_key  = os.environ.get("R2_SECRET_ACCESS_KEY")
        bucket      = os.environ.get("R2_BUCKET_NAME")

        if not all([account_id, access_key, secret_key, bucket]):
            logger.info("[R2] Credentials not configured — using local filesystem only")
            return False

        boto3 = _get_boto3()
        if boto3 is None:
            return False

        try:
            self._client = boto3.client(
                "s3",
                endpoint_url=f"https://{account_id}.r2.cloudflarestorage.com",
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                region_name="auto",
            )
            self._bucket = bucket
            logger.info("[R2] Connected to bucket: %s", bucket)
            return True
        except Exception as exc:
            logger.error("[R2] Failed to initialise client: %s", exc)
            return False

    @property
    def available(self) -> bool:
        """True if R2 is configured and the client initialised successfully."""
        return self._init()

    # ── Write ──────────────────────────────────────────────────────────────

    def upload_bytes(self, key: str, data: bytes, content_type: str = "application/octet-stream") -> bool:
        """
        Upload raw bytes to R2.

        Args:
            key: Object key (e.g. "surgedps/cells/ian/2_3_damage.geojson")
            data: Raw bytes to upload
            content_type: MIME type (use "application/geo+json" for GeoJSON,
                          "application/vnd.pmtiles" for PMTiles)

        Returns:
            True on success, False on failure
        """
        if not self._init():
            return False
        try:
            self._client.put_object(
                Bucket=self._bucket,
                Key=key,
                Body=data,
                ContentType=content_type,
            )
            logger.debug("[R2] Uploaded %d bytes → %s", len(data), key)
            return True
        except Exception as exc:
            logger.error("[R2] Upload failed for %s: %s", key, exc)
            return False

    def upload_file(self, key: str, local_path: str) -> bool:
        """Upload a local file to R2 by path."""
        if not self._init():
            return False
        try:
            self._client.upload_file(local_path, self._bucket, key)
            logger.debug("[R2] Uploaded file %s → %s", local_path, key)
            return True
        except Exception as exc:
            logger.error("[R2] File upload failed for %s: %s", key, exc)
            return False

    # ── Read ───────────────────────────────────────────────────────────────

    def download_bytes(self, key: str) -> Optional[bytes]:
        """
        Download an object from R2 as raw bytes.

        Returns None if the key does not exist or download fails.
        """
        if not self._init():
            return None
        try:
            resp = self._client.get_object(Bucket=self._bucket, Key=key)
            return resp["Body"].read()
        except self._client.exceptions.NoSuchKey:
            return None
        except Exception as exc:
            logger.error("[R2] Download failed for %s: %s", key, exc)
            return None

    def exists(self, key: str) -> bool:
        """Return True if the key exists in R2."""
        if not self._init():
            return False
        try:
            self._client.head_object(Bucket=self._bucket, Key=key)
            return True
        except Exception:
            return False

    def list_prefix(self, prefix: str) -> list[str]:
        """Return all object keys under a given prefix."""
        if not self._init():
            return []
        try:
            paginator = self._client.get_paginator("list_objects_v2")
            keys = []
            for page in paginator.paginate(Bucket=self._bucket, Prefix=prefix):
                for obj in page.get("Contents", []):
                    keys.append(obj["Key"])
            return keys
        except Exception as exc:
            logger.error("[R2] list_prefix failed for %s: %s", prefix, exc)
            return []


# ── Module-level singleton ─────────────────────────────────────────────────────
r2 = R2Client()
