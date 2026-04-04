"""
Output Publisher

Uploads tile products to S3, generates the storm manifest JSON,
and invalidates the CloudFront CDN cache so users see fresh data.

The manifest is the single file the frontend fetches to discover
what data is available for the current storm.
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class ManifestLayer:
    """Metadata for a single flood layer in the manifest."""

    name: str                 # "surge", "rainfall", "compound", "overlap"
    display_name: str         # "Storm Surge"
    color_ramp: str           # "cyan", "magenta", "violet"
    timesteps: List[int]      # Available forecast hours
    free_tiles: Dict[int, str] = field(default_factory=dict)    # hour -> URL
    premium_tiles: Dict[int, str] = field(default_factory=dict)
    cog_urls: Dict[int, str] = field(default_factory=dict)
    max_depth_m: float = 0.0
    max_depth_ft: float = 0.0


@dataclass
class OverlayLayer:
    """Metadata for a vector overlay layer in the manifest."""

    name: str                 # "storm_cone", "storm_track", "reaches"
    display_name: str         # "Forecast Cone"
    layer_type: str           # "fill", "line", "circle"
    tiles_url: str = ""       # PMTiles URL
    feature_count: int = 0
    style: Dict = field(default_factory=dict)


@dataclass
class StormManifest:
    """
    Storm manifest JSON — the frontend's single source of truth.

    The web app fetches this file on load to discover:
      - Storm metadata (name, position, strength)
      - Available layers and their tile URLs
      - Available timesteps
      - Bounding box for initial map view
      - Vector overlay layers (cone, track, reaches)
    """

    storm_id: str
    storm_name: str
    storm_type: str
    advisory_number: str
    center: List[float]       # [lon, lat]
    wind_mph: Optional[int] = None
    pressure_mb: Optional[int] = None
    movement: Optional[str] = None
    bounds: Optional[List[float]] = None  # [west, south, east, north]
    layers: List[ManifestLayer] = field(default_factory=list)
    overlays: List[OverlayLayer] = field(default_factory=list)
    damage_summary: Optional[Dict] = None
    generated_at: str = ""
    tile_base_url: str = ""
    next_update: Optional[str] = None

    def to_dict(self) -> dict:
        """Serialize to a JSON-compatible dict."""
        return {
            "storm_id": self.storm_id,
            "storm_name": self.storm_name,
            "storm_type": self.storm_type,
            "advisory_number": self.advisory_number,
            "center": self.center,
            "wind_mph": self.wind_mph,
            "pressure_mb": self.pressure_mb,
            "movement": self.movement,
            "bounds": self.bounds,
            "generated_at": self.generated_at,
            "tile_base_url": self.tile_base_url,
            "next_update": self.next_update,
            "layers": [
                {
                    "name": layer.name,
                    "display_name": layer.display_name,
                    "color_ramp": layer.color_ramp,
                    "timesteps": layer.timesteps,
                    "max_depth_m": layer.max_depth_m,
                    "max_depth_ft": layer.max_depth_ft,
                    "free_tiles": layer.free_tiles,
                    "premium_tiles": layer.premium_tiles,
                    "cog_urls": layer.cog_urls,
                }
                for layer in self.layers
            ],
            "overlays": [
                {
                    "name": overlay.name,
                    "display_name": overlay.display_name,
                    "layer_type": overlay.layer_type,
                    "tiles_url": overlay.tiles_url,
                    "feature_count": overlay.feature_count,
                    "style": overlay.style,
                }
                for overlay in self.overlays
            ],
            "damage_summary": self.damage_summary,
        }


class OutputPublisher:
    """
    Publishes tile products to S3 and manages the storm manifest.
    """

    def __init__(
        self,
        bucket: str,
        cloudfront_distribution_id: str = "",
        tile_base_url: str = "",
        s3_client=None,
        cloudfront_client=None,
        dry_run: bool = False,
        local_output_dir: str = "",
    ):
        self.bucket = bucket
        self.cf_distribution_id = cloudfront_distribution_id
        self.tile_base_url = tile_base_url
        self.s3 = s3_client
        self.cf = cloudfront_client
        self.dry_run = dry_run
        self.local_output_dir = local_output_dir or os.path.join(
            tempfile.gettempdir(), "surgedps"
        )

    def publish_tiles(
        self,
        local_tile_dir: str,
        storm_id: str,
        advisory_num: str,
    ) -> Dict[str, str]:
        """
        Upload all tiles from the local output directory to S3.

        Returns a dict mapping local paths to S3 URLs.
        """
        s3_prefix = f"storms/{storm_id}/advisory_{advisory_num}"
        uploaded = {}

        if self.dry_run or not self.s3:
            logger.info(f"[DRY RUN] Would upload {local_tile_dir} -> s3://{self.bucket}/{s3_prefix}/")
            # Walk the directory and log what would be uploaded
            for root, _, files in os.walk(local_tile_dir):
                for fname in files:
                    local_path = os.path.join(root, fname)
                    rel_path = os.path.relpath(local_path, local_tile_dir)
                    s3_key = f"{s3_prefix}/{rel_path}"
                    s3_url = f"{self.tile_base_url}/{s3_key}"
                    uploaded[local_path] = s3_url
                    logger.debug(f"  [DRY RUN] {rel_path} -> {s3_key}")
            return uploaded

        for root, _, files in os.walk(local_tile_dir):
            for fname in files:
                local_path = os.path.join(root, fname)
                rel_path = os.path.relpath(local_path, local_tile_dir)
                s3_key = f"{s3_prefix}/{rel_path}"

                # Set content type
                content_type = self._content_type(fname)
                extra_args = {"ContentType": content_type}

                # Free tiles are public; premium tiles are private
                if "/free/" in rel_path:
                    extra_args["ACL"] = "public-read"
                    extra_args["CacheControl"] = "public, max-age=3600"

                self.s3.upload_file(
                    local_path,
                    self.bucket,
                    s3_key,
                    ExtraArgs=extra_args,
                )

                s3_url = f"{self.tile_base_url}/{s3_key}"
                uploaded[local_path] = s3_url

        logger.info(f"Uploaded {len(uploaded)} files to s3://{self.bucket}/{s3_prefix}/")
        return uploaded

    def publish_manifest(
        self,
        manifest: StormManifest,
        storm_id: str,
        advisory_num: str,
    ) -> str:
        """
        Write the storm manifest JSON to S3.

        Also writes a 'latest' symlink manifest for convenience.
        """
        manifest.generated_at = datetime.utcnow().isoformat() + "Z"
        manifest.tile_base_url = self.tile_base_url

        manifest_json = json.dumps(manifest.to_dict(), indent=2)

        # Advisory-specific manifest
        adv_key = f"storms/{storm_id}/advisory_{advisory_num}/manifest.json"
        # Latest manifest (always points to most recent advisory)
        latest_key = f"storms/{storm_id}/manifest.json"

        if self.dry_run or not self.s3:
            logger.info(f"[DRY RUN] Manifest:\n{manifest_json[:500]}...")
            # Write locally for dev
            local_path = os.path.join(
                self.local_output_dir, storm_id, "manifest.json"
            )
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            with open(local_path, "w") as f:
                f.write(manifest_json)
            return local_path

        for key in [adv_key, latest_key]:
            self.s3.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=manifest_json,
                ContentType="application/json",
                CacheControl="public, max-age=300",  # 5 min cache
            )

        logger.info(f"Manifest published: s3://{self.bucket}/{latest_key}")
        return f"{self.tile_base_url}/{latest_key}"

    def invalidate_cdn(self, storm_id: str) -> Optional[str]:
        """
        Invalidate CloudFront cache for this storm's tiles and manifest.

        This ensures users see fresh data after a new advisory is processed.
        """
        if not self.cf or not self.cf_distribution_id or self.dry_run:
            logger.info("[DRY RUN] Would invalidate CDN cache")
            return None

        paths = [
            f"/storms/{storm_id}/*",
            f"/storms/{storm_id}/manifest.json",
        ]

        try:
            response = self.cf.create_invalidation(
                DistributionId=self.cf_distribution_id,
                InvalidationBatch={
                    "Paths": {
                        "Quantity": len(paths),
                        "Items": paths,
                    },
                    "CallerReference": (
                        f"{storm_id}-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
                    ),
                },
            )
            inv_id = response["Invalidation"]["Id"]
            logger.info(f"CDN invalidation created: {inv_id}")
            return inv_id

        except Exception as e:
            logger.error(f"CDN invalidation failed: {e}")
            return None

    @staticmethod
    def _content_type(filename: str) -> str:
        """Determine Content-Type from file extension."""
        ext = filename.rsplit(".", 1)[-1].lower()
        types = {
            "tif": "image/tiff",
            "tiff": "image/tiff",
            "pmtiles": "application/octet-stream",
            "geojson": "application/geo+json",
            "json": "application/json",
            "pbf": "application/x-protobuf",
        }
        return types.get(ext, "application/octet-stream")
